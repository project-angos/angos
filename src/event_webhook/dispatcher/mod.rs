use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use hmac::{Hmac, KeyInit, Mac};
use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use regex::Regex;
use reqwest::Client;
use sha2::Sha256;
use tokio::{sync::Mutex, task::JoinSet};
use tracing::warn;

#[cfg(test)]
mod tests;

use crate::{
    configuration::Error,
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        event::{Event, EventKind},
    },
};

static DELIVERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "event_webhook_deliveries_total",
        "Total event webhook deliveries",
        &["webhook", "event", "result"]
    )
    .unwrap()
});

static DELIVERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "event_webhook_delivery_duration_seconds",
        "Event webhook delivery duration",
        &["webhook", "event"]
    )
    .unwrap()
});

pub fn compute_signature(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
pub fn matches_event(
    config: &EventWebhookConfig,
    event_kind: &EventKind,
    repository: &str,
) -> bool {
    if !config.events.contains(event_kind) {
        return false;
    }

    match &config.repository_filter {
        None => true,
        Some(filters) => filters
            .iter()
            .any(|pattern| Regex::new(pattern).is_ok_and(|re| re.is_match(repository))),
    }
}

struct WebhookEndpoint {
    client: Client,
    config: EventWebhookConfig,
    compiled_filters: Vec<Regex>,
}

impl WebhookEndpoint {
    fn matches_event(&self, event_kind: &EventKind, repository: &str) -> bool {
        if !self.config.events.contains(event_kind) {
            return false;
        }
        if self.compiled_filters.is_empty() {
            return true;
        }
        self.compiled_filters
            .iter()
            .any(|re| re.is_match(repository))
    }
}

pub struct EventDispatcher {
    endpoints: HashMap<String, WebhookEndpoint>,
    shutdown: Arc<AtomicBool>,
    in_flight: Arc<Mutex<JoinSet<()>>>,
}

async fn send_request(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
) -> Result<(), String> {
    let mut request = client
        .post(url)
        .header("content-type", "application/json")
        .header("X-Registry-Event", event_kind_header);

    if let Some(token) = token {
        let signature = compute_signature(token, body);
        request = request
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Registry-Signature-256", format!("sha256={signature}"));
    }

    let response = request
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("returned status {}", response.status()));
    }

    Ok(())
}

async fn send_with_retries(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
    max_retries: u32,
) -> Result<(), String> {
    let mut last_err = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
            tokio::time::sleep(backoff).await;
        }

        match send_request(client, url, token, body, event_kind_header).await {
            Ok(()) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| "unknown error".to_string()))
}

async fn deliver_async(
    client: Client,
    url: String,
    token: Option<String>,
    body: Vec<u8>,
    event_kind_header: String,
    max_retries: u32,
    name: String,
) {
    if let Err(e) = send_and_record(
        &client,
        &url,
        token.as_deref(),
        &body,
        &event_kind_header,
        max_retries,
        &name,
    )
    .await
    {
        warn!("Async webhook '{name}' failed: {e}");
    }
}

async fn send_and_record(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
    max_retries: u32,
    webhook_name: &str,
) -> Result<(), String> {
    let result = send_with_retries(client, url, token, body, event_kind_header, max_retries).await;
    let result_label = if result.is_ok() { "success" } else { "error" };
    DELIVERY_TOTAL
        .with_label_values(&[webhook_name, event_kind_header, result_label])
        .inc();
    result
}

impl EventDispatcher {
    pub fn new(webhooks: HashMap<String, EventWebhookConfig>) -> Result<Self, Error> {
        let mut endpoints = HashMap::new();

        for (name, config) in webhooks {
            let client = Client::builder()
                .timeout(Duration::from_millis(config.timeout_ms))
                .build()
                .map_err(|e| {
                    Error::Initialization(format!(
                        "Failed to create HTTP client for webhook '{name}': {e}"
                    ))
                })?;

            let compiled_filters = config
                .repository_filter
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter_map(|pattern| {
                    Regex::new(pattern)
                        .map_err(|e| {
                            warn!("Invalid repository_filter regex '{pattern}' for webhook '{name}': {e}");
                        })
                        .ok()
                })
                .collect();

            endpoints.insert(
                name,
                WebhookEndpoint {
                    client,
                    config,
                    compiled_filters,
                },
            );
        }

        Ok(Self {
            endpoints,
            shutdown: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(Mutex::new(JoinSet::new())),
        })
    }

    #[cfg(test)]
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.drain_in_flight().await;
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        self.shutdown.store(true, Ordering::SeqCst);
        if tokio::time::timeout(timeout, self.drain_in_flight())
            .await
            .is_err()
        {
            warn!("Shutdown timed out; some in-flight async deliveries may not have completed");
        }
    }

    async fn drain_in_flight(&self) {
        let mut in_flight = self.in_flight.lock().await;
        while in_flight.join_next().await.is_some() {}
    }

    pub async fn dispatch(&self, event: &Event) -> Result<(), Error> {
        let body = serde_json::to_vec(event)
            .map_err(|e| Error::Initialization(format!("Failed to serialize event: {e}")))?;

        let event_kind_header = serde_json::to_value(&event.kind)
            .map_err(|e| Error::Initialization(format!("Failed to serialize event kind: {e}")))?
            .as_str()
            .unwrap_or_default()
            .to_string();

        for (name, endpoint) in &self.endpoints {
            if !endpoint.matches_event(&event.kind, &event.repository) {
                continue;
            }

            let timer = DELIVERY_DURATION
                .with_label_values(&[name.as_str(), event_kind_header.as_str()])
                .start_timer();

            match endpoint.config.policy {
                DeliveryPolicy::Async => {
                    if self.shutdown.load(Ordering::SeqCst) {
                        warn!("Async webhook '{name}' skipped: dispatcher is shut down");
                        continue;
                    }
                    let mut in_flight_guard = self.in_flight.lock().await;
                    in_flight_guard.spawn(deliver_async(
                        endpoint.client.clone(),
                        endpoint.config.url.clone(),
                        endpoint.config.token.clone(),
                        body.clone(),
                        event_kind_header.clone(),
                        endpoint.config.max_retries,
                        name.clone(),
                    ));
                }
                DeliveryPolicy::Required => {
                    send_and_record(
                        &endpoint.client,
                        &endpoint.config.url,
                        endpoint.config.token.as_deref(),
                        &body,
                        &event_kind_header,
                        endpoint.config.max_retries,
                        name,
                    )
                    .await
                    .map_err(|e| Error::Initialization(format!("Webhook '{name}' failed: {e}")))?;
                }
                DeliveryPolicy::Optional => {
                    if let Err(e) = send_and_record(
                        &endpoint.client,
                        &endpoint.config.url,
                        endpoint.config.token.as_deref(),
                        &body,
                        &event_kind_header,
                        endpoint.config.max_retries,
                        name,
                    )
                    .await
                    {
                        warn!("Optional webhook '{name}' failed: {e}");
                    }
                }
            }

            timer.observe_duration();
        }

        Ok(())
    }
}
