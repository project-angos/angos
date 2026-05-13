use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use reqwest::Client;
use sha2::Sha256;
use tokio::{sync::Mutex, task::JoinSet};
use tracing::warn;

#[cfg(test)]
mod tests;

use crate::{
    event_webhook::{
        Error,
        config::{DeliveryPolicy, EventWebhookConfig},
        event::{Event, EventKind},
    },
    http_client::HttpClientBuilder,
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

pub struct EventDispatcher {
    endpoints: HashMap<String, WebhookEndpoint>,
    shutdown: Arc<AtomicBool>,
    in_flight: Arc<Mutex<JoinSet<()>>>,
}

struct WebhookEndpoint {
    client: Client,
    config: Arc<EventWebhookConfig>,
}

impl WebhookEndpoint {
    fn matches_event(&self, event_kind: &EventKind, repository: &str) -> bool {
        if !self.config.events.contains(event_kind) {
            return false;
        }
        match &self.config.repository_filter {
            None => true,
            Some(filters) => filters.iter().any(|p| p.is_match(repository)),
        }
    }

    fn build_request<'a>(&'a self, body: Bytes, event_kind_header: &'a str) -> DeliveryRequest<'a> {
        DeliveryRequest {
            client: &self.client,
            url: self.config.url.as_str(),
            token: self.config.token.as_ref().map(|t| t.expose().as_str()),
            body,
            event_kind_header,
        }
    }
}

struct DeliveryRequest<'a> {
    client: &'a Client,
    url: &'a str,
    token: Option<&'a str>,
    body: Bytes,
    event_kind_header: &'a str,
}

struct DeliveryJob {
    client: Client,
    url: String,
    token: Option<String>,
    body: Bytes,
    event_kind_header: String,
    max_retries: u32,
    name: String,
}

impl DeliveryJob {
    fn as_request(&self) -> DeliveryRequest<'_> {
        DeliveryRequest {
            client: &self.client,
            url: &self.url,
            token: self.token.as_deref(),
            body: self.body.clone(),
            event_kind_header: &self.event_kind_header,
        }
    }
}

async fn deliver_async(job: DeliveryJob) {
    if let Err(e) = send_and_record(&job.as_request(), job.max_retries, &job.name).await {
        warn!("Async webhook '{}' failed: {e}", job.name);
    }
}

async fn send_and_record(
    req: &DeliveryRequest<'_>,
    max_retries: u32,
    webhook_name: &str,
) -> Result<(), String> {
    let result = send_with_retries(req, max_retries).await;
    let result_label = if result.is_ok() { "success" } else { "error" };
    DELIVERY_TOTAL
        .with_label_values(&[webhook_name, req.event_kind_header, result_label])
        .inc();
    result
}

fn serialize_event(event: &Event) -> Result<(Bytes, &'static str), Error> {
    let body = serde_json::to_vec(event)
        .map_err(|e| Error::Dispatch(format!("Failed to serialize event: {e}")))?;
    Ok((Bytes::from(body), event.kind.as_str()))
}

fn compute_signature(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

async fn send_request(req: &DeliveryRequest<'_>) -> Result<(), String> {
    let mut request = req
        .client
        .post(req.url)
        .header("content-type", "application/json")
        .header("X-Registry-Event", req.event_kind_header);

    if let Some(token) = req.token {
        let signature = compute_signature(token, &req.body);
        request = request
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Registry-Signature-256", format!("sha256={signature}"));
    }

    let response = request
        .body(req.body.clone())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("returned status {}", response.status()));
    }

    Ok(())
}

async fn send_with_retries(req: &DeliveryRequest<'_>, max_retries: u32) -> Result<(), String> {
    let mut first_err: Option<String> = None;
    let mut last_err: Option<String> = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            tokio::time::sleep(backoff_for_attempt(attempt)).await;
        }

        match send_request(req).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e.clone());
                }
                last_err = Some(e);
            }
        }
    }

    Err(format_retry_failure(
        max_retries + 1,
        first_err.as_deref(),
        last_err.as_deref(),
    ))
}

fn format_retry_failure(attempts: u32, first: Option<&str>, last: Option<&str>) -> String {
    match (first, last) {
        (None, _) | (_, None) => format!("after {attempts} attempt(s): unknown error"),
        (Some(f), Some(l)) if f == l => {
            format!("after {attempts} attempt(s): {f}")
        }
        (Some(f), Some(l)) => {
            format!("after {attempts} attempt(s); first error: {f}; last error: {l}")
        }
    }
}

fn backoff_for_attempt(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(2u64.saturating_pow(attempt - 1)))
}

impl EventDispatcher {
    pub fn new(webhooks: HashMap<String, EventWebhookConfig>) -> Result<Self, Error> {
        let mut endpoints = HashMap::with_capacity(webhooks.len());

        for (name, config) in webhooks {
            let client = HttpClientBuilder::new()
                .rustls_tls()
                .timeout(Duration::from_millis(config.timeout_ms))
                .build()
                .map_err(|e| {
                    Error::Initialization(format!(
                        "Failed to create HTTP client for webhook '{name}': {e}"
                    ))
                })?;

            endpoints.insert(
                name,
                WebhookEndpoint {
                    client,
                    config: Arc::new(config),
                },
            );
        }

        Ok(Self {
            endpoints,
            shutdown: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(Mutex::new(JoinSet::new())),
        })
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

    async fn send_async(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: Bytes,
        event_kind_header: &str,
    ) -> bool {
        if self.shutdown.load(Ordering::SeqCst) {
            warn!("Async webhook '{name}' skipped: dispatcher is shut down");
            return false;
        }
        let mut in_flight_guard = self.in_flight.lock().await;
        in_flight_guard.spawn(deliver_async(DeliveryJob {
            client: endpoint.client.clone(),
            url: endpoint.config.url.to_string(),
            token: endpoint.config.token.as_ref().map(|t| t.expose().clone()),
            body,
            event_kind_header: event_kind_header.to_string(),
            max_retries: endpoint.config.max_retries,
            name: name.to_string(),
        }));
        true
    }

    async fn send_required(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: Bytes,
        event_kind_header: &str,
    ) -> Result<(), Error> {
        let req = endpoint.build_request(body, event_kind_header);
        send_and_record(&req, endpoint.config.max_retries, name)
            .await
            .map_err(|e| Error::Dispatch(format!("Webhook '{name}' failed: {e}")))
    }

    async fn send_optional(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: Bytes,
        event_kind_header: &str,
    ) {
        let req = endpoint.build_request(body, event_kind_header);
        if let Err(e) = send_and_record(&req, endpoint.config.max_retries, name).await {
            warn!("Optional webhook '{name}' failed: {e}");
        }
    }

    async fn deliver_for_endpoint(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: Bytes,
        event_kind_header: &str,
    ) -> Result<bool, Error> {
        match endpoint.config.policy {
            DeliveryPolicy::Async => Ok(self
                .send_async(name, endpoint, body, event_kind_header)
                .await),
            DeliveryPolicy::Required => {
                self.send_required(name, endpoint, body, event_kind_header)
                    .await?;
                Ok(true)
            }
            DeliveryPolicy::Optional => {
                self.send_optional(name, endpoint, body, event_kind_header)
                    .await;
                Ok(true)
            }
        }
    }

    pub async fn dispatch(&self, event: &Event) -> Result<(), Error> {
        let (body, event_kind_header) = serialize_event(event)?;

        for (name, endpoint) in &self.endpoints {
            if !endpoint.matches_event(&event.kind, &event.repository) {
                continue;
            }

            let timer = DELIVERY_DURATION
                .with_label_values(&[name.as_str(), event_kind_header])
                .start_timer();

            if !self
                .deliver_for_endpoint(name, endpoint, body.clone(), event_kind_header)
                .await?
            {
                continue;
            }

            timer.observe_duration();
        }

        Ok(())
    }
}
