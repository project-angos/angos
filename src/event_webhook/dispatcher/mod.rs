use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use reqwest::Client;
use tokio::{sync::Mutex, task::JoinSet};
use tracing::warn;

mod endpoint;
mod signature;
mod transport;

#[cfg(test)]
mod tests;

use endpoint::WebhookEndpoint;
#[cfg(test)]
pub use endpoint::matches_event;
#[cfg(test)]
pub use signature::compute_signature;
use transport::send_with_retries;

use crate::{
    configuration::Error,
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        event::Event,
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

pub struct EventDispatcher {
    endpoints: HashMap<String, WebhookEndpoint>,
    shutdown: Arc<AtomicBool>,
    in_flight: Arc<Mutex<JoinSet<()>>>,
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

fn serialize_event(event: &Event) -> Result<(Vec<u8>, String), Error> {
    let body = serde_json::to_vec(event)
        .map_err(|e| Error::Initialization(format!("Failed to serialize event: {e}")))?;
    let event_kind_header = serde_json::to_value(&event.kind)
        .map_err(|e| Error::Initialization(format!("Failed to serialize event kind: {e}")))?
        .as_str()
        .unwrap_or_default()
        .to_string();
    Ok((body, event_kind_header))
}

impl EventDispatcher {
    pub fn new(webhooks: HashMap<String, Arc<EventWebhookConfig>>) -> Result<Self, Error> {
        let mut endpoints = HashMap::with_capacity(webhooks.len());

        for (name, config) in webhooks {
            let client = Client::builder()
                .timeout(Duration::from_millis(config.timeout_ms))
                .build()
                .map_err(|e| {
                    Error::Initialization(format!(
                        "Failed to create HTTP client for webhook '{name}': {e}"
                    ))
                })?;

            endpoints.insert(name, WebhookEndpoint { client, config });
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

    async fn spawn_async_delivery(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: &[u8],
        event_kind_header: &str,
    ) -> bool {
        if self.shutdown.load(Ordering::SeqCst) {
            warn!("Async webhook '{name}' skipped: dispatcher is shut down");
            return false;
        }
        let mut in_flight_guard = self.in_flight.lock().await;
        in_flight_guard.spawn(deliver_async(
            endpoint.client.clone(),
            endpoint.config.url.to_string(),
            endpoint.config.token.clone(),
            body.to_vec(),
            event_kind_header.to_string(),
            endpoint.config.max_retries,
            name.to_string(),
        ));
        true
    }

    async fn deliver_for_endpoint(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: &[u8],
        event_kind_header: &str,
    ) -> Result<bool, Error> {
        match endpoint.config.policy {
            DeliveryPolicy::Async => {
                let spawned = self
                    .spawn_async_delivery(name, endpoint, body, event_kind_header)
                    .await;
                Ok(spawned)
            }
            DeliveryPolicy::Required => {
                send_and_record(
                    &endpoint.client,
                    endpoint.config.url.as_str(),
                    endpoint.config.token.as_deref(),
                    body,
                    event_kind_header,
                    endpoint.config.max_retries,
                    name,
                )
                .await
                .map_err(|e| Error::Initialization(format!("Webhook '{name}' failed: {e}")))?;
                Ok(true)
            }
            DeliveryPolicy::Optional => {
                if let Err(e) = send_and_record(
                    &endpoint.client,
                    endpoint.config.url.as_str(),
                    endpoint.config.token.as_deref(),
                    body,
                    event_kind_header,
                    endpoint.config.max_retries,
                    name,
                )
                .await
                {
                    warn!("Optional webhook '{name}' failed: {e}");
                }
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
                .with_label_values(&[name.as_str(), event_kind_header.as_str()])
                .start_timer();

            if !self
                .deliver_for_endpoint(name, endpoint, &body, &event_kind_header)
                .await?
            {
                continue;
            }

            timer.observe_duration();
        }

        Ok(())
    }
}
