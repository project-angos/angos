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
use transport::{DeliveryRequest, send_with_retries};

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

struct DeliveryJob {
    client: Client,
    url: String,
    token: Option<String>,
    body: Vec<u8>,
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
            body: &self.body,
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

fn serialize_event(event: &Event) -> Result<(Vec<u8>, &'static str), Error> {
    let body = serde_json::to_vec(event)
        .map_err(|e| Error::Initialization(format!("Failed to serialize event: {e}")))?;
    Ok((body, event.kind.as_str()))
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
        body: &[u8],
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
            body: body.to_vec(),
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
        body: &[u8],
        event_kind_header: &str,
    ) -> Result<(), Error> {
        let req = endpoint.build_request(body, event_kind_header);
        send_and_record(&req, endpoint.config.max_retries, name)
            .await
            .map_err(|e| Error::Initialization(format!("Webhook '{name}' failed: {e}")))
    }

    async fn send_optional(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: &[u8],
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
        body: &[u8],
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
                .deliver_for_endpoint(name, endpoint, &body, event_kind_header)
                .await?
            {
                continue;
            }

            timer.observe_duration();
        }

        Ok(())
    }
}
