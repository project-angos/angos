use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use angos_backoff::Backoff;
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Client;
use sha2::Sha256;
use tokio::{sync::Mutex, task::JoinSet, time::sleep};
use tracing::warn;
use url::Url;

#[cfg(test)]
mod tests;

use crate::{
    configuration::RegexPattern,
    event_webhook::{
        Error,
        config::{DeliveryPolicy, EventWebhookConfig},
        event::{Event, EventKind},
    },
    http_client::HttpClientBuilder,
    metrics_provider::metrics_provider,
    secret::Secret,
};

pub struct EventDispatcher {
    endpoints: HashMap<String, WebhookEndpoint>,
    shutdown: AtomicBool,
    in_flight: Mutex<JoinSet<()>>,
    delivery_backoff: Backoff,
}

struct WebhookEndpoint {
    client: Client,
    url: Url,
    policy: DeliveryPolicy,
    token: Option<Secret<String>>,
    max_retries: u32,
    events: Vec<EventKind>,
    repository_filter: Option<Vec<RegexPattern>>,
}

impl WebhookEndpoint {
    fn matches_event(&self, event_kind: &EventKind, repository: &str) -> bool {
        if !self.events.contains(event_kind) {
            return false;
        }
        match &self.repository_filter {
            None => true,
            Some(filters) => filters.iter().any(|p| p.is_match(repository)),
        }
    }

    fn delivery(
        &self,
        name: &str,
        body: Bytes,
        event_kind_header: &'static str,
        backoff: Backoff,
    ) -> Delivery {
        Delivery {
            client: self.client.clone(),
            url: self.url.to_string(),
            token: self.token.clone(),
            body,
            event_kind_header,
            name: name.to_string(),
            max_retries: self.max_retries,
            backoff,
        }
    }
}

/// One resolved webhook POST, owned so the same value serves the synchronous
/// policies and the spawned async task.
struct Delivery {
    client: Client,
    url: String,
    token: Option<Secret<String>>,
    body: Bytes,
    event_kind_header: &'static str,
    name: String,
    max_retries: u32,
    backoff: Backoff,
}

impl Delivery {
    /// Deliver with retries, recording the duration and outcome metrics; the
    /// async policy runs this inside its spawned task, so the histogram
    /// measures real deliveries under every policy.
    async fn send_and_record(&self) -> Result<(), String> {
        let timer = metrics_provider()
            .event_webhook_delivery_duration
            .with_label_values(&[self.name.as_str(), self.event_kind_header])
            .start_timer();
        let result = self.send_with_retries().await;
        timer.observe_duration();

        let result_label = if result.is_ok() { "success" } else { "error" };
        metrics_provider()
            .event_webhook_deliveries
            .with_label_values(&[self.name.as_str(), self.event_kind_header, result_label])
            .inc();
        result
    }

    async fn send_request(&self) -> Result<(), String> {
        let mut request = self
            .client
            .post(self.url.as_str())
            .header("content-type", "application/json")
            .header("X-Registry-Event", self.event_kind_header);

        if let Some(token) = &self.token {
            let token = token.expose();
            let signature = compute_signature(token, &self.body)?;
            request = request
                .header("Authorization", format!("Bearer {token}"))
                .header("X-Registry-Signature-256", format!("sha256={signature}"));
        }

        let response = request
            .body(self.body.clone())
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !response.status().is_success() {
            return Err(format!("returned status {}", response.status()));
        }

        Ok(())
    }

    async fn send_with_retries(&self) -> Result<(), String> {
        // A `loop` (not a bounded `for`) so the exhaustion path carries the last
        // error in hand and returns from inside, with no post-loop `Option` to
        // unwrap: the final attempt is the only exit besides an early success.
        let mut first_err: Option<String> = None;
        let mut attempt = 0;

        loop {
            if attempt > 0 {
                sleep(self.backoff.delay(attempt - 1)).await;
            }

            let last_err = match self.send_request().await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };

            if attempt == self.max_retries {
                let first_err = first_err.as_deref().unwrap_or(&last_err);
                return Err(format_retry_failure(
                    self.max_retries + 1,
                    (first_err, &last_err),
                ));
            }
            first_err.get_or_insert(last_err);
            attempt += 1;
        }
    }

    /// The spawned-task body for the async policy: deliver and log a failure.
    async fn deliver_logged(self) {
        if let Err(e) = self.send_and_record().await {
            warn!("Async webhook '{}' failed: {e}", self.name);
        }
    }
}

fn serialize_event(event: &Event) -> Result<(Bytes, &'static str), Error> {
    let body = serde_json::to_vec(event)
        .map_err(|e| Error::Dispatch(format!("Failed to serialize event: {e}")))?;
    Ok((Bytes::from(body), event.kind.as_str()))
}

fn compute_signature(secret: &str, body: &[u8]) -> Result<String, String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|e| format!("failed to initialize HMAC for the webhook signature: {e}"))?;
    mac.update(body);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn format_retry_failure(attempts: u32, errors: (&str, &str)) -> String {
    match errors {
        (f, l) if f == l => format!("after {attempts} attempt(s): {f}"),
        (f, l) => {
            format!("after {attempts} attempt(s); first error: {f}; last error: {l}")
        }
    }
}

impl EventDispatcher {
    /// Build a dispatcher over the full webhook map (name → config); each
    /// config is resolved into its endpoint's individual fields here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when a webhook's HTTP client cannot
    /// be constructed.
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
                    url: config.url,
                    policy: config.policy,
                    token: config.token,
                    max_retries: config.max_retries,
                    events: config.events,
                    repository_filter: config.repository_filter,
                },
            );
        }

        Ok(Self {
            endpoints,
            shutdown: AtomicBool::new(false),
            in_flight: Mutex::new(JoinSet::new()),
            delivery_backoff: Backoff::exponential(
                Duration::from_millis(100),
                Duration::from_secs(10),
            ),
        })
    }

    /// Build the dispatcher from the configured webhook map, `None` when no
    /// webhook is defined. The single construction seam shared by the server
    /// and the maintenance commands.
    pub fn from_config(
        webhooks: &HashMap<String, EventWebhookConfig>,
    ) -> Result<Option<Arc<Self>>, Error> {
        if webhooks.is_empty() {
            return Ok(None);
        }
        let dispatcher = Self::new(webhooks.clone())?;
        Ok(Some(Arc::new(dispatcher)))
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.drain_in_flight().await;
    }

    async fn drain_in_flight(&self) {
        let mut in_flight = self.in_flight.lock().await;
        while in_flight.join_next().await.is_some() {}
    }

    async fn spawn_async(&self, delivery: Delivery) {
        if self.shutdown.load(Ordering::Acquire) {
            warn!(
                "Async webhook '{}' skipped: dispatcher is shut down",
                delivery.name
            );
            return;
        }
        let mut in_flight = self.in_flight.lock().await;
        // Reap completed deliveries so the set does not grow for the life of
        // the process.
        while in_flight.try_join_next().is_some() {}
        in_flight.spawn(delivery.deliver_logged());
    }

    async fn deliver(&self, endpoint: &WebhookEndpoint, delivery: Delivery) -> Result<(), Error> {
        match endpoint.policy {
            DeliveryPolicy::Async => {
                self.spawn_async(delivery).await;
                Ok(())
            }
            DeliveryPolicy::Required => {
                let result = delivery.send_and_record().await;
                result.map_err(|e| {
                    Error::Dispatch(format!("Webhook '{}' failed: {e}", delivery.name))
                })
            }
            DeliveryPolicy::Optional => {
                if let Err(e) = delivery.send_and_record().await {
                    warn!("Optional webhook '{}' failed: {e}", delivery.name);
                }
                Ok(())
            }
        }
    }

    /// Deliver `event` to every matching endpoint. A required-policy failure
    /// is surfaced only after all endpoints got their delivery, so one failing
    /// webhook cannot starve the others.
    pub async fn dispatch(&self, event: &Event) -> Result<(), Error> {
        let (body, event_kind_header) = serialize_event(event)?;

        let mut first_required_failure = None;
        for (name, endpoint) in &self.endpoints {
            if !endpoint.matches_event(&event.kind, &event.repository) {
                continue;
            }
            let delivery =
                endpoint.delivery(name, body.clone(), event_kind_header, self.delivery_backoff);
            if let Err(e) = self.deliver(endpoint, delivery).await
                && first_required_failure.is_none()
            {
                first_required_failure = Some(e);
            }
        }

        first_required_failure.map_or(Ok(()), Err)
    }
}
