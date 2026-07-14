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
use tokio::{sync::Mutex, task::JoinSet};
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
    shutdown: Arc<AtomicBool>,
    in_flight: Arc<Mutex<JoinSet<()>>>,
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

    fn build_request<'a>(&'a self, body: Bytes, event_kind_header: &'a str) -> DeliveryRequest<'a> {
        DeliveryRequest {
            client: &self.client,
            url: self.url.as_str(),
            token: self.token.as_ref().map(|t| t.expose().as_str()),
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
    delivery_backoff: Backoff,
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
            shutdown: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(Mutex::new(JoinSet::new())),
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

    async fn deliver_async(job: DeliveryJob) {
        if let Err(e) = Self::send_and_record(
            &job.as_request(),
            job.max_retries,
            &job.name,
            job.delivery_backoff,
        )
        .await
        {
            warn!("Async webhook '{}' failed: {e}", job.name);
        }
    }

    async fn send_and_record(
        req: &DeliveryRequest<'_>,
        max_retries: u32,
        webhook_name: &str,
        backoff: Backoff,
    ) -> Result<(), String> {
        let result = Self::send_with_retries(req, max_retries, backoff).await;
        let result_label = if result.is_ok() { "success" } else { "error" };
        metrics_provider()
            .event_webhook_deliveries
            .with_label_values(&[webhook_name, req.event_kind_header, result_label])
            .inc();
        result
    }

    async fn send_request(req: &DeliveryRequest<'_>) -> Result<(), String> {
        let mut request = req
            .client
            .post(req.url)
            .header("content-type", "application/json")
            .header("X-Registry-Event", req.event_kind_header);

        if let Some(token) = req.token {
            let signature = compute_signature(token, &req.body)?;
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

    async fn send_with_retries(
        req: &DeliveryRequest<'_>,
        max_retries: u32,
        backoff: Backoff,
    ) -> Result<(), String> {
        // A `loop` (not a bounded `for`) so the exhaustion path carries the last
        // error in hand and returns from inside, with no post-loop `Option` to
        // unwrap: the final attempt is the only exit besides an early success.
        let mut first_err: Option<String> = None;
        let mut attempt = 0;

        loop {
            if attempt > 0 {
                tokio::time::sleep(backoff.delay(attempt - 1)).await;
            }

            let last_err = match Self::send_request(req).await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };

            if attempt == max_retries {
                let first_err = first_err.as_deref().unwrap_or(&last_err);
                return Err(Self::format_retry_failure(
                    max_retries + 1,
                    (first_err, &last_err),
                ));
            }
            first_err.get_or_insert(last_err);
            attempt += 1;
        }
    }

    fn format_retry_failure(attempts: u32, errors: (&str, &str)) -> String {
        match errors {
            (f, l) if f == l => format!("after {attempts} attempt(s): {f}"),
            (f, l) => {
                format!("after {attempts} attempt(s); first error: {f}; last error: {l}")
            }
        }
    }

    async fn send_async(
        &self,
        name: &str,
        endpoint: &WebhookEndpoint,
        body: Bytes,
        event_kind_header: &str,
    ) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            warn!("Async webhook '{name}' skipped: dispatcher is shut down");
            return false;
        }
        let mut in_flight_guard = self.in_flight.lock().await;
        in_flight_guard.spawn(Self::deliver_async(DeliveryJob {
            client: endpoint.client.clone(),
            url: endpoint.url.to_string(),
            token: endpoint.token.as_ref().map(|t| t.expose().clone()),
            body,
            event_kind_header: event_kind_header.to_string(),
            max_retries: endpoint.max_retries,
            name: name.to_string(),
            delivery_backoff: self.delivery_backoff,
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
        Self::send_and_record(&req, endpoint.max_retries, name, self.delivery_backoff)
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
        if let Err(e) =
            Self::send_and_record(&req, endpoint.max_retries, name, self.delivery_backoff).await
        {
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
        match endpoint.policy {
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

            let timer = metrics_provider()
                .event_webhook_delivery_duration
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
