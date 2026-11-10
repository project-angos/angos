use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use angos_backoff::Backoff;
use angos_mtls_client::build_client;
use angos_secret::Secret;
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Client;
use sha2::Sha256;
use tokio::time::sleep;
use tokio_util::task::TaskTracker;
use tracing::warn;
use url::Url;

#[cfg(test)]
mod tests;

use crate::configuration::{Configuration, RegexPattern};
use crate::event_webhook::Error;
use crate::event_webhook::config::{DeliveryPolicy, EventWebhookConfig};
use crate::event_webhook::event::{Event, EventKind};
use crate::metrics_provider::metrics_provider;

pub struct EventDispatcher {
    endpoints: HashMap<String, Arc<WebhookEndpoint>>,
    /// The webhooks an event goes to: those its repository names, else these.
    global: Vec<String>,
    repositories: HashMap<String, Vec<String>>,
    shutdown: AtomicBool,
    /// Tracked rather than owned: a `JoinSet` aborts what it holds when it is
    /// dropped, and a configuration reload drops the dispatcher it displaces,
    /// which would cancel that dispatcher's deliveries mid-flight.
    in_flight: TaskTracker,
}

const DELIVERY_BACKOFF: Backoff =
    Backoff::exponential(Duration::from_millis(100), Duration::from_secs(10));

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
}

/// One webhook POST, owned so the same value serves the synchronous policies
/// and the spawned async task.
struct Delivery {
    endpoint: Arc<WebhookEndpoint>,
    name: String,
    body: Bytes,
    event_kind_header: &'static str,
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
        let endpoint = &self.endpoint;
        let mut request = endpoint
            .client
            .post(endpoint.url.clone())
            .header("content-type", "application/json")
            .header("X-Registry-Event", self.event_kind_header);

        if let Some(token) = &endpoint.token {
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
        // A `loop` rather than a `for` so the final attempt returns with its
        // error in hand, leaving no post-loop `Option` to unwrap.
        let mut first_err: Option<String> = None;
        let mut attempt = 0;

        loop {
            if attempt > 0 {
                sleep(DELIVERY_BACKOFF.delay(attempt - 1)).await;
            }

            let last_err = match self.send_request().await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };

            if attempt == self.endpoint.max_retries {
                let attempts = attempt + 1;
                return Err(match first_err {
                    Some(first) if first != last_err => format!(
                        "after {attempts} attempt(s); first error: {first}; last error: {last_err}"
                    ),
                    _ => format!("after {attempts} attempt(s): {last_err}"),
                });
            }
            first_err.get_or_insert(last_err);
            attempt += 1;
        }
    }
}

fn compute_signature(secret: &str, body: &[u8]) -> Result<String, String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|e| format!("failed to initialize HMAC for the webhook signature: {e}"))?;
    mac.update(body);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

impl EventDispatcher {
    /// Build a dispatcher over the full webhook map (name → config), each
    /// config resolved into its endpoint's individual fields here. An event
    /// reaches the webhooks its repository names in `repositories`, else those
    /// named in `global`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when a webhook's HTTP client cannot
    /// be constructed.
    pub fn new(
        webhooks: HashMap<String, EventWebhookConfig>,
        global: Vec<String>,
        repositories: HashMap<String, Vec<String>>,
    ) -> Result<Self, Error> {
        let mut endpoints = HashMap::with_capacity(webhooks.len());

        for (name, config) in webhooks {
            let client = build_client(
                Client::builder().timeout(Duration::from_millis(config.timeout_ms)),
                &config.tls,
            )
            .map_err(|e| {
                Error::Initialization(format!(
                    "Failed to create HTTP client for webhook '{name}': {e}"
                ))
            })?;

            endpoints.insert(
                name,
                Arc::new(WebhookEndpoint {
                    client,
                    max_retries: config.max_retries(),
                    url: config.url,
                    policy: config.policy,
                    token: config.token,
                    events: config.events,
                    repository_filter: config.repository_filter,
                }),
            );
        }

        Ok(Self {
            endpoints,
            global,
            repositories,
            shutdown: AtomicBool::new(false),
            in_flight: TaskTracker::new(),
        })
    }

    /// Build the dispatcher from the configuration, `None` when no webhook is
    /// defined.
    pub fn from_config(config: &Configuration) -> Result<Option<Arc<Self>>, Error> {
        if config.event_webhook.is_empty() {
            return Ok(None);
        }
        // A repository naming no webhook inherits the global ones.
        let repositories: HashMap<String, Vec<String>> = config
            .repository
            .iter()
            .filter(|(_, repository)| !repository.event_webhooks.is_empty())
            .map(|(name, repository)| (name.clone(), repository.event_webhooks.clone()))
            .collect();
        let dispatcher = Self::new(
            config.event_webhook.clone(),
            config.global.event_webhooks.clone(),
            repositories,
        )?;
        Ok(Some(Arc::new(dispatcher)))
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Closing is what lets the wait return; the shutdown flag above is
        // what stops another delivery being tracked after it.
        self.in_flight.close();
        self.in_flight.wait().await;
    }

    async fn deliver(&self, delivery: Delivery) -> Result<(), Error> {
        match delivery.endpoint.policy {
            DeliveryPolicy::Async => {
                if self.shutdown.load(Ordering::Acquire) {
                    warn!(
                        "Async webhook '{}' skipped: dispatcher is shut down",
                        delivery.name
                    );
                } else {
                    self.in_flight.spawn(async move {
                        if let Err(e) = delivery.send_and_record().await {
                            warn!("Async webhook '{}' failed: {e}", delivery.name);
                        }
                    });
                }
                Ok(())
            }
            DeliveryPolicy::Required => match delivery.send_and_record().await {
                Ok(()) => Ok(()),
                Err(e) => {
                    // `dispatch` returns only the first required failure, so a
                    // later one would otherwise reach metrics alone.
                    let message = format!("Webhook '{}' failed: {e}", delivery.name);
                    warn!("Required {message}");
                    Err(Error::Dispatch(message))
                }
            },
            DeliveryPolicy::Optional => {
                if let Err(e) = delivery.send_and_record().await {
                    warn!("Optional webhook '{}' failed: {e}", delivery.name);
                }
                Ok(())
            }
        }
    }

    /// Deliver `event` to every matching webhook its repository enables. A
    /// required-policy failure is surfaced only after all endpoints got their
    /// delivery, so one failing webhook cannot starve the others.
    pub async fn dispatch(&self, event: &Event) -> Result<(), Error> {
        let body = serde_json::to_vec(event)
            .map(Bytes::from)
            .map_err(|e| Error::Dispatch(format!("Failed to serialize event: {e}")))?;
        let event_kind_header = event.kind().as_str();
        let enabled = self
            .repositories
            .get(&event.repository)
            .unwrap_or(&self.global);

        let mut first_required_failure = None;
        for name in enabled {
            let Some(endpoint) = self.endpoints.get(name) else {
                continue;
            };
            if !endpoint.matches_event(event.kind(), &event.repository) {
                continue;
            }
            let delivery = Delivery {
                endpoint: endpoint.clone(),
                name: name.clone(),
                body: body.clone(),
                event_kind_header,
            };
            if let Err(e) = self.deliver(delivery).await
                && first_required_failure.is_none()
            {
                first_required_failure = Some(e);
            }
        }

        first_required_failure.map_or(Ok(()), Err)
    }
}
