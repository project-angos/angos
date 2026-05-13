use std::{sync::Arc, time::Duration};

use hyper::http::{HeaderMap, request::Parts};
use reqwest::Client;
use tracing::warn;

use crate::{
    auth::webhook::{
        cache::lookup_cached_decision,
        config::Config,
        headers::{build_cache_key, build_headers},
        metrics::{WEBHOOK_DURATION, WEBHOOK_REQUESTS},
    },
    cache::Cache,
    command::server::Error,
    http_client::HttpClientBuilder,
    identity::{Action, ClientIdentity},
};

pub struct WebhookAuthorizer {
    name: String,
    config: Config,
    client: Client,
    cache: Arc<Cache>,
}

impl WebhookAuthorizer {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn new(name: String, config: Config, cache: Arc<Cache>) -> Result<Self, Error> {
        config.validate().map_err(Error::Initialization)?;

        let client = HttpClientBuilder::new()
            .rustls_tls()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(config.timeout_ms))
            .tls_files(
                config.server_ca_bundle.as_deref(),
                config.client_certificate_bundle.as_deref(),
                config.client_private_key.as_deref(),
            )
            .map_err(Error::Initialization)?
            .build()
            .map_err(Error::Initialization)?;

        Ok(Self {
            name,
            config,
            client,
            cache,
        })
    }

    async fn do_request(&self, headers: &HeaderMap) -> Result<reqwest::Response, reqwest::Error> {
        let mut request = self.client.get(self.config.url.clone());
        for (key, value) in headers {
            request = request.header(key, value);
        }
        if let Some(auth) = &self.config.auth {
            request = auth.apply_to(request);
        }
        request.send().await
    }

    fn record_outcome(&self, label: &str) {
        WEBHOOK_REQUESTS
            .with_label_values(&[self.name.as_str(), label])
            .inc();
    }

    /// Best-effort: a cache-store failure does not affect the authorization decision;
    /// the warn surfaces a misbehaving cache that would silently double webhook traffic.
    async fn cache_outcome(&self, cache_key: &str, allowed: bool) {
        if let Err(e) = self
            .cache
            .store(cache_key, &allowed, self.config.cache_ttl)
            .await
        {
            warn!(
                "Webhook '{}' cache store failed: {e}; authorization unaffected",
                self.name
            );
        }
    }

    pub async fn authorize(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        parts: &Parts,
    ) -> Result<bool, Error> {
        let cache_key = build_cache_key(&self.name, action, identity);

        if let Ok(cache_key) = &cache_key
            && let Some(cached) =
                lookup_cached_decision(self.cache.as_ref(), &self.name, cache_key).await
        {
            return Ok(cached);
        }

        let timer = WEBHOOK_DURATION
            .with_label_values(&[&self.name])
            .start_timer();
        let headers = build_headers(&self.config.forward_headers, action, identity, parts)?;
        let send_result = self.do_request(&headers).await;
        timer.observe_duration();

        match send_result {
            Ok(resp) => {
                let allowed = resp.status().is_success();
                self.record_outcome(if allowed { "allow" } else { "deny" });
                if let Ok(cache_key) = &cache_key {
                    self.cache_outcome(cache_key, allowed).await;
                }
                Ok(allowed)
            }
            Err(e) => {
                // Webhook unreachable: fail closed and surface the transport cause.
                // Cache is intentionally not updated so a transient outage does not
                // pin a stale deny for cache_ttl.
                warn!("Webhook '{}' request failed: {e}", self.name);
                self.record_outcome("transport_error");
                Err(Error::Unauthorized(format!(
                    "authorization webhook '{}' unreachable: {e}",
                    self.name
                )))
            }
        }
    }
}
