use std::{sync::Arc, time::Duration};

use hyper::http::{HeaderMap, request::Parts};
use reqwest::{Client, StatusCode};
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

    pub fn new(
        name: String,
        config: Config,
        client: Client,
        cache: Arc<Cache>,
    ) -> Result<Self, Error> {
        config.validate().map_err(Error::Initialization)?;

        Ok(Self {
            name,
            config,
            client,
            cache,
        })
    }

    async fn do_request(&self, headers: &HeaderMap) -> Result<reqwest::Response, reqwest::Error> {
        let mut request = self
            .client
            .get(self.config.url.clone())
            .timeout(Duration::from_millis(self.config.timeout_ms));
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
                let status = resp.status();
                if status.is_success() {
                    self.record_outcome("allow");
                    if let Ok(cache_key) = &cache_key {
                        self.cache_outcome(cache_key, true).await;
                    }
                    Ok(true)
                } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                    // 401/403 are explicit deny decisions, safe to cache.
                    self.record_outcome("deny");
                    if let Ok(cache_key) = &cache_key {
                        self.cache_outcome(cache_key, false).await;
                    }
                    Ok(false)
                } else {
                    // Do not cache: a transient outage must not pin denials.
                    warn!(
                        "Webhook '{}' returned unavailable status {status}; failing closed without caching",
                        self.name
                    );
                    self.record_outcome("unavailable");
                    Err(Error::Unauthorized(format!(
                        "authorization webhook '{}' returned status {status}",
                        self.name
                    )))
                }
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
