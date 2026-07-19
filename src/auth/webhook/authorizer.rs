use std::{sync::Arc, time::Duration};

use hyper::http::{HeaderMap, request::Parts};
use reqwest::{Client, StatusCode};
use tracing::warn;

use crate::{
    auth::Error,
    auth::webhook::{
        config::Config,
        headers::{build_cache_key, build_headers},
    },
    cache::Cache,
    identity::{Action, ClientIdentity},
    metrics_provider::metrics_provider,
};

pub async fn lookup_cached_decision(cache: &Cache, name: &str, cache_key: &str) -> Option<bool> {
    match cache.retrieve::<bool>(cache_key).await {
        Ok(Some(value)) => {
            let label = if value { "cached_allow" } else { "cached_deny" };
            metrics_provider()
                .webhook_auth_requests
                .with_label_values(&[name, label])
                .inc();
            Some(value)
        }
        Ok(None) => None,
        Err(err) => {
            warn!("Webhook '{name}' cache retrieve failed for key {cache_key}: {err}");
            None
        }
    }
}

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
        metrics_provider()
            .webhook_auth_requests
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
        // Build the forwarded headers first: the cache key is a digest of them,
        // so a decision is only ever reused for an identical forwarded context.
        let headers = build_headers(&self.config.forward_headers, action, identity, parts)?;
        let cache_key = build_cache_key(&self.name, &headers);

        if let Some(cached) =
            lookup_cached_decision(self.cache.as_ref(), &self.name, &cache_key).await
        {
            return Ok(cached);
        }

        let timer = metrics_provider()
            .webhook_auth_duration
            .with_label_values(&[&self.name])
            .start_timer();
        let send_result = self.do_request(&headers).await;
        timer.observe_duration();

        match send_result {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    self.record_outcome("allow");
                    self.cache_outcome(&cache_key, true).await;
                    Ok(true)
                } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                    // 401/403 are explicit deny decisions, safe to cache.
                    self.record_outcome("deny");
                    self.cache_outcome(&cache_key, false).await;
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

#[cfg(test)]
mod tests {
    use super::lookup_cached_decision;
    use crate::{
        cache::{Cache, Config, stub},
        metrics_provider,
    };

    #[tokio::test]
    async fn lookup_cached_decision_unwraps_hit_to_some_value() {
        metrics_provider::init_for_tests();
        let cache = Config::Memory.to_backend().unwrap();
        cache.store("allow-key", &true, 60).await.unwrap();
        cache.store("deny-key", &false, 60).await.unwrap();

        assert_eq!(
            lookup_cached_decision(cache.as_ref(), "wh", "allow-key").await,
            Some(true)
        );
        assert_eq!(
            lookup_cached_decision(cache.as_ref(), "wh", "deny-key").await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn lookup_cached_decision_returns_none_on_miss() {
        let cache = Config::Memory.to_backend().unwrap();
        assert_eq!(
            lookup_cached_decision(cache.as_ref(), "wh", "no-such-key").await,
            None,
            "missing key must return None"
        );
    }

    #[tokio::test]
    async fn lookup_cached_decision_returns_none_on_error() {
        let backend = stub::Backend::new();
        backend.set_retrieve_error(Some("injected retrieve failure".to_string()));
        let cache = Cache::Stub(backend);
        assert_eq!(
            lookup_cached_decision(&cache, "wh", "any-key").await,
            None,
            "backend error must return None"
        );
    }
}
