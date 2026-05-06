use std::{sync::Arc, time::Duration};

use hyper::http::request::Parts;
use reqwest::{Client, redirect::Policy};
use tracing::warn;

use super::{
    cache::cache_retrieve,
    config::Config,
    headers::{build_cache_key, build_headers},
    metrics::{WEBHOOK_DURATION, WEBHOOK_REQUESTS},
    tls::{load_certificate_bundle, load_identity},
};
use crate::{
    cache::{Cache, CacheExt},
    command::server::Error,
    identity::{Action, ClientIdentity},
};

pub struct WebhookAuthorizer {
    name: String,
    config: Config,
    client: Client,
    cache: Arc<dyn Cache>,
}

impl WebhookAuthorizer {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn new(name: String, config: Config, cache: Arc<dyn Cache>) -> Result<Self, Error> {
        let mut client_builder = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_millis(config.timeout_ms));

        if let Some(ca_bundle) = &config.server_ca_bundle {
            let ca_bundle_certs = load_certificate_bundle(ca_bundle)?;
            for cert in ca_bundle_certs {
                client_builder = client_builder.add_root_certificate(cert);
            }
        }

        let identity = load_identity(
            config.client_certificate_bundle.as_ref(),
            config.client_private_key.as_ref(),
        )?;
        if let Some(identity) = identity {
            client_builder = client_builder.identity(identity);
        }

        let client = client_builder
            .build()
            .map_err(|e| Error::Initialization(format!("Failed to create HTTP client: {e}")))?;

        Ok(Self {
            name,
            config,
            client,
            cache,
        })
    }

    pub async fn authorize(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        parts: &Parts,
    ) -> Result<bool, Error> {
        let cache_key = build_cache_key(&self.name, action, identity);

        if let Ok(cache_key) = &cache_key
            && let Ok(Some(cached)) = cache_retrieve(&self.cache, &self.name, cache_key).await
        {
            return Ok(cached);
        }

        let timer = WEBHOOK_DURATION
            .with_label_values(&[&self.name])
            .start_timer();

        let headers = build_headers(&self.config.forward_headers, action, identity, parts)?;
        let mut request = self.client.get(self.config.url.clone());
        for (key, value) in &headers {
            request = request.header(key, value);
        }

        if let Some(auth) = &self.config.auth {
            request = auth.apply_to(request);
        }

        let send_result = request.send().await;
        timer.observe_duration();

        match send_result {
            Ok(resp) => {
                let allowed = resp.status().is_success();
                let result_label = if allowed { "allow" } else { "deny" }.to_string();
                WEBHOOK_REQUESTS
                    .with_label_values(&[&self.name, &result_label])
                    .inc();
                if let Ok(cache_key) = &cache_key
                    && let Err(e) = self
                        .cache
                        .store(cache_key, &allowed, self.config.cache_ttl)
                        .await
                {
                    // Best-effort: cache-write failure must not affect the authorization
                    // decision, but a misbehaving cache silently doubling webhook traffic
                    // is exactly the symptom the warn surfaces.
                    warn!(
                        "Webhook '{}' cache store failed: {e}; authorization unaffected",
                        self.name
                    );
                }
                Ok(allowed)
            }
            Err(e) => {
                // Authorization webhook unreachable: fail closed and surface the
                // transport-failure cause to operators. The "transport_error" metric
                // label distinguishes this from explicit deny on dashboards. The
                // cache is intentionally NOT updated — a transient outage must not
                // pin a stale deny for cache_ttl.
                warn!("Webhook '{}' request failed: {e}", self.name);
                WEBHOOK_REQUESTS
                    .with_label_values(&[&self.name, &"transport_error".to_string()])
                    .inc();
                Err(Error::Unauthorized(format!(
                    "authorization webhook '{}' unreachable: {e}",
                    self.name
                )))
            }
        }
    }
}
