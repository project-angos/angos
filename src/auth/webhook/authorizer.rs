use std::time::Duration;

use http::{HeaderMap, request::Parts};
use reqwest::{Client, StatusCode};
use tracing::warn;

use crate::{
    auth::Error,
    auth::webhook::{config::Config, headers::build_headers},
    identity::{Action, ClientIdentity},
    metrics_provider::metrics_provider,
};

pub struct WebhookAuthorizer {
    name: String,
    config: Config,
    client: Client,
}

impl WebhookAuthorizer {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn new(name: String, config: Config, client: Client) -> Result<Self, Error> {
        config.validate().map_err(Error::Initialization)?;

        Ok(Self {
            name,
            config,
            client,
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

    pub async fn authorize(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        parts: &Parts,
    ) -> Result<bool, Error> {
        let headers = build_headers(&self.config.forward_headers, action, identity, parts)?;

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
                    Ok(true)
                } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                    self.record_outcome("deny");
                    Ok(false)
                } else {
                    warn!(
                        "Webhook '{}' returned unavailable status {status}; failing closed",
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
