use std::path::PathBuf;

use reqwest::{RequestBuilder, header::AUTHORIZATION};
use serde::Deserialize;
use url::Url;

use crate::{auth::webhook::headers::build_header_name, secret::Secret};

/// The DTO always parses; [`Config::validate`] runs in
/// [`WebhookAuthorizer::new`](super::WebhookAuthorizer), the single
/// enforcement point, so programmatic construction is checked too.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub url: Url,
    pub timeout_ms: u64,
    #[serde(flatten)]
    pub auth: Option<WebhookAuth>,
    pub client_certificate_bundle: Option<PathBuf>,
    pub client_private_key: Option<PathBuf>,
    pub server_ca_bundle: Option<PathBuf>,
    #[serde(default)]
    pub forward_headers: Vec<String>,
    #[serde(default = "Config::default_cache_ttl")]
    pub cache_ttl: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookAuth {
    BasicAuth {
        username: String,
        password: Secret<String>,
    },
    BearerToken(Secret<String>),
}

impl WebhookAuth {
    pub fn apply_to(&self, request: RequestBuilder) -> RequestBuilder {
        match self {
            Self::BearerToken(token) => {
                request.header(AUTHORIZATION, format!("Bearer {}", token.expose()))
            }
            Self::BasicAuth { username, password } => {
                request.basic_auth(username, Some(password.expose()))
            }
        }
    }
}

impl Config {
    fn default_cache_ttl() -> u64 {
        60
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.client_certificate_bundle.is_some() != self.client_private_key.is_some() {
            return Err(
                "both client_certificate_bundle and client_private_key are required for mTLS"
                    .to_string(),
            );
        }

        for header in &self.forward_headers {
            build_header_name(header)
                .map_err(|e| format!("invalid forward_headers entry '{header}': {e}"))?;
        }

        Ok(())
    }
}
