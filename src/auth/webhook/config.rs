use reqwest::{RequestBuilder, header::AUTHORIZATION};
use serde::Deserialize;
use url::Url;

use angos_mtls_client::ClientTls;
use angos_secret::Secret;

use crate::auth::webhook::headers::build_header_name;

/// The DTO always parses; [`Config::validate`] runs in
/// [`WebhookAuthorizer::new`](super::WebhookAuthorizer), the single
/// enforcement point, so programmatic construction is checked too.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub url: Url,
    pub timeout_ms: u64,
    #[serde(flatten)]
    pub auth: Option<WebhookAuth>,
    #[serde(flatten)]
    pub tls: ClientTls,
    #[serde(default)]
    pub forward_headers: Vec<String>,
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
    pub fn validate(&self) -> Result<(), String> {
        for header in &self.forward_headers {
            build_header_name(header)
                .map_err(|e| format!("invalid forward_headers entry '{header}': {e}"))?;
        }

        Ok(())
    }
}
