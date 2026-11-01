use std::str::FromStr;

use http::HeaderName;
use reqwest::RequestBuilder;
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use url::Url;

use angos_mtls_client::ClientTls;
use angos_secret::Secret;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub url: Url,
    pub timeout_ms: u64,
    #[serde(flatten)]
    pub auth: Option<WebhookAuth>,
    #[serde(flatten)]
    pub tls: ClientTls,
    #[serde(default)]
    pub forward_headers: Vec<ForwardHeader>,
}

/// A client header name forwarded verbatim to the webhook.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "String")]
pub struct ForwardHeader(HeaderName);

impl ForwardHeader {
    pub fn name(&self) -> &HeaderName {
        &self.0
    }
}

impl TryFrom<String> for ForwardHeader {
    type Error = String;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        HeaderName::from_str(&name)
            .map(Self)
            .map_err(|e| format!("invalid forward_headers entry '{name}': {e}"))
    }
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
