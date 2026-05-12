use std::path::PathBuf;

use reqwest::{RequestBuilder, header::AUTHORIZATION};
use serde::Deserialize;
use url::Url;

use crate::{auth::webhook::headers::build_header_name, secret::Secret};

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "ConfigFields")]
pub struct Config {
    pub url: Url,
    pub timeout_ms: u64,
    pub auth: Option<WebhookAuth>,
    pub client_certificate_bundle: Option<PathBuf>,
    pub client_private_key: Option<PathBuf>,
    pub server_ca_bundle: Option<PathBuf>,
    pub forward_headers: Vec<String>,
    pub cache_ttl: u64,
}

#[derive(Deserialize)]
struct ConfigFields {
    url: Url,
    timeout_ms: u64,
    #[serde(flatten)]
    auth: Option<WebhookAuth>,
    client_certificate_bundle: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
    server_ca_bundle: Option<PathBuf>,
    #[serde(default)]
    forward_headers: Vec<String>,
    #[serde(default = "Config::default_cache_ttl")]
    cache_ttl: u64,
}

impl TryFrom<ConfigFields> for Config {
    type Error = String;

    fn try_from(fields: ConfigFields) -> Result<Self, Self::Error> {
        if fields.client_certificate_bundle.is_some() != fields.client_private_key.is_some() {
            return Err(
                "both client_certificate_bundle and client_private_key are required for mTLS"
                    .to_string(),
            );
        }

        for header in &fields.forward_headers {
            build_header_name(header)
                .map_err(|e| format!("invalid forward_headers entry '{header}': {e}"))?;
        }

        Ok(Self {
            url: fields.url,
            timeout_ms: fields.timeout_ms,
            auth: fields.auth,
            client_certificate_bundle: fields.client_certificate_bundle,
            client_private_key: fields.client_private_key,
            server_ca_bundle: fields.server_ca_bundle,
            forward_headers: fields.forward_headers,
            cache_ttl: fields.cache_ttl,
        })
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

impl Config {
    fn default_cache_ttl() -> u64 {
        60
    }
}
