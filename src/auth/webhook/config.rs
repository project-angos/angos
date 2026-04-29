use std::path::PathBuf;

use reqwest::{RequestBuilder, header::AUTHORIZATION};
use serde::Deserialize;
use url::Url;

use crate::{command::server::Error, secret::Secret};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Populated during configuration resolution; not present in TOML.
    #[serde(skip, default)]
    pub name: String,

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

    pub fn validate(&self) -> Result<(), Error> {
        if self.client_certificate_bundle.is_some() != self.client_private_key.is_some() {
            let msg = "Both certificate and key required for mTLS".to_string();
            return Err(Error::Initialization(msg));
        }

        Ok(())
    }
}
