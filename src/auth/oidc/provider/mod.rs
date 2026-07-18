pub mod generic;
pub mod github;

use std::collections::HashMap;

use jsonwebtoken::Algorithm;
use serde::{Deserialize, Serialize};

use crate::auth::Error;

/// Shared OIDC provider configuration. The generic provider deserializes into
/// it directly; the GitHub provider builds it from its own defaults.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BaseConfig {
    pub issuer: String,
    #[serde(default)]
    pub jwks_uri: Option<String>,
    #[serde(default = "BaseConfig::default_jwks_refresh_interval")]
    pub jwks_refresh_interval: u64,
    #[serde(default)]
    pub required_audience: Option<String>,
    #[serde(default = "BaseConfig::default_clock_skew_tolerance")]
    pub clock_skew_tolerance: u64,
    #[serde(default = "BaseConfig::default_allowed_algorithms")]
    pub allowed_algorithms: Vec<Algorithm>,
}

impl BaseConfig {
    pub fn default_jwks_refresh_interval() -> u64 {
        3600
    }

    pub fn default_clock_skew_tolerance() -> u64 {
        60
    }

    pub fn default_allowed_algorithms() -> Vec<Algorithm> {
        vec![Algorithm::RS256]
    }
}

/// An OIDC provider: its shared [`BaseConfig`] plus provider-specific claim
/// validation. Field access goes through [`OidcProvider::base_config`].
pub trait OidcProvider: Send + Sync {
    fn base_config(&self) -> &BaseConfig;

    fn name(&self) -> &'static str;

    fn validate_provider_claims(
        &self,
        _claims: &HashMap<String, serde_json::Value>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
