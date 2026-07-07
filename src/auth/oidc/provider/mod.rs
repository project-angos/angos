pub mod generic;
pub mod github;

use std::collections::HashMap;

use jsonwebtoken::Algorithm;

use crate::auth::Error;

pub struct BaseConfig {
    pub issuer: String,
    pub jwks_uri: Option<String>,
    pub jwks_refresh_interval: u64,
    pub required_audience: Option<String>,
    pub clock_skew_tolerance: u64,
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

/// Provides the shared OIDC configuration required by every provider.
pub trait HasBaseConfig {
    fn base_config(&self) -> &BaseConfig;
}

/// OIDC provider behavior layered on top of the shared base configuration.
pub trait OidcProvider: HasBaseConfig + Send + Sync {
    fn name(&self) -> &'static str;

    fn issuer(&self) -> &str {
        &self.base_config().issuer
    }

    fn jwks_uri(&self) -> Option<&str> {
        self.base_config().jwks_uri.as_deref()
    }

    fn jwks_refresh_interval(&self) -> u64 {
        self.base_config().jwks_refresh_interval
    }

    fn required_audience(&self) -> Option<&str> {
        self.base_config().required_audience.as_deref()
    }

    fn clock_skew_tolerance(&self) -> u64 {
        self.base_config().clock_skew_tolerance
    }

    fn allowed_algorithms(&self) -> &[Algorithm] {
        &self.base_config().allowed_algorithms
    }

    fn validate_provider_claims(
        &self,
        _claims: &HashMap<String, serde_json::Value>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
