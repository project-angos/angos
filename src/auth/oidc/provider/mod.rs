pub mod generic;
pub mod github;

use std::collections::HashMap;

use jsonwebtoken::Algorithm;

use crate::command::server::Error;

pub struct BaseConfig {
    pub issuer: String,
    pub jwks_uri: Option<String>,
    pub jwks_refresh_interval: u64,
    pub required_audience: Option<String>,
    pub clock_skew_tolerance: u64,
    pub allowed_algorithms: Vec<Algorithm>,
}

pub fn default_allowed_algorithms() -> Vec<Algorithm> {
    vec![Algorithm::RS256]
}

pub trait OidcProvider: Send + Sync {
    fn base(&self) -> &BaseConfig;

    fn name(&self) -> &'static str;

    fn issuer(&self) -> &str {
        &self.base().issuer
    }

    fn jwks_uri(&self) -> Option<&str> {
        self.base().jwks_uri.as_deref()
    }

    fn jwks_refresh_interval(&self) -> u64 {
        self.base().jwks_refresh_interval
    }

    fn required_audience(&self) -> Option<&str> {
        self.base().required_audience.as_deref()
    }

    fn clock_skew_tolerance(&self) -> u64 {
        self.base().clock_skew_tolerance
    }

    fn allowed_algorithms(&self) -> &[Algorithm] {
        &self.base().allowed_algorithms
    }

    fn validate_provider_claims(
        &self,
        _claims: &HashMap<String, serde_json::Value>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
