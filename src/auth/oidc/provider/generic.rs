use async_trait::async_trait;
use jsonwebtoken::Algorithm;
use serde::{Deserialize, Serialize};

use crate::auth::oidc::provider::{BaseConfig, OidcProvider, default_allowed_algorithms};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderConfig {
    pub issuer: String,
    #[serde(default)]
    pub jwks_uri: Option<String>,
    #[serde(default = "default_jwks_refresh_interval")]
    pub jwks_refresh_interval: u64,
    #[serde(default)]
    pub required_audience: Option<String>,
    #[serde(default = "default_clock_skew_tolerance")]
    pub clock_skew_tolerance: u64,
    #[serde(default = "default_allowed_algorithms")]
    pub allowed_algorithms: Vec<Algorithm>,
}

fn default_jwks_refresh_interval() -> u64 {
    3600
}

fn default_clock_skew_tolerance() -> u64 {
    60
}

pub struct Provider {
    base: BaseConfig,
}

impl Provider {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            base: BaseConfig {
                issuer: config.issuer,
                jwks_uri: config.jwks_uri,
                jwks_refresh_interval: config.jwks_refresh_interval,
                required_audience: config.required_audience,
                clock_skew_tolerance: config.clock_skew_tolerance,
                allowed_algorithms: config.allowed_algorithms,
            },
        }
    }
}

#[async_trait]
impl OidcProvider for Provider {
    fn base(&self) -> &BaseConfig {
        &self.base
    }

    fn name(&self) -> &'static str {
        "Generic OIDC"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_config_deserialize_minimal() {
        let toml = r#"
            issuer = "https://example.com"
        "#;

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://example.com");
        assert!(config.jwks_uri.is_none());
        assert_eq!(config.jwks_refresh_interval, 3600);
        assert!(config.required_audience.is_none());
        assert_eq!(config.clock_skew_tolerance, 60);
        assert_eq!(config.allowed_algorithms, vec![Algorithm::RS256]);
    }

    #[test]
    fn test_config_deserialize_full() {
        let toml = r#"
            issuer = "https://auth.example.com"
            jwks_uri = "https://auth.example.com/jwks"
            jwks_refresh_interval = 7200
            required_audience = "my-app"
            clock_skew_tolerance = 120
            allowed_algorithms = ["RS256", "ES256"]
        "#;

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://auth.example.com");
        assert_eq!(
            config.jwks_uri,
            Some("https://auth.example.com/jwks".to_string())
        );
        assert_eq!(config.jwks_refresh_interval, 7200);
        assert_eq!(config.required_audience, Some("my-app".to_string()));
        assert_eq!(config.clock_skew_tolerance, 120);
        assert_eq!(
            config.allowed_algorithms,
            vec![Algorithm::RS256, Algorithm::ES256]
        );
    }

    #[test]
    fn test_default_functions() {
        assert_eq!(default_jwks_refresh_interval(), 3600);
        assert_eq!(default_clock_skew_tolerance(), 60);
    }

    #[test]
    fn test_create_provider() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
            allowed_algorithms: vec![Algorithm::RS256],
        };

        let provider = Provider::new(config);
        assert_eq!(provider.issuer(), "https://example.com");
        assert_eq!(provider.name(), "Generic OIDC");
        assert!(provider.jwks_uri().is_none());
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.required_audience(), Some("test-audience"));
        assert_eq!(provider.clock_skew_tolerance(), 60);
        assert_eq!(provider.allowed_algorithms(), &[Algorithm::RS256]);
    }

    #[test]
    fn test_provider_with_jwks_uri() {
        let config = ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/.well-known/jwks".to_string()),
            jwks_refresh_interval: 7200,
            required_audience: None,
            clock_skew_tolerance: 120,
            allowed_algorithms: vec![Algorithm::RS256],
        };

        let provider = Provider::new(config);
        assert_eq!(provider.issuer(), "https://auth.example.com");
        assert_eq!(
            provider.jwks_uri(),
            Some("https://auth.example.com/.well-known/jwks")
        );
        assert_eq!(provider.jwks_refresh_interval(), 7200);
        assert!(provider.required_audience().is_none());
        assert_eq!(provider.clock_skew_tolerance(), 120);
    }

    #[test]
    fn test_provider_with_defaults() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: default_jwks_refresh_interval(),
            required_audience: None,
            clock_skew_tolerance: default_clock_skew_tolerance(),
            allowed_algorithms: default_allowed_algorithms(),
        };

        let provider = Provider::new(config);
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.clock_skew_tolerance(), 60);
        assert_eq!(provider.allowed_algorithms(), &[Algorithm::RS256]);
    }

    #[test]
    fn test_provider_name() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
            allowed_algorithms: vec![Algorithm::RS256],
        };

        let provider = Provider::new(config);
        assert_eq!(provider.name(), "Generic OIDC");
    }

    #[test]
    fn test_provider_validate_provider_claims_default() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
            allowed_algorithms: vec![Algorithm::RS256],
        };

        let provider = Provider::new(config);
        let claims = HashMap::new();
        assert!(provider.validate_provider_claims(&claims).is_ok());
    }
}
