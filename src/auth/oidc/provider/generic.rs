use jsonwebtoken::Algorithm;
use serde::{Deserialize, Serialize};

use crate::auth::oidc::provider::{BaseConfig, HasBaseConfig, OidcProvider};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderConfig {
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

impl HasBaseConfig for Provider {
    fn base_config(&self) -> &BaseConfig {
        &self.base
    }
}

impl OidcProvider for Provider {
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
    fn test_provider_getters() {
        // One Provider per row, covering every getter across both the
        // present (Some) and absent (None) variants of the optional fields,
        // including the explicit BaseConfig::default_* values used by the
        // "with_defaults" case.
        let cases = [
            (
                ProviderConfig {
                    issuer: "https://example.com".to_string(),
                    jwks_uri: None,
                    jwks_refresh_interval: 3600,
                    required_audience: Some("test-audience".to_string()),
                    clock_skew_tolerance: 60,
                    allowed_algorithms: vec![Algorithm::RS256],
                },
                "https://example.com",
                None,
                3600u64,
                Some("test-audience"),
                60u64,
                vec![Algorithm::RS256],
            ),
            (
                ProviderConfig {
                    issuer: "https://auth.example.com".to_string(),
                    jwks_uri: Some("https://auth.example.com/.well-known/jwks".to_string()),
                    jwks_refresh_interval: 7200,
                    required_audience: None,
                    clock_skew_tolerance: 120,
                    allowed_algorithms: vec![Algorithm::RS256],
                },
                "https://auth.example.com",
                Some("https://auth.example.com/.well-known/jwks"),
                7200u64,
                None,
                120u64,
                vec![Algorithm::RS256],
            ),
            (
                ProviderConfig {
                    issuer: "https://example.com".to_string(),
                    jwks_uri: None,
                    jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
                    required_audience: None,
                    clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
                    allowed_algorithms: BaseConfig::default_allowed_algorithms(),
                },
                "https://example.com",
                None,
                3600u64,
                None,
                60u64,
                vec![Algorithm::RS256],
            ),
        ];

        for (
            config,
            issuer,
            jwks_uri,
            jwks_refresh_interval,
            required_audience,
            clock_skew_tolerance,
            allowed_algorithms,
        ) in cases
        {
            let provider = Provider::new(config);
            assert_eq!(provider.issuer(), issuer);
            assert_eq!(provider.jwks_uri(), jwks_uri);
            assert_eq!(provider.jwks_refresh_interval(), jwks_refresh_interval);
            assert_eq!(provider.required_audience(), required_audience);
            assert_eq!(provider.clock_skew_tolerance(), clock_skew_tolerance);
            assert_eq!(provider.allowed_algorithms(), allowed_algorithms.as_slice());
            assert_eq!(provider.name(), "Generic OIDC");
            assert!(provider.validate_provider_claims(&HashMap::new()).is_ok());
        }
    }
}
