use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::auth::oidc::OidcProvider;

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
}

fn default_jwks_refresh_interval() -> u64 {
    3600
}

fn default_clock_skew_tolerance() -> u64 {
    60
}

pub struct Provider {
    config: ProviderConfig,
}

impl Provider {
    pub fn new(config: ProviderConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl OidcProvider for Provider {
    fn issuer(&self) -> &str {
        &self.config.issuer
    }

    fn jwks_uri(&self) -> Option<&str> {
        self.config.jwks_uri.as_deref()
    }

    fn name(&self) -> &'static str {
        "Generic OIDC"
    }

    fn jwks_refresh_interval(&self) -> u64 {
        self.config.jwks_refresh_interval
    }

    fn required_audience(&self) -> Option<&str> {
        self.config.required_audience.as_deref()
    }

    fn clock_skew_tolerance(&self) -> u64 {
        self.config.clock_skew_tolerance
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use jsonwebtoken::{Algorithm, Validation};

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
    }

    #[test]
    fn test_config_deserialize_full() {
        let toml = r#"
            issuer = "https://auth.example.com"
            jwks_uri = "https://auth.example.com/jwks"
            jwks_refresh_interval = 7200
            required_audience = "my-app"
            clock_skew_tolerance = 120
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
        };

        let provider = Provider::new(config);
        assert_eq!(provider.issuer(), "https://example.com");
        assert_eq!(provider.name(), "Generic OIDC");
        assert!(provider.jwks_uri().is_none());
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.required_audience(), Some("test-audience"));
        assert_eq!(provider.clock_skew_tolerance(), 60);
    }

    #[test]
    fn test_provider_with_jwks_uri() {
        let config = ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/.well-known/jwks".to_string()),
            jwks_refresh_interval: 7200,
            required_audience: None,
            clock_skew_tolerance: 120,
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
        };

        let provider = Provider::new(config);
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.clock_skew_tolerance(), 60);
    }

    #[test]
    fn test_provider_name() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
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
        };

        let provider = Provider::new(config);
        let claims = HashMap::new();
        assert!(provider.validate_provider_claims(&claims).is_ok());
    }

    #[test]
    fn test_validation_new_with_rs256() {
        let header = jsonwebtoken::Header {
            alg: Algorithm::RS256,
            ..Default::default()
        };

        let validation = Validation::new(header.alg);
        assert!(validation.algorithms.contains(&Algorithm::RS256));
        assert_eq!(validation.algorithms.len(), 1);
    }

    #[test]
    fn test_validation_algorithms_behavior() {
        for alg in [
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::ES256,
            Algorithm::ES384,
        ] {
            let validation = Validation::new(alg);
            assert!(
                validation.algorithms.contains(&alg),
                "Algorithm {alg:?} not found in validation.algorithms"
            );
            assert_eq!(
                validation.algorithms.len(),
                1,
                "Expected single algorithm, got {:?}",
                validation.algorithms
            );
        }
    }

    #[test]
    fn test_github_actions_token_header() {
        let header = jsonwebtoken::Header {
            alg: Algorithm::RS256,
            kid: Some("cc413527-173f-5a05-976e-9c52b1d7b431".to_string()),
            typ: Some("JWT".to_string()),
            ..Default::default()
        };

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&["https://token.actions.githubusercontent.com"]);
        validation.set_audience(&["https://github.com/angos"]);
        validation.leeway = 60;
        validation.validate_exp = true;
        validation.validate_nbf = true;

        assert!(validation.algorithms.contains(&Algorithm::RS256));
        assert_eq!(validation.algorithms.len(), 1);

        assert!(validation.iss.is_some());
        assert!(validation.aud.is_some());
        assert_eq!(validation.leeway, 60);
    }
}
