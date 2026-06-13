use std::collections::HashMap;

use jsonwebtoken::Algorithm;
use serde::{Deserialize, Serialize};

use crate::{
    auth::oidc::provider::{BaseConfig, HasBaseConfig, OidcProvider},
    command::server::Error,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderConfig {
    #[serde(default = "default_github_issuer")]
    pub issuer: String,
    #[serde(default = "default_github_jwks_uri")]
    pub jwks_uri: String,
    #[serde(default = "BaseConfig::default_jwks_refresh_interval")]
    pub jwks_refresh_interval: u64,
    #[serde(default)]
    pub required_audience: Option<String>,
    #[serde(default = "BaseConfig::default_clock_skew_tolerance")]
    pub clock_skew_tolerance: u64,
    #[serde(default = "BaseConfig::default_allowed_algorithms")]
    pub allowed_algorithms: Vec<Algorithm>,
}

fn default_github_issuer() -> String {
    "https://token.actions.githubusercontent.com".to_string()
}

fn default_github_jwks_uri() -> String {
    "https://token.actions.githubusercontent.com/.well-known/jwks".to_string()
}

pub struct Provider {
    base: BaseConfig,
}

impl Provider {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            base: BaseConfig {
                issuer: config.issuer,
                jwks_uri: Some(config.jwks_uri),
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
        "GitHub Actions"
    }

    fn validate_provider_claims(
        &self,
        claims: &HashMap<String, serde_json::Value>,
    ) -> Result<(), Error> {
        if !claims.contains_key("repository") {
            let msg = "Missing repository claim in GitHub token".to_string();
            return Err(Error::Unauthorized(msg));
        }
        if !claims.contains_key("actor") {
            let msg = "Missing actor claim in GitHub token".to_string();
            return Err(Error::Unauthorized(msg));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;

    use super::*;
    use crate::command::server::Error;

    #[test]
    fn test_config_deserialize_minimal() {
        let toml = r#"
            issuer = "https://token.actions.githubusercontent.com"
            jwks_uri = "https://token.actions.githubusercontent.com/.well-known/jwks"
        "#;

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://token.actions.githubusercontent.com");
        assert_eq!(
            config.jwks_uri,
            "https://token.actions.githubusercontent.com/.well-known/jwks"
        );
        assert_eq!(config.jwks_refresh_interval, 3600);
        assert!(config.required_audience.is_none());
        assert_eq!(config.clock_skew_tolerance, 60);
        assert_eq!(config.allowed_algorithms, vec![Algorithm::RS256]);
    }

    #[test]
    fn test_config_deserialize_with_defaults() {
        let toml = r"";

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://token.actions.githubusercontent.com");
        assert_eq!(
            config.jwks_uri,
            "https://token.actions.githubusercontent.com/.well-known/jwks"
        );
        assert_eq!(config.jwks_refresh_interval, 3600);
        assert_eq!(config.clock_skew_tolerance, 60);
        assert_eq!(config.allowed_algorithms, vec![Algorithm::RS256]);
    }

    #[test]
    fn test_config_deserialize_partial_override() {
        // Setting only one of issuer/jwks_uri in TOML must override that field
        // while the unspecified field still takes its per-field serde default.
        let custom_issuer: ProviderConfig = toml::from_str(
            r#"
            issuer = "https://custom.example.com"
        "#,
        )
        .unwrap();
        assert_eq!(custom_issuer.issuer, "https://custom.example.com");
        assert_eq!(
            custom_issuer.jwks_uri,
            "https://token.actions.githubusercontent.com/.well-known/jwks"
        );

        let custom_jwks: ProviderConfig = toml::from_str(
            r#"
            jwks_uri = "https://custom.example.com/.well-known/jwks"
        "#,
        )
        .unwrap();
        assert_eq!(
            custom_jwks.issuer,
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(
            custom_jwks.jwks_uri,
            "https://custom.example.com/.well-known/jwks"
        );
    }

    #[test]
    fn test_config_deserialize_full() {
        let toml = r#"
            issuer = "https://custom.github.com"
            jwks_uri = "https://custom.github.com/jwks"
            jwks_refresh_interval = 7200
            required_audience = "my-app"
            clock_skew_tolerance = 120
            allowed_algorithms = ["RS256", "ES256"]
        "#;

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://custom.github.com");
        assert_eq!(config.jwks_uri, "https://custom.github.com/jwks");
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
        // Construct a Provider from a ProviderConfig and assert every getter the
        // pass-through accessors expose, covering default issuer/jwks_uri, the
        // required_audience None and Some variants, and custom refresh-interval
        // and clock-skew values that must flow through Provider::new unchanged.
        struct Case {
            config: ProviderConfig,
            issuer: &'static str,
            jwks_uri: Option<&'static str>,
            jwks_refresh_interval: u64,
            required_audience: Option<&'static str>,
            clock_skew_tolerance: u64,
            allowed_algorithms: Vec<Algorithm>,
        }

        let cases = [
            // Canonical GitHub Actions config with explicit defaults.
            Case {
                config: ProviderConfig {
                    issuer: "https://token.actions.githubusercontent.com".to_string(),
                    jwks_uri: "https://token.actions.githubusercontent.com/.well-known/jwks"
                        .to_string(),
                    jwks_refresh_interval: 3600,
                    required_audience: None,
                    clock_skew_tolerance: 60,
                    allowed_algorithms: vec![Algorithm::RS256],
                },
                issuer: "https://token.actions.githubusercontent.com",
                jwks_uri: Some("https://token.actions.githubusercontent.com/.well-known/jwks"),
                jwks_refresh_interval: 3600,
                required_audience: None,
                clock_skew_tolerance: 60,
                allowed_algorithms: vec![Algorithm::RS256],
            },
            // Defaults with a populated required_audience.
            Case {
                config: ProviderConfig {
                    issuer: default_github_issuer(),
                    jwks_uri: default_github_jwks_uri(),
                    jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
                    required_audience: Some("my-audience".to_string()),
                    clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
                    allowed_algorithms: BaseConfig::default_allowed_algorithms(),
                },
                issuer: "https://token.actions.githubusercontent.com",
                jwks_uri: Some("https://token.actions.githubusercontent.com/.well-known/jwks"),
                jwks_refresh_interval: 3600,
                required_audience: Some("my-audience"),
                clock_skew_tolerance: 60,
                allowed_algorithms: vec![Algorithm::RS256],
            },
            // Custom refresh interval flows through unchanged.
            Case {
                config: ProviderConfig {
                    issuer: default_github_issuer(),
                    jwks_uri: default_github_jwks_uri(),
                    jwks_refresh_interval: 7200,
                    required_audience: None,
                    clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
                    allowed_algorithms: BaseConfig::default_allowed_algorithms(),
                },
                issuer: "https://token.actions.githubusercontent.com",
                jwks_uri: Some("https://token.actions.githubusercontent.com/.well-known/jwks"),
                jwks_refresh_interval: 7200,
                required_audience: None,
                clock_skew_tolerance: 60,
                allowed_algorithms: vec![Algorithm::RS256],
            },
            // Custom clock-skew tolerance flows through unchanged.
            Case {
                config: ProviderConfig {
                    issuer: default_github_issuer(),
                    jwks_uri: default_github_jwks_uri(),
                    jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
                    required_audience: None,
                    clock_skew_tolerance: 120,
                    allowed_algorithms: BaseConfig::default_allowed_algorithms(),
                },
                issuer: "https://token.actions.githubusercontent.com",
                jwks_uri: Some("https://token.actions.githubusercontent.com/.well-known/jwks"),
                jwks_refresh_interval: 3600,
                required_audience: None,
                clock_skew_tolerance: 120,
                allowed_algorithms: vec![Algorithm::RS256],
            },
        ];

        for case in cases {
            let provider = Provider::new(case.config);
            // name() is a fixed constant for the GitHub Actions provider.
            assert_eq!(provider.name(), "GitHub Actions");
            assert_eq!(provider.issuer(), case.issuer);
            assert_eq!(provider.jwks_uri(), case.jwks_uri);
            assert_eq!(provider.jwks_refresh_interval(), case.jwks_refresh_interval);
            assert_eq!(provider.required_audience(), case.required_audience);
            assert_eq!(provider.clock_skew_tolerance(), case.clock_skew_tolerance);
            assert_eq!(
                provider.allowed_algorithms(),
                case.allowed_algorithms.as_slice()
            );
        }
    }

    #[test]
    fn test_provider_getters_from_deserialized_toml() {
        // The serde -> toml -> Provider::new -> getter path: empty TOML must yield
        // the default issuer/jwks_uri through the accessors, and a single-field
        // override must flow through Provider::new into that accessor while the
        // unspecified field keeps its per-field serde default.
        let defaults: ProviderConfig = toml::from_str("").unwrap();
        let provider = Provider::new(defaults);
        assert_eq!(
            provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(
            provider.jwks_uri(),
            Some("https://token.actions.githubusercontent.com/.well-known/jwks")
        );
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.required_audience(), None);
        assert_eq!(provider.clock_skew_tolerance(), 60);
        assert_eq!(provider.allowed_algorithms(), [Algorithm::RS256].as_slice());

        let custom_issuer: ProviderConfig = toml::from_str(
            r#"
            issuer = "https://custom.example.com"
        "#,
        )
        .unwrap();
        let provider = Provider::new(custom_issuer);
        assert_eq!(provider.issuer(), "https://custom.example.com");
        assert_eq!(
            provider.jwks_uri(),
            Some("https://token.actions.githubusercontent.com/.well-known/jwks")
        );

        let custom_jwks: ProviderConfig = toml::from_str(
            r#"
            jwks_uri = "https://custom.example.com/.well-known/jwks"
        "#,
        )
        .unwrap();
        let provider = Provider::new(custom_jwks);
        assert_eq!(
            provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(
            provider.jwks_uri(),
            Some("https://custom.example.com/.well-known/jwks")
        );
    }

    #[test]
    fn test_validate_provider_claims_success() {
        let config = ProviderConfig {
            issuer: default_github_issuer(),
            jwks_uri: default_github_jwks_uri(),
            jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
            required_audience: None,
            clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
            allowed_algorithms: BaseConfig::default_allowed_algorithms(),
        };

        let provider = Provider::new(config);

        let mut claims = HashMap::new();
        claims.insert("repository".to_string(), serde_json::json!("org/repo"));
        claims.insert("actor".to_string(), serde_json::json!("user"));
        claims.insert("extra".to_string(), serde_json::json!("data"));

        assert!(provider.validate_provider_claims(&claims).is_ok());
    }

    #[test]
    fn test_validate_provider_claims_missing_repository() {
        let config = ProviderConfig {
            issuer: default_github_issuer(),
            jwks_uri: default_github_jwks_uri(),
            jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
            required_audience: None,
            clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
            allowed_algorithms: BaseConfig::default_allowed_algorithms(),
        };

        let provider = Provider::new(config);

        let mut claims = HashMap::new();
        claims.insert("actor".to_string(), serde_json::json!("user"));

        let result = provider.validate_provider_claims(&claims);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code(), StatusCode::UNAUTHORIZED);
        match err {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("repository"));
            }
            err => panic!("Expected Unauthorized error, got {err:?}"),
        }
    }

    #[test]
    fn test_validate_provider_claims_missing_actor() {
        let config = ProviderConfig {
            issuer: default_github_issuer(),
            jwks_uri: default_github_jwks_uri(),
            jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
            required_audience: None,
            clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
            allowed_algorithms: BaseConfig::default_allowed_algorithms(),
        };

        let provider = Provider::new(config);

        let mut claims = HashMap::new();
        claims.insert("repository".to_string(), serde_json::json!("org/repo"));

        let result = provider.validate_provider_claims(&claims);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status_code(), StatusCode::UNAUTHORIZED);
        match err {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("actor"));
            }
            err => panic!("Expected Unauthorized error, got {err:?}"),
        }
    }

    #[test]
    fn test_validate_provider_claims_empty() {
        let config = ProviderConfig {
            issuer: default_github_issuer(),
            jwks_uri: default_github_jwks_uri(),
            jwks_refresh_interval: BaseConfig::default_jwks_refresh_interval(),
            required_audience: None,
            clock_skew_tolerance: BaseConfig::default_clock_skew_tolerance(),
            allowed_algorithms: BaseConfig::default_allowed_algorithms(),
        };

        let provider = Provider::new(config);
        let claims = HashMap::new();
        let result = provider.validate_provider_claims(&claims);
        assert!(matches!(&result, Err(Error::Unauthorized(_))));
        assert_eq!(result.unwrap_err().status_code(), StatusCode::UNAUTHORIZED);
    }
}
