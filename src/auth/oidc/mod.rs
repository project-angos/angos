pub mod jwk;
pub mod provider;
pub mod validator;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use hyper::http::request::Parts;
pub use jwk::Jwk;
pub use provider::OidcProvider;
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

use super::{AuthMiddleware, AuthResult};
use crate::{
    auth::oidc::provider::{generic, github},
    cache::Cache,
    command::server::{Error, request_ext::HeaderExt},
    identity::{ClientIdentity, OidcClaims},
};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum Config {
    Generic(generic::ProviderConfig),
    GitHub(github::ProviderConfig),
}

impl Config {
    pub fn to_backend(&self) -> Arc<dyn OidcProvider + Send + Sync> {
        match self {
            Config::Generic(config) => Arc::new(generic::Provider::new(config.clone())),
            Config::GitHub(config) => Arc::new(github::Provider::new(config.clone())),
        }
    }
}

pub struct OidcValidator {
    provider_name: String,
    provider: Arc<dyn OidcProvider>,
    client: Arc<Client>,
    cache: Arc<dyn Cache>,
}

impl OidcValidator {
    pub fn new(
        provider_name: String,
        provider_config: &Config,
        cache: Arc<dyn Cache>,
    ) -> Result<Self, Error> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map(Arc::new)
            .map_err(|e| Error::Initialization(format!("Failed to build HTTP client: {e}")))?;

        let provider = provider_config.to_backend();

        Ok(Self {
            provider_name,
            provider,
            client,
            cache,
        })
    }

    pub async fn validate_token(&self, token: &str) -> Result<OidcClaims, Error> {
        validator::validate_oidc_token(
            &self.provider_name,
            &*self.provider,
            token,
            &self.client,
            &*self.cache,
        )
        .await
    }
}

#[async_trait]
impl AuthMiddleware for OidcValidator {
    async fn authenticate(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<AuthResult, Error> {
        let Some(token) = extract_oidc_credential(parts, &self.provider_name) else {
            return Ok(AuthResult::NoCredentials);
        };

        match self.validate_token(&token).await {
            Ok(claims) => {
                debug!(
                    "Successfully validated OIDC token for provider '{}' with claims: {:?}",
                    self.provider_name, claims
                );
                identity.oidc = Some(claims);
                Ok(AuthResult::Authenticated)
            }
            Err(e) => {
                debug!(
                    "OIDC token validation failed for provider '{}': {}",
                    self.provider_name, e
                );
                Err(e)
            }
        }
    }
}

/// Extracts an OIDC credential string from `parts`:
/// - `Authorization: Bearer <token>` → `Some(token)` (any provider can claim a Bearer header).
/// - `Authorization: Basic <user:pass>` where `user == provider_name` → `Some(password)`
///   (the OIDC token is in the password field; the username gates which provider claims it).
/// - Anything else → `None`.
fn extract_oidc_credential(parts: &Parts, provider_name: &str) -> Option<String> {
    if let Some(bearer_token) = parts.bearer_token() {
        debug!("Found Bearer token for OIDC provider '{provider_name}'");
        return Some(bearer_token);
    }
    if let Some((username, password)) = parts.basic_auth() {
        debug!("Found Basic auth credentials with username '{username}'");
        if username == provider_name {
            return Some(password);
        }
        debug!(
            "Basic auth username '{username}' doesn't match OIDC provider name \
             '{provider_name}', skipping"
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr};

    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use hyper::{Request, header::AUTHORIZATION};
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        auth::oidc::validator::tests::{TEST_JWK_E, TEST_JWK_N, TEST_KID, make_token},
        cache,
        identity::ClientIdentity,
    };

    fn build_config(mock_server: &MockServer) -> Config {
        Config::Generic(generic::ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        })
    }

    fn static_jwks_response() -> serde_json::Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": TEST_KID,
                "n": TEST_JWK_N,
                "e": TEST_JWK_E,
                "alg": "RS256"
            }]
        })
    }

    fn make_test_token(issuer: &str) -> String {
        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), json!(issuer));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));
        make_token(&claims, TEST_KID)
    }

    #[test]
    fn test_config_deserialize_generic() {
        let toml = r#"
            provider = "generic"
            issuer = "https://auth.example.com"
            jwks_uri = "https://auth.example.com/jwks"
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        match config {
            Config::Generic(cfg) => {
                assert_eq!(cfg.issuer, "https://auth.example.com");
                assert_eq!(
                    cfg.jwks_uri,
                    Some("https://auth.example.com/jwks".to_string())
                );
            }
            Config::GitHub(_) => panic!("Expected Generic config"),
        }
    }

    #[test]
    fn test_config_deserialize_github() {
        let toml = r#"
            provider = "github"
            issuer = "https://token.actions.githubusercontent.com"
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        match config {
            Config::GitHub(cfg) => {
                assert_eq!(cfg.issuer, "https://token.actions.githubusercontent.com");
            }
            Config::Generic(_) => panic!("Expected GitHub config"),
        }
    }

    #[test]
    fn test_config_to_backend_generic() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let provider = config.to_backend();
        assert_eq!(provider.issuer(), "https://auth.example.com");
        assert_eq!(provider.name(), "Generic OIDC");
    }

    #[test]
    fn test_config_to_backend_github() {
        let config = Config::GitHub(github::ProviderConfig {
            issuer: "https://token.actions.githubusercontent.com".to_string(),
            jwks_uri: "https://token.actions.githubusercontent.com/.well-known/jwks".to_string(),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let provider = config.to_backend();
        assert_eq!(
            provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(provider.name(), "GitHub Actions");
    }

    #[test]
    fn test_oidc_validator_new_generic() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache);

        assert!(validator.is_ok());
        let validator = validator.unwrap();
        assert_eq!(validator.provider_name, "test-provider");
        assert_eq!(validator.provider.issuer(), "https://auth.example.com");
    }

    #[test]
    fn test_oidc_validator_new_github() {
        let config = Config::GitHub(github::ProviderConfig {
            issuer: "https://token.actions.githubusercontent.com".to_string(),
            jwks_uri: "https://token.actions.githubusercontent.com/.well-known/jwks".to_string(),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("github".to_string(), &config, cache);

        assert!(validator.is_ok());
        let validator = validator.unwrap();
        assert_eq!(validator.provider_name, "github");
        assert_eq!(
            validator.provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
    }

    #[tokio::test]
    async fn test_validate_token_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let token = make_test_token(&mock_server.uri());
        let result = validator.validate_token(&token).await;

        assert!(result.is_ok());
        let oidc_claims = result.unwrap();
        assert_eq!(oidc_claims.provider_name, "test-provider");
        assert_eq!(oidc_claims.provider_type, "Generic OIDC");
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "test-user");
    }

    #[tokio::test]
    async fn test_validate_token_invalid() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let result = validator.validate_token("invalid-token").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_authenticate_with_bearer_token() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let token = make_test_token(&mock_server.uri());
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert!(identity.oidc.is_some());
        let oidc_claims = identity.oidc.unwrap();
        assert_eq!(oidc_claims.provider_name, "test-provider");
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "test-user");
    }

    #[tokio::test]
    async fn test_authenticate_with_basic_auth_matching_provider() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("github".to_string(), &config, cache).unwrap();

        let token = make_test_token(&mock_server.uri());
        let credentials = BASE64_STANDARD.encode(format!("github:{token}"));
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Basic {credentials}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert!(identity.oidc.is_some());
    }

    #[tokio::test]
    async fn test_authenticate_with_basic_auth_non_matching_provider() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("github".to_string(), &config, cache).unwrap();

        let credentials = BASE64_STANDARD.encode("wrong-provider:token");
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Basic {credentials}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_no_credentials() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let request = Request::builder().body(()).unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_with_invalid_token() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let request = Request::builder()
            .header(AUTHORIZATION, "Bearer invalid-token")
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_err());
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_populates_identity() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("my-provider".to_string(), &config, cache).unwrap();

        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("user-123"));
        claims.insert("email".to_string(), json!("user@example.com"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = make_token(&claims, TEST_KID);
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let socket_addr: SocketAddr = "192.168.1.1:8080".parse().unwrap();
        let mut identity = ClientIdentity::new(Some(socket_addr));

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(identity.oidc.is_some());

        let oidc_claims = identity.oidc.unwrap();
        assert_eq!(oidc_claims.provider_name, "my-provider");
        assert_eq!(oidc_claims.provider_type, "Generic OIDC");
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "user-123");
        assert_eq!(oidc_claims.claims.get("email").unwrap(), "user@example.com");
        assert_eq!(identity.client_ip, Some("192.168.1.1".to_string()));
    }
}
