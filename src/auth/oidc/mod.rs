pub mod jwk;
pub mod provider;
pub mod validator;

use std::sync::Arc;

use async_trait::async_trait;
use hyper::http::request::Parts;
pub use jwk::Jwk;
pub use provider::OidcProvider;
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

use crate::{
    auth::{
        AuthMiddleware, AuthResult,
        oidc::provider::{generic, github},
    },
    cache::Cache,
    command::server::{Error, RequestHeaders},
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
    cache: Arc<Cache>,
}

impl OidcValidator {
    pub fn new(
        provider_name: String,
        provider_config: &Config,
        client: Arc<Client>,
        cache: Arc<Cache>,
    ) -> Self {
        let provider = provider_config.to_backend();

        Self {
            provider_name,
            provider,
            client,
            cache,
        }
    }

    pub async fn validate_token(&self, token: &str) -> Result<OidcClaims, Error> {
        validator::validate_oidc_token(
            &self.provider_name,
            &*self.provider,
            token,
            &self.client,
            self.cache.as_ref(),
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
                // Do not log the full claims map: it contains user/CI metadata.
                let subject = claims.claims.get("sub").and_then(|v| v.as_str());
                let issuer = claims.claims.get("iss").and_then(|v| v.as_str());
                debug!(
                    "OIDC token validated for provider '{}' (type='{}', sub={:?}, iss={:?})",
                    claims.provider_name, claims.provider_type, subject, issuer
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
    let headers = RequestHeaders::new(&parts.headers);

    if let Some(bearer_token) = headers.bearer_token() {
        debug!("Found Bearer token for OIDC provider '{provider_name}'");
        return Some(bearer_token);
    }
    if let Some((username, password)) = headers.basic_auth() {
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
    use std::{collections::HashMap, net::SocketAddr, sync::Arc};

    use serde_json::json;
    use wiremock::MockServer;

    use super::*;
    use crate::{
        auth::oidc::{
            provider::github::tests::default_github_config,
            validator::tests::{build_test_provider_config, make_token, valid_claims},
        },
        cache,
        identity::ClientIdentity,
        test_fixtures::{
            mocks::{mount_jwks, static_jwks_response},
            oidc::KID,
            requests::{empty_parts, parts_with_authorization, parts_with_basic_auth},
        },
    };

    fn build_config(issuer: &str) -> Config {
        Config::Generic(generic::ProviderConfig {
            required_audience: None,
            ..build_test_provider_config(issuer)
        })
    }

    fn make_test_token(issuer: &str) -> String {
        let mut claims = valid_claims(issuer, "unused");
        claims.remove("aud");
        claims.insert("sub".to_string(), json!("test-user"));
        make_token(&claims, KID)
    }

    fn test_http_client() -> Arc<Client> {
        Arc::new(Client::new())
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
        let config = build_config("https://auth.example.com");

        let provider = config.to_backend();
        assert_eq!(provider.issuer(), "https://auth.example.com");
        assert_eq!(provider.name(), "Generic OIDC");
    }

    #[test]
    fn test_config_to_backend_github() {
        let config = Config::GitHub(default_github_config());

        let provider = config.to_backend();
        assert_eq!(
            provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(provider.name(), "GitHub Actions");
    }

    #[test]
    fn test_oidc_validator_new_generic() {
        let config = Config::Generic(build_test_provider_config("https://auth.example.com"));

        let cache = cache::Config::Memory.to_backend().unwrap();
        let client = test_http_client();
        let validator =
            OidcValidator::new("test-provider".to_string(), &config, client.clone(), cache);

        assert_eq!(validator.provider_name, "test-provider");
        assert_eq!(validator.provider.issuer(), "https://auth.example.com");
        assert!(Arc::ptr_eq(&validator.client, &client));
    }

    #[test]
    fn test_oidc_validator_new_github() {
        let config = Config::GitHub(default_github_config());

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator =
            OidcValidator::new("github".to_string(), &config, test_http_client(), cache);

        assert_eq!(validator.provider_name, "github");
        assert_eq!(
            validator.provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
    }

    #[tokio::test]
    async fn test_validate_token_success() {
        let mock_server = MockServer::start().await;

        mount_jwks(&mock_server, static_jwks_response()).await;

        let config = build_config(&mock_server.uri());
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new(
            "test-provider".to_string(),
            &config,
            test_http_client(),
            cache,
        );

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
        let config = build_config("https://auth.example.com");

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new(
            "test-provider".to_string(),
            &config,
            test_http_client(),
            cache,
        );

        let result = validator.validate_token("invalid-token").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_authenticate_with_bearer_token() {
        let mock_server = MockServer::start().await;

        mount_jwks(&mock_server, static_jwks_response()).await;

        let config = build_config(&mock_server.uri());
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new(
            "test-provider".to_string(),
            &config,
            test_http_client(),
            cache,
        );

        let token = make_test_token(&mock_server.uri());
        let parts = parts_with_authorization(&format!("Bearer {token}"));
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

        mount_jwks(&mock_server, static_jwks_response()).await;

        let config = build_config(&mock_server.uri());
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator =
            OidcValidator::new("github".to_string(), &config, test_http_client(), cache);

        let token = make_test_token(&mock_server.uri());
        let parts = parts_with_basic_auth("github", &token);
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert!(identity.oidc.is_some());
    }

    #[tokio::test]
    async fn test_authenticate_with_basic_auth_non_matching_provider() {
        let config = build_config("https://auth.example.com");

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator =
            OidcValidator::new("github".to_string(), &config, test_http_client(), cache);

        let parts = parts_with_basic_auth("wrong-provider", "token");
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_no_credentials() {
        let config = build_config("https://auth.example.com");

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new(
            "test-provider".to_string(),
            &config,
            test_http_client(),
            cache,
        );

        let parts = empty_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_with_invalid_token() {
        let config = build_config("https://auth.example.com");

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new(
            "test-provider".to_string(),
            &config,
            test_http_client(),
            cache,
        );

        let parts = parts_with_authorization("Bearer invalid-token");
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_err());
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_populates_identity() {
        let mock_server = MockServer::start().await;

        mount_jwks(&mock_server, static_jwks_response()).await;

        let config = build_config(&mock_server.uri());
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new(
            "my-provider".to_string(),
            &config,
            test_http_client(),
            cache,
        );

        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("user-123"));
        claims.insert("email".to_string(), json!("user@example.com"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = make_token(&claims, KID);
        let parts = parts_with_authorization(&format!("Bearer {token}"));
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
