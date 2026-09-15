pub mod jwk;
pub mod validator;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use http::request::Parts;
use jsonwebtoken::{Algorithm, dangerous::insecure_decode};
pub use jwk::Jwk;
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

use crate::{
    auth::{
        AuthMiddleware, AuthResult, Error,
        authorization::{basic_credentials, bearer_token},
    },
    identity::{ClientIdentity, OidcClaims},
};
use angos_cache::Cache;

/// An OIDC provider: the issuer to trust, and how tokens it signed are validated.
///
/// Providers differ only by configuration, so there is one type rather than one
/// per vendor. What a vendor "is" reduces to its issuer, its JWKS location, and
/// the claims its tokens are expected to carry.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub issuer: String,
    /// CA bundle trusted for the discovery and JWKS fetches.
    #[serde(default)]
    pub server_ca_bundle: Option<PathBuf>,
    /// Client certificate and key presented on those same fetches.
    /// Both or neither; a lone one is refused at startup.
    #[serde(default)]
    pub client_certificate_bundle: Option<PathBuf>,
    #[serde(default)]
    pub client_private_key: Option<PathBuf>,
    /// File holding a bearer token sent on those same fetches, for an issuer
    /// authenticating callers with one. Read per fetch, so a token rotated in
    /// place (a Kubernetes projected service-account token) stays current.
    #[serde(default)]
    pub bearer_token_file: Option<PathBuf>,
    /// Discovered from the issuer's `.well-known/openid-configuration` when omitted.
    #[serde(default)]
    pub jwks_uri: Option<String>,
    /// Claims a token must carry beyond the ones JWT validation itself checks.
    /// Presence only: predicates over claim *values* belong in the access
    /// policy, which sees the whole claim map.
    #[serde(default)]
    pub required_claims: Vec<String>,
    #[serde(default = "Config::default_jwks_refresh_interval")]
    pub jwks_refresh_interval: u64,
    #[serde(default)]
    pub required_audience: Option<String>,
    #[serde(default = "Config::default_clock_skew_tolerance")]
    pub clock_skew_tolerance: u64,
    #[serde(default = "Config::default_allowed_algorithms")]
    pub allowed_algorithms: Vec<Algorithm>,
    /// Timeout for an OIDC HTTP fetch (JWKS or discovery document).
    #[serde(default = "Config::default_http_request_timeout_secs")]
    pub http_request_timeout_secs: u64,
    /// Timeout for the forced JWKS refetch triggered when a cached JWKS is
    /// missing the token's key id (a rotated signing key).
    #[serde(default = "Config::default_jwks_refresh_timeout_secs")]
    pub jwks_refresh_timeout_secs: u64,
}

impl Config {
    fn default_jwks_refresh_interval() -> u64 {
        3600
    }

    fn default_clock_skew_tolerance() -> u64 {
        60
    }

    fn default_allowed_algorithms() -> Vec<Algorithm> {
        vec![Algorithm::RS256]
    }

    fn default_http_request_timeout_secs() -> u64 {
        30
    }

    fn default_jwks_refresh_timeout_secs() -> u64 {
        5
    }

    /// Rejects a token missing any claim the provider requires. A claim present
    /// but null counts as missing: a null carries no more identity than an
    /// absent key, and a policy reading it would see the same nothing.
    pub(crate) fn verify_required_claims(
        &self,
        claims: &HashMap<String, serde_json::Value>,
    ) -> Result<(), Error> {
        for name in &self.required_claims {
            if claims.get(name).is_none_or(serde_json::Value::is_null) {
                return Err(Error::Unauthorized(format!(
                    "token is missing required claim '{name}'"
                )));
            }
        }
        Ok(())
    }
}

pub struct OidcValidator {
    provider_name: String,
    config: Config,
    client: Arc<Client>,
    cache: Arc<Cache>,
}

impl OidcValidator {
    pub fn new(
        provider_name: String,
        config: &Config,
        client: Arc<Client>,
        cache: Arc<Cache>,
    ) -> Self {
        Self {
            provider_name,
            config: config.clone(),
            client,
            cache,
        }
    }

    pub async fn validate_token(&self, token: &str) -> Result<OidcClaims, Error> {
        validator::validate_oidc_token(
            &self.provider_name,
            &self.config,
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
                    "OIDC token validated for provider '{}' (sub={:?}, iss={:?})",
                    claims.provider_name, subject, issuer
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

/// The `iss` a bearer names, read without verifying anything.
///
/// It only decides which provider validates the token first: the provider it
/// names then verifies signature, issuer, audience and expiry in full, so a
/// forged `iss` buys nothing but a different order of rejections.
pub fn unverified_issuer(token: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct IssuerClaim {
        iss: String,
    }

    insecure_decode::<IssuerClaim>(token)
        .ok()
        .map(|token| token.claims.iss)
}

/// Extracts an OIDC credential string from `parts`:
/// - `Authorization: Bearer <token>` → `Some(token)` (any provider can claim a Bearer header).
/// - `Authorization: Basic <user:pass>` where `user == provider_name` → `Some(password)`
///   (the OIDC token is in the password field; the username gates which provider claims it).
/// - Anything else → `None`.
fn extract_oidc_credential(parts: &Parts, provider_name: &str) -> Option<String> {
    if let Some(token) = bearer_token(&parts.headers) {
        debug!("Found Bearer token for OIDC provider '{provider_name}'");
        return Some(token);
    }
    if let Some((username, password)) = basic_credentials(&parts.headers) {
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
    use std::{collections::HashMap, net::SocketAddr, path::Path, sync::Arc};

    use serde_json::json;
    use wiremock::MockServer;

    use super::*;
    use crate::{
        auth::oidc::validator::tests::{build_test_provider_config, make_token, valid_claims},
        identity::ClientIdentity,
        test_fixtures::{
            mocks::{mount_jwks, static_jwks_response},
            oidc::KID,
            requests::{empty_parts, parts_with_authorization, parts_with_basic_auth},
        },
    };

    fn build_config(issuer: &str) -> Config {
        Config {
            required_audience: None,
            ..build_test_provider_config(issuer)
        }
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
    fn test_config_deserialize_minimal() {
        let toml = r#"
            issuer = "https://auth.example.com"
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://auth.example.com");
        assert!(config.jwks_uri.is_none());
        assert!(config.bearer_token_file.is_none());
        assert!(config.required_claims.is_empty());
        assert_eq!(config.jwks_refresh_interval, 3600);
        assert!(config.required_audience.is_none());
        assert_eq!(config.clock_skew_tolerance, 60);
        assert_eq!(config.allowed_algorithms, vec![Algorithm::RS256]);
        assert_eq!(config.http_request_timeout_secs, 30);
        assert_eq!(config.jwks_refresh_timeout_secs, 5);
    }

    #[test]
    fn test_config_deserialize_full() {
        let toml = r#"
            issuer = "https://auth.example.com"
            jwks_uri = "https://auth.example.com/jwks"
            bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
            required_claims = ["repository", "actor"]
            jwks_refresh_interval = 7200
            required_audience = "my-app"
            clock_skew_tolerance = 120
            allowed_algorithms = ["RS256", "ES256"]
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(
            config.jwks_uri,
            Some("https://auth.example.com/jwks".to_string())
        );
        assert_eq!(
            config.bearer_token_file.as_deref(),
            Some(Path::new(
                "/var/run/secrets/kubernetes.io/serviceaccount/token"
            ))
        );
        assert_eq!(config.required_claims, vec!["repository", "actor"]);
        assert_eq!(config.jwks_refresh_interval, 7200);
        assert_eq!(config.required_audience, Some("my-app".to_string()));
        assert_eq!(config.clock_skew_tolerance, 120);
        assert_eq!(
            config.allowed_algorithms,
            vec![Algorithm::RS256, Algorithm::ES256]
        );
    }

    /// GitHub Actions used to be its own provider type. Its entire definition
    /// is now a config entry, so this pins that the shape still loads without
    /// a dedicated variant.
    #[test]
    fn test_config_deserialize_github_actions_shape() {
        let toml = r#"
            issuer = "https://token.actions.githubusercontent.com"
            jwks_uri = "https://token.actions.githubusercontent.com/.well-known/jwks"
            required_claims = ["repository", "actor"]
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://token.actions.githubusercontent.com");
        assert_eq!(config.required_claims, vec!["repository", "actor"]);
    }

    #[test]
    fn test_oidc_validator_new() {
        let config = build_test_provider_config("https://auth.example.com");

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
        let client = test_http_client();
        let validator =
            OidcValidator::new("test-provider".to_string(), &config, client.clone(), cache);

        assert_eq!(validator.provider_name, "test-provider");
        assert_eq!(validator.config.issuer, "https://auth.example.com");
        assert!(Arc::ptr_eq(&validator.client, &client));
    }

    #[tokio::test]
    async fn test_validate_token_success() {
        let mock_server = MockServer::start().await;

        mount_jwks(&mock_server, static_jwks_response()).await;

        let config = build_config(&mock_server.uri());
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "test-user");
    }

    #[tokio::test]
    async fn test_validate_token_invalid() {
        let config = build_config("https://auth.example.com");

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "user-123");
        assert_eq!(oidc_claims.claims.get("email").unwrap(), "user@example.com");
        assert_eq!(identity.client_ip, Some("192.168.1.1".to_string()));
    }
}
