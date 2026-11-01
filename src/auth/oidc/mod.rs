pub mod jwk;
pub mod validator;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use http::request::Parts;
use jsonwebtoken::Algorithm;
use jsonwebtoken::dangerous::insecure_decode;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tracing::debug;

use angos_mtls_client::ClientTls;

use crate::auth::authorization::{basic_credentials, bearer_token};
use crate::auth::{AuthMiddleware, AuthResult, Error};
use crate::identity::ClientIdentity;
use angos_cache::Cache;

/// An OIDC provider: the issuer to trust, and how tokens it signed are validated.
///
/// Providers differ only by configuration, so there is one type rather than one
/// per vendor. What a vendor "is" reduces to its issuer, its JWKS location, and
/// the claims its tokens are expected to carry.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub issuer: String,
    /// CA bundle trusted, and client identity presented, on the discovery and
    /// JWKS fetches.
    #[serde(flatten)]
    pub tls: ClientTls,
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
    pub fn verify_required_claims(&self, claims: &HashMap<String, Value>) -> Result<(), Error> {
        for name in &self.required_claims {
            if claims.get(name).is_none_or(Value::is_null) {
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

        let validated = validator::validate_oidc_token(
            &self.provider_name,
            &self.config,
            &token,
            &self.client,
            self.cache.as_ref(),
        )
        .await;
        match validated {
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
    use std::sync::Arc;

    use serde_json::json;
    use wiremock::MockServer;

    use super::*;
    use crate::auth::oidc::validator::tests::{
        build_test_provider_config, make_token, valid_claims,
    };
    use crate::identity::ClientIdentity;
    use crate::test_fixtures::mocks::{mount_jwks, static_jwks_response};
    use crate::test_fixtures::oidc::KID;
    use crate::test_fixtures::requests::{
        empty_parts, parts_with_authorization, parts_with_basic_auth,
    };
    use angos_cache::Config as CacheConfig;

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
        assert!(
            config.required_claims.is_empty(),
            "{:?}",
            config.required_claims
        );
        assert_eq!(config.jwks_refresh_interval, 3600);
        assert!(config.required_audience.is_none());
        assert_eq!(config.clock_skew_tolerance, 60);
        assert_eq!(config.allowed_algorithms, vec![Algorithm::RS256]);
        assert_eq!(config.http_request_timeout_secs, 30);
        assert_eq!(config.jwks_refresh_timeout_secs, 5);
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

    #[tokio::test]
    async fn test_authenticate_with_bearer_token() {
        let mock_server = MockServer::start().await;

        mount_jwks(&mock_server, static_jwks_response()).await;

        let config = build_config(&mock_server.uri());
        let cache = CacheConfig::Memory.to_backend().unwrap();
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
        let cache = CacheConfig::Memory.to_backend().unwrap();
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

        let cache = CacheConfig::Memory.to_backend().unwrap();
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

        let cache = CacheConfig::Memory.to_backend().unwrap();
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

        let cache = CacheConfig::Memory.to_backend().unwrap();
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
}
