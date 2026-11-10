pub mod jwk;
pub mod validator;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use http::request::Parts;
use jsonwebtoken::Algorithm;
use jsonwebtoken::dangerous::insecure_decode;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tracing::debug;

use angos_mtls_client::ClientTls;

use crate::auth::authorization::{basic_credentials, bearer_token};
use crate::auth::{AuthResult, Error};
use crate::identity::ClientIdentity;
use angos_cache::Cache;

/// An OIDC provider: the issuer to trust, and how tokens it signed are validated.
/// Vendors differ only by issuer, JWKS location and required claims.
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
    pub provider_name: String,
    pub config: Config,
    client: Client,
    cache: Arc<Cache>,
}

impl OidcValidator {
    pub fn new(provider_name: String, config: &Config, client: Client, cache: Arc<Cache>) -> Self {
        Self {
            provider_name,
            config: config.clone(),
            client,
            cache,
        }
    }

    pub async fn authenticate(
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

/// A bearer token, which any provider claims, or the password of a Basic
/// credential whose username names this provider.
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

    fn validator(name: &str, issuer: &str) -> OidcValidator {
        let config = Config {
            required_audience: None,
            ..build_test_provider_config(issuer)
        };
        let cache = CacheConfig::Memory.to_backend().unwrap();
        OidcValidator::new(name.to_string(), &config, Client::new(), cache)
    }

    fn make_test_token(issuer: &str) -> String {
        let mut claims = valid_claims(issuer, "unused");
        claims.remove("aud");
        claims.insert("sub".to_string(), json!("test-user"));
        make_token(&claims, KID)
    }

    /// The token rides as a bearer, or as the password of a Basic credential
    /// naming the provider.
    #[tokio::test]
    async fn authenticates_a_bearer_or_a_basic_credential_naming_the_provider() {
        let server = MockServer::start().await;
        mount_jwks(&server, static_jwks_response()).await;
        let validator = validator("github", &server.uri());
        let token = make_test_token(&server.uri());

        for parts in [
            parts_with_authorization(&format!("Bearer {token}")),
            parts_with_basic_auth("github", &token),
        ] {
            let mut identity = ClientIdentity::new(None);
            let result = validator.authenticate(&parts, &mut identity).await;
            assert!(
                matches!(result, Ok(AuthResult::Authenticated)),
                "{result:?}"
            );
            let claims = identity.oidc.unwrap();
            assert_eq!(claims.provider_name, "github");
            assert_eq!(claims.claims["sub"], "test-user");
        }
    }

    #[tokio::test]
    async fn no_credential_for_this_provider_is_no_credentials() {
        let validator = validator("github", "https://auth.example.com");

        for parts in [
            empty_parts(),
            parts_with_basic_auth("wrong-provider", "token"),
        ] {
            let mut identity = ClientIdentity::new(None);
            let result = validator.authenticate(&parts, &mut identity).await;
            assert!(
                matches!(result, Ok(AuthResult::NoCredentials)),
                "{result:?}"
            );
            assert!(identity.oidc.is_none());
        }
    }

    #[tokio::test]
    async fn an_invalid_token_is_an_error() {
        let validator = validator("test-provider", "https://auth.example.com");
        let parts = parts_with_authorization("Bearer invalid-token");
        let mut identity = ClientIdentity::new(None);

        assert!(validator.authenticate(&parts, &mut identity).await.is_err());
        assert!(identity.oidc.is_none());
    }
}
