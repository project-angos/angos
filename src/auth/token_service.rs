//! Registry-issued bearer tokens, the OCI distribution token service.
//!
//! Clients whose credential is short-lived (a GitHub Actions OIDC token lives
//! about ten minutes) exchange it once at `/token` for a registry-signed token
//! that outlives a long push. The token freezes the identity; authorization is
//! still evaluated per request against live configuration.
//!
//! Issuing and validating are separate types built from one configuration: the
//! validator joins the authentication chain, the issuer serves the endpoint.

use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use hyper::{header::HeaderValue, http::request::Parts};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    auth::{AuthMiddleware, AuthResult, Error, authorization::bearer_token},
    configuration::Base64String,
    identity::{ClientIdentity, OidcClaims},
    secret::Secret,
};

/// Shortest HMAC key accepted, in decoded bytes, matching the HS256 hash output
/// RFC 7518 requires. Every issued token is a public value handed to a CI job,
/// so a short key is brute-forceable offline.
const MIN_SECRET_LEN: usize = 32;

/// Longest token lifetime accepted. An issued token cannot be revoked before it
/// expires, so its lifetime is the window a stolen one stays usable.
const MAX_TTL_SECS: u64 = 86400;

/// The path the registry serves the token service on. A configured realm must
/// end with it, or clients follow the challenge to a 404.
const TOKEN_PATH: &str = "/token";

/// Public so the authenticator can refuse an OIDC provider that allows the same
/// algorithm, which would make the validator claim that provider's bearers.
pub const ALGORITHM: Algorithm = Algorithm::HS256;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub secret_key: Secret<Base64String>,
    /// Absolute URL clients fetch tokens from. Derived from the request's `Host`
    /// when unset, which is what a registry reachable under several hostnames
    /// wants.
    #[serde(default)]
    pub realm: Option<String>,
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: u64,
}

fn default_ttl_secs() -> u64 {
    3600
}

impl Config {
    /// Base64-ness is the type's business; this is the length HS256 needs, and
    /// both halves take the key from here so it has one enforcement point.
    fn signing_key(&self) -> Result<&[u8], Error> {
        let key = self.secret_key.expose().as_bytes();
        if key.len() < MIN_SECRET_LEN {
            return Err(Error::Initialization(format!(
                "auth.token_service.secret_key must decode to at least {MIN_SECRET_LEN} bytes"
            )));
        }
        Ok(key)
    }
}

/// The payload of an issued token. `oidc` stays nested rather than flattened:
/// a provider claim named `exp` would otherwise collide with this one.
#[derive(Debug, Deserialize, Serialize)]
struct TokenClaims {
    exp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oidc: Option<OidcClaims>,
}

/// Mints tokens and advertises where to get one.
pub struct TokenIssuer {
    encoding: EncodingKey,
    ttl_secs: u64,
    configured_challenge: Option<HeaderValue>,
}

impl TokenIssuer {
    pub fn new(config: &Config) -> Result<Self, Error> {
        let key = config.signing_key()?;
        if !(1..=MAX_TTL_SECS).contains(&config.ttl_secs) {
            return Err(Error::Initialization(format!(
                "auth.token_service.ttl_secs must be between 1 and {MAX_TTL_SECS}"
            )));
        }

        Ok(Self {
            encoding: EncodingKey::from_secret(key),
            ttl_secs: config.ttl_secs,
            configured_challenge: config.realm.as_deref().map(build_challenge).transpose()?,
        })
    }

    /// Signs `identity` into a token, returning it with its lifetime in seconds.
    ///
    /// The certificate and client IP are left out on purpose: both are re-derived
    /// from the live request, so a certificate-bound identity never becomes a
    /// replayable bearer credential.
    pub fn issue(&self, identity: &ClientIdentity) -> Result<(String, u64), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Execution(format!("System clock is before the Unix epoch: {e}")))?
            .as_secs();

        let claims = TokenClaims {
            exp: now.saturating_add(self.ttl_secs),
            id: identity.id.clone(),
            username: identity.username.clone(),
            oidc: identity.oidc.clone(),
        };

        let token = encode(&Header::new(ALGORITHM), &claims, &self.encoding)
            .map_err(|e| Error::Execution(format!("Failed to sign token: {e}")))?;
        Ok((token, self.ttl_secs))
    }

    /// The `WWW-Authenticate` value pointing clients at the token endpoint, using
    /// the configured realm when there is one and the request's own host otherwise.
    pub fn challenge(&self, scheme: &str, host: &str) -> Option<HeaderValue> {
        if let Some(challenge) = &self.configured_challenge {
            return Some(challenge.clone());
        }
        build_challenge(&format!("{scheme}://{host}{TOKEN_PATH}")).ok()
    }
}

fn build_challenge(realm: &str) -> Result<HeaderValue, Error> {
    let url = Url::parse(realm).map_err(|e| {
        Error::Initialization(format!("auth.token_service.realm is not a URL: {e}"))
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::Initialization(
            "auth.token_service.realm must be an http or https URL".to_string(),
        ));
    }
    // A suffix rather than the whole path, so a registry behind a proxy that
    // strips a prefix can advertise the prefixed URL its clients must call.
    if !url.path().ends_with(TOKEN_PATH) {
        return Err(Error::Initialization(format!(
            "auth.token_service.realm must have a path ending in {TOKEN_PATH}"
        )));
    }

    // Rebuilt from the parsed parts rather than echoed: a configured realm may
    // carry userinfo, a query or a fragment, none of which belong in a header
    // every client receives.
    let host = url
        .host_str()
        .ok_or_else(|| Error::Initialization("auth.token_service.realm has no host".to_string()))?;
    let service = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let realm = format!("{}://{service}{}", url.scheme(), url.path());

    HeaderValue::from_str(&format!(r#"Bearer realm="{realm}",service="{service}""#))
        .map_err(|e| Error::Initialization(format!("auth.token_service.realm is unusable: {e}")))
}

/// Restores the identity an issued token carries, as one link of the
/// authentication chain.
pub struct TokenValidator {
    decoding: DecodingKey,
    validation: Validation,
    oidc_providers: HashSet<String>,
}

impl TokenValidator {
    /// `oidc_providers` are the currently configured provider names. A token
    /// naming one that has since been removed or renamed is refused, which is
    /// the only way an operator can invalidate tokens before they expire.
    pub fn new(config: &Config, oidc_providers: &[String]) -> Result<Self, Error> {
        let mut validation = Validation::new(ALGORITHM);
        validation.validate_aud = false;

        Ok(Self {
            decoding: DecodingKey::from_secret(config.signing_key()?),
            validation,
            oidc_providers: oidc_providers.iter().cloned().collect(),
        })
    }
}

#[async_trait]
impl AuthMiddleware for TokenValidator {
    async fn authenticate(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<AuthResult, Error> {
        let Some(token) = bearer_token(&parts.headers) else {
            return Ok(AuthResult::NoCredentials);
        };
        // Only an HS256 token can be ours. Any other bearer belongs to an OIDC
        // provider and must reach those middlewares untouched.
        if !matches!(decode_header(&token), Ok(header) if header.alg == ALGORITHM) {
            return Ok(AuthResult::NoCredentials);
        }

        let claims = decode::<TokenClaims>(&token, &self.decoding, &self.validation)
            .map_err(|e| Error::Unauthorized(format!("Registry token rejected: {e}")))?
            .claims;

        if let Some(oidc) = &claims.oidc
            && !self.oidc_providers.contains(&oidc.provider_name)
        {
            return Err(Error::Unauthorized(format!(
                "Registry token names OIDC provider '{}', which is no longer configured",
                oidc.provider_name
            )));
        }

        identity.id = claims.id;
        identity.username = claims.username;
        identity.oidc = claims.oidc;
        identity.from_registry_token = true;
        Ok(AuthResult::Authenticated)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::{
        auth::oidc::validator::tests::make_token,
        identity::{ClientCertificate, OidcClaims},
        test_fixtures::{oidc::KID, requests::parts_with_authorization},
    };

    const SECRET: [u8; 32] = [7; 32];
    const PROVIDER: &str = "github-actions";

    fn config() -> Config {
        Config {
            secret_key: Secret::new(SECRET.to_vec().into()),
            realm: None,
            ttl_secs: default_ttl_secs(),
        }
    }

    fn issuer() -> TokenIssuer {
        TokenIssuer::new(&config()).expect("valid issuer config")
    }

    fn validator() -> TokenValidator {
        TokenValidator::new(&config(), &[PROVIDER.to_string()]).expect("valid validator config")
    }

    /// Every JSON shape a provider can put in a claim, so the round trip is
    /// pinned on type fidelity and not just on the keys surviving.
    fn oidc_claims() -> OidcClaims {
        let mut claims = HashMap::new();
        claims.insert("repository".to_string(), json!("myorg/myapp"));
        claims.insert("run_number".to_string(), json!(4_812_u64));
        claims.insert("offset".to_string(), json!(-17_i64));
        claims.insert("large".to_string(), json!(i64::MAX as u64 - 1));
        claims.insert("ratio".to_string(), json!(0.25_f64));
        claims.insert("email_verified".to_string(), json!(true));
        claims.insert("missing".to_string(), json!(null));
        claims.insert("groups".to_string(), json!(["admins", "devs"]));
        claims.insert("nested".to_string(), json!({"a": {"b": [1, 2]}}));

        OidcClaims {
            provider_name: PROVIDER.to_string(),
            provider_type: "GitHub Actions".to_string(),
            claims,
        }
    }

    fn oidc_identity() -> ClientIdentity {
        ClientIdentity {
            id: Some("ci".to_string()),
            username: Some("ci-bot".to_string()),
            oidc: Some(oidc_claims()),
            ..Default::default()
        }
    }

    async fn reissue(identity: &ClientIdentity) -> ClientIdentity {
        let (token, _) = issuer().issue(identity).expect("issuing must succeed");
        let parts = parts_with_authorization(&format!("Bearer {token}"));
        let mut restored = ClientIdentity::default();
        let result = validator()
            .authenticate(&parts, &mut restored)
            .await
            .expect("a freshly issued token must validate");
        assert!(matches!(result, AuthResult::Authenticated));
        restored
    }

    /// The CEL policy context is built by serializing `ClientIdentity`, so this
    /// equality is the policy contract: a reissued identity must decide every
    /// rule exactly as the original did.
    #[tokio::test]
    async fn identity_survives_a_token_round_trip_unchanged() {
        let original = oidc_identity();
        let restored = reissue(&original).await;

        assert_eq!(
            serde_json::to_value(&restored).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
    }

    #[tokio::test]
    async fn certificate_and_client_ip_never_ride_in_the_token() {
        let mut issued_from = oidc_identity();
        issued_from.certificate = ClientCertificate {
            organizations: vec!["IssuerOrg".to_string()],
            common_names: vec!["issuer-cn".to_string()],
        };
        issued_from.client_ip = Some("10.0.0.1".to_string());

        let (token, _) = issuer().issue(&issued_from).expect("issuing must succeed");
        let parts = parts_with_authorization(&format!("Bearer {token}"));

        // What the live request established must survive validation untouched.
        let mut identity = ClientIdentity {
            certificate: ClientCertificate {
                organizations: vec!["LiveOrg".to_string()],
                common_names: vec!["live-cn".to_string()],
            },
            client_ip: Some("192.0.2.7".to_string()),
            ..Default::default()
        };
        validator()
            .authenticate(&parts, &mut identity)
            .await
            .unwrap();

        assert_eq!(identity.certificate.organizations, vec!["LiveOrg"]);
        assert_eq!(identity.certificate.common_names, vec!["live-cn"]);
        assert_eq!(identity.client_ip.as_deref(), Some("192.0.2.7"));
    }

    #[tokio::test]
    async fn anonymous_identity_reissues_as_anonymous() {
        let restored = reissue(&ClientIdentity::default()).await;

        assert!(restored.id.is_none());
        assert!(restored.username.is_none());
        assert!(restored.oidc.is_none());
    }

    #[tokio::test]
    async fn rejects_an_expired_token() {
        // Validation::new leaves a 60s leeway, so the token must be older than that.
        let claims = TokenClaims {
            exp: 1_000_000,
            id: None,
            username: Some("ci-bot".to_string()),
            oidc: None,
        };
        let token = encode(&Header::new(ALGORITHM), &claims, &issuer().encoding).unwrap();
        let parts = parts_with_authorization(&format!("Bearer {token}"));

        let error = validator()
            .authenticate(&parts, &mut ClientIdentity::default())
            .await
            .expect_err("an expired token must be refused");
        assert!(matches!(error, Error::Unauthorized(_)));
    }

    #[tokio::test]
    async fn rejects_a_token_signed_with_another_secret() {
        let other = Config {
            secret_key: Secret::new(vec![9; 32].into()),
            ..config()
        };
        let (token, _) = TokenIssuer::new(&other)
            .unwrap()
            .issue(&oidc_identity())
            .unwrap();
        let parts = parts_with_authorization(&format!("Bearer {token}"));

        let error = validator()
            .authenticate(&parts, &mut ClientIdentity::default())
            .await
            .expect_err("a token from another key must be refused");
        assert!(matches!(error, Error::Unauthorized(_)));
    }

    #[tokio::test]
    async fn rejects_a_token_naming_a_removed_provider() {
        let (token, _) = issuer().issue(&oidc_identity()).unwrap();
        let parts = parts_with_authorization(&format!("Bearer {token}"));

        let validator = TokenValidator::new(&config(), &[]).unwrap();
        let error = validator
            .authenticate(&parts, &mut ClientIdentity::default())
            .await
            .expect_err("a token naming an unconfigured provider must be refused");
        assert!(matches!(error, Error::Unauthorized(_)));
    }

    /// A provider's own bearer must fall through rather than error, or the OIDC
    /// middlewares never get to see it.
    #[tokio::test]
    async fn a_non_hs256_bearer_yields_no_credentials() {
        let mut claims = HashMap::new();
        claims.insert("sub".to_string(), json!("someone"));
        let token = make_token(&claims, KID);
        let parts = parts_with_authorization(&format!("Bearer {token}"));

        let result = validator()
            .authenticate(&parts, &mut ClientIdentity::default())
            .await
            .expect("another scheme's bearer must not fail the request");
        assert!(matches!(result, AuthResult::NoCredentials));
    }

    #[tokio::test]
    async fn an_unsigned_token_yields_no_credentials() {
        let parts = parts_with_authorization(
            "Bearer eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJleHAiOjk5OTk5OTk5OTl9.",
        );

        let result = validator()
            .authenticate(&parts, &mut ClientIdentity::default())
            .await
            .expect("an alg=none token must not authenticate");
        assert!(matches!(result, AuthResult::NoCredentials));
    }

    /// Both halves take the key from the same place, so neither can be built
    /// with a weaker one than the other.
    #[test]
    fn neither_half_accepts_a_key_under_32_bytes() {
        let config = Config {
            secret_key: Secret::new(vec![7; 31].into()),
            ..config()
        };

        assert!(TokenIssuer::new(&config).is_err());
        assert!(TokenValidator::new(&config, &[]).is_err());
    }

    #[test]
    fn rejects_a_ttl_outside_the_allowed_range() {
        for ttl_secs in [0, MAX_TTL_SECS + 1] {
            let config = Config {
                ttl_secs,
                ..config()
            };
            assert!(TokenIssuer::new(&config).is_err());
        }
    }

    #[test]
    fn rejects_a_realm_that_is_not_an_absolute_token_url() {
        for realm in [
            "/token",
            "registry.example.com/token",
            "ftp://registry.example.com/token",
            "https://registry.example.com/tokens",
            "https://registry.example.com/token/exchange",
        ] {
            let config = Config {
                realm: Some(realm.to_string()),
                ..config()
            };
            assert!(
                TokenIssuer::new(&config).is_err(),
                "{realm} must be refused"
            );
        }
    }

    #[test]
    fn the_configured_realm_wins_over_the_request_host() {
        let config = Config {
            realm: Some("https://registry.example.com/token".to_string()),
            ..config()
        };
        let issuer = TokenIssuer::new(&config).unwrap();

        assert_eq!(
            issuer.challenge("http", "other.example.com").unwrap(),
            r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#
        );
    }

    #[test]
    fn derives_the_challenge_from_the_request_host_when_no_realm_is_set() {
        let issuer = issuer();

        assert_eq!(
            issuer.challenge("https", "registry.example.com").unwrap(),
            r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#
        );
        assert!(issuer.challenge("https", "not a host").is_none());
    }

    /// A proxy that strips a path prefix before forwarding needs the prefixed
    /// URL advertised, or clients follow the challenge to a 404.
    #[test]
    fn a_prefixed_realm_is_advertised_as_configured() {
        let config = Config {
            realm: Some("https://registry.example.com/registry/token".to_string()),
            ..config()
        };
        let issuer = TokenIssuer::new(&config).unwrap();

        assert_eq!(
            issuer.challenge("https", "other.example.com").unwrap(),
            r#"Bearer realm="https://registry.example.com/registry/token",service="registry.example.com""#
        );
    }

    /// The challenge reaches every anonymous client, so anything the realm URL
    /// carries beyond scheme, authority and path must be dropped.
    #[test]
    fn the_challenge_drops_realm_userinfo_and_query() {
        let config = Config {
            realm: Some("https://bot:hunter2@registry.example.com/token?x=1#f".to_string()),
            ..config()
        };
        let issuer = TokenIssuer::new(&config).unwrap();

        assert_eq!(
            issuer.challenge("https", "other.example.com").unwrap(),
            r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#
        );
    }

    /// A client sends `service` back verbatim, so dropping the port would name a
    /// service the registry is not reachable at.
    #[test]
    fn the_service_carries_the_realm_port() {
        let issuer = issuer();

        assert_eq!(
            issuer
                .challenge("https", "registry.example.com:8443")
                .unwrap(),
            r#"Bearer realm="https://registry.example.com:8443/token",service="registry.example.com:8443""#
        );
    }
}
