use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use hyper::http::request::Parts;
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, info, instrument, warn};

use crate::{
    auth::{
        AuthMiddleware, AuthResult, BasicAuthValidator, MtlsValidator, OidcValidator, basic_auth,
        oidc, webhook,
    },
    cache::Cache,
    command::server::Error,
    configuration::Configuration,
    http_client::HttpClientBuilder,
    identity::ClientIdentity,
    metrics_provider::metrics_provider,
};

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub identity: HashMap<String, basic_auth::Config>,
    #[serde(default)]
    pub oidc: HashMap<String, oidc::Config>,
    #[serde(default)]
    pub webhook: HashMap<String, webhook::Config>,
}

type OidcValidators = Vec<(String, Arc<dyn AuthMiddleware>)>;

/// Returns the strongest method that succeeded, using first-wins priority: mTLS > OIDC > Basic.
fn select_auth_method(mtls: bool, oidc: bool, basic: bool) -> Option<&'static str> {
    if mtls {
        return Some("mtls");
    }
    if oidc {
        return Some("oidc");
    }
    if basic {
        return Some("basic");
    }
    None
}

struct AuthOutcome {
    method: Option<&'static str>,
    identity: ClientIdentity,
}

/// Coordinates all authentication methods and handles the authentication chain
pub struct Authenticator {
    mtls_validator: MtlsValidator,
    oidc_validators: OidcValidators,
    basic_auth_validator: BasicAuthValidator,
}

impl Authenticator {
    pub fn new(config: &Configuration, cache: &Arc<Cache>) -> Result<Self, Error> {
        let auth_config = &config.auth;
        let oidc_client = Arc::new(
            HttpClientBuilder::new()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(Error::Initialization)?,
        );

        let mtls_validator = MtlsValidator::new();
        let oidc_validators = Self::build_oidc_validators(auth_config, &oidc_client, cache);
        let basic_auth_validator = BasicAuthValidator::new(&auth_config.identity);

        Ok(Self {
            mtls_validator,
            oidc_validators,
            basic_auth_validator,
        })
    }

    fn build_oidc_validators(
        auth_config: &AuthConfig,
        client: &Arc<Client>,
        cache: &Arc<Cache>,
    ) -> OidcValidators {
        let mut validators = Vec::with_capacity(auth_config.oidc.len());

        for (name, oidc_config) in &auth_config.oidc {
            let validator = OidcValidator::new(
                name.clone(),
                oidc_config,
                Arc::clone(client),
                Arc::clone(cache),
            );
            validators.push((name.clone(), Arc::new(validator) as Arc<dyn AuthMiddleware>));
        }

        validators.sort_by(|a, b| a.0.cmp(&b.0));
        validators
    }

    /// Authentication order: mTLS → OIDC → Basic Auth
    #[instrument(skip(self, parts), fields(auth_method = tracing::field::Empty))]
    pub async fn authenticate_request(
        &self,
        parts: &Parts,
        remote_address: Option<SocketAddr>,
    ) -> Result<ClientIdentity, Error> {
        let mut identity = ClientIdentity::new(remote_address);

        let mtls_ok = self.try_mtls_authentication(parts, &mut identity).await;
        let oidc_ok = self.try_oidc_authentication(parts, &mut identity).await?;
        let basic_ok = if oidc_ok {
            false
        } else {
            self.try_basic_authentication(parts, &mut identity).await?
        };

        let outcome = AuthOutcome {
            method: select_auth_method(mtls_ok, oidc_ok, basic_ok),
            identity,
        };
        tracing::Span::current().record("auth_method", outcome.method.unwrap_or("anonymous"));
        Ok(outcome.identity)
    }

    /// Attempts mTLS authentication. Returns `true` if a valid certificate was extracted.
    /// Errors are logged and suppressed: mTLS is non-fatal so other methods can follow.
    async fn try_mtls_authentication(&self, parts: &Parts, identity: &mut ClientIdentity) -> bool {
        match self.mtls_validator.authenticate(parts, identity).await {
            Ok(AuthResult::Authenticated) => {
                debug!("mTLS authentication extracted certificate info");
                if !identity.certificate.common_names.is_empty()
                    || !identity.certificate.organizations.is_empty()
                {
                    metrics_provider()
                        .auth_attempts
                        .with_label_values(&["mtls", "success"])
                        .inc();
                    return true;
                }
            }
            Ok(AuthResult::NoCredentials) => {}
            Err(e) => {
                warn!("mTLS validation error: {e}");
                metrics_provider()
                    .auth_attempts
                    .with_label_values(&["mtls", "failed"])
                    .inc();
            }
        }
        false
    }

    /// Tries each OIDC provider in sorted order, returning `true` on first success.
    /// A failure from one provider does not prevent subsequent providers from being tried.
    /// If no provider succeeds and at least one returned an error, the first error is returned.
    /// First rather than last so that deterministic sort order also makes error reporting deterministic.
    ///
    /// One `AUTH_ATTEMPTS` increment is emitted per request, reflecting the chain's overall
    /// outcome (success or failure). Per-provider failures during the chain are still logged
    /// individually for diagnostic context but are not counted as separate attempts.
    async fn try_oidc_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<bool, Error> {
        let mut first_error: Option<Error> = None;
        for (provider_name, validator) in &self.oidc_validators {
            match validator.authenticate(parts, identity).await {
                Ok(AuthResult::Authenticated) => {
                    debug!("OIDC authentication succeeded with provider: {provider_name}");
                    metrics_provider()
                        .auth_attempts
                        .with_label_values(&["oidc", "success"])
                        .inc();
                    return Ok(true);
                }
                Ok(AuthResult::NoCredentials) => {}
                Err(e) => {
                    info!("OIDC validation failed for provider {provider_name}: {e}");
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        match first_error {
            Some(e) => {
                metrics_provider()
                    .auth_attempts
                    .with_label_values(&["oidc", "failed"])
                    .inc();
                Err(e)
            }
            None => Ok(false),
        }
    }

    /// Attempts basic auth authentication, returning `true` on success.
    /// Returns `Err` if credentials were presented but invalid.
    async fn try_basic_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<bool, Error> {
        match self
            .basic_auth_validator
            .authenticate(parts, identity)
            .await
        {
            Ok(AuthResult::Authenticated) => {
                debug!("Basic authentication succeeded");
                metrics_provider()
                    .auth_attempts
                    .with_label_values(&["basic", "success"])
                    .inc();
                Ok(true)
            }
            Ok(AuthResult::NoCredentials) => Ok(false),
            Err(e) => {
                warn!("Basic auth validation failed: {e}");
                metrics_provider()
                    .auth_attempts
                    .with_label_values(&["basic", "failed"])
                    .inc();
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use argon2::{
        Algorithm, Argon2, Params, PasswordHasher, Version,
        password_hash::{SaltString, rand_core::OsRng},
    };
    use async_trait::async_trait;

    use super::*;
    use crate::{
        auth::PeerCertificate,
        cache,
        configuration::Configuration,
        identity::OidcClaims,
        metrics_provider,
        test_fixtures::{
            configuration::{load_config, minimal_config},
            mtls::cert_der,
            requests::{empty_parts, parts_with_basic_auth},
        },
    };

    fn create_minimal_config() -> Configuration {
        metrics_provider::init_for_tests();
        minimal_config()
    }

    fn test_http_client() -> Arc<Client> {
        Arc::new(Client::new())
    }

    #[test]
    fn test_auth_config_empty() {
        let config = create_minimal_config();
        assert!(config.auth.identity.is_empty());
        assert!(config.auth.oidc.is_empty());
        assert!(config.auth.webhook.is_empty());
    }

    #[test]
    fn test_auth_config_with_identity() {
        let config = load_config(
            r#"
            [auth.identity.user1]
            username = "user1"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"
        "#,
        );

        assert_eq!(config.auth.identity.len(), 1);
        assert!(config.auth.identity.contains_key("user1"));
    }

    #[test]
    fn test_auth_config_with_oidc() {
        let config = load_config(
            r#"
            [auth.oidc.github]
            provider = "github"
        "#,
        );

        assert_eq!(config.auth.oidc.len(), 1);
        assert!(config.auth.oidc.contains_key("github"));
    }

    #[test]
    fn test_auth_config_with_webhook() {
        let config = load_config(
            r#"
            [auth.webhook.test]
            url = "http://localhost:8080/auth"
            timeout_ms = 5000
        "#,
        );

        assert_eq!(config.auth.webhook.len(), 1);
        assert!(config.auth.webhook.contains_key("test"));
    }

    #[test]
    fn test_authenticator_new_minimal() {
        let config = create_minimal_config();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authenticator = Authenticator::new(&config, &cache);

        assert!(authenticator.is_ok());
    }

    #[test]
    fn test_authenticator_new_with_basic_auth() {
        let config = load_config(
            r#"
            [auth.identity.testuser]
            username = "testuser"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"
        "#,
        );

        let cache = cache::Config::Memory.to_backend().unwrap();

        let authenticator = Authenticator::new(&config, &cache);

        assert!(authenticator.is_ok());
    }

    #[test]
    fn test_build_oidc_validators_empty() {
        let auth_config = AuthConfig::default();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators =
            Authenticator::build_oidc_validators(&auth_config, &test_http_client(), &cache);

        assert!(validators.is_empty());
    }

    #[test]
    fn test_build_oidc_validators_with_github() {
        let config = load_config(
            r#"
            [auth.oidc.github]
            provider = "github"
        "#,
        );

        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators =
            Authenticator::build_oidc_validators(&config.auth, &test_http_client(), &cache);

        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].0, "github");
    }

    #[test]
    fn test_build_oidc_validators_with_generic() {
        let config = load_config(
            r#"
            [auth.oidc.custom]
            provider = "generic"
            issuer = "https://auth.example.com"
        "#,
        );

        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators =
            Authenticator::build_oidc_validators(&config.auth, &test_http_client(), &cache);

        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].0, "custom");
    }

    #[tokio::test]
    async fn test_authenticate_request_no_credentials() {
        let config = create_minimal_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let parts = empty_parts();

        let result = authenticator.authenticate_request(&parts, None).await;

        assert!(result.is_ok());
        let identity = result.unwrap();
        assert!(identity.username.is_none());
        assert!(identity.oidc.is_none());
        assert!(identity.certificate.common_names.is_empty());
    }

    #[tokio::test]
    async fn test_authenticate_request_with_basic_auth() {
        let salt = SaltString::generate(OsRng);
        let config = Params::default();
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, config);
        let password_hash = argon.hash_password(b"testpass", &salt).unwrap().to_string();

        let config = load_config(&format!(
            r#"
            [auth.identity.testuser]
            username = "testuser"
            password = "{password_hash}"
        "#,
        ));

        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let parts = parts_with_basic_auth("testuser", "testpass");

        let result = authenticator.authenticate_request(&parts, None).await;

        assert!(result.is_ok());
        let identity = result.unwrap();
        assert_eq!(identity.username, Some("testuser".to_string()));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_request_with_invalid_basic_auth() {
        metrics_provider::init_for_tests();
        let salt = SaltString::generate(OsRng);
        let config = Params::default();
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, config);
        let password_hash = argon.hash_password(b"testpass", &salt).unwrap().to_string();

        let config = load_config(&format!(
            r#"
            [auth.identity.testuser]
            username = "testuser"
            password = "{password_hash}"
        "#,
        ));

        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let parts = parts_with_basic_auth("testuser", "wrongpass");

        let result = authenticator.authenticate_request(&parts, None).await;

        assert!(matches!(result, Err(Error::Unauthorized(_))));
    }

    #[test]
    fn select_auth_method_returns_mtls_when_only_mtls_succeeds() {
        assert_eq!(select_auth_method(true, false, false), Some("mtls"));
    }

    #[test]
    fn select_auth_method_keeps_mtls_when_basic_also_succeeds() {
        // Bug: basic-auth success used to overwrite mtls. Must not.
        assert_eq!(select_auth_method(true, false, true), Some("mtls"));
    }

    #[test]
    fn select_auth_method_keeps_mtls_when_oidc_also_succeeds() {
        assert_eq!(select_auth_method(true, true, false), Some("mtls"));
    }

    #[test]
    fn select_auth_method_returns_oidc_when_no_mtls() {
        assert_eq!(select_auth_method(false, true, false), Some("oidc"));
    }

    #[test]
    fn select_auth_method_returns_basic_when_no_mtls_no_oidc() {
        assert_eq!(select_auth_method(false, false, true), Some("basic"));
    }

    #[test]
    fn select_auth_method_returns_none_when_nothing_succeeded() {
        assert_eq!(select_auth_method(false, false, false), None);
    }

    #[tokio::test]
    async fn test_authenticate_request_preserves_client_ip() {
        let config = create_minimal_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let parts = empty_parts();
        let socket_addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();

        let result = authenticator
            .authenticate_request(&parts, Some(socket_addr))
            .await;

        assert!(result.is_ok());
        let identity = result.unwrap();
        assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
    }

    #[test]
    fn test_build_oidc_validators_multiple_are_sorted_by_name() {
        // "custom" < "github" alphabetically; HashMap iteration order is non-deterministic,
        // so this test would be flaky without the explicit sort added in build_oidc_validators.
        let config = load_config(
            r#"
            [auth.oidc.github]
            provider = "github"

            [auth.oidc.custom]
            provider = "generic"
            issuer = "https://auth.example.com"
        "#,
        );

        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators =
            Authenticator::build_oidc_validators(&config.auth, &test_http_client(), &cache);

        assert_eq!(validators.len(), 2);
        assert_eq!(validators[0].0, "custom");
        assert_eq!(validators[1].0, "github");
    }

    // ---------------------------------------------------------------------------
    // Mock AuthMiddleware for unit-testing try_oidc_authentication in isolation.
    // ---------------------------------------------------------------------------

    #[derive(Clone)]
    enum MockOutcome {
        Authenticated,
        NoCredentials,
        Fail(String),
    }

    struct MockValidator {
        outcome: MockOutcome,
    }

    #[async_trait]
    impl AuthMiddleware for MockValidator {
        async fn authenticate(
            &self,
            _parts: &Parts,
            identity: &mut ClientIdentity,
        ) -> Result<AuthResult, Error> {
            match &self.outcome {
                MockOutcome::Authenticated => {
                    identity.oidc = Some(OidcClaims {
                        provider_name: "mock".to_string(),
                        provider_type: "Mock".to_string(),
                        claims: HashMap::new(),
                    });
                    Ok(AuthResult::Authenticated)
                }
                MockOutcome::NoCredentials => Ok(AuthResult::NoCredentials),
                MockOutcome::Fail(msg) => Err(Error::Unauthorized(msg.clone())),
            }
        }
    }

    fn make_authenticator_with_mocks(
        validators: Vec<(&'static str, MockOutcome)>,
    ) -> Authenticator {
        let oidc_validators: OidcValidators = validators
            .into_iter()
            .map(|(name, outcome)| {
                let v: Arc<dyn AuthMiddleware> = Arc::new(MockValidator { outcome });
                (name.to_string(), v)
            })
            .collect();

        Authenticator {
            mtls_validator: MtlsValidator::new(),
            oidc_validators,
            basic_auth_validator: BasicAuthValidator::new(&HashMap::new()),
        }
    }

    #[tokio::test]
    async fn test_try_oidc_no_providers_returns_false() {
        let authenticator = make_authenticator_with_mocks(vec![]);

        let parts = empty_parts();
        let mut identity = ClientIdentity::new(None);

        let result = authenticator
            .try_oidc_authentication(&parts, &mut identity)
            .await;

        assert!(!result.unwrap());
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_try_oidc_falls_through_error_to_success() {
        // Provider "alpha" (first in sorted order) returns Err; provider "beta" returns Authenticated.
        // The old code would have returned the error from "alpha" without ever trying "beta".
        // With the fix, "beta" succeeds and the overall result is Ok(true).
        let authenticator = make_authenticator_with_mocks(vec![
            ("alpha", MockOutcome::Fail("alpha auth failed".to_string())),
            ("beta", MockOutcome::Authenticated),
        ]);

        let parts = empty_parts();
        let mut identity = ClientIdentity::new(None);

        let result = authenticator
            .try_oidc_authentication(&parts, &mut identity)
            .await;

        assert!(result.unwrap());
        assert!(identity.oidc.is_some());
    }

    #[tokio::test]
    async fn test_try_oidc_returns_first_error_when_all_fail() {
        // Both providers fail; the error from the alphabetically-first provider is returned.
        let authenticator = make_authenticator_with_mocks(vec![
            ("alpha", MockOutcome::Fail("alpha error".to_string())),
            ("beta", MockOutcome::Fail("beta error".to_string())),
        ]);

        let parts = empty_parts();
        let mut identity = ClientIdentity::new(None);

        let result = authenticator
            .try_oidc_authentication(&parts, &mut identity)
            .await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, Error::Unauthorized(msg) if msg == "alpha error"),
            "expected alpha's error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_try_oidc_all_no_credentials_returns_false() {
        let authenticator = make_authenticator_with_mocks(vec![
            ("alpha", MockOutcome::NoCredentials),
            ("beta", MockOutcome::NoCredentials),
        ]);

        let parts = empty_parts();
        let mut identity = ClientIdentity::new(None);

        let result = authenticator
            .try_oidc_authentication(&parts, &mut identity)
            .await;

        assert!(!result.unwrap());
        assert!(identity.oidc.is_none());
    }

    // ---------------------------------------------------------------------------
    // Helpers shared by method-tracking integration tests below.
    // ---------------------------------------------------------------------------

    fn make_authenticator_with_cert_and_mocks(
        validators: Vec<(&'static str, MockOutcome)>,
    ) -> (Authenticator, PeerCertificate) {
        let peer_cert = PeerCertificate(Arc::new(cert_der()));
        let authenticator = make_authenticator_with_mocks(validators);
        (authenticator, peer_cert)
    }

    // ---------------------------------------------------------------------------
    // Method-tracking integration tests.
    // ---------------------------------------------------------------------------

    /// mTLS succeeds + all OIDC providers return `NoCredentials`.
    /// The identity must carry certificate info and no OIDC claims.
    /// `select_auth_method(true, false, false)` → "mtls"; certificate not downgraded.
    #[tokio::test]
    async fn method_tracking_mtls_success_oidc_no_credentials_preserves_cert() {
        metrics_provider::init_for_tests();
        let (authenticator, peer_cert) = make_authenticator_with_cert_and_mocks(vec![
            ("alpha", MockOutcome::NoCredentials),
            ("beta", MockOutcome::NoCredentials),
        ]);

        let mut parts = empty_parts();
        parts.extensions.insert(peer_cert);

        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        // mTLS succeeded: cert info present.
        assert!(
            !identity.certificate.common_names.is_empty()
                || !identity.certificate.organizations.is_empty(),
            "certificate info must be populated when mTLS succeeds"
        );
        // OIDC must not have been set (NoCredentials from all providers).
        assert!(
            identity.oidc.is_none(),
            "oidc claims must not be set when no OIDC provider had credentials"
        );
    }

    /// mTLS has no certificate (`NoCredentials`) + one OIDC provider succeeds.
    /// The identity must carry OIDC claims and no certificate info.
    /// `select_auth_method(false, true, false)` → "oidc".
    #[tokio::test]
    async fn method_tracking_no_mtls_oidc_success_sets_oidc_identity() {
        metrics_provider::init_for_tests();
        let authenticator =
            make_authenticator_with_mocks(vec![("provider", MockOutcome::Authenticated)]);

        let parts = empty_parts();

        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        // OIDC succeeded: claims present.
        assert!(
            identity.oidc.is_some(),
            "oidc claims must be set when an OIDC provider succeeds"
        );
        // mTLS must not have populated certificate info (no cert in request).
        assert!(
            identity.certificate.common_names.is_empty()
                && identity.certificate.organizations.is_empty(),
            "certificate info must be empty when no mTLS cert was presented"
        );
    }

    /// mTLS succeeds + OIDC provider A fails, provider B also fails.
    /// The chain propagates the first OIDC error via `?`, so `authenticate_request`
    /// returns `Err`.  The test verifies the error is the one from the
    /// alphabetically-first provider ("alpha"). The method-label computation
    /// (`select_auth_method`) is never reached in this path, which is correct
    /// behaviour: an explicit OIDC credential rejection overrides mTLS success.
    #[tokio::test]
    async fn method_tracking_mtls_success_oidc_all_fail_returns_oidc_error() {
        metrics_provider::init_for_tests();
        let (authenticator, peer_cert) = make_authenticator_with_cert_and_mocks(vec![
            ("alpha", MockOutcome::Fail("alpha rejected".to_string())),
            ("beta", MockOutcome::Fail("beta rejected".to_string())),
        ]);

        let mut parts = empty_parts();
        parts.extensions.insert(peer_cert);

        let result = authenticator.authenticate_request(&parts, None).await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, Error::Unauthorized(msg) if msg == "alpha rejected"),
            "expected alpha's error to propagate, got: {err:?}"
        );
    }

    /// When OIDC succeeds, basic auth is skipped entirely.
    /// Even if valid basic-auth credentials are present in the request, the
    /// OIDC success short-circuits the basic-auth path (`if oidc_ok { false }`).
    /// The identity carries OIDC claims; username is None (basic never ran).
    #[tokio::test]
    async fn method_tracking_oidc_success_skips_basic_auth() {
        metrics_provider::init_for_tests();

        // Generate a valid Argon2 hash, then embed it in a config string so that
        // `basic_auth::PasswordHash` is constructed through the normal deserialisation path
        // (its inner type is not publicly constructible).
        let salt = SaltString::generate(OsRng);
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default());
        let password_hash = argon.hash_password(b"secret", &salt).unwrap().to_string();

        let config = load_config(&format!(
            r#"
            [auth.identity.admin]
            username = "admin"
            password = "{password_hash}"
        "#,
        ));
        let basic_auth_validator = BasicAuthValidator::new(&config.auth.identity);

        let oidc_validators: OidcValidators = vec![(
            "mock-provider".to_string(),
            Arc::new(MockValidator {
                outcome: MockOutcome::Authenticated,
            }) as Arc<dyn AuthMiddleware>,
        )];

        let authenticator = Authenticator {
            mtls_validator: MtlsValidator::new(),
            oidc_validators,
            basic_auth_validator,
        };

        // Include valid basic-auth credentials in the request.
        let parts = parts_with_basic_auth("admin", "secret");

        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        // OIDC won: claims must be present.
        assert!(
            identity.oidc.is_some(),
            "oidc claims must be set when OIDC provider succeeds"
        );
        // Basic auth must have been skipped: username stays None.
        assert!(
            identity.username.is_none(),
            "basic auth must be skipped when OIDC already succeeded; username must be None"
        );
    }
}
