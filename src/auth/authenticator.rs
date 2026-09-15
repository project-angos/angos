use std::{collections::HashMap, fs, net::SocketAddr, sync::Arc};

use http::request::Parts;
use reqwest::Client;
use serde::Deserialize;
use tracing::{Span, debug, info, instrument, warn};

use crate::{
    auth::Error,
    auth::{
        AuthMiddleware, AuthResult, BasicAuthValidator, MtlsValidator, OidcValidator,
        TokenValidator, authorization::bearer_token, basic_auth, oidc, oidc::unverified_issuer,
        token_service, webhook,
    },
    configuration::Configuration,
    identity::{AuthMethod, ClientIdentity},
    metrics_provider::metrics_provider,
};
use angos_cache::Cache;
use angos_mtls_client::MtlsClientBuilder;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub identity: HashMap<String, basic_auth::Config>,
    #[serde(default)]
    pub oidc: HashMap<String, oidc::Config>,
    #[serde(default)]
    pub webhook: HashMap<String, webhook::Config>,
    #[serde(default)]
    pub token_service: Option<token_service::Config>,
}

/// One configured OIDC provider: the validator plus the issuer it pins, which
/// is what a bearer names to be validated here first.
struct OidcProvider {
    name: String,
    issuer: String,
    validator: Arc<dyn AuthMiddleware>,
}

type OidcValidators = Vec<OidcProvider>;

/// Coordinates all authentication methods and handles the authentication chain
pub struct Authenticator {
    mtls_validator: MtlsValidator,
    token_validator: Option<TokenValidator>,
    oidc_validators: OidcValidators,
    basic_auth_validator: BasicAuthValidator,
}

impl Authenticator {
    pub fn new(config: &Configuration, cache: &Arc<Cache>) -> Result<Self, Error> {
        let auth_config = &config.auth;
        reject_provider_name_collision(auth_config)?;

        let mtls_validator = MtlsValidator;
        let oidc_validators = Self::build_oidc_validators(auth_config, cache)?;
        let basic_auth_validator = BasicAuthValidator::new(&auth_config.identity)?;

        let provider_names: Vec<String> = oidc_validators
            .iter()
            .map(|provider| provider.name.clone())
            .collect();
        let identity_ids: Vec<String> = auth_config.identity.keys().cloned().collect();
        let token_validator = auth_config
            .token_service
            .as_ref()
            .map(|config| TokenValidator::new(config, &provider_names, &identity_ids))
            .transpose()?;

        Ok(Self {
            mtls_validator,
            token_validator,
            oidc_validators,
            basic_auth_validator,
        })
    }

    fn build_oidc_validators(
        auth_config: &AuthConfig,
        cache: &Arc<Cache>,
    ) -> Result<OidcValidators, Error> {
        let mut validators = Vec::with_capacity(auth_config.oidc.len());

        for (name, oidc_config) in &auth_config.oidc {
            let validator = OidcValidator::new(
                name.clone(),
                oidc_config,
                build_oidc_client(name, oidc_config)?,
                Arc::clone(cache),
            );
            validators.push(OidcProvider {
                name: name.clone(),
                issuer: oidc_config.issuer.clone(),
                validator: Arc::new(validator),
            });
        }

        validators.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(validators)
    }

    /// Authentication order: mTLS → Registry token → OIDC → Basic Auth
    ///
    /// A registry token short-circuits OIDC and Basic: the OIDC middlewares claim
    /// any bearer header, so letting them run would reject the token they cannot
    /// validate.
    #[instrument(skip(self, parts), fields(auth_method = tracing::field::Empty))]
    pub async fn authenticate_request(
        &self,
        parts: &Parts,
        remote_address: Option<SocketAddr>,
    ) -> Result<ClientIdentity, Error> {
        let mut identity = ClientIdentity::new(remote_address);

        let mtls = self.try_mtls_authentication(parts, &mut identity).await;
        let token = self.try_token_authentication(parts, &mut identity).await?;
        let oidc = if token.is_none() {
            self.try_oidc_authentication(parts, &mut identity).await?
        } else {
            None
        };
        let basic = if token.is_none() && oidc.is_none() {
            self.try_basic_authentication(parts, &mut identity).await?
        } else {
            None
        };

        // First-wins priority, strongest first: a request can satisfy several
        // methods at once and the identity states one answer.
        identity.auth_method = mtls
            .or(token)
            .or(oidc)
            .or(basic)
            .unwrap_or(AuthMethod::Anonymous);
        Span::current().record("auth_method", identity.auth_method.as_str());
        Ok(identity)
    }

    /// Errors are logged and suppressed: mTLS is non-fatal so other methods can follow.
    async fn try_mtls_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Option<AuthMethod> {
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
                    return Some(AuthMethod::Mtls);
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
        None
    }

    /// Returns `Err` when the bearer is one of ours but no longer valid; a bearer
    /// belonging to another scheme is left for the OIDC middlewares.
    async fn try_token_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<Option<AuthMethod>, Error> {
        let Some(token_validator) = &self.token_validator else {
            return Ok(None);
        };

        match token_validator.authenticate(parts, identity).await {
            Ok(AuthResult::Authenticated) => {
                debug!("Registry token authentication succeeded");
                metrics_provider()
                    .auth_attempts
                    .with_label_values(&["token", "success"])
                    .inc();
                Ok(Some(AuthMethod::Token))
            }
            Ok(AuthResult::NoCredentials) => Ok(None),
            Err(e) => {
                info!("Registry token validation failed: {e}");
                metrics_provider()
                    .auth_attempts
                    .with_label_values(&["token", "failed"])
                    .inc();
                Err(e)
            }
        }
    }

    /// The provider whose configured issuer the bearer names is tried first,
    /// the rest in sorted order after it: every other provider would otherwise
    /// pay a JWKS lookup, and on a `kid` miss a forced refresh, for a token it
    /// is about to reject on the issuer alone. A failure from one does not stop
    /// the next from being tried, so a token no provider accepts is still
    /// refused by all of them.
    /// If no provider succeeds and at least one returned an error, the first error is returned.
    /// First rather than last so that a deterministic order also makes error reporting deterministic.
    ///
    /// One `AUTH_ATTEMPTS` increment is emitted per request, reflecting the chain's overall
    /// outcome (success or failure). Per-provider failures during the chain are still logged
    /// individually for diagnostic context but are not counted as separate attempts.
    async fn try_oidc_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<Option<AuthMethod>, Error> {
        let mut first_error: Option<Error> = None;
        let named = bearer_token(&parts.headers).and_then(|token| unverified_issuer(&token));
        let names_issuer = |provider: &&OidcProvider| named.as_deref() == Some(&provider.issuer);
        let ordered = self
            .oidc_validators
            .iter()
            .filter(names_issuer)
            .chain(self.oidc_validators.iter().filter(|p| !names_issuer(p)));
        for OidcProvider {
            name: provider_name,
            validator,
            ..
        } in ordered
        {
            match validator.authenticate(parts, identity).await {
                Ok(AuthResult::Authenticated) => {
                    debug!("OIDC authentication succeeded with provider: {provider_name}");
                    metrics_provider()
                        .auth_attempts
                        .with_label_values(&["oidc", "success"])
                        .inc();
                    return Ok(Some(AuthMethod::Oidc));
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
            None => Ok(None),
        }
    }

    /// Returns `Err` if credentials were presented but invalid.
    async fn try_basic_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<Option<AuthMethod>, Error> {
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
                Ok(Some(AuthMethod::Basic))
            }
            Ok(AuthResult::NoCredentials) => Ok(None),
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

/// One client per provider: its TLS material is baked in when it is built, so a
/// provider trusting its own issuer, or authenticating to it, cannot share one
/// with the others.
fn build_oidc_client(name: &str, config: &oidc::Config) -> Result<Arc<Client>, Error> {
    let initialization_error = |e: String| {
        Error::Initialization(format!(
            "Failed to create HTTP client for auth.oidc.{name}: {e}"
        ))
    };

    // A lone certificate or key would otherwise fetch anonymously and fail as an
    // unauthorized issuer at runtime.
    if config.client_certificate_bundle.is_some() != config.client_private_key.is_some() {
        return Err(initialization_error(
            "both client_certificate_bundle and client_private_key are required for mTLS"
                .to_string(),
        ));
    }

    // The token itself is read per fetch since it rotates; this only refuses an
    // unreadable path at startup rather than at the first JWKS fetch.
    if let Some(path) = config.bearer_token_file.as_deref()
        && let Err(e) = fs::read(path)
    {
        return Err(initialization_error(format!(
            "Failed to read bearer token file {}: {e}",
            path.display()
        )));
    }

    // No client-level timeout: each fetch carries a per-request timeout from the
    // provider config (`http_request_timeout_secs`, `jwks_refresh_timeout_secs`).
    MtlsClientBuilder::new()
        .with_server_ca_bundle(config.server_ca_bundle.as_deref())
        .with_client_certificate((
            config.client_certificate_bundle.as_deref(),
            config.client_private_key.as_deref(),
        ))
        .build()
        .map(Arc::new)
        .map_err(initialization_error)
}

/// A Basic credential whose username names a provider is read as that provider's
/// token, and the validation failure ends the chain before basic auth runs, so
/// the user could never authenticate. Refused at startup rather than at runtime.
fn reject_provider_name_collision(auth_config: &AuthConfig) -> Result<(), Error> {
    let collision = auth_config
        .identity
        .values()
        .find(|identity| auth_config.oidc.contains_key(&identity.username));

    match collision {
        Some(identity) => Err(Error::Initialization(format!(
            "basic-auth username '{}' is also an OIDC provider name",
            identity.username
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use argon2::{
        Algorithm, Argon2, Params, PasswordHasher, Version,
        password_hash::{SaltString, rand_core::OsRng},
    };
    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        auth::{PeerCertificate, TokenIssuer, oidc::validator::tests::make_token},
        configuration::Configuration,
        identity::OidcClaims,
        metrics_provider,
        test_fixtures::{
            configuration::{load_config, minimal_config},
            mtls::cert_der,
            oidc::KID,
            requests::{empty_parts, parts_with_authorization, parts_with_basic_auth},
            webhook::{ca_bundle_pem, client_cert_pem, client_key_pem},
        },
    };
    use angos_secret::Secret;

    fn create_minimal_config() -> Configuration {
        metrics_provider::init_for_tests();
        minimal_config()
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
            issuer = "https://token.actions.githubusercontent.com"
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
        let cache = angos_cache::Config::Memory.to_backend().unwrap();

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

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let authenticator = Authenticator::new(&config, &cache);

        assert!(authenticator.is_ok());
    }

    /// The Basic username field selects the provider, so the two names cannot
    /// both be honoured and the collision is a configuration mistake.
    #[test]
    fn a_basic_username_may_not_name_an_oidc_provider() {
        let config = load_config(
            r#"
            [auth.identity.ci]
            username = "github-actions"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"

            [auth.oidc.github-actions]
            issuer = "https://token.actions.githubusercontent.com"
        "#,
        );

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let Err(error) = Authenticator::new(&config, &cache) else {
            panic!("a colliding name must be refused at startup");
        };

        assert!(
            matches!(&error, Error::Initialization(msg) if msg.contains("github-actions")),
            "got: {error:?}"
        );
    }

    #[test]
    fn test_build_oidc_validators_empty() {
        let auth_config = AuthConfig::default();
        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&auth_config, &cache).unwrap();

        assert!(validators.is_empty());
    }

    #[test]
    fn test_build_oidc_validators_with_github() {
        let config = load_config(
            r#"
            [auth.oidc.github]
            issuer = "https://token.actions.githubusercontent.com"
        "#,
        );

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&config.auth, &cache).unwrap();

        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].name, "github");
    }

    #[test]
    fn test_build_oidc_validators_with_generic() {
        let config = load_config(
            r#"
            [auth.oidc.custom]
            issuer = "https://auth.example.com"
        "#,
        );

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&config.auth, &cache).unwrap();

        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].name, "custom");
    }

    /// An issuer whose certificate the system roots do not cover, such as a
    /// kube-apiserver, is reachable only through its own CA bundle.
    #[test]
    fn a_provider_may_trust_its_own_ca_bundle() {
        let bundle = tempdir().unwrap();
        let bundle_path = bundle.path().join("ca.pem");
        fs::write(&bundle_path, ca_bundle_pem()).unwrap();

        let config = load_config(&format!(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            server_ca_bundle = "{}"
        "#,
            bundle_path.display()
        ));

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        assert_eq!(
            config.auth.oidc["kube"].server_ca_bundle.as_deref(),
            Some(bundle_path.as_path())
        );
        assert!(Authenticator::build_oidc_validators(&config.auth, &cache).is_ok());
    }

    /// An issuer that refuses an anonymous caller, such as a kube-apiserver
    /// serving discovery to authenticated users only, is reached with a client
    /// certificate.
    #[test]
    fn a_provider_may_present_a_client_certificate() {
        let material = tempdir().unwrap();
        let certificate_path = material.path().join("client.pem");
        let key_path = material.path().join("client-key.pem");
        fs::write(&certificate_path, client_cert_pem()).unwrap();
        fs::write(&key_path, client_key_pem()).unwrap();

        let config = load_config(&format!(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            client_certificate_bundle = "{}"
            client_private_key = "{}"
        "#,
            certificate_path.display(),
            key_path.display()
        ));

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        assert!(Authenticator::build_oidc_validators(&config.auth, &cache).is_ok());
    }

    #[test]
    fn a_client_certificate_without_its_key_is_refused_at_startup() {
        let material = tempdir().unwrap();
        let certificate_path = material.path().join("client.pem");
        fs::write(&certificate_path, client_cert_pem()).unwrap();

        let config = load_config(&format!(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            client_certificate_bundle = "{}"
        "#,
            certificate_path.display()
        ));

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let Err(error) = Authenticator::build_oidc_validators(&config.auth, &cache) else {
            panic!("half a client identity must be refused rather than fetch anonymously");
        };
        assert!(
            matches!(&error, Error::Initialization(msg) if msg.contains("auth.oidc.kube")),
            "got: {error:?}"
        );
    }

    /// A bearer is validated by the provider whose issuer it names, and by
    /// that one alone: every other provider would otherwise fetch its own JWKS
    /// for a token it rejects on the issuer. The unreachable provider sorts
    /// first by name, so only the ordering can keep it out of the exchange.
    #[tokio::test]
    async fn a_bearer_only_reaches_the_provider_whose_issuer_it_names() {
        use wiremock::MockServer;

        use crate::test_fixtures::mocks::{mount_jwks, static_jwks_response};

        metrics_provider::init_for_tests();
        let unnamed = MockServer::start().await;
        let named = MockServer::start().await;
        mount_jwks(&named, static_jwks_response()).await;

        let config = load_config(&format!(
            r#"
            [auth.oidc.aaa-unnamed]
            issuer = "{unnamed}"
            jwks_uri = "{unnamed}/.well-known/jwks"
            allowed_algorithms = ["ES256"]

            [auth.oidc.zzz-named]
            issuer = "{named}"
            jwks_uri = "{named}/.well-known/jwks"
            allowed_algorithms = ["ES256"]
        "#,
            unnamed = unnamed.uri(),
            named = named.uri(),
        ));
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator {
            mtls_validator: MtlsValidator,
            token_validator: None,
            oidc_validators: Authenticator::build_oidc_validators(&config.auth, &cache).unwrap(),
            basic_auth_validator: BasicAuthValidator::new(&config.auth.identity).unwrap(),
        };

        let claims = crate::auth::oidc::validator::tests::valid_claims(&named.uri(), "unused");
        let parts = parts_with_authorization(&format!("Bearer {}", make_token(&claims, KID)));
        let mut identity = ClientIdentity::default();

        let outcome = authenticator
            .try_oidc_authentication(&parts, &mut identity)
            .await;

        assert!(
            matches!(outcome, Ok(Some(AuthMethod::Oidc))),
            "the provider the token names must validate it: {outcome:?}"
        );
        assert!(
            unnamed.received_requests().await.unwrap().is_empty(),
            "a provider the token does not name must not be consulted at all"
        );
    }

    /// A Kubernetes projected service-account token reaches the apiserver's
    /// discovery endpoints without the operator signing a client certificate.
    #[test]
    fn a_provider_may_present_a_bearer_token() {
        let material = tempdir().unwrap();
        let token_path = material.path().join("token");
        fs::write(&token_path, "service-account-token").unwrap();

        let config = load_config(&format!(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            bearer_token_file = "{}"
        "#,
            token_path.display()
        ));

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        assert!(Authenticator::build_oidc_validators(&config.auth, &cache).is_ok());
    }

    #[test]
    fn a_bearer_token_file_that_does_not_load_is_refused_at_startup() {
        let config = load_config(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            bearer_token_file = "/nonexistent/token"
        "#,
        );

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let Err(error) = Authenticator::build_oidc_validators(&config.auth, &cache) else {
            panic!("an unreadable bearer token file must be refused");
        };
        assert!(
            matches!(&error, Error::Initialization(msg) if msg.contains("auth.oidc.kube")),
            "got: {error:?}"
        );
    }

    #[test]
    fn a_ca_bundle_that_does_not_load_is_refused_at_startup() {
        let config = load_config(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            server_ca_bundle = "/nonexistent/ca.pem"
        "#,
        );

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let Err(error) = Authenticator::build_oidc_validators(&config.auth, &cache) else {
            panic!("an unreadable CA bundle must be refused");
        };
        assert!(
            matches!(&error, Error::Initialization(msg) if msg.contains("auth.oidc.kube")),
            "got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_authenticate_request_no_credentials() {
        let config = create_minimal_config();
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

        let cache = angos_cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let parts = parts_with_basic_auth("testuser", "wrongpass");

        let result = authenticator.authenticate_request(&parts, None).await;

        assert!(matches!(result, Err(Error::Unauthorized(_))));
    }

    #[tokio::test]
    async fn test_authenticate_request_preserves_client_ip() {
        let config = create_minimal_config();
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
            issuer = "https://token.actions.githubusercontent.com"

            [auth.oidc.custom]
            issuer = "https://auth.example.com"
        "#,
        );

        let cache = angos_cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&config.auth, &cache).unwrap();

        assert_eq!(validators.len(), 2);
        assert_eq!(validators[0].name, "custom");
        assert_eq!(validators[1].name, "github");
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
            .map(|(name, outcome)| OidcProvider {
                name: name.to_string(),
                issuer: format!("https://issuer.test/{name}"),
                validator: Arc::new(MockValidator { outcome }),
            })
            .collect();

        Authenticator {
            mtls_validator: MtlsValidator,
            token_validator: None,
            oidc_validators,
            basic_auth_validator: BasicAuthValidator::new(&HashMap::new()).unwrap(),
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

        assert!(result.unwrap().is_none());
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

        assert_eq!(result.unwrap(), Some(AuthMethod::Oidc));
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

        assert!(result.unwrap().is_none());
        assert!(identity.oidc.is_none());
    }

    // ---------------------------------------------------------------------------
    // Helpers shared by method-tracking integration tests below.
    // ---------------------------------------------------------------------------

    /// Built through `load_config` because `basic_auth::PasswordHash` is not
    /// publicly constructible: its only path is deserialisation.
    fn admin_basic_auth_validator() -> BasicAuthValidator {
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

        BasicAuthValidator::new(&config.auth.identity).unwrap()
    }

    fn make_authenticator_with_cert_and_mocks(
        validators: Vec<(&'static str, MockOutcome)>,
    ) -> (Authenticator, PeerCertificate) {
        let peer_cert = PeerCertificate(Arc::new(cert_der()));
        let authenticator = make_authenticator_with_mocks(validators);
        (authenticator, peer_cert)
    }

    /// The two halves the token service splits into, built from one config so
    /// the issued token is the one the chain's validator accepts.
    fn make_authenticator_with_token_service(
        validators: Vec<(&'static str, MockOutcome)>,
    ) -> (Authenticator, TokenIssuer) {
        let config = token_service::Config {
            secret_key: Secret::new(vec![7; 32].into()),
            realm: None,
            ttl_secs: 3600,
        };
        let authenticator = Authenticator {
            token_validator: Some(
                // The identity these tests issue from carries no `id`, so no
                // `[auth.identity]` entry has to back it.
                TokenValidator::new(&config, &["mock".to_string()], &[]).unwrap(),
            ),
            ..make_authenticator_with_mocks(validators)
        };

        (authenticator, TokenIssuer::new(&config).unwrap())
    }

    // ---------------------------------------------------------------------------
    // Registry token tests.
    // ---------------------------------------------------------------------------

    /// The OIDC middlewares claim any bearer header and fail the request when they
    /// cannot validate it, so without the short-circuit no reissued token works.
    #[tokio::test]
    async fn a_valid_token_skips_oidc_and_basic() {
        metrics_provider::init_for_tests();
        let (authenticator, issuer) = make_authenticator_with_token_service(vec![(
            "mock",
            MockOutcome::Fail("must not be reached".to_string()),
        )]);
        let issued_from = ClientIdentity {
            username: Some("ci-bot".to_string()),
            ..Default::default()
        };
        let (token, _) = issuer.issue(&issued_from).unwrap();

        let parts = parts_with_authorization(&format!("Bearer {token}"));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Token);
        assert_eq!(identity.username.as_deref(), Some("ci-bot"));
    }

    #[tokio::test]
    async fn a_bearer_that_is_not_ours_still_reaches_oidc() {
        metrics_provider::init_for_tests();
        let (authenticator, _) =
            make_authenticator_with_token_service(vec![("mock", MockOutcome::Authenticated)]);

        let token = make_token(&HashMap::new(), KID);
        let parts = parts_with_authorization(&format!("Bearer {token}"));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Oidc);
    }

    #[tokio::test]
    async fn mtls_outranks_a_registry_token() {
        metrics_provider::init_for_tests();
        let (authenticator, issuer) =
            make_authenticator_with_token_service(vec![("mock", MockOutcome::NoCredentials)]);
        let (token, _) = issuer.issue(&ClientIdentity::default()).unwrap();

        let mut parts = parts_with_authorization(&format!("Bearer {token}"));
        parts
            .extensions
            .insert(PeerCertificate(Arc::new(cert_der())));

        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Mtls);
    }

    // ---------------------------------------------------------------------------
    // Method-tracking integration tests.
    // ---------------------------------------------------------------------------

    /// mTLS succeeds + all OIDC providers return `NoCredentials`.
    /// The identity must carry certificate info and no OIDC claims.
    /// The reported method is `Mtls`; certificate not downgraded.
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
        assert_eq!(identity.auth_method, AuthMethod::Mtls);
    }

    /// mTLS and OIDC both succeed. The identity carries both credentials and
    /// reports the stronger one.
    #[tokio::test]
    async fn mtls_outranks_a_successful_oidc_provider() {
        metrics_provider::init_for_tests();
        let (authenticator, peer_cert) =
            make_authenticator_with_cert_and_mocks(vec![("provider", MockOutcome::Authenticated)]);

        let mut parts = empty_parts();
        parts.extensions.insert(peer_cert);

        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert!(identity.oidc.is_some());
        assert_eq!(identity.auth_method, AuthMethod::Mtls);
    }

    #[tokio::test]
    async fn a_request_with_no_credentials_is_anonymous() {
        metrics_provider::init_for_tests();
        let authenticator = make_authenticator_with_mocks(vec![
            ("alpha", MockOutcome::NoCredentials),
            ("beta", MockOutcome::NoCredentials),
        ]);

        let identity = authenticator
            .authenticate_request(&empty_parts(), None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Anonymous);
    }

    /// mTLS has no certificate (`NoCredentials`) + one OIDC provider succeeds.
    /// The identity must carry OIDC claims and no certificate info.
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
        assert_eq!(identity.auth_method, AuthMethod::Oidc);
    }

    /// mTLS succeeds + OIDC provider A fails, provider B also fails.
    /// The chain propagates the first OIDC error via `?`, so `authenticate_request`
    /// returns `Err`.  The test verifies the error is the one from the
    /// alphabetically-first provider ("alpha"). The method label is never
    /// computed in this path, which is correct behaviour: an explicit OIDC
    /// credential rejection overrides mTLS success.
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
    /// OIDC success short-circuits the basic-auth path.
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
        let basic_auth_validator = BasicAuthValidator::new(&config.auth.identity).unwrap();

        let oidc_validators: OidcValidators = vec![OidcProvider {
            name: "mock-provider".to_string(),
            issuer: "https://issuer.test/mock-provider".to_string(),
            validator: Arc::new(MockValidator {
                outcome: MockOutcome::Authenticated,
            }),
        }];

        let authenticator = Authenticator {
            mtls_validator: MtlsValidator,
            token_validator: None,
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
        assert_eq!(identity.auth_method, AuthMethod::Oidc);
    }

    #[tokio::test]
    async fn basic_auth_is_reported_when_nothing_stronger_succeeds() {
        metrics_provider::init_for_tests();

        let authenticator = Authenticator {
            basic_auth_validator: admin_basic_auth_validator(),
            ..make_authenticator_with_mocks(vec![("provider", MockOutcome::NoCredentials)])
        };

        let parts = parts_with_basic_auth("admin", "secret");
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.username.as_deref(), Some("admin"));
        assert_eq!(identity.auth_method, AuthMethod::Basic);
    }

    /// Bug: a basic-auth success used to overwrite mTLS. Must not.
    #[tokio::test]
    async fn mtls_outranks_successful_basic_auth() {
        metrics_provider::init_for_tests();

        let authenticator = Authenticator {
            basic_auth_validator: admin_basic_auth_validator(),
            ..make_authenticator_with_mocks(vec![("provider", MockOutcome::NoCredentials)])
        };

        let mut parts = parts_with_basic_auth("admin", "secret");
        parts
            .extensions
            .insert(PeerCertificate(Arc::new(cert_der())));

        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Mtls);
        assert!(!identity.certificate.organizations.is_empty());
    }
}
