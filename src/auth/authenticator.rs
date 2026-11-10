use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use http::request::Parts;
use reqwest::Client;
use serde::Deserialize;
use tracing::{Span, debug, info, instrument, warn};

use crate::auth::authorization::bearer_token;
use crate::auth::oidc::unverified_issuer;
use crate::auth::{
    AuthResult, BasicAuthValidator, Error, OidcValidator, TokenValidator, basic_auth, mtls, oidc,
    token_service, webhook,
};
use crate::configuration::Configuration;
use crate::identity::{AuthMethod, ClientIdentity};
use crate::metrics_provider::metrics_provider;
use angos_cache::Cache;
use angos_mtls_client::build_client;

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

/// Runs the authentication chain over a request.
pub struct Authenticator {
    token_validator: Option<TokenValidator>,
    /// Sorted by provider name.
    oidc_validators: Vec<OidcValidator>,
    basic_auth_validator: BasicAuthValidator,
}

fn record_attempt(method: &str, outcome: &str) {
    metrics_provider()
        .auth_attempts
        .with_label_values(&[method, outcome])
        .inc();
}

impl Authenticator {
    pub fn new(config: &Configuration, cache: &Arc<Cache>) -> Result<Self, Error> {
        let auth_config = &config.auth;
        reject_provider_name_collision(auth_config)?;

        Ok(Self {
            token_validator: auth_config
                .token_service
                .as_ref()
                .map(|config| TokenValidator::new(config, auth_config))
                .transpose()?,
            oidc_validators: Self::build_oidc_validators(auth_config, cache)?,
            basic_auth_validator: BasicAuthValidator::new(&auth_config.identity)?,
        })
    }

    fn build_oidc_validators(
        auth_config: &AuthConfig,
        cache: &Arc<Cache>,
    ) -> Result<Vec<OidcValidator>, Error> {
        let mut validators = Vec::with_capacity(auth_config.oidc.len());
        for (name, oidc_config) in &auth_config.oidc {
            validators.push(OidcValidator::new(
                name.clone(),
                oidc_config,
                build_oidc_client(name, oidc_config)?,
                Arc::clone(cache),
            ));
        }
        validators.sort_by(|a, b| a.provider_name.cmp(&b.provider_name));
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

        let mtls = Self::try_mtls_authentication(parts, &mut identity);
        let token = self.try_token_authentication(parts, &mut identity)?;
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

    /// A certificate without organization or common name fills nothing a policy
    /// could match, so it does not count as mTLS.
    fn try_mtls_authentication(parts: &Parts, identity: &mut ClientIdentity) -> Option<AuthMethod> {
        let certificate = mtls::authenticate(parts)?;
        let named = !certificate.common_names.is_empty() || !certificate.organizations.is_empty();
        identity.certificate = certificate;
        named.then(|| {
            debug!("mTLS authentication extracted certificate info");
            record_attempt("mtls", "success");
            AuthMethod::Mtls
        })
    }

    /// Returns `Err` when the bearer is one of ours but no longer valid; a bearer
    /// belonging to another scheme is left for the OIDC middlewares.
    fn try_token_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<Option<AuthMethod>, Error> {
        let Some(token_validator) = &self.token_validator else {
            return Ok(None);
        };

        match token_validator.authenticate(parts, identity) {
            Ok(AuthResult::Authenticated) => {
                debug!("Registry token authentication succeeded");
                record_attempt("token", "success");
                Ok(Some(AuthMethod::Token))
            }
            Ok(AuthResult::NoCredentials) => Ok(None),
            Err(e) => {
                info!("Registry token validation failed: {e}");
                record_attempt("token", "failed");
                Err(e)
            }
        }
    }

    /// The provider whose issuer the bearer names is tried first, the rest in
    /// sorted order, so the others skip a JWKS lookup (and, on a `kid` miss, a
    /// forced refresh) for a token they would reject on the issuer alone. A
    /// failure does not stop the chain; when none succeeds the first error is
    /// returned, keeping error reporting deterministic. `AUTH_ATTEMPTS` counts
    /// the chain's outcome once per request.
    async fn try_oidc_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<Option<AuthMethod>, Error> {
        let mut first_error: Option<Error> = None;
        let named = bearer_token(&parts.headers).and_then(|token| unverified_issuer(&token));
        let names_issuer =
            |validator: &&OidcValidator| named.as_deref() == Some(&validator.config.issuer);
        let ordered = self
            .oidc_validators
            .iter()
            .filter(names_issuer)
            .chain(self.oidc_validators.iter().filter(|v| !names_issuer(v)));
        for validator in ordered {
            let provider_name = &validator.provider_name;
            match validator.authenticate(parts, identity).await {
                Ok(AuthResult::Authenticated) => {
                    debug!("OIDC authentication succeeded with provider: {provider_name}");
                    record_attempt("oidc", "success");
                    return Ok(Some(AuthMethod::Oidc));
                }
                Ok(AuthResult::NoCredentials) => {}
                Err(e) => {
                    info!("OIDC validation failed for provider {provider_name}: {e}");
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => {
                record_attempt("oidc", "failed");
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
                record_attempt("basic", "success");
                Ok(Some(AuthMethod::Basic))
            }
            Ok(AuthResult::NoCredentials) => Ok(None),
            Err(e) => {
                warn!("Basic auth validation failed: {e}");
                record_attempt("basic", "failed");
                Err(e)
            }
        }
    }
}

/// One client per provider: its TLS material is baked in when it is built, so a
/// provider trusting its own issuer, or authenticating to it, cannot share one
/// with the others.
fn build_oidc_client(name: &str, config: &oidc::Config) -> Result<Client, Error> {
    let initialization_error = |e: String| {
        Error::Initialization(format!(
            "Failed to create HTTP client for auth.oidc.{name}: {e}"
        ))
    };

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
    build_client(Client::builder(), &config.tls).map_err(initialization_error)
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

    use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version};
    use serde_json::{Value, json};
    use tempfile::tempdir;
    use wiremock::MockServer;

    use super::*;
    use crate::auth::oidc::validator::tests::{make_token, valid_claims};
    use crate::auth::{PeerCertificate, TokenIssuer};
    use crate::configuration::Base64String;
    use crate::metrics_provider;
    use crate::test_fixtures::configuration::{load_config, minimal_config, try_load_config};
    use crate::test_fixtures::mocks::{mount_jwks, static_jwks_response};
    use crate::test_fixtures::mtls::cert_der;
    use crate::test_fixtures::oidc::KID;
    use crate::test_fixtures::requests::{
        empty_parts, parts_with_authorization, parts_with_basic_auth,
    };
    use crate::test_fixtures::webhook::{ca_bundle_pem, client_cert_pem, client_key_pem};
    use angos_cache::Config as CacheConfig;
    use angos_secret::Secret;

    fn build_validators(config: &Configuration) -> Result<Vec<OidcValidator>, Error> {
        Authenticator::build_oidc_validators(
            &config.auth,
            &CacheConfig::Memory.to_backend().unwrap(),
        )
    }

    /// The Basic username field selects the provider, so the two names cannot
    /// both be honoured and the collision is a configuration mistake.
    #[test]
    fn a_basic_username_may_not_name_an_oidc_provider() {
        let config = load_config(
            r#"
            [auth.identity.ci]
            username = "github-actions"
            password = "$argon2id$v=19$m=19456,t=2,p=1$dGVzdHNhbHQ"

            [auth.oidc.github-actions]
            issuer = "https://token.actions.githubusercontent.com"
        "#,
        );

        let cache = CacheConfig::Memory.to_backend().unwrap();
        let Err(error) = Authenticator::new(&config, &cache) else {
            panic!("a colliding name must be refused at startup");
        };
        assert!(
            matches!(&error, Error::Initialization(msg) if msg.contains("github-actions")),
            "got: {error:?}"
        );
    }

    /// An issuer the system roots do not cover, such as a kube-apiserver, is
    /// reached through its own CA bundle, a client certificate, or a projected
    /// service-account token.
    #[test]
    fn a_provider_may_carry_its_own_tls_and_credentials() {
        let material = tempdir().unwrap();
        let path = |name: &str, contents: &str| {
            let path = material.path().join(name);
            fs::write(&path, contents).unwrap();
            path.display().to_string()
        };
        let ca = path("ca.pem", ca_bundle_pem());
        let certificate = path("client.pem", client_cert_pem());
        let key = path("client-key.pem", client_key_pem());
        let token = path("token", "service-account-token");

        for settings in [
            format!(r#"server_ca_bundle = "{ca}""#),
            format!(
                "client_certificate_bundle = \"{certificate}\"\nclient_private_key = \"{key}\""
            ),
            format!(r#"bearer_token_file = "{token}""#),
        ] {
            let config = load_config(&format!(
                "[auth.oidc.kube]\nissuer = \"https://kubernetes.default.svc\"\n{settings}"
            ));
            assert!(build_validators(&config).is_ok(), "{settings}");
        }
    }

    #[test]
    fn a_client_certificate_without_its_key_is_refused_at_startup() {
        let material = tempdir().unwrap();
        let certificate_path = material.path().join("client.pem");
        fs::write(&certificate_path, client_cert_pem()).unwrap();

        let error = try_load_config(&format!(
            r#"
            [auth.oidc.kube]
            issuer = "https://kubernetes.default.svc"
            client_certificate_bundle = "{}"
        "#,
            certificate_path.display()
        ))
        .expect_err("half a client identity must be refused rather than fetch anonymously");
        assert!(
            error.to_string().contains("client_private_key"),
            "got: {error}"
        );
    }

    #[test]
    fn unreadable_provider_files_are_refused_at_startup() {
        for setting in [
            r#"bearer_token_file = "/nonexistent/token""#,
            r#"server_ca_bundle = "/nonexistent/ca.pem""#,
        ] {
            let config = load_config(&format!(
                "[auth.oidc.kube]\nissuer = \"https://kubernetes.default.svc\"\n{setting}"
            ));
            let Err(error) = build_validators(&config) else {
                panic!("{setting} must be refused");
            };
            assert!(
                matches!(&error, Error::Initialization(msg) if msg.contains("auth.oidc.kube")),
                "got: {error:?}"
            );
        }
    }

    #[test]
    fn oidc_validators_are_sorted_by_name() {
        let config = load_config(
            r#"
            [auth.oidc.github]
            issuer = "https://token.actions.githubusercontent.com"

            [auth.oidc.custom]
            issuer = "https://auth.example.com"
        "#,
        );

        let names: Vec<String> = build_validators(&config)
            .unwrap()
            .into_iter()
            .map(|validator| validator.provider_name)
            .collect();
        assert_eq!(names, ["custom", "github"]);
    }

    #[tokio::test]
    async fn test_authenticate_request_preserves_client_ip() {
        metrics_provider::initialize_metrics();
        let cache = CacheConfig::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&minimal_config(), &cache).unwrap();

        let identity = authenticator
            .authenticate_request(&empty_parts(), Some("192.168.1.100:8080".parse().unwrap()))
            .await
            .unwrap();
        assert_eq!(identity.client_ip.as_deref(), Some("192.168.1.100"));
    }

    /// A wiremock issuer serving `keys` as its JWKS.
    async fn issuer_serving(keys: Value) -> MockServer {
        let server = MockServer::start().await;
        mount_jwks(&server, keys).await;
        server
    }

    /// A bearer naming `issuer`, signed with the static test key.
    fn token_from(issuer: &str) -> String {
        make_token(&valid_claims(issuer, "unused"), KID)
    }

    /// The TOML of provider `name` trusting `issuer`, with its keys at `jwks`.
    fn provider(name: &str, issuer: &str, jwks: &str) -> String {
        format!(
            "[auth.oidc.{name}]\nissuer = \"{issuer}\"\n\
             jwks_uri = \"{jwks}/.well-known/jwks\"\nallowed_algorithms = [\"ES256\"]\n"
        )
    }

    /// Two providers no request in these tests holds a bearer for.
    fn idle_providers() -> String {
        provider(
            "alpha",
            "https://issuer.test/alpha",
            "https://issuer.test/alpha",
        ) + &provider(
            "beta",
            "https://issuer.test/beta",
            "https://issuer.test/beta",
        )
    }

    /// An authenticator over the OIDC `providers` TOML, with no token service
    /// and no basic identity.
    fn authenticator_with(providers: &str) -> Authenticator {
        Authenticator {
            token_validator: None,
            oidc_validators: build_validators(&load_config(providers)).unwrap(),
            basic_auth_validator: BasicAuthValidator::new(&HashMap::new()).unwrap(),
        }
    }

    /// Built through `load_config` because `basic_auth::PasswordHash` is not
    /// publicly constructible: its only path is deserialisation.
    fn admin_basic_auth_validator() -> BasicAuthValidator {
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default());
        let password_hash = argon.hash_password(b"secret").unwrap().to_string();
        let config = load_config(&format!(
            r#"
            [auth.identity.admin]
            username = "admin"
            password = "{password_hash}"
        "#,
        ));

        BasicAuthValidator::new(&config.auth.identity).unwrap()
    }

    fn with_basic_admin(providers: &str) -> Authenticator {
        Authenticator {
            basic_auth_validator: admin_basic_auth_validator(),
            ..authenticator_with(providers)
        }
    }

    /// The two halves the token service splits into, built from one config so
    /// the issued token is the one the chain's validator accepts.
    fn make_authenticator_with_token_service(providers: &str) -> (Authenticator, TokenIssuer) {
        let config = token_service::Config {
            secret_key: Secret::new(Base64String(vec![7; 32])),
            realm: None,
            ttl_secs: 3600,
        };
        // The identities these tests issue from carry no `id` and no OIDC
        // claims, so no configured credential has to back them.
        let auth = AuthConfig::default();
        let authenticator = Authenticator {
            token_validator: Some(TokenValidator::new(&config, &auth).unwrap()),
            ..authenticator_with(providers)
        };

        (authenticator, TokenIssuer::new(&config, &auth).unwrap())
    }

    fn with_certificate(mut parts: Parts) -> Parts {
        parts
            .extensions
            .insert(PeerCertificate(Arc::new(cert_der())));
        parts
    }

    /// A bearer is validated by the provider whose issuer it names, and by
    /// that one alone: every other provider would otherwise fetch its own JWKS
    /// for a token it rejects on the issuer. The unreachable provider sorts
    /// first by name, so only the ordering can keep it out of the exchange.
    #[tokio::test]
    async fn a_bearer_only_reaches_the_provider_whose_issuer_it_names() {
        metrics_provider::initialize_metrics();
        let unnamed = MockServer::start().await;
        let named = issuer_serving(static_jwks_response()).await;
        let authenticator = authenticator_with(
            &(provider("aaa-unnamed", &unnamed.uri(), &unnamed.uri())
                + &provider("zzz-named", &named.uri(), &named.uri())),
        );

        let parts = parts_with_authorization(&format!("Bearer {}", token_from(&named.uri())));
        let outcome = authenticator
            .try_oidc_authentication(&parts, &mut ClientIdentity::default())
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

    #[tokio::test]
    async fn test_try_oidc_falls_through_error_to_success() {
        // Both providers trust the token's issuer; "alpha" sorts first and
        // holds no key for it, so it fails before "beta" accepts.
        metrics_provider::initialize_metrics();
        let keyless = issuer_serving(json!({ "keys": [] })).await;
        let keyed = issuer_serving(static_jwks_response()).await;
        let issuer = keyed.uri();
        let authenticator = authenticator_with(
            &(provider("alpha", &issuer, &keyless.uri()) + &provider("beta", &issuer, &issuer)),
        );

        let parts = parts_with_authorization(&format!("Bearer {}", token_from(&issuer)));
        let mut identity = ClientIdentity::new(None);
        let result = authenticator
            .try_oidc_authentication(&parts, &mut identity)
            .await;

        assert_eq!(result.unwrap(), Some(AuthMethod::Oidc));
        assert!(
            identity
                .oidc
                .is_some_and(|claims| claims.provider_name == "beta")
        );
    }

    /// When every provider refuses, the alphabetically-first provider's error
    /// fails the request, overriding an mTLS success.
    #[tokio::test]
    async fn the_first_oidc_error_fails_the_request() {
        metrics_provider::initialize_metrics();
        // "alpha" holds no key, "beta" holds the key but trusts another issuer.
        let keyless = issuer_serving(json!({ "keys": [] })).await;
        let keyed = issuer_serving(static_jwks_response()).await;
        let authenticator = authenticator_with(
            &(provider("alpha", "https://issuer.test/alpha", &keyless.uri())
                + &provider("beta", "https://issuer.test/beta", &keyed.uri())),
        );

        let parts = with_certificate(parts_with_authorization(&format!(
            "Bearer {}",
            token_from("https://issuer.test/nobody")
        )));
        let err = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Unauthorized(msg) if msg.contains("No matching key")),
            "expected alpha's error to propagate, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn an_invalid_basic_credential_fails_the_request() {
        metrics_provider::initialize_metrics();
        let authenticator = with_basic_admin(&idle_providers());

        let result = authenticator
            .authenticate_request(&parts_with_basic_auth("admin", "wrongpass"), None)
            .await;
        assert!(matches!(result, Err(Error::Unauthorized(_))));
    }

    /// The OIDC middlewares claim any bearer header and fail the request when they
    /// cannot validate it, so without the short-circuit no reissued token works.
    #[tokio::test]
    async fn a_valid_token_skips_oidc_and_basic() {
        metrics_provider::initialize_metrics();
        // A provider reached with the token would fail the request.
        let (authenticator, issuer) = make_authenticator_with_token_service(&idle_providers());
        let issued_from = ClientIdentity {
            username: Some("ci-bot".to_string()),
            ..Default::default()
        };
        let token = issuer.issue(&issued_from).unwrap().token;

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
        metrics_provider::initialize_metrics();
        let keyed = issuer_serving(static_jwks_response()).await;
        let (authenticator, _) =
            make_authenticator_with_token_service(&provider("mock", &keyed.uri(), &keyed.uri()));

        let parts = parts_with_authorization(&format!("Bearer {}", token_from(&keyed.uri())));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert!(identity.oidc.is_some());
        assert_eq!(identity.auth_method, AuthMethod::Oidc);
    }

    #[tokio::test]
    async fn mtls_outranks_a_registry_token() {
        metrics_provider::initialize_metrics();
        let (authenticator, issuer) = make_authenticator_with_token_service(&idle_providers());
        let token = issuer.issue(&ClientIdentity::default()).unwrap().token;

        let parts = with_certificate(parts_with_authorization(&format!("Bearer {token}")));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Mtls);
    }

    /// The identity carries both credentials and reports the stronger one.
    #[tokio::test]
    async fn mtls_outranks_a_successful_oidc_provider() {
        metrics_provider::initialize_metrics();
        let keyed = issuer_serving(static_jwks_response()).await;
        let authenticator = authenticator_with(&provider("provider", &keyed.uri(), &keyed.uri()));

        let parts = with_certificate(parts_with_authorization(&format!(
            "Bearer {}",
            token_from(&keyed.uri())
        )));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert!(identity.oidc.is_some());
        assert_eq!(identity.auth_method, AuthMethod::Mtls);
    }

    #[tokio::test]
    async fn a_request_with_no_credentials_is_anonymous() {
        metrics_provider::initialize_metrics();
        let authenticator = authenticator_with(&idle_providers());

        let identity = authenticator
            .authenticate_request(&empty_parts(), None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Anonymous);
    }

    /// The OIDC token rides in a Basic header under the provider's name; were
    /// basic auth tried too, that username would fail the request.
    #[tokio::test]
    async fn oidc_success_skips_basic_auth() {
        metrics_provider::initialize_metrics();
        let keyed = issuer_serving(static_jwks_response()).await;
        let authenticator =
            with_basic_admin(&provider("mock-provider", &keyed.uri(), &keyed.uri()));

        let parts = parts_with_basic_auth("mock-provider", &token_from(&keyed.uri()));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert!(identity.oidc.is_some());
        assert!(identity.username.is_none());
        assert_eq!(identity.auth_method, AuthMethod::Oidc);
    }

    #[tokio::test]
    async fn basic_auth_is_reported_when_nothing_stronger_succeeds() {
        metrics_provider::initialize_metrics();
        let authenticator = with_basic_admin(&idle_providers());

        let identity = authenticator
            .authenticate_request(&parts_with_basic_auth("admin", "secret"), None)
            .await
            .unwrap();

        assert_eq!(identity.username.as_deref(), Some("admin"));
        assert_eq!(identity.auth_method, AuthMethod::Basic);
    }

    /// A basic-auth success must not overwrite mTLS.
    #[tokio::test]
    async fn mtls_outranks_successful_basic_auth() {
        metrics_provider::initialize_metrics();
        let authenticator = with_basic_admin(&idle_providers());

        let parts = with_certificate(parts_with_basic_auth("admin", "secret"));
        let identity = authenticator
            .authenticate_request(&parts, None)
            .await
            .unwrap();

        assert_eq!(identity.auth_method, AuthMethod::Mtls);
        assert!(
            !identity.certificate.organizations.is_empty(),
            "the certificate carries no organization"
        );
    }
}
