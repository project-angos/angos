use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use hyper::http::request::Parts;
use tracing::{debug, instrument, warn};

use super::{
    AuthMiddleware, AuthResult, BasicAuthValidator, MtlsValidator, basic_auth, oidc,
    oidc::OidcValidator, webhook,
};
use crate::{
    cache::Cache, command::server::Error, configuration::Configuration, identity::ClientIdentity,
    metrics_provider::AUTH_ATTEMPTS,
};

#[derive(Clone, Debug, Default)]
pub struct AuthConfig {
    pub identity: HashMap<String, basic_auth::Config>,
    pub oidc: HashMap<String, oidc::Config>,
    pub webhook: HashMap<String, Arc<webhook::Config>>,
}

type OidcValidators = Vec<(String, Arc<dyn AuthMiddleware>)>;

/// Coordinates all authentication methods and handles the authentication chain
pub struct Authenticator {
    mtls_validator: MtlsValidator,
    oidc_validators: OidcValidators,
    basic_auth_validator: BasicAuthValidator,
}

impl Authenticator {
    pub fn new(config: &Configuration, cache: &Arc<dyn Cache>) -> Result<Self, Error> {
        let auth_config = &config.auth;

        let mtls_validator = MtlsValidator::new();
        let oidc_validators = Self::build_oidc_validators(auth_config, cache)?;
        let basic_auth_validator = BasicAuthValidator::new(&auth_config.identity);

        Ok(Self {
            mtls_validator,
            oidc_validators,
            basic_auth_validator,
        })
    }

    fn build_oidc_validators(
        auth_config: &AuthConfig,
        cache: &Arc<dyn Cache>,
    ) -> Result<OidcValidators, Error> {
        let mut validators = Vec::new();

        for (name, oidc_config) in &auth_config.oidc {
            let validator = OidcValidator::new(name.clone(), oidc_config, cache.clone())?;
            validators.push((name.clone(), Arc::new(validator) as Arc<dyn AuthMiddleware>));
        }

        Ok(validators)
    }

    /// Authentication order: mTLS → OIDC → Basic Auth
    #[instrument(skip(self, parts), fields(auth_method = tracing::field::Empty))]
    pub async fn authenticate_request(
        &self,
        parts: &Parts,
        remote_address: Option<SocketAddr>,
    ) -> Result<ClientIdentity, Error> {
        let mut identity = ClientIdentity::new(remote_address);
        let mut authenticated_method = None;

        if self.try_mtls_authentication(parts, &mut identity).await {
            authenticated_method = Some("mtls");
        }

        if self.try_oidc_authentication(parts, &mut identity).await? {
            authenticated_method = Some("oidc");
        } else if self.try_basic_authentication(parts, &mut identity).await? {
            authenticated_method = Some("basic");
        }

        tracing::Span::current().record("auth_method", authenticated_method.unwrap_or("anonymous"));

        Ok(identity)
    }

    /// Attempts mTLS authentication. Returns `true` if a valid certificate was extracted.
    /// Errors are logged and suppressed — mTLS is non-fatal so other methods can follow.
    async fn try_mtls_authentication(&self, parts: &Parts, identity: &mut ClientIdentity) -> bool {
        match self.mtls_validator.authenticate(parts, identity).await {
            Ok(AuthResult::Authenticated) => {
                debug!("mTLS authentication extracted certificate info");
                if !identity.certificate.common_names.is_empty()
                    || !identity.certificate.organizations.is_empty()
                {
                    AUTH_ATTEMPTS.with_label_values(&["mtls", "success"]).inc();
                    return true;
                }
            }
            Ok(AuthResult::NoCredentials) => {}
            Err(e) => {
                warn!("mTLS validation error: {e}");
                AUTH_ATTEMPTS.with_label_values(&["mtls", "failed"]).inc();
            }
        }
        false
    }

    /// Tries each OIDC provider in order, returning `true` on first success.
    /// Returns `Err` if credentials were presented but invalid.
    async fn try_oidc_authentication(
        &self,
        parts: &Parts,
        identity: &mut ClientIdentity,
    ) -> Result<bool, Error> {
        for (provider_name, validator) in &self.oidc_validators {
            match validator.authenticate(parts, identity).await {
                Ok(AuthResult::Authenticated) => {
                    debug!("OIDC authentication succeeded with provider: {provider_name}");
                    AUTH_ATTEMPTS.with_label_values(&["oidc", "success"]).inc();
                    return Ok(true);
                }
                Ok(AuthResult::NoCredentials) => {}
                Err(e) => {
                    warn!("OIDC validation failed for provider {provider_name}: {e}");
                    AUTH_ATTEMPTS.with_label_values(&["oidc", "failed"]).inc();
                    return Err(e);
                }
            }
        }
        Ok(false)
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
                AUTH_ATTEMPTS.with_label_values(&["basic", "success"]).inc();
                Ok(true)
            }
            Ok(AuthResult::NoCredentials) => Ok(false),
            Err(e) => {
                warn!("Basic auth validation failed: {e}");
                AUTH_ATTEMPTS.with_label_values(&["basic", "failed"]).inc();
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
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use hyper::{Request, header::AUTHORIZATION};

    use super::*;
    use crate::{cache, configuration::Configuration};

    fn create_minimal_config() -> Configuration {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
        "#;

        Configuration::load_from_str(toml).unwrap()
    }

    fn minimal_config_prefix() -> &'static str {
        r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
        "#
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
        let toml = format!(
            r#"{}
            [auth.identity.user1]
            username = "user1"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        assert_eq!(config.auth.identity.len(), 1);
        assert!(config.auth.identity.contains_key("user1"));
    }

    #[test]
    fn test_auth_config_with_oidc() {
        let toml = format!(
            r#"{}
            [auth.oidc.github]
            provider = "github"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        assert_eq!(config.auth.oidc.len(), 1);
        assert!(config.auth.oidc.contains_key("github"));
    }

    #[test]
    fn test_auth_config_with_webhook() {
        let toml = format!(
            r#"{}
            [auth.webhook.test]
            url = "http://localhost:8080/auth"
            timeout_ms = 5000
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        assert_eq!(config.auth.webhook.len(), 1);
        assert!(config.auth.webhook.contains_key("test"));
        assert_eq!(config.auth.webhook["test"].name, "test");
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
        let toml = format!(
            r#"{}
            [auth.identity.testuser]
            username = "testuser"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authenticator = Authenticator::new(&config, &cache);

        assert!(authenticator.is_ok());
    }

    #[test]
    fn test_build_oidc_validators_empty() {
        let auth_config = AuthConfig::default();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&auth_config, &cache);

        assert!(validators.is_ok());
        assert!(validators.unwrap().is_empty());
    }

    #[test]
    fn test_build_oidc_validators_with_github() {
        let toml = format!(
            r#"{}
            [auth.oidc.github]
            provider = "github"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&config.auth, &cache);

        assert!(validators.is_ok());
        let validators = validators.unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].0, "github");
    }

    #[test]
    fn test_build_oidc_validators_with_generic() {
        let toml = format!(
            r#"{}
            [auth.oidc.custom]
            provider = "generic"
            issuer = "https://auth.example.com"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&config.auth, &cache);

        assert!(validators.is_ok());
        let validators = validators.unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].0, "custom");
    }

    #[test]
    fn test_build_oidc_validators_multiple() {
        let toml = format!(
            r#"{}
            [auth.oidc.github]
            provider = "github"

            [auth.oidc.custom]
            provider = "generic"
            issuer = "https://auth.example.com"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&config.auth, &cache);

        assert!(validators.is_ok());
        let validators = validators.unwrap();
        assert_eq!(validators.len(), 2);
    }

    #[tokio::test]
    async fn test_authenticate_request_no_credentials() {
        let config = create_minimal_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let request = Request::builder().body(()).unwrap();
        let (parts, ()) = request.into_parts();

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

        let toml = format!(
            r#"{}
            [auth.identity.testuser]
            username = "testuser"
            password = "{password_hash}"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let credentials = BASE64_STANDARD.encode("testuser:testpass");
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Basic {credentials}"))
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        let result = authenticator.authenticate_request(&parts, None).await;

        assert!(result.is_ok());
        let identity = result.unwrap();
        assert_eq!(identity.username, Some("testuser".to_string()));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_request_with_invalid_basic_auth() {
        let salt = SaltString::generate(OsRng);
        let config = Params::default();
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, config);
        let password_hash = argon.hash_password(b"testpass", &salt).unwrap().to_string();

        let toml = format!(
            r#"{}
            [auth.identity.testuser]
            username = "testuser"
            password = "{password_hash}"
        "#,
            minimal_config_prefix()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let credentials = BASE64_STANDARD.encode("testuser:wrongpass");
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Basic {credentials}"))
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        let result = authenticator.authenticate_request(&parts, None).await;

        assert!(result.is_ok());
        let identity = result.unwrap();
        assert!(identity.username.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_request_preserves_client_ip() {
        let config = create_minimal_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authenticator = Authenticator::new(&config, &cache).unwrap();

        let request = Request::builder().body(()).unwrap();
        let (parts, ()) = request.into_parts();
        let socket_addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();

        let result = authenticator
            .authenticate_request(&parts, Some(socket_addr))
            .await;

        assert!(result.is_ok());
        let identity = result.unwrap();
        assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
    }
}
