use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use hyper::http::request::Parts;
use serde::Deserialize;
use tracing::{debug, instrument, warn};

use super::{
    AuthMiddleware, AuthResult, BasicAuthValidator, MtlsValidator, basic_auth, oidc,
    oidc::OidcValidator, webhook,
};
use crate::{
    cache::Cache, command::server::error::Error, configuration::Configuration,
    identity::ClientIdentity, metrics_provider::AUTH_ATTEMPTS,
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

        match self.mtls_validator.authenticate(parts, &mut identity).await {
            Ok(AuthResult::Authenticated) => {
                debug!("mTLS authentication extracted certificate info");
                if authenticated_method.is_none()
                    && (!identity.certificate.common_names.is_empty()
                        || !identity.certificate.organizations.is_empty())
                {
                    AUTH_ATTEMPTS.with_label_values(&["mtls", "success"]).inc();
                    authenticated_method = Some("mtls");
                }
            }
            Ok(AuthResult::NoCredentials) => {}
            Err(e) => {
                warn!("mTLS validation error: {}", e);
                AUTH_ATTEMPTS.with_label_values(&["mtls", "failed"]).inc();
            }
        }

        let mut oidc_authenticated = false;
        for (provider_name, validator) in &self.oidc_validators {
            match validator.authenticate(parts, &mut identity).await {
                Ok(AuthResult::Authenticated) => {
                    debug!(
                        "OIDC authentication succeeded with provider: {}",
                        provider_name
                    );
                    AUTH_ATTEMPTS.with_label_values(&["oidc", "success"]).inc();
                    authenticated_method = Some("oidc");
                    oidc_authenticated = true;
                    break;
                }
                Ok(AuthResult::NoCredentials) => {}
                Err(e) => {
                    warn!(
                        "OIDC validation failed for provider {}: {}",
                        provider_name, e
                    );
                    AUTH_ATTEMPTS.with_label_values(&["oidc", "failed"]).inc();
                    return Err(e);
                }
            }
        }

        if !oidc_authenticated {
            match self
                .basic_auth_validator
                .authenticate(parts, &mut identity)
                .await
            {
                Ok(AuthResult::Authenticated) => {
                    debug!("Basic authentication succeeded");
                    AUTH_ATTEMPTS.with_label_values(&["basic", "success"]).inc();
                    authenticated_method = Some("basic");
                }
                Ok(AuthResult::NoCredentials) => {}
                Err(e) => {
                    warn!("Basic auth validation failed: {}", e);
                    AUTH_ATTEMPTS.with_label_values(&["basic", "failed"]).inc();
                    return Err(e);
                }
            }
        }

        if let Some(method) = authenticated_method {
            tracing::Span::current().record("auth_method", method);
        } else {
            tracing::Span::current().record("auth_method", "anonymous");
        }

        Ok(identity)
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

        toml::from_str(toml).unwrap()
    }

    #[test]
    fn test_auth_config_deserialize_empty() {
        let toml = r"";
        let config: AuthConfig = toml::from_str(toml).unwrap();
        assert!(config.identity.is_empty());
        assert!(config.oidc.is_empty());
        assert!(config.webhook.is_empty());
    }

    #[test]
    fn test_auth_config_deserialize_with_identity() {
        let toml = r#"
            [identity.user1]
            username = "user1"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"
        "#;

        let config: AuthConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.identity.len(), 1);
        assert!(config.identity.contains_key("user1"));
    }

    #[test]
    fn test_auth_config_deserialize_with_oidc() {
        let toml = r#"
            [oidc.github]
            provider = "github"
        "#;

        let config: AuthConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.oidc.len(), 1);
        assert!(config.oidc.contains_key("github"));
    }

    #[test]
    fn test_auth_config_deserialize_with_webhook() {
        let toml = r#"
            [webhook.test]
            url = "http://localhost:8080/auth"
            timeout_ms = 5000
        "#;

        let config: AuthConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.webhook.len(), 1);
        assert!(config.webhook.contains_key("test"));
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

            [auth.identity.testuser]
            username = "testuser"
            password = "$argon2id$v=19$m=19456,t=2,p=1$test"
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
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
        let toml = r#"
            [oidc.github]
            provider = "github"
        "#;

        let auth_config: AuthConfig = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&auth_config, &cache);

        assert!(validators.is_ok());
        let validators = validators.unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].0, "github");
    }

    #[test]
    fn test_build_oidc_validators_with_generic() {
        let toml = r#"
            [oidc.custom]
            provider = "generic"
            issuer = "https://auth.example.com"
        "#;

        let auth_config: AuthConfig = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&auth_config, &cache);

        assert!(validators.is_ok());
        let validators = validators.unwrap();
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].0, "custom");
    }

    #[test]
    fn test_build_oidc_validators_multiple() {
        let toml = r#"
            [oidc.github]
            provider = "github"

            [oidc.custom]
            provider = "generic"
            issuer = "https://auth.example.com"
        "#;

        let auth_config: AuthConfig = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let validators = Authenticator::build_oidc_validators(&auth_config, &cache);

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

            [auth.identity.testuser]
            username = "testuser"
            password = "{password_hash}"
        "#
        );

        let config: Configuration = toml::from_str(&toml).unwrap();
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

            [auth.identity.testuser]
            username = "testuser"
            password = "{password_hash}"
        "#
        );

        let config: Configuration = toml::from_str(&toml).unwrap();
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
