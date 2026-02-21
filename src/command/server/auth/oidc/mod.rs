pub mod jwk;
pub mod provider;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use hyper::http::request::Parts;
pub use jwk::Jwk;
pub use provider::OidcProvider;
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

use super::{AuthMiddleware, AuthResult};
use crate::{
    cache::Cache,
    command::server::{
        auth::oidc::provider::{generic, github},
        error::Error,
        request_ext::HeaderExt,
    },
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
    cache: Arc<dyn Cache>,
}

impl OidcValidator {
    pub fn new(
        provider_name: String,
        provider_config: &Config,
        cache: Arc<dyn Cache>,
    ) -> Result<Self, Error> {
        let client = Client::builder().timeout(Duration::from_secs(30)).build();

        let client = match client {
            Ok(client) => Ok(Arc::new(client)),
            Err(err) => {
                let msg = format!("Failed to build HTTP client: {err}");
                Err(Error::Initialization(msg))
            }
        }?;

        let provider = provider_config.to_backend();

        Ok(Self {
            provider_name,
            provider,
            client,
            cache,
        })
    }

    pub async fn validate_token(&self, token: &str) -> Result<OidcClaims, Error> {
        generic::validate_oidc_token(
            &self.provider_name,
            &*self.provider,
            token,
            &self.client,
            &*self.cache,
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
        let token = if let Some(bearer_token) = parts.bearer_token() {
            debug!(
                "Found Bearer token for OIDC provider '{}'",
                self.provider_name
            );
            bearer_token
        } else if let Some((username, password)) = parts.basic_auth() {
            debug!("Found Basic auth credentials with username '{}'", username);
            if username != self.provider_name {
                debug!(
                    "Basic auth username '{}' doesn't match OIDC provider name '{}', skipping",
                    username, self.provider_name
                );
                return Ok(AuthResult::NoCredentials);
            }
            password
        } else {
            return Ok(AuthResult::NoCredentials);
        };

        match self.validate_token(&token).await {
            Ok(claims) => {
                debug!(
                    "Successfully validated OIDC token for provider '{}' with claims: {:?}",
                    self.provider_name, claims
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use hyper::{Request, header::AUTHORIZATION};
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{cache, identity::ClientIdentity};

    fn build_config(mock_server: &MockServer) -> Config {
        Config::Generic(generic::ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        })
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
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let provider = config.to_backend();
        assert_eq!(provider.issuer(), "https://auth.example.com");
        assert_eq!(provider.name(), "Generic OIDC");
    }

    #[test]
    fn test_config_to_backend_github() {
        let config = Config::GitHub(github::ProviderConfig {
            issuer: "https://token.actions.githubusercontent.com".to_string(),
            jwks_uri: "https://token.actions.githubusercontent.com/.well-known/jwks".to_string(),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let provider = config.to_backend();
        assert_eq!(
            provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(provider.name(), "GitHub Actions");
    }

    #[test]
    fn test_oidc_validator_new_generic() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache);

        assert!(validator.is_ok());
        let validator = validator.unwrap();
        assert_eq!(validator.provider_name, "test-provider");
        assert_eq!(validator.provider.issuer(), "https://auth.example.com");
    }

    #[test]
    fn test_oidc_validator_new_github() {
        let config = Config::GitHub(github::ProviderConfig {
            issuer: "https://token.actions.githubusercontent.com".to_string(),
            jwks_uri: "https://token.actions.githubusercontent.com/.well-known/jwks".to_string(),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("github".to_string(), &config, cache);

        assert!(validator.is_ok());
        let validator = validator.unwrap();
        assert_eq!(validator.provider_name, "github");
        assert_eq!(
            validator.provider.issuer(),
            "https://token.actions.githubusercontent.com"
        );
    }

    pub fn create_rsa_keypair() -> (String, String) {
        use std::process::Command;

        let output = Command::new("openssl")
            .args(["genrsa", "2048"])
            .output()
            .expect("Failed to generate RSA key");

        let private_key = String::from_utf8(output.stdout).expect("Invalid UTF-8");

        let output = Command::new("openssl")
            .args(["rsa", "-pubout"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(private_key.as_bytes())?;
                child.wait_with_output()
            })
            .expect("Failed to extract public key");

        let public_key = String::from_utf8(output.stdout).expect("Invalid UTF-8");

        (private_key, public_key)
    }

    pub fn rsa_public_key_to_jwk(public_key_pem: &str) -> serde_json::Value {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

        let public_key_pem = public_key_pem
            .replace("-----BEGIN PUBLIC KEY-----", "")
            .replace("-----END PUBLIC KEY-----", "")
            .replace('\n', "");

        let der = base64::engine::general_purpose::STANDARD
            .decode(public_key_pem)
            .expect("Failed to decode base64");

        let n = URL_SAFE_NO_PAD.encode(&der[33..289]);
        let e = URL_SAFE_NO_PAD.encode(&der[291..294]);

        json!({
            "kty": "RSA",
            "use": "sig",
            "kid": "test-key-1",
            "n": n,
            "e": e,
            "alg": "RS256"
        })
    }

    #[tokio::test]
    async fn test_validate_token_success() {
        use jsonwebtoken::{EncodingKey, Header, encode};

        let mock_server = MockServer::start().await;
        let (private_key, public_key) = create_rsa_keypair();
        let jwk = rsa_public_key_to_jwk(&public_key);

        let jwks_response = json!({
            "keys": [jwk]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims = std::collections::HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap(),
        )
        .unwrap();

        let result = validator.validate_token(&token).await;

        assert!(result.is_ok());
        let oidc_claims = result.unwrap();
        assert_eq!(oidc_claims.provider_name, "test-provider");
        assert_eq!(oidc_claims.provider_type, "Generic OIDC");
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "test-user");
    }

    #[tokio::test]
    async fn test_validate_token_invalid() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let result = validator.validate_token("invalid-token").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_authenticate_with_bearer_token() {
        use jsonwebtoken::{EncodingKey, Header, encode};

        let mock_server = MockServer::start().await;
        let (private_key, public_key) = create_rsa_keypair();
        let jwk = rsa_public_key_to_jwk(&public_key);

        let jwks_response = json!({
            "keys": [jwk]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims = std::collections::HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap(),
        )
        .unwrap();

        let request = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
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
        use jsonwebtoken::{EncodingKey, Header, encode};

        let mock_server = MockServer::start().await;
        let (private_key, public_key) = create_rsa_keypair();
        let jwk = rsa_public_key_to_jwk(&public_key);

        let jwks_response = json!({
            "keys": [jwk]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("github".to_string(), &config, cache).unwrap();

        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims = std::collections::HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap(),
        )
        .unwrap();

        let credentials = BASE64_STANDARD.encode(format!("github:{token}"));
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Basic {credentials}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::Authenticated));
        assert!(identity.oidc.is_some());
    }

    #[tokio::test]
    async fn test_authenticate_with_basic_auth_non_matching_provider() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("github".to_string(), &config, cache).unwrap();

        let credentials = BASE64_STANDARD.encode("wrong-provider:token");
        let request = Request::builder()
            .header(AUTHORIZATION, format!("Basic {credentials}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_no_credentials() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let request = Request::builder().body(()).unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), AuthResult::NoCredentials));
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_with_invalid_token() {
        let config = Config::Generic(generic::ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/jwks".to_string()),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        });

        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("test-provider".to_string(), &config, cache).unwrap();

        let request = Request::builder()
            .header(AUTHORIZATION, "Bearer invalid-token")
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
        let mut identity = ClientIdentity::new(None);

        let result = validator.authenticate(&parts, &mut identity).await;

        assert!(result.is_err());
        assert!(identity.oidc.is_none());
    }

    #[tokio::test]
    async fn test_authenticate_populates_identity() {
        use jsonwebtoken::{EncodingKey, Header, encode};

        let mock_server = MockServer::start().await;
        let (private_key, public_key) = create_rsa_keypair();
        let jwk = rsa_public_key_to_jwk(&public_key);

        let jwks_response = json!({
            "keys": [jwk]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = build_config(&mock_server);
        let cache = cache::Config::Memory.to_backend().unwrap();
        let validator = OidcValidator::new("my-provider".to_string(), &config, cache).unwrap();

        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims = std::collections::HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("user-123"));
        claims.insert("email".to_string(), json!("user@example.com"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap(),
        )
        .unwrap();

        let request = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap();

        let (parts, ()) = request.into_parts();
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
