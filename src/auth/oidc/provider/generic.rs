use std::collections::HashMap;

use async_trait::async_trait;
use jsonwebtoken::{Validation, decode, decode_header};
use reqwest::{Client, header::ACCEPT};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{debug, info, warn};

use crate::{
    auth::oidc::{Jwk, OidcProvider},
    cache::{Cache, CacheExt},
    command::server::{Error, sha256_hash},
    identity::OidcClaims,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderConfig {
    pub issuer: String,
    #[serde(default)]
    pub jwks_uri: Option<String>,
    #[serde(default = "default_jwks_refresh_interval")]
    pub jwks_refresh_interval: u64,
    #[serde(default)]
    pub required_audience: Option<String>,
    #[serde(default = "default_clock_skew_tolerance")]
    pub clock_skew_tolerance: u64,
}

fn default_jwks_refresh_interval() -> u64 {
    3600
}

fn default_clock_skew_tolerance() -> u64 {
    60
}

pub struct Provider {
    config: ProviderConfig,
}

impl Provider {
    pub fn new(config: ProviderConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl OidcProvider for Provider {
    fn issuer(&self) -> &str {
        &self.config.issuer
    }

    fn jwks_uri(&self) -> Option<&str> {
        self.config.jwks_uri.as_deref()
    }

    fn name(&self) -> &'static str {
        "Generic OIDC"
    }

    fn jwks_refresh_interval(&self) -> u64 {
        self.config.jwks_refresh_interval
    }

    fn required_audience(&self) -> Option<&str> {
        self.config.required_audience.as_deref()
    }

    fn clock_skew_tolerance(&self) -> u64 {
        self.config.clock_skew_tolerance
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OpenIdConfiguration {
    issuer: String,
    jwks_uri: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

pub async fn validate_oidc_token(
    provider_name: &str,
    provider: &dyn OidcProvider,
    token: &str,
    client: &Client,
    cache: &dyn Cache,
) -> Result<OidcClaims, Error> {
    let jwks = fetch_jwks(provider, client, cache).await?;
    verify_jwt(token, &jwks, provider_name, provider)
}

/// Pure JWT verification — no I/O. Validates the token against the supplied JWKS and provider
/// configuration and returns structured claims on success.
fn verify_jwt(
    token: &str,
    jwks: &Jwks,
    provider_name: &str,
    provider: &dyn OidcProvider,
) -> Result<OidcClaims, Error> {
    let header = decode_header(token)
        .map_err(|e| Error::Unauthorized(format!("Failed to decode JWT header: {e}")))?;

    debug!(
        "JWT header: alg={:?}, kid={:?}, typ={:?}",
        header.alg, header.kid, header.typ
    );

    debug!(
        "Available JWKs: {:?}",
        jwks.keys.iter().map(|k| (k.kid(), k)).collect::<Vec<_>>()
    );

    let jwk = jwks
        .keys
        .iter()
        .find(|k| k.kid() == header.kid.as_deref())
        .ok_or_else(|| {
            Error::Unauthorized(format!("No matching key found for kid: {:?}", header.kid))
        })?;

    debug!("Found matching JWK: {:?}", jwk);

    let decoding_key = jwk.to_decoding_key()?;

    let mut validation = Validation::new(header.alg);
    validation.set_issuer(&[provider.issuer()]);
    if let Some(aud) = provider.required_audience() {
        validation.set_audience(&[aud]);
    } else {
        validation.validate_aud = false;
    }
    validation.leeway = provider.clock_skew_tolerance();
    validation.validate_exp = true;
    validation.validate_nbf = true;

    debug!(
        "Validation settings: issuer={:?}, audience={:?}, leeway={}, validate_exp={}, validate_nbf={}, algorithms={:?}",
        validation.iss,
        validation.aud,
        validation.leeway,
        validation.validate_exp,
        validation.validate_nbf,
        validation.algorithms
    );

    let token_data =
        decode::<HashMap<String, serde_json::Value>>(token, &decoding_key, &validation).map_err(
            |e| {
                warn!("JWT decode failed with error: {:?}", e);
                Error::Unauthorized(format!("JWT validation failed: {e}"))
            },
        )?;

    provider.validate_provider_claims(&token_data.claims)?;

    debug!("{} provider: Token validated successfully", provider.name());
    Ok(OidcClaims {
        provider_name: provider_name.to_string(),
        provider_type: provider.name().to_string(),
        claims: token_data.claims,
    })
}

async fn query_json<T>(client: &Client, url: &str) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let response = client
        .get(url)
        .header(ACCEPT, "application/json")
        .send()
        .await;

    let response =
        response.map_err(|e| Error::Unauthorized(format!("Failed to fetch URL {url}: {e}")))?;

    if !response.status().is_success() {
        let msg = format!("Failed to fetch URL {url}: HTTP {}", response.status());
        return Err(Error::Unauthorized(msg));
    }

    let data: T = response
        .json()
        .await
        .map_err(|e| Error::Unauthorized(format!("Failed to parse JSON from {url}: {e}")))?;

    Ok(data)
}

async fn get_jwks_url(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &dyn Cache,
) -> Result<String, Error> {
    if let Some(uri) = provider.jwks_uri() {
        return Ok(uri.to_string());
    }
    let oidc_config = fetch_oidc_configuration(provider, client, cache).await?;

    Ok(oidc_config.jwks_uri)
}

async fn fetch_jwks(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &dyn Cache,
) -> Result<Jwks, Error> {
    let provider_name = provider.name();
    let issuer_hash = sha256_hash(provider.issuer());
    let cache_key = format!("oidc:{provider_name}:jwks:{issuer_hash}");

    if let Ok(Some(cached)) = cache.retrieve::<Jwks>(&cache_key).await {
        debug!("Using cached JWKS for provider: {provider_name}");
        return Ok(cached);
    }

    let jwks_url = get_jwks_url(provider, client, cache).await?;
    let jwks = query_json::<Jwks>(client, &jwks_url).await?;

    let _ = cache
        .store(&cache_key, &jwks, provider.jwks_refresh_interval())
        .await;
    info!("Fetched JWKS from {jwks_url}");
    Ok(jwks)
}

async fn fetch_oidc_configuration(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &dyn Cache,
) -> Result<OpenIdConfiguration, Error> {
    let provider_name = provider.name();
    let issuer_hash = sha256_hash(provider.issuer());
    let cache_key = format!("oidc:{provider_name}:config:{issuer_hash}");

    if let Ok(Some(cached)) = cache.retrieve::<OpenIdConfiguration>(&cache_key).await {
        debug!("Using cached OIDC configuration");
        return Ok(cached);
    }

    let config_url = format!("{}/.well-known/openid-configuration", provider.issuer());
    let config = query_json::<OpenIdConfiguration>(client, &config_url).await?;

    if config.issuer != provider.issuer() {
        return Err(Error::Unauthorized(format!(
            "OIDC configuration issuer mismatch: expected {}, got {}",
            provider.issuer(),
            config.issuer
        )));
    }

    let _ = cache
        .store(&cache_key, &config, provider.jwks_refresh_interval())
        .await;
    info!("Fetched OIDC configuration from {config_url}");
    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        auth::oidc::tests::{create_rsa_keypair, rsa_public_key_to_jwk},
        cache,
    };

    fn build_test_provider_config(uri: &str) -> ProviderConfig {
        ProviderConfig {
            issuer: uri.to_string(),
            jwks_uri: Some(format!("{uri}/.well-known/jwks")),
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
        }
    }

    #[test]
    fn test_config_deserialize_minimal() {
        let toml = r#"
            issuer = "https://example.com"
        "#;

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://example.com");
        assert!(config.jwks_uri.is_none());
        assert_eq!(config.jwks_refresh_interval, 3600);
        assert!(config.required_audience.is_none());
        assert_eq!(config.clock_skew_tolerance, 60);
    }

    #[test]
    fn test_config_deserialize_full() {
        let toml = r#"
            issuer = "https://auth.example.com"
            jwks_uri = "https://auth.example.com/jwks"
            jwks_refresh_interval = 7200
            required_audience = "my-app"
            clock_skew_tolerance = 120
        "#;

        let config: ProviderConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.issuer, "https://auth.example.com");
        assert_eq!(
            config.jwks_uri,
            Some("https://auth.example.com/jwks".to_string())
        );
        assert_eq!(config.jwks_refresh_interval, 7200);
        assert_eq!(config.required_audience, Some("my-app".to_string()));
        assert_eq!(config.clock_skew_tolerance, 120);
    }

    #[test]
    fn test_default_functions() {
        assert_eq!(default_jwks_refresh_interval(), 3600);
        assert_eq!(default_clock_skew_tolerance(), 60);
    }

    #[test]
    fn test_create_provider() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        assert_eq!(provider.issuer(), "https://example.com");
        assert_eq!(provider.name(), "Generic OIDC");
        assert!(provider.jwks_uri().is_none());
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.required_audience(), Some("test-audience"));
        assert_eq!(provider.clock_skew_tolerance(), 60);
    }

    #[test]
    fn test_provider_with_jwks_uri() {
        let config = ProviderConfig {
            issuer: "https://auth.example.com".to_string(),
            jwks_uri: Some("https://auth.example.com/.well-known/jwks".to_string()),
            jwks_refresh_interval: 7200,
            required_audience: None,
            clock_skew_tolerance: 120,
        };

        let provider = Provider::new(config);
        assert_eq!(provider.issuer(), "https://auth.example.com");
        assert_eq!(
            provider.jwks_uri(),
            Some("https://auth.example.com/.well-known/jwks")
        );
        assert_eq!(provider.jwks_refresh_interval(), 7200);
        assert!(provider.required_audience().is_none());
        assert_eq!(provider.clock_skew_tolerance(), 120);
    }

    #[test]
    fn test_provider_with_defaults() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: default_jwks_refresh_interval(),
            required_audience: None,
            clock_skew_tolerance: default_clock_skew_tolerance(),
        };

        let provider = Provider::new(config);
        assert_eq!(provider.jwks_refresh_interval(), 3600);
        assert_eq!(provider.clock_skew_tolerance(), 60);
    }

    #[test]
    fn test_provider_name() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        assert_eq!(provider.name(), "Generic OIDC");
    }

    #[test]
    fn test_provider_validate_provider_claims_default() {
        let config = ProviderConfig {
            issuer: "https://example.com".to_string(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let claims = HashMap::new();
        assert!(provider.validate_provider_claims(&claims).is_ok());
    }

    #[test]
    fn test_validation_new_with_rs256() {
        let header = jsonwebtoken::Header {
            alg: Algorithm::RS256,
            ..Default::default()
        };

        let validation = Validation::new(header.alg);
        assert!(validation.algorithms.contains(&Algorithm::RS256));
        assert_eq!(validation.algorithms.len(), 1);
    }

    #[test]
    fn test_validation_algorithms_behavior() {
        for alg in [
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::ES256,
            Algorithm::ES384,
        ] {
            let validation = Validation::new(alg);
            assert!(
                validation.algorithms.contains(&alg),
                "Algorithm {alg:?} not found in validation.algorithms"
            );
            assert_eq!(
                validation.algorithms.len(),
                1,
                "Expected single algorithm, got {:?}",
                validation.algorithms
            );
        }
    }

    #[test]
    fn test_github_actions_token_header() {
        let header = jsonwebtoken::Header {
            alg: Algorithm::RS256,
            kid: Some("cc413527-173f-5a05-976e-9c52b1d7b431".to_string()),
            typ: Some("JWT".to_string()),
            ..Default::default()
        };

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&["https://token.actions.githubusercontent.com"]);
        validation.set_audience(&["https://github.com/angos"]);
        validation.leeway = 60;
        validation.validate_exp = true;
        validation.validate_nbf = true;

        assert!(validation.algorithms.contains(&Algorithm::RS256));
        assert_eq!(validation.algorithms.len(), 1);

        assert!(validation.iss.is_some());
        assert!(validation.aud.is_some());
        assert_eq!(validation.leeway, 60);
    }

    #[tokio::test]
    async fn test_fetch_jwks_with_explicit_uri() {
        let mock_server = MockServer::start().await;

        let jwks_response = json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": "test-key-id",
                "n": "xGOr-H7A-PWz8-H7A",
                "e": "AQAB"
            }]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, &*cache).await;

        assert!(result.is_ok());
        let jwks = result.unwrap();
        assert_eq!(jwks.keys.len(), 1);
    }

    #[tokio::test]
    async fn test_fetch_jwks_with_discovery() {
        let mock_server = MockServer::start().await;

        let oidc_config = json!({
            "issuer": mock_server.uri(),
            "jwks_uri": format!("{}/.well-known/jwks", mock_server.uri())
        });

        let jwks_response = json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": "test-key-id",
                "n": "xGOr-H7A-PWz8-H7A",
                "e": "AQAB"
            }]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&oidc_config))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, &*cache).await;

        assert!(result.is_ok());
        let jwks = result.unwrap();
        assert_eq!(jwks.keys.len(), 1);
    }

    #[tokio::test]
    async fn test_fetch_jwks_uses_cache() {
        let mock_server = MockServer::start().await;

        let jwks_response = json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": "test-key-id",
                "n": "xGOr-H7A-PWz8-H7A",
                "e": "AQAB"
            }]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result1 = fetch_jwks(&provider, &client, &*cache).await;
        assert!(result1.is_ok());

        let result2 = fetch_jwks(&provider, &client, &*cache).await;
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_jwks_http_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, &*cache).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fetch_oidc_configuration_success() {
        let mock_server = MockServer::start().await;

        let oidc_config = json!({
            "issuer": mock_server.uri(),
            "jwks_uri": format!("{}/.well-known/jwks", mock_server.uri()),
            "authorization_endpoint": format!("{}/authorize", mock_server.uri())
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&oidc_config))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_oidc_configuration(&provider, &client, &*cache).await;

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.issuer, mock_server.uri());
        assert_eq!(
            config.jwks_uri,
            format!("{}/.well-known/jwks", mock_server.uri())
        );
    }

    #[tokio::test]
    async fn test_fetch_oidc_configuration_uses_cache() {
        let mock_server = MockServer::start().await;

        let oidc_config = json!({
            "issuer": mock_server.uri(),
            "jwks_uri": format!("{}/.well-known/jwks", mock_server.uri())
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&oidc_config))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result1 = fetch_oidc_configuration(&provider, &client, &*cache).await;
        assert!(result1.is_ok());

        let result2 = fetch_oidc_configuration(&provider, &client, &*cache).await;
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_oidc_configuration_issuer_mismatch() {
        let mock_server = MockServer::start().await;

        let oidc_config = json!({
            "issuer": "https://wrong-issuer.com",
            "jwks_uri": format!("{}/.well-known/jwks", mock_server.uri())
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&oidc_config))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_oidc_configuration(&provider, &client, &*cache).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("mismatch"));
            }
            _ => panic!("Expected Unauthorized error"),
        }
    }

    #[tokio::test]
    async fn test_fetch_oidc_configuration_http_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: None,
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_oidc_configuration(&provider, &client, &*cache).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_oidc_token_success() {
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

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
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

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_ok());
        let oidc_claims = result.unwrap();
        assert_eq!(oidc_claims.provider_name, "test-provider");
        assert_eq!(oidc_claims.provider_type, "Generic OIDC");
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "test-user");
    }

    #[tokio::test]
    async fn test_validate_oidc_token_invalid_signature() {
        let mock_server = MockServer::start().await;
        let (_, public_key) = create_rsa_keypair();
        let (wrong_private_key, _) = create_rsa_keypair();
        let jwk = rsa_public_key_to_jwk(&public_key);

        let jwks_response = json!({
            "keys": [jwk]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(wrong_private_key.as_bytes()).unwrap(),
        )
        .unwrap();

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("validation failed"));
            }
            _ => panic!("Expected Unauthorized error"),
        }
    }

    #[tokio::test]
    async fn test_validate_oidc_token_expired() {
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

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() - chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert(
            "iat".to_string(),
            json!((chrono::Utc::now() - chrono::Duration::hours(2)).timestamp()),
        );

        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap(),
        )
        .unwrap();

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("validation failed"));
            }
            _ => panic!("Expected Unauthorized error"),
        }
    }

    #[tokio::test]
    async fn test_validate_oidc_token_wrong_issuer() {
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

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!("https://wrong-issuer.com"));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
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

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_oidc_token_wrong_audience() {
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

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("wrong-audience"));
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

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_oidc_token_missing_kid() {
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

        let header = Header::new(Algorithm::RS256);

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
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

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("No matching key"));
            }
            _ => panic!("Expected Unauthorized error"),
        }
    }

    #[tokio::test]
    async fn test_validate_oidc_token_no_audience_validation() {
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

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
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

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, &*cache).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_oidc_token_invalid_jwt_format() {

        let mock_server = MockServer::start().await;

        let jwks_response = json!({ "keys": [] });
        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&jwks_response))
            .mount(&mock_server)
            .await;

        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = validate_oidc_token(
            "test-provider",
            &provider,
            "not-a-valid-jwt",
            &client,
            &*cache,
        )
        .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("Failed to decode JWT header"));
            }
            _ => panic!("Expected Unauthorized error"),
        }
    }

    // RSA-2048 PKCS8 private key (test-only).
    const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
            MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1ZWEn1DBX9KCN\n\
            BrYXJ86eBdk2GPlqxyFuJHVnLaia+Y4Ndue7GIEjSrLTx0FOwR115kobIyLUxJMw\n\
            J1PP65hVDDZtJf9bXNTBZR2swCP6qBuUOdaZHnDbvgs5qI3JCm4qO8VpLI5FapWo\n\
            uhuk/YW+h2ok5L0ZNsOlJ9PWksl9L/mioMWKmvKSiYPIIRPqpURhPTHRRP2om3T4\n\
            K11WgVhwxJZ9ApCC3B5LfE+eQW5Qh44CplRatNBnS7taupeGC/mQoZ74B1Drt1jz\n\
            moFy7ih7AyOSoMhJVztZ7iqzKlL/xZDDVkb36MluMZoD3ju/kthkXEZ5gAUUgq4g\n\
            xDtztbnpAgMBAAECggEAJCR5ATiUEVJakv7dVSvHTVEZAGBlgdL/ZDS7d71vNMG4\n\
            fhJNBchSIrgFfZDcAFcWCaHC7jlH/WDVeui7GFh216tBROonQQr0ETyWdw8pnA3W\n\
            wOftTED2d7IcBncBGSeM37ldEiGgj1A3VZEPZQZmmZndmlBBJT72KHgRC1Xf86M/\n\
            h2hxnGJeVhOsYENr9um2NHsW2EKB2GJtInMy4Krbl4ySE2Kl4HIl+CasYcF6dC73\n\
            oGjsuoOyxZYVCnaKHU4L1vbEqRp6nxSYfFIuR0M8ihtg77y+mFEmEmwUZLntnTRp\n\
            fjX+n0jWdJyakdgPqcOqorXKZSFbnkmQYnB0Bu7kqQKBgQDgIzf7Qfr3NAjGS8lV\n\
            DceixVVxqYIkY1HEHMHbj+aGp3DYmDbQG2y/M/cx8linJlXauPbyq9x9b/KUiPsP\n\
            w+CXtIkYdQGKsucKkWjkSgB9cJBA0d8C+UPStUYXm9DNBztt1/bEf75EBV31DmjH\n\
            MD9yyJTxkOFpY1+QmE9+4AI05wKBgQDPLrditvYciCS0yEu+MIupQEk9NS78qNX+\n\
            FT1owzzoPt/icCIG/82pn6S1XYgpRil4VCSss7zkLHXrU9qm7ueQ+FCaRqgRjWeB\n\
            2RCCWfMzWLaAgsXNhqkklWaoJo6HNA8zS/7rfK+QjkpZHleWUqRve2ISE8tU7Oij\n\
            +6M67C3wrwKBgQCBm7v+ffvqwNsmF6L3nP1JIYU0McoA0rHwjpSHK2IpkV+O0A46\n\
            LvGmax1Rc4tSNLfGv9iFIV5h5r9GpyNOzXztHMd+LgLTOnqhwM3/3M8FunagFPw5\n\
            kvxmNs6uTjripID4Fr8qh5f1a4kWcNuj+0FlVZnTSm0ebQlQ6tJlUpHUFwKBgEw0\n\
            CZ2UYBeEd8PKvBk7L9NT4txReHPUAcmPtGOZFAj0P1LBHbLnWZTfNCzFNzS/Kreo\n\
            c0jWX06pj8G7uPuXebLXsoXcISs7kGuxFCJtxUcIhS/laa27rvDWxshoThoqqsCa\n\
            XivtU4He5De9MkgHI5YhkqPFhg85iCPwhUxB3G/fAoGANeHenNFrYY7NVIgIU4//\n\
            /7TL+n7tTgo7K05RnMcDlN34ZkWZ90WLsFaDFAY0NndHd5ZH9NXeKMrebfHHiHRb\n\
            CqmT+3Bc8S+MAqHHwdjPl1TRhkIS5iAXWDkw++un2PtwJYJILWC3Xqz6d+FSS8FO\n\
            IHgjvvya3tp4E/ZbyQLAntc=\n\
            -----END PRIVATE KEY-----";

        // JWK n/e components that correspond to TEST_PRIVATE_KEY_PEM.
        // Derived from the SubjectPublicKeyInfo DER at offsets [33..289] and [291..294].
    const TEST_JWK_N: &str = "tWVhJ9QwV_SgjQa2FyfOngXZNhj5aschbiR1Zy2omvmODXbnuxiBI0qy08dBTsEddeZKGyMi1MSTMCdTz-uYVQw2bSX_W1zUwWUdrMAj-qgblDnWmR5w274LOaiNyQpuKjvFaSyORWqVqLobpP2FvodqJOS9GTbDpSfT1pLJfS_5oqDFiprykomDyCET6qVEYT0x0UT9qJt0-CtdVoFYcMSWfQKQgtweS3xPnkFuUIeOAqZUWrTQZ0u7WrqXhgv5kKGe-AdQ67dY85qBcu4oewMjkqDISVc7We4qsypS_8WQw1ZG9-jJbjGaA947v5LYZFxGeYAFFIKuIMQ7c7W56Q";
    const TEST_JWK_E: &str = "AQAB";
    const TEST_KID: &str = "unit-test-key-1";

    fn test_jwks() -> Jwks {
        Jwks {
            keys: vec![Jwk::Rsa {
                key_use: Some("sig".to_string()),
                kid: Some(TEST_KID.to_string()),
                alg: Some("RS256".to_string()),
                n: TEST_JWK_N.to_string(),
                e: TEST_JWK_E.to_string(),
            }],
        }
    }

    fn encoding_key() -> EncodingKey {
        EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes())
            .expect("hardcoded test key must parse")
    }

    fn make_token(claims: &HashMap<String, serde_json::Value>, kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &encoding_key()).expect("token encoding must succeed")
    }

    fn valid_claims(issuer: &str, audience: &str) -> HashMap<String, serde_json::Value> {
        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), json!(issuer));
        claims.insert("sub".to_string(), json!("unit-test-subject"));
        claims.insert("aud".to_string(), json!(audience));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));
        claims
    }

    struct TestProvider {
        issuer: String,
        audience: Option<String>,
        clock_skew: u64,
        claim_error: Option<String>,
    }

    impl TestProvider {
        fn new(issuer: &str, audience: Option<&str>) -> Self {
            Self {
                issuer: issuer.to_string(),
                audience: audience.map(str::to_string),
                clock_skew: 0,
                claim_error: None,
            }
        }

        fn with_claim_error(mut self, msg: &str) -> Self {
            self.claim_error = Some(msg.to_string());
            self
        }
    }

    impl OidcProvider for TestProvider {
        fn issuer(&self) -> &str {
            &self.issuer
        }

        fn jwks_uri(&self) -> Option<&str> {
            None
        }

        fn name(&self) -> &'static str {
            "Test"
        }

        fn jwks_refresh_interval(&self) -> u64 {
            3600
        }

        fn required_audience(&self) -> Option<&str> {
            self.audience.as_deref()
        }

        fn clock_skew_tolerance(&self) -> u64 {
            self.clock_skew
        }

        fn validate_provider_claims(
            &self,
            _claims: &HashMap<String, serde_json::Value>,
        ) -> Result<(), Error> {
            match &self.claim_error {
                Some(msg) => Err(Error::Unauthorized(msg.clone())),
                None => Ok(()),
            }
        }
    }

    #[test]
    fn verify_jwt_accepts_valid_token() {

        let issuer = "https://issuer.example.com";
        let audience = "my-audience";
        let provider = TestProvider::new(issuer, Some(audience));
        let jwks = test_jwks();
        let claims = valid_claims(issuer, audience);
        let token = make_token(&claims, TEST_KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let oidc = result.unwrap();
        assert_eq!(oidc.provider_name, "test-provider");
        assert_eq!(oidc.provider_type, "Test");
        assert_eq!(
            oidc.claims.get("sub").and_then(|v| v.as_str()),
            Some("unit-test-subject")
        );
    }

    #[test]
    fn verify_jwt_rejects_unknown_kid() {

        let issuer = "https://issuer.example.com";
        let provider = TestProvider::new(issuer, None);
        let jwks = test_jwks();
        let claims = valid_claims(issuer, "any");
        let token = make_token(&claims, "unknown-kid-that-is-not-in-jwks");

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert!(msg.contains("No matching key")),
            e => panic!("expected Unauthorized, got {e:?}"),
        }
    }

    #[test]
    fn verify_jwt_rejects_expired_token() {

        let issuer = "https://issuer.example.com";
        // clock_skew = 0 so even a 1-second-old exp is rejected
        let provider = TestProvider::new(issuer, None);
        let jwks = test_jwks();

        let mut claims = std::collections::HashMap::new();
        claims.insert("iss".to_string(), serde_json::json!(issuer));
        claims.insert("sub".to_string(), serde_json::json!("user"));
        claims.insert(
            "exp".to_string(),
            serde_json::json!((chrono::Utc::now() - chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert(
            "iat".to_string(),
            serde_json::json!((chrono::Utc::now() - chrono::Duration::hours(2)).timestamp()),
        );

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());
        let token = encode(&header, &claims, &encoding_key()).unwrap();

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_err(), "expected expired token to be rejected");
    }

    #[test]
    fn verify_jwt_rejects_wrong_issuer() {

        let provider = TestProvider::new("https://expected-issuer.example.com", None);
        let jwks = test_jwks();
        let claims = valid_claims("https://wrong-issuer.example.com", "any");
        let token = make_token(&claims, TEST_KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_err(), "expected wrong issuer to be rejected");
    }

    #[test]
    fn verify_jwt_rejects_wrong_audience() {

        let issuer = "https://issuer.example.com";
        let provider = TestProvider::new(issuer, Some("required-audience"));
        let jwks = test_jwks();
        let claims = valid_claims(issuer, "wrong-audience");
        let token = make_token(&claims, TEST_KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_err(), "expected wrong audience to be rejected");
    }

    #[test]
    fn verify_jwt_skips_audience_when_provider_has_none() {

        let issuer = "https://issuer.example.com";
        // required_audience = None → validate_aud is disabled
        let provider = TestProvider::new(issuer, None);
        let jwks = test_jwks();
        // Token has an audience claim, but the provider doesn't require a specific one
        let claims = valid_claims(issuer, "any-audience-value");
        let token = make_token(&claims, TEST_KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(
            result.is_ok(),
            "expected Ok when provider has no required audience, got {result:?}"
        );
    }

    #[test]
    fn verify_jwt_rejects_invalid_signature() {

        let issuer = "https://issuer.example.com";
        let provider = TestProvider::new(issuer, None);
        let jwks = test_jwks(); // contains public key for TEST_PRIVATE_KEY_PEM

        // Sign with a freshly generated, different key — signature will not match JWKS
        let (alt_private_key, _) = crate::auth::oidc::tests::create_rsa_keypair();
        let alt_encoding_key =
            EncodingKey::from_rsa_pem(alt_private_key.as_bytes()).expect("alt key must parse");

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string()); // kid matches, but signature won't verify
        let claims = valid_claims(issuer, "any");
        let token = encode(&header, &claims, &alt_encoding_key).unwrap();

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert!(
                msg.contains("validation failed"),
                "expected 'validation failed' in message, got: {msg}"
            ),
            e => panic!("expected Unauthorized, got {e:?}"),
        }
    }

    #[test]
    fn verify_jwt_rejects_malformed_header() {

        let provider = TestProvider::new("https://issuer.example.com", None);
        let jwks = test_jwks();

        let result = verify_jwt("not-a-valid-jwt", &jwks, "test-provider", &provider);

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert!(
                msg.contains("Failed to decode JWT header"),
                "expected header decode failure message, got: {msg}"
            ),
            e => panic!("expected Unauthorized, got {e:?}"),
        }
    }

    #[test]
    fn verify_jwt_propagates_provider_claim_validation_error() {

        let issuer = "https://issuer.example.com";
        let provider =
            TestProvider::new(issuer, None).with_claim_error("custom claim check failed");
        let jwks = test_jwks();
        let claims = valid_claims(issuer, "any");
        let token = make_token(&claims, TEST_KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert_eq!(msg, "custom claim check failed"),
            e => panic!("expected Unauthorized, got {e:?}"),
        }
    }
}
