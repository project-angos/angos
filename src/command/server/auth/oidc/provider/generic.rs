use std::collections::HashMap;

use async_trait::async_trait;
use jsonwebtoken::{Validation, decode, decode_header};
use reqwest::{Client, header::ACCEPT};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{debug, info, warn};

use crate::{
    cache::{Cache, CacheExt},
    command::server::{
        auth::oidc::{Jwk, OidcProvider},
        error::Error,
        sha256_hash,
    },
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
    let header = decode_header(token)
        .map_err(|e| Error::Unauthorized(format!("Failed to decode JWT header: {e}")))?;

    // XXX: rewrite that log
    debug!(
        "JWT header: alg={:?}, kid={:?}, typ={:?}",
        header.alg, header.kid, header.typ
    );
    let jwks = fetch_jwks(provider, client, cache).await?;

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
    use jsonwebtoken::Algorithm;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        cache,
        command::server::auth::oidc::tests::{create_rsa_keypair, rsa_public_key_to_jwk},
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
        use jsonwebtoken::{EncodingKey, Header, encode};

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
}
