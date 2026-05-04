use std::collections::HashMap;

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

    match cache.retrieve::<Jwks>(&cache_key).await {
        Ok(Some(cached)) => {
            debug!("Using cached JWKS for provider: {provider_name}");
            return Ok(cached);
        }
        Err(err) => warn!("OIDC JWKS cache retrieve failed for {provider_name}: {err}"),
        Ok(None) => {}
    }

    let jwks_url = get_jwks_url(provider, client, cache).await?;
    let jwks = query_json::<Jwks>(client, &jwks_url).await?;

    if let Err(err) = cache
        .store(&cache_key, &jwks, provider.jwks_refresh_interval())
        .await
    {
        warn!("OIDC JWKS cache store failed for {provider_name}: {err}");
    }
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

    match cache.retrieve::<OpenIdConfiguration>(&cache_key).await {
        Ok(Some(cached)) => {
            debug!("Using cached OIDC configuration");
            return Ok(cached);
        }
        Err(err) => warn!("OIDC configuration cache retrieve failed for {provider_name}: {err}"),
        Ok(None) => {}
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

    if let Err(err) = cache
        .store(&cache_key, &config, provider.jwks_refresh_interval())
        .await
    {
        warn!("OIDC configuration cache store failed for {provider_name}: {err}");
    }
    info!("Fetched OIDC configuration from {config_url}");
    Ok(config)
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use reqwest::Client;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::{
        auth::oidc::{
            OidcProvider,
            provider::{
                BaseConfig,
                generic::{Provider, ProviderConfig},
            },
        },
        cache,
        command::server::Error,
        test_fixtures::oidc::{KID, alt_private_key_pem, jwk_x, jwk_y, private_key_pem},
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

    /// Returns the JWKS JSON body for the `private_key_pem()` fixture.
    fn static_jwks_response() -> serde_json::Value {
        json!({
            "keys": [{
                "kty": "EC",
                "use": "sig",
                "kid": KID,
                "crv": "P-256",
                "x": jwk_x(),
                "y": jwk_y(),
                "alg": "ES256"
            }]
        })
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

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = make_token(&claims, KID);

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

        // JWKS advertises private_key_pem()'s public key; token is signed with the alt key.
        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(KID.to_string());

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let alt_key =
            EncodingKey::from_ec_pem(alt_private_key_pem().as_bytes()).expect("alt key must parse");
        let token = encode(&header, &claims, &alt_key).unwrap();

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

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

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

        let token = make_token(&claims, KID);

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

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!("https://wrong-issuer.com"));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = make_token(&claims, KID);

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

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("wrong-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = make_token(&claims, KID);

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

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        // No kid in header — validator must reject because JWKS keys require kid matching.
        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert("aud".to_string(), json!("test-audience"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let header = Header::new(Algorithm::ES256);
        let token = encode(&header, &claims, &encoding_key()).unwrap();

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

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .mount(&mock_server)
            .await;

        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("iss".to_string(), json!(mock_server.uri()));
        claims.insert("sub".to_string(), json!("test-user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));

        let token = make_token(&claims, KID);

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

    fn test_jwks() -> Jwks {
        Jwks {
            keys: vec![Jwk::Ec {
                key_use: Some("sig".to_string()),
                kid: Some(KID.to_string()),
                alg: Some("ES256".to_string()),
                x: jwk_x().to_string(),
                y: jwk_y().to_string(),
            }],
        }
    }

    pub fn encoding_key() -> EncodingKey {
        EncodingKey::from_ec_pem(private_key_pem().as_bytes())
            .expect("generated test key must parse")
    }

    pub fn make_token(claims: &HashMap<String, serde_json::Value>, kid: &str) -> String {
        let mut header = Header::new(Algorithm::ES256);
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
        base: BaseConfig,
        claim_error: Option<String>,
    }

    impl TestProvider {
        fn new(issuer: &str, audience: Option<&str>) -> Self {
            Self {
                base: BaseConfig {
                    issuer: issuer.to_string(),
                    jwks_uri: None,
                    jwks_refresh_interval: 3600,
                    required_audience: audience.map(str::to_string),
                    clock_skew_tolerance: 0,
                },
                claim_error: None,
            }
        }

        fn with_claim_error(mut self, msg: &str) -> Self {
            self.claim_error = Some(msg.to_string());
            self
        }
    }

    impl OidcProvider for TestProvider {
        fn base(&self) -> &BaseConfig {
            &self.base
        }

        fn name(&self) -> &'static str {
            "Test"
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
        let token = make_token(&claims, KID);

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

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(KID.to_string());
        let token = encode(&header, &claims, &encoding_key()).unwrap();

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_err(), "expected expired token to be rejected");
    }

    #[test]
    fn verify_jwt_rejects_wrong_issuer() {
        let provider = TestProvider::new("https://expected-issuer.example.com", None);
        let jwks = test_jwks();
        let claims = valid_claims("https://wrong-issuer.example.com", "any");
        let token = make_token(&claims, KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_err(), "expected wrong issuer to be rejected");
    }

    #[test]
    fn verify_jwt_rejects_wrong_audience() {
        let issuer = "https://issuer.example.com";
        let provider = TestProvider::new(issuer, Some("required-audience"));
        let jwks = test_jwks();
        let claims = valid_claims(issuer, "wrong-audience");
        let token = make_token(&claims, KID);

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
        let token = make_token(&claims, KID);

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
        let jwks = test_jwks(); // contains public key for private_key_pem()

        // Sign with the alt key — kid matches, but signature won't verify against JWKS.
        let alt_encoding_key =
            EncodingKey::from_ec_pem(alt_private_key_pem().as_bytes()).expect("alt key must parse");

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(KID.to_string());
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
        let token = make_token(&claims, KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert_eq!(msg, "custom claim check failed"),
            e => panic!("expected Unauthorized, got {e:?}"),
        }
    }

    /// A token whose `nbf` (not-before) is in the future must be rejected.
    /// The story calls this "future iat"; the actual enforcement mechanism in
    /// `verify_jwt` is `validation.validate_nbf = true`.
    #[test]
    fn verify_jwt_rejects_future_nbf() {
        let issuer = "https://issuer.example.com";
        // clock_skew = 0 so a future nbf is not tolerated
        let provider = TestProvider::new(issuer, None);
        let jwks = test_jwks();

        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), json!(issuer));
        claims.insert("sub".to_string(), json!("user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(2)).timestamp()),
        );
        claims.insert("iat".to_string(), json!(chrono::Utc::now().timestamp()));
        claims.insert(
            "nbf".to_string(),
            json!((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
        );

        let token = make_token(&claims, KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(result.is_err(), "expected future nbf token to be rejected");
    }

    /// When the JWKS contains multiple keys, `verify_jwt` must select the key
    /// whose `kid` matches the JWT header and successfully validate the token.
    #[test]
    fn verify_jwt_selects_correct_key_from_multi_key_jwks() {
        let issuer = "https://issuer.example.com";
        let audience = "my-audience";
        let provider = TestProvider::new(issuer, Some(audience));

        // Add a second EC key with a different kid as a decoy.
        // The x/y values below are from the JWK.rs test — they form a valid
        // P-256 public key so `to_decoding_key()` succeeds, but the kid won't
        // match KID and the signature won't verify with it.
        let decoy_x = "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4";
        let decoy_y = "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM";

        let jwks = Jwks {
            keys: vec![
                Jwk::Ec {
                    key_use: Some("sig".to_string()),
                    kid: Some("decoy-key".to_string()),
                    alg: Some("ES256".to_string()),
                    x: decoy_x.to_string(),
                    y: decoy_y.to_string(),
                },
                Jwk::Ec {
                    key_use: Some("sig".to_string()),
                    kid: Some(KID.to_string()),
                    alg: Some("ES256".to_string()),
                    x: jwk_x().to_string(),
                    y: jwk_y().to_string(),
                },
            ],
        };

        let claims = valid_claims(issuer, audience);
        let token = make_token(&claims, KID);

        let result = verify_jwt(&token, &jwks, "test-provider", &provider);

        assert!(
            result.is_ok(),
            "expected correct key to be found in multi-key JWKS, got {result:?}"
        );
        let oidc = result.unwrap();
        assert_eq!(
            oidc.claims.get("sub").and_then(|v| v.as_str()),
            Some("unit-test-subject")
        );
    }

    /// Custom claims present in the token payload must appear verbatim in
    /// `OidcClaims::claims`.  This mirrors real-world GitHub Actions tokens
    /// that carry fields like `repository`, `run_id`, and `job_workflow_ref`.
    #[test]
    fn verify_jwt_preserves_custom_claims() {
        let issuer = "https://token.actions.githubusercontent.com";
        let provider = TestProvider::new(issuer, None);
        let jwks = test_jwks();

        let mut claims = valid_claims(issuer, "any");
        claims.insert("repository".to_string(), json!("owner/repo"));
        claims.insert("run_id".to_string(), json!("12345678"));
        claims.insert(
            "job_workflow_ref".to_string(),
            json!("owner/repo/.github/workflows/ci.yml@refs/heads/main"),
        );

        let token = make_token(&claims, KID);

        let result = verify_jwt(&token, &jwks, "github-provider", &provider);

        assert!(
            result.is_ok(),
            "expected custom claims to be accepted, got {result:?}"
        );
        let oidc = result.unwrap();
        assert_eq!(
            oidc.claims.get("repository").and_then(|v| v.as_str()),
            Some("owner/repo")
        );
        assert_eq!(
            oidc.claims.get("run_id").and_then(|v| v.as_str()),
            Some("12345678")
        );
        assert_eq!(
            oidc.claims.get("job_workflow_ref").and_then(|v| v.as_str()),
            Some("owner/repo/.github/workflows/ci.yml@refs/heads/main")
        );
    }
}
