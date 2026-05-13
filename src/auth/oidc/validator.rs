use std::{collections::HashMap, time::Duration};

use jsonwebtoken::{Algorithm, Header, Validation, decode, decode_header};
use reqwest::{Client, header::ACCEPT};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::{
    auth::oidc::{Jwk, OidcProvider},
    cache::Cache,
    command::server::Error,
    identity::OidcClaims,
    util::sha256,
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

#[derive(Debug)]
struct FetchedJwks {
    jwks: Jwks,
    from_cache: bool,
}

struct CachedJson<T> {
    value: T,
    from_cache: bool,
}

const JWKS_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn validate_oidc_token(
    provider_name: &str,
    provider: &dyn OidcProvider,
    token: &str,
    client: &Client,
    cache: &Cache,
) -> Result<OidcClaims, Error> {
    let header = decode_header(token)
        .map_err(|e| Error::Unauthorized(format!("Failed to decode JWT header: {e}")))?;
    verify_allowed_algorithm(provider, header.alg)?;

    let mut fetched_jwks = fetch_jwks(provider, client, cache).await?;
    if fetched_jwks.from_cache && cached_jwks_misses_kid(&fetched_jwks.jwks, &header) {
        info!(
            "Cached JWKS for provider {} does not contain kid {:?}; refreshing",
            provider.name(),
            header.kid
        );
        fetched_jwks =
            fetch_jwks_with_cache(provider, client, cache, false, Some(JWKS_REFRESH_TIMEOUT))
                .await?;
    }

    verify_jwt_with_header(token, &header, &fetched_jwks.jwks, provider_name, provider)
}

fn verify_jwt_with_header(
    token: &str,
    header: &Header,
    jwks: &Jwks,
    provider_name: &str,
    provider: &dyn OidcProvider,
) -> Result<OidcClaims, Error> {
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

    let validation = build_validation(provider, header.alg);

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

fn verify_allowed_algorithm(provider: &dyn OidcProvider, alg: Algorithm) -> Result<(), Error> {
    if provider.allowed_algorithms().contains(&alg) {
        return Ok(());
    }
    Err(Error::Unauthorized(format!(
        "algorithm {alg:?} not allowed for provider {}",
        provider.name()
    )))
}

fn build_validation(provider: &dyn OidcProvider, alg: Algorithm) -> Validation {
    let mut validation = Validation::new(alg);
    validation.algorithms = provider.allowed_algorithms().to_vec();
    validation.set_issuer(&[provider.issuer()]);
    if let Some(aud) = provider.required_audience() {
        validation.set_audience(&[aud]);
    } else {
        validation.validate_aud = false;
    }
    validation.leeway = provider.clock_skew_tolerance();
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation
}

fn cached_jwks_misses_kid(jwks: &Jwks, header: &Header) -> bool {
    let Some(kid) = header.kid.as_deref() else {
        return false;
    };
    !jwks.keys.iter().any(|key| key.kid() == Some(kid))
}

async fn query_json<T>(
    client: &Client,
    url: &str,
    fetch_timeout: Option<Duration>,
) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let fetch = async {
        let response = client
            .get(url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| Error::ProviderUnavailable(format!("Failed to fetch URL {url}: {e}")))?;

        if !response.status().is_success() {
            let msg = format!("Failed to fetch URL {url}: HTTP {}", response.status());
            return Err(Error::ProviderUnavailable(msg));
        }

        response.json().await.map_err(|e| {
            Error::ProviderUnavailable(format!("Failed to parse JSON from {url}: {e}"))
        })
    };

    match fetch_timeout {
        Some(duration) => timeout(duration, fetch)
            .await
            .map_err(|_| Error::ProviderUnavailable(format!("Timed out fetching URL {url}")))?,
        None => fetch.await,
    }
}

async fn get_jwks_url(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &Cache,
    fetch_timeout: Option<Duration>,
) -> Result<String, Error> {
    if let Some(uri) = provider.jwks_uri() {
        return Ok(uri.to_string());
    }
    let oidc_config =
        fetch_oidc_configuration_with_timeout(provider, client, cache, fetch_timeout).await?;

    Ok(oidc_config.jwks_uri)
}

fn jwks_cache_key(provider: &dyn OidcProvider) -> String {
    let provider_name = provider.name();
    let issuer_hash = sha256::hex(provider.issuer());
    format!("oidc:{provider_name}:jwks:{issuer_hash}")
}

fn oidc_configuration_cache_key(provider: &dyn OidcProvider) -> String {
    let provider_name = provider.name();
    let issuer_hash = sha256::hex(provider.issuer());
    format!("oidc:{provider_name}:config:{issuer_hash}")
}

async fn fetch_cached_json<T, F>(
    client: &Client,
    cache: &Cache,
    cache_key: &str,
    url: &str,
    ttl: u64,
    read_cache: bool,
    fetch_timeout: Option<Duration>,
    validate_fresh: F,
) -> Result<CachedJson<T>, Error>
where
    T: DeserializeOwned + Serialize,
    F: FnOnce(&T) -> Result<(), Error>,
{
    if read_cache {
        match cache.retrieve::<T>(cache_key).await {
            Ok(Some(value)) => {
                debug!("Using cached OIDC JSON for {cache_key}");
                return Ok(CachedJson {
                    value,
                    from_cache: true,
                });
            }
            Err(err) => {
                warn!("OIDC cache retrieve failed for {cache_key}: {err}");
            }
            Ok(None) => {}
        }
    }

    let value = query_json::<T>(client, url, fetch_timeout).await?;
    validate_fresh(&value)?;

    if let Err(err) = cache.store(cache_key, &value, ttl).await {
        warn!("OIDC cache store failed for {cache_key}: {err}");
    }

    Ok(CachedJson {
        value,
        from_cache: false,
    })
}

async fn fetch_jwks(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &Cache,
) -> Result<FetchedJwks, Error> {
    fetch_jwks_with_cache(provider, client, cache, true, None).await
}

async fn fetch_jwks_with_cache(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &Cache,
    read_cache: bool,
    fetch_timeout: Option<Duration>,
) -> Result<FetchedJwks, Error> {
    let cache_key = jwks_cache_key(provider);
    let jwks_url = get_jwks_url(provider, client, cache, fetch_timeout).await?;
    let fetched = fetch_cached_json::<Jwks, _>(
        client,
        cache,
        &cache_key,
        &jwks_url,
        provider.jwks_refresh_interval(),
        read_cache,
        fetch_timeout,
        |_| Ok(()),
    )
    .await?;

    if !fetched.from_cache {
        info!("Fetched JWKS from {jwks_url}");
    }

    Ok(FetchedJwks {
        jwks: fetched.value,
        from_cache: fetched.from_cache,
    })
}

async fn fetch_oidc_configuration(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &Cache,
) -> Result<OpenIdConfiguration, Error> {
    fetch_oidc_configuration_with_timeout(provider, client, cache, None).await
}

async fn fetch_oidc_configuration_with_timeout(
    provider: &dyn OidcProvider,
    client: &Client,
    cache: &Cache,
    fetch_timeout: Option<Duration>,
) -> Result<OpenIdConfiguration, Error> {
    let cache_key = oidc_configuration_cache_key(provider);
    let config_url = format!("{}/.well-known/openid-configuration", provider.issuer());
    let fetched = fetch_cached_json::<OpenIdConfiguration, _>(
        client,
        cache,
        &cache_key,
        &config_url,
        provider.jwks_refresh_interval(),
        true,
        fetch_timeout,
        |config| validate_oidc_configuration(provider, config),
    )
    .await?;

    if !fetched.from_cache {
        info!("Fetched OIDC configuration from {config_url}");
    }
    Ok(fetched.value)
}

fn validate_oidc_configuration(
    provider: &dyn OidcProvider,
    config: &OpenIdConfiguration,
) -> Result<(), Error> {
    if config.issuer != provider.issuer() {
        return Err(Error::Unauthorized(format!(
            "OIDC configuration issuer mismatch: expected {}, got {}",
            provider.issuer(),
            config.issuer
        )));
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashMap, net::TcpListener, time::Duration};

    use jsonwebtoken::{Algorithm, EncodingKey, Header, decode_header, encode};
    use reqwest::Client;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        auth::oidc::{
            Jwk, OidcProvider,
            provider::{
                BaseConfig, HasBaseConfig,
                generic::{Provider, ProviderConfig},
            },
            validator::{
                Jwks, OpenIdConfiguration, fetch_jwks, fetch_oidc_configuration, jwks_cache_key,
                oidc_configuration_cache_key, validate_oidc_token, verify_allowed_algorithm,
                verify_jwt_with_header,
            },
        },
        cache,
        command::server::Error,
        identity::OidcClaims,
        test_fixtures::oidc::{KID, alt_private_key_pem, jwk_x, jwk_y, private_key_pem},
    };

    fn build_test_provider_config(uri: &str) -> ProviderConfig {
        ProviderConfig {
            issuer: uri.to_string(),
            jwks_uri: Some(format!("{uri}/.well-known/jwks")),
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
            allowed_algorithms: vec![Algorithm::ES256],
        }
    }

    fn verify_jwt(
        token: &str,
        jwks: &Jwks,
        provider_name: &str,
        provider: &dyn OidcProvider,
    ) -> Result<OidcClaims, Error> {
        let header = decode_header(token)
            .map_err(|e| Error::Unauthorized(format!("Failed to decode JWT header: {e}")))?;
        verify_allowed_algorithm(provider, header.alg)?;
        verify_jwt_with_header(token, &header, jwks, provider_name, provider)
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, cache.as_ref()).await;

        assert!(result.is_ok());
        let jwks = result.unwrap();
        assert_eq!(jwks.jwks.keys.len(), 1);
        assert!(!jwks.from_cache);
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, cache.as_ref()).await;

        assert!(result.is_ok());
        let jwks = result.unwrap();
        assert_eq!(jwks.jwks.keys.len(), 1);
        assert!(!jwks.from_cache);
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result1 = fetch_jwks(&provider, &client, cache.as_ref()).await;
        assert!(result1.is_ok());

        let result2 = fetch_jwks(&provider, &client, cache.as_ref()).await;
        assert!(result2.is_ok());
        assert!(result2.unwrap().from_cache);
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, cache.as_ref()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ProviderUnavailable(msg) => assert!(msg.contains("HTTP 500")),
            err => panic!("Expected ProviderUnavailable error, got {err:?}"),
        }
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_oidc_configuration(&provider, &client, cache.as_ref()).await;

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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result1 = fetch_oidc_configuration(&provider, &client, cache.as_ref()).await;
        assert!(result1.is_ok());

        let result2 = fetch_oidc_configuration(&provider, &client, cache.as_ref()).await;
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_oidc_configuration(&provider, &client, cache.as_ref()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("mismatch"));
            }
            _ => panic!("Expected Unauthorized error"),
        }

        let cached = cache
            .retrieve::<OpenIdConfiguration>(&oidc_configuration_cache_key(&provider))
            .await
            .unwrap();
        assert!(cached.is_none());
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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_oidc_configuration(&provider, &client, cache.as_ref()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ProviderUnavailable(msg) => assert!(msg.contains("HTTP 404")),
            err => panic!("Expected ProviderUnavailable error, got {err:?}"),
        }
    }

    #[tokio::test]
    async fn test_fetch_jwks_network_error_returns_provider_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let config = ProviderConfig {
            issuer: url.clone(),
            jwks_uri: Some(format!("{url}/.well-known/jwks")),
            jwks_refresh_interval: 3600,
            required_audience: None,
            clock_skew_tolerance: 60,
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = fetch_jwks(&provider, &client, cache.as_ref()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ProviderUnavailable(msg) => assert!(msg.contains("Failed to fetch URL")),
            err => panic!("Expected ProviderUnavailable error, got {err:?}"),
        }
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
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

        assert!(result.is_ok());
        let oidc_claims = result.unwrap();
        assert_eq!(oidc_claims.provider_name, "test-provider");
        assert_eq!(oidc_claims.provider_type, "Generic OIDC");
        assert_eq!(oidc_claims.claims.get("sub").unwrap(), "test-user");
    }

    #[tokio::test]
    async fn test_validate_oidc_token_refreshes_jwks_once_when_cached_kid_is_missing() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let stale_jwks = Jwks {
            keys: vec![Jwk::Ec {
                key_use: Some("sig".to_string()),
                kid: Some("old-kid".to_string()),
                alg: Some("ES256".to_string()),
                x: jwk_x().to_string(),
                y: jwk_y().to_string(),
            }],
        };
        cache
            .store(&jwks_cache_key(&provider), &stale_jwks, 3600)
            .await
            .unwrap();

        let claims = valid_claims(&mock_server.uri(), "test-audience");
        let token = make_token(&claims, KID);

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

        assert!(
            result.is_ok(),
            "expected rotated key to validate, got {result:?}"
        );
        let cached_jwks = cache
            .retrieve::<Jwks>(&jwks_cache_key(&provider))
            .await
            .unwrap()
            .unwrap();
        assert!(cached_jwks.keys.iter().any(|key| key.kid() == Some(KID)));
    }

    #[tokio::test]
    async fn test_validate_oidc_token_returns_unauthorized_when_refreshed_jwks_still_misses_kid() {
        let mock_server = MockServer::start().await;
        let refreshed_jwks = json!({
            "keys": [{
                "kty": "EC",
                "use": "sig",
                "kid": "different-kid",
                "crv": "P-256",
                "x": jwk_x(),
                "y": jwk_y(),
                "alg": "ES256"
            }]
        });

        Mock::given(method("GET"))
            .and(path("/.well-known/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(refreshed_jwks))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = Provider::new(build_test_provider_config(&mock_server.uri()));
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let stale_jwks = Jwks { keys: Vec::new() };
        cache
            .store(&jwks_cache_key(&provider), &stale_jwks, 3600)
            .await
            .unwrap();

        let claims = valid_claims(&mock_server.uri(), "test-audience");
        let token = make_token(&claims, KID);

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert!(msg.contains("No matching key")),
            err => panic!("expected Unauthorized, got {err:?}"),
        }
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
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Unauthorized(msg) => {
                assert!(msg.contains("validation failed"));
            }
            _ => panic!("Expected Unauthorized error"),
        }
    }

    #[tokio::test]
    async fn test_validate_oidc_token_rejects_disallowed_algorithm_before_jwks_fetch() {
        let mock_server = MockServer::start().await;
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
        let config = ProviderConfig {
            issuer: mock_server.uri(),
            jwks_uri: Some(format!("{}/.well-known/jwks", mock_server.uri())),
            jwks_refresh_interval: 3600,
            required_audience: Some("test-audience".to_string()),
            clock_skew_tolerance: 60,
            allowed_algorithms: vec![Algorithm::RS256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

        match result.unwrap_err() {
            Error::Unauthorized(msg) => assert!(msg.contains("algorithm ES256 not allowed")),
            e => panic!("expected Unauthorized, got {e:?}"),
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
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

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
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

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
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

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
            allowed_algorithms: vec![Algorithm::ES256],
        };

        let provider = Provider::new(config);
        let client = Client::new();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let result = validate_oidc_token(
            "test-provider",
            &provider,
            "not-a-valid-jwt",
            &client,
            cache.as_ref(),
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
                    allowed_algorithms: vec![Algorithm::ES256],
                },
                claim_error: None,
            }
        }

        fn with_claim_error(mut self, msg: &str) -> Self {
            self.claim_error = Some(msg.to_string());
            self
        }
    }

    impl HasBaseConfig for TestProvider {
        fn base_config(&self) -> &BaseConfig {
            &self.base
        }
    }

    impl OidcProvider for TestProvider {
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

        let mut claims = HashMap::new();
        claims.insert("iss".to_string(), json!(issuer));
        claims.insert("sub".to_string(), json!("user"));
        claims.insert(
            "exp".to_string(),
            json!((chrono::Utc::now() - chrono::Duration::hours(1)).timestamp()),
        );
        claims.insert(
            "iat".to_string(),
            json!((chrono::Utc::now() - chrono::Duration::hours(2)).timestamp()),
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
    /// The enforcement mechanism in `verify_jwt` is `validation.validate_nbf = true`.
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
