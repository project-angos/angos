use std::{collections::HashMap, time::Duration};

use jsonwebtoken::{Algorithm, Header, Validation, decode, decode_header};
use reqwest::{Client, header::ACCEPT};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::{
    auth::Error,
    auth::{
        oidc::{Jwk, OidcProvider},
        sha256_hex,
    },
    cache::Cache,
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

#[derive(Debug)]
struct FetchedJwks {
    jwks: Jwks,
    from_cache: bool,
}

struct CachedJson<T> {
    value: T,
    from_cache: bool,
}

struct CachedJsonRequest<'a> {
    client: &'a Client,
    cache: &'a Cache,
    cache_key: &'a str,
    url: &'a str,
    ttl: u64,
    read_cache: bool,
    fetch_timeout: Option<Duration>,
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
    let issuer_hash = sha256_hex(provider.issuer());
    format!("oidc:{provider_name}:jwks:{issuer_hash}")
}

fn oidc_configuration_cache_key(provider: &dyn OidcProvider) -> String {
    let provider_name = provider.name();
    let issuer_hash = sha256_hex(provider.issuer());
    format!("oidc:{provider_name}:config:{issuer_hash}")
}

async fn fetch_cached_json<T, F>(
    request: CachedJsonRequest<'_>,
    validate_fresh: F,
) -> Result<CachedJson<T>, Error>
where
    T: DeserializeOwned + Serialize,
    F: FnOnce(&T) -> Result<(), Error>,
{
    if request.read_cache {
        match request.cache.retrieve::<T>(request.cache_key).await {
            Ok(Some(value)) => {
                debug!("Using cached OIDC JSON for {}", request.cache_key);
                return Ok(CachedJson {
                    value,
                    from_cache: true,
                });
            }
            Err(err) => {
                warn!(
                    "OIDC cache retrieve failed for {}: {err}",
                    request.cache_key
                );
            }
            Ok(None) => {}
        }
    }

    let value = query_json::<T>(request.client, request.url, request.fetch_timeout).await?;
    validate_fresh(&value)?;

    if let Err(err) = request
        .cache
        .store(request.cache_key, &value, request.ttl)
        .await
    {
        warn!("OIDC cache store failed for {}: {err}", request.cache_key);
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
        CachedJsonRequest {
            client,
            cache,
            cache_key: &cache_key,
            url: &jwks_url,
            ttl: provider.jwks_refresh_interval(),
            read_cache,
            fetch_timeout,
        },
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

#[cfg(test)]
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
        CachedJsonRequest {
            client,
            cache,
            cache_key: &cache_key,
            url: &config_url,
            ttl: provider.jwks_refresh_interval(),
            read_cache: true,
            fetch_timeout,
        },
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
pub mod tests;
