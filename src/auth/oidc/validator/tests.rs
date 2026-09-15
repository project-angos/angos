use std::{collections::HashMap, fs, net::TcpListener, time::Duration};

use jsonwebtoken::{Algorithm, EncodingKey, Header, decode_header, encode};
use reqwest::Client;
use serde_json::json;
use tempfile::tempdir;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

use crate::{
    auth::Error,
    auth::oidc::{
        Config, Jwk,
        validator::{
            Jwks, OpenIdConfiguration, fetch_jwks, fetch_oidc_configuration, issuer_bearer_token,
            issuer_key, validate_oidc_token, verify_jwt_with_header,
        },
    },
    identity::OidcClaims,
    test_fixtures::{
        mocks::{mount_jwks, static_jwks_response},
        oidc::{KID, alt_private_key_pem, jwk_x, jwk_y, private_key_pem},
    },
};

pub fn build_test_provider_config(uri: &str) -> Config {
    Config {
        server_ca_bundle: None,
        client_certificate_bundle: None,
        client_private_key: None,
        bearer_token_file: None,
        issuer: uri.to_string(),
        jwks_uri: Some(format!("{uri}/.well-known/jwks")),
        required_claims: Vec::new(),
        jwks_refresh_interval: 3600,
        required_audience: Some("test-audience".to_string()),
        clock_skew_tolerance: 60,
        allowed_algorithms: vec![Algorithm::ES256],
        http_request_timeout_secs: 30,
        jwks_refresh_timeout_secs: 5,
    }
}

fn verify_jwt(
    token: &str,
    jwks: &Jwks,
    provider_name: &str,
    provider: &Config,
) -> Result<OidcClaims, Error> {
    let header = decode_header(token)
        .map_err(|e| Error::Unauthorized(format!("Failed to decode JWT header: {e}")))?;
    verify_jwt_with_header(token, &header, jwks, provider_name, provider)
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

    mount_jwks(&mock_server, jwks_response).await;

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result = fetch_jwks(&provider, &client, cache.as_ref(), true).await;

    assert!(result.is_ok());
    let jwks = result.unwrap();
    assert_eq!(jwks.value.keys.len(), 1);
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

    mount_jwks(&mock_server, jwks_response).await;

    let provider = Config {
        jwks_uri: None,
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result = fetch_jwks(&provider, &client, cache.as_ref(), true).await;

    assert!(result.is_ok());
    let jwks = result.unwrap();
    assert_eq!(jwks.value.keys.len(), 1);
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

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result1 = fetch_jwks(&provider, &client, cache.as_ref(), true).await;
    assert!(result1.is_ok());

    let result2 = fetch_jwks(&provider, &client, cache.as_ref(), true).await;
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

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result = fetch_jwks(&provider, &client, cache.as_ref(), true).await;

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

    let provider = Config {
        jwks_uri: None,
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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

    let provider = Config {
        jwks_uri: None,
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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

    let provider = Config {
        jwks_uri: None,
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result = fetch_oidc_configuration(&provider, &client, cache.as_ref()).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Unauthorized(msg) => {
            assert!(msg.contains("mismatch"));
        }
        _ => panic!("Expected Unauthorized error"),
    }

    let cached = cache
        .retrieve::<OpenIdConfiguration>(&issuer_key(&provider, "config"))
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

    let provider = Config {
        jwks_uri: None,
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&url)
    };

    let client = Client::builder()
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result = fetch_jwks(&provider, &client, cache.as_ref(), true).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        Error::ProviderUnavailable(msg) => assert!(msg.contains("Failed to fetch URL")),
        err => panic!("Expected ProviderUnavailable error, got {err:?}"),
    }
}

/// An issuer that says nothing is remembered for the cooldown, so a cold cache
/// during an outage costs one connection attempt rather than one per request.
#[tokio::test]
async fn test_fetch_jwks_silence_is_remembered_for_the_cooldown() {
    // A port nothing listens on: the refused connection is silence.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&url)
    };
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let first = fetch_jwks(&provider, &client, cache.as_ref(), true).await;
    assert!(
        matches!(&first, Err(Error::ProviderUnavailable(msg)) if msg.contains("Failed to fetch URL")),
        "{first:?}"
    );

    let second = fetch_jwks(&provider, &client, cache.as_ref(), true).await;
    assert!(
        matches!(&second, Err(Error::ProviderUnavailable(msg)) if msg.contains("did not answer")),
        "the second fetch must fail from the marker, not from its own attempt: {second:?}"
    );
}

/// A refusal is an answer, so it is never remembered: two provider entries can
/// share an issuer, and a 401 to the one carrying no bearer token must leave
/// the one that does still able to fetch.
#[tokio::test]
async fn test_fetch_jwks_refusal_is_not_remembered() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/jwks"))
        .respond_with(ResponseTemplate::new(503))
        .expect(2)
        .mount(&mock_server)
        .await;

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    for _ in 0..2 {
        let result = fetch_jwks(&provider, &client, cache.as_ref(), true).await;
        assert!(
            matches!(&result, Err(Error::ProviderUnavailable(msg)) if msg.contains("HTTP 503")),
            "{result:?}"
        );
    }
}

#[tokio::test]
async fn test_validate_oidc_token_success() {
    let mock_server = MockServer::start().await;

    mount_jwks(&mock_server, static_jwks_response()).await;

    let mut claims = valid_claims(&mock_server.uri(), "test-audience");
    claims.insert("sub".to_string(), json!("test-user"));

    let token = make_token(&claims, KID);

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    assert!(result.is_ok());
    let oidc_claims = result.unwrap();
    assert_eq!(oidc_claims.provider_name, "test-provider");
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

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        .store(&issuer_key(&provider, "jwks"), &stale_jwks, 3600)
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
        .retrieve::<Jwks>(&issuer_key(&provider, "jwks"))
        .await
        .unwrap()
        .unwrap();
    assert!(cached_jwks.keys.iter().any(|key| key.kid() == Some(KID)));
}

/// An unknown kid forces a refetch, so without a cooldown a client sending
/// random kids drives one outbound `IdP` fetch per request. The second attempt
/// must be served from the cached JWKS and fail on the missing key.
#[tokio::test]
async fn unknown_kids_cost_one_jwks_fetch_per_cooldown_not_one_per_request() {
    let mock_server = MockServer::start().await;

    // Two unknown-kid requests, but the JWKS may be fetched only once.
    Mock::given(method("GET"))
        .and(path("/.well-known/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        .store(&issuer_key(&provider, "jwks"), &stale_jwks, 3600)
        .await
        .unwrap();

    let claims = valid_claims(&mock_server.uri(), "test-audience");
    for kid in ["attacker-kid-1", "attacker-kid-2"] {
        let token = make_token(&claims, kid);
        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;
        assert!(
            matches!(result, Err(Error::Unauthorized(_))),
            "an unknown kid must be rejected, got {result:?}"
        );
    }
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

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let stale_jwks = Jwks { keys: Vec::new() };
    cache
        .store(&issuer_key(&provider, "jwks"), &stale_jwks, 3600)
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
    mount_jwks(&mock_server, static_jwks_response()).await;

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(KID.to_string());

    let claims = valid_claims(&mock_server.uri(), "test-audience");

    let alt_key =
        EncodingKey::from_ec_pem(alt_private_key_pem().as_bytes()).expect("alt key must parse");
    let token = encode(&header, &claims, &alt_key).unwrap();

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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
async fn test_validate_oidc_token_rejects_disallowed_algorithm() {
    let mock_server = MockServer::start().await;

    mount_jwks(&mock_server, static_jwks_response()).await;

    let claims = valid_claims(&mock_server.uri(), "test-audience");
    // The token is signed ES256 and the key resolves, so only the allowlist can
    // refuse it.
    let token = make_token(&claims, KID);
    let provider = Config {
        allowed_algorithms: vec![Algorithm::RS256],
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    match result.unwrap_err() {
        Error::Unauthorized(msg) => assert!(msg.contains("InvalidAlgorithm")),
        e => panic!("expected Unauthorized, got {e:?}"),
    }
}

#[tokio::test]
async fn test_validate_oidc_token_accepts_multi_family_algorithm_list() {
    let mock_server = MockServer::start().await;

    mount_jwks(&mock_server, static_jwks_response()).await;

    let claims = valid_claims(&mock_server.uri(), "test-audience");
    let token = make_token(&claims, KID);
    // An allowlist spanning two key families must still admit a token signed
    // with one of them.
    let provider = Config {
        allowed_algorithms: vec![Algorithm::RS256, Algorithm::ES256],
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let claims = validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref())
        .await
        .expect("ES256 token must validate under a mixed-family allowlist");
    assert_eq!(claims.claims.get("sub").unwrap(), "unit-test-subject");
}

#[tokio::test]
async fn test_validate_oidc_token_expired() {
    let mock_server = MockServer::start().await;

    mount_jwks(&mock_server, static_jwks_response()).await;

    let mut claims = valid_claims(&mock_server.uri(), "test-audience");
    claims.insert(
        "exp".to_string(),
        json!((chrono::Utc::now() - chrono::Duration::hours(1)).timestamp()),
    );
    claims.insert(
        "iat".to_string(),
        json!((chrono::Utc::now() - chrono::Duration::hours(2)).timestamp()),
    );

    let token = make_token(&claims, KID);

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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

    mount_jwks(&mock_server, static_jwks_response()).await;

    let claims = valid_claims("https://wrong-issuer.com", "test-audience");

    let token = make_token(&claims, KID);

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_oidc_token_wrong_audience() {
    let mock_server = MockServer::start().await;

    mount_jwks(&mock_server, static_jwks_response()).await;

    let claims = valid_claims(&mock_server.uri(), "wrong-audience");

    let token = make_token(&claims, KID);

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    assert!(result.is_err());
}

/// A token carrying no `aud` must not satisfy `required_audience`: the
/// operator asked for an audience, and an issuer that mints claim-less tokens
/// would otherwise hand out a registry identity.
#[tokio::test]
async fn test_validate_oidc_token_missing_audience_is_rejected() {
    let mock_server = MockServer::start().await;
    mount_jwks(&mock_server, static_jwks_response()).await;

    let mut claims = valid_claims(&mock_server.uri(), "test-audience");
    claims.remove("aud");
    let token = make_token(&claims, KID);

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    assert!(
        result.is_err(),
        "a token without `aud` must not pass the audience pin"
    );
}

/// The same for the issuer pin, which every provider carries.
#[tokio::test]
async fn test_validate_oidc_token_missing_issuer_is_rejected() {
    let mock_server = MockServer::start().await;
    mount_jwks(&mock_server, static_jwks_response()).await;

    let mut claims = valid_claims(&mock_server.uri(), "test-audience");
    claims.remove("iss");
    let token = make_token(&claims, KID);

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    assert!(
        result.is_err(),
        "a token without `iss` must not pass the issuer pin"
    );
}

#[tokio::test]
async fn test_validate_oidc_token_missing_kid() {
    let mock_server = MockServer::start().await;

    mount_jwks(&mock_server, static_jwks_response()).await;

    // No kid in header, so the validator must reject because JWKS keys require kid matching.
    let claims = valid_claims(&mock_server.uri(), "test-audience");

    let header = Header::new(Algorithm::ES256);
    let token = encode(&header, &claims, &encoding_key()).unwrap();

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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

    mount_jwks(&mock_server, static_jwks_response()).await;

    let mut claims = valid_claims(&mock_server.uri(), "unused");
    claims.remove("aud");

    let token = make_token(&claims, KID);

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let result =
        validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_oidc_token_invalid_jwt_format() {
    let mock_server = MockServer::start().await;

    let jwks_response = json!({ "keys": [] });
    mount_jwks(&mock_server, jwks_response).await;

    let provider = Config {
        required_audience: None,
        ..build_test_provider_config(&mock_server.uri())
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

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
    EncodingKey::from_ec_pem(private_key_pem().as_bytes()).expect("generated test key must parse")
}

pub fn make_token(claims: &HashMap<String, serde_json::Value>, kid: &str) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    encode(&header, claims, &encoding_key()).expect("token encoding must succeed")
}

pub fn valid_claims(issuer: &str, audience: &str) -> HashMap<String, serde_json::Value> {
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

/// A provider with no JWKS URI and no clock skew, for the tests that verify a
/// token against a JWKS they hold rather than one they fetch.
fn test_provider(issuer: &str, audience: Option<&str>) -> Config {
    Config {
        server_ca_bundle: None,
        client_certificate_bundle: None,
        client_private_key: None,
        bearer_token_file: None,
        issuer: issuer.to_string(),
        jwks_uri: None,
        required_claims: Vec::new(),
        jwks_refresh_interval: 3600,
        required_audience: audience.map(str::to_string),
        clock_skew_tolerance: 0,
        allowed_algorithms: vec![Algorithm::ES256],
        http_request_timeout_secs: 30,
        jwks_refresh_timeout_secs: 5,
    }
}

#[test]
fn verify_jwt_accepts_valid_token() {
    let issuer = "https://issuer.example.com";
    let audience = "my-audience";
    let provider = test_provider(issuer, Some(audience));
    let jwks = test_jwks();
    let claims = valid_claims(issuer, audience);
    let token = make_token(&claims, KID);

    let result = verify_jwt(&token, &jwks, "test-provider", &provider);

    assert!(result.is_ok(), "expected Ok, got {result:?}");
    let oidc = result.unwrap();
    assert_eq!(oidc.provider_name, "test-provider");
    assert_eq!(
        oidc.claims.get("sub").and_then(|v| v.as_str()),
        Some("unit-test-subject")
    );
}

#[test]
fn verify_jwt_rejects_unknown_kid() {
    let issuer = "https://issuer.example.com";
    let provider = test_provider(issuer, None);
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
    let provider = test_provider(issuer, None);
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
    let provider = test_provider("https://expected-issuer.example.com", None);
    let jwks = test_jwks();
    let claims = valid_claims("https://wrong-issuer.example.com", "any");
    let token = make_token(&claims, KID);

    let result = verify_jwt(&token, &jwks, "test-provider", &provider);

    assert!(result.is_err(), "expected wrong issuer to be rejected");
}

#[test]
fn verify_jwt_rejects_wrong_audience() {
    let issuer = "https://issuer.example.com";
    let provider = test_provider(issuer, Some("required-audience"));
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
    let provider = test_provider(issuer, None);
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
    let provider = test_provider(issuer, None);
    let jwks = test_jwks(); // contains public key for private_key_pem()

    // Sign with the alt key: kid matches, but signature won't verify against JWKS.
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
    let provider = test_provider("https://issuer.example.com", None);
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

/// `required_claims` is what replaced the GitHub provider's hard-coded
/// repository/actor check, so the rejection it used to perform is pinned here.
#[test]
fn verify_jwt_rejects_token_missing_a_required_claim() {
    let issuer = "https://issuer.example.com";
    let provider = Config {
        required_claims: vec!["repository".to_string(), "actor".to_string()],
        ..test_provider(issuer, None)
    };
    let jwks = test_jwks();
    let mut claims = valid_claims(issuer, "any");
    claims.insert("repository".to_string(), json!("myorg/myapp"));
    let token = make_token(&claims, KID);

    let result = verify_jwt(&token, &jwks, "test-provider", &provider);

    match result.unwrap_err() {
        Error::Unauthorized(msg) => assert_eq!(msg, "token is missing required claim 'actor'"),
        e => panic!("expected Unauthorized, got {e:?}"),
    }
}

#[test]
fn verify_jwt_accepts_token_carrying_every_required_claim() {
    let issuer = "https://issuer.example.com";
    let provider = Config {
        required_claims: vec!["repository".to_string(), "actor".to_string()],
        ..test_provider(issuer, None)
    };
    let jwks = test_jwks();
    let mut claims = valid_claims(issuer, "any");
    claims.insert("repository".to_string(), json!("myorg/myapp"));
    claims.insert("actor".to_string(), json!("octocat"));
    let token = make_token(&claims, KID);

    let result = verify_jwt(&token, &jwks, "test-provider", &provider);

    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

/// A null claim carries no more identity than an absent one, so it must not
/// satisfy the requirement just by having the key present.
#[test]
fn verify_jwt_rejects_a_required_claim_present_but_null() {
    let issuer = "https://issuer.example.com";
    let provider = Config {
        required_claims: vec!["repository".to_string()],
        ..test_provider(issuer, None)
    };
    let jwks = test_jwks();
    let mut claims = valid_claims(issuer, "any");
    claims.insert("repository".to_string(), json!(null));
    let token = make_token(&claims, KID);

    let result = verify_jwt(&token, &jwks, "test-provider", &provider);

    match result.unwrap_err() {
        Error::Unauthorized(msg) => assert_eq!(msg, "token is missing required claim 'repository'"),
        e => panic!("expected Unauthorized, got {e:?}"),
    }
}

/// A token whose `nbf` (not-before) is in the future must be rejected.
/// The enforcement mechanism in `verify_jwt` is `validation.validate_nbf = true`.
#[test]
fn verify_jwt_rejects_future_nbf() {
    let issuer = "https://issuer.example.com";
    // clock_skew = 0 so a future nbf is not tolerated
    let provider = test_provider(issuer, None);
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
    let provider = test_provider(issuer, Some(audience));

    // Add a second EC key with a different kid as a decoy.
    // The x/y values below are from the JWK.rs test; they form a valid
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
    let provider = test_provider(issuer, None);
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

/// The token is read at fetch time rather than baked into the HTTP client, so
/// a Kubernetes projected token rotated in place stays usable.
#[test]
fn a_rotated_bearer_token_is_picked_up_on_the_next_fetch() {
    let material = tempdir().unwrap();
    let token_path = material.path().join("token");
    fs::write(&token_path, "first-token\n").unwrap();

    let provider = Config {
        bearer_token_file: Some(token_path.clone()),
        ..build_test_provider_config("https://kubernetes.default.svc")
    };
    let jwks_url = "https://kubernetes.default.svc/openid/v1/jwks";

    assert_eq!(
        issuer_bearer_token(&provider, jwks_url).unwrap(),
        Some("first-token".to_string())
    );

    fs::write(&token_path, "second-token\n").unwrap();
    assert_eq!(
        issuer_bearer_token(&provider, jwks_url).unwrap(),
        Some("second-token".to_string())
    );
}

/// `jwks_uri` comes out of the discovery document, so an issuer naming a host
/// of its own choosing must not be handed the token.
#[test]
fn a_bearer_token_never_leaves_the_issuer_origin() {
    let material = tempdir().unwrap();
    let token_path = material.path().join("token");
    fs::write(&token_path, "service-account-token").unwrap();

    let provider = Config {
        bearer_token_file: Some(token_path),
        ..build_test_provider_config("https://kubernetes.default.svc")
    };

    for url in [
        "https://keys.example.com/openid/v1/jwks",
        "https://kubernetes.default.svc:8443/openid/v1/jwks",
        "http://kubernetes.default.svc/openid/v1/jwks",
    ] {
        assert_eq!(
            issuer_bearer_token(&provider, url).unwrap(),
            None,
            "{url} is not the issuer's origin"
        );
    }
}

#[test]
fn an_unreadable_bearer_token_file_fails_the_fetch() {
    let provider = Config {
        bearer_token_file: Some("/nonexistent/token".into()),
        ..build_test_provider_config("https://kubernetes.default.svc")
    };

    let error = issuer_bearer_token(&provider, "https://kubernetes.default.svc/jwks").unwrap_err();
    assert!(
        matches!(&error, Error::ProviderUnavailable(msg) if msg.contains("/nonexistent/token")),
        "got: {error:?}"
    );
}

#[tokio::test]
async fn the_bearer_token_reaches_an_issuer_that_requires_it() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/jwks"))
        .and(header("Authorization", "Bearer service-account-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
        .mount(&mock_server)
        .await;

    let material = tempdir().unwrap();
    let token_path = material.path().join("token");
    fs::write(&token_path, "service-account-token").unwrap();

    let anonymous = build_test_provider_config(&mock_server.uri());
    let authenticated = Config {
        bearer_token_file: Some(token_path),
        ..anonymous.clone()
    };

    let client = Client::new();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    assert!(
        fetch_jwks(&anonymous, &client, cache.as_ref(), true)
            .await
            .is_err(),
        "the issuer serves its JWKS to authenticated callers only"
    );
    assert!(
        fetch_jwks(&authenticated, &client, cache.as_ref(), true)
            .await
            .is_ok()
    );
}
