use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;

use chrono::{TimeDelta, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, decode_header, encode};
use reqwest::Client;
use serde_json::{Value, json};
use tempfile::tempdir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use angos_cache::{Cache, Config as CacheConfig};
use angos_mtls_client::ClientTls;

use crate::auth::Error;
use crate::auth::oidc::Config;
use crate::auth::oidc::jwk::Jwk;
use crate::auth::oidc::validator::{
    Jwks, JwksRead, OpenIdConfiguration, fetch_jwks, issuer_bearer_token, issuer_key,
    validate_oidc_token, verify_jwt_with_header,
};
use crate::test_fixtures::mocks::{mount_jwks, static_jwks_response};
use crate::test_fixtures::oidc::{KID, alt_private_key_pem, jwk_x, jwk_y, private_key_pem};

pub fn build_test_provider_config(uri: &str) -> Config {
    Config {
        tls: ClientTls::default(),
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

fn ec_key(kid: &str) -> Jwk {
    Jwk::Ec {
        key_use: Some("sig".to_string()),
        kid: Some(kid.to_string()),
        alg: Some("ES256".to_string()),
        x: jwk_x().to_string(),
        y: jwk_y().to_string(),
    }
}

/// A provider whose cached JWKS holds only a key the test tokens do not name.
async fn provider_with_stale_jwks(uri: &str, cache: &Cache) -> Config {
    let provider = build_test_provider_config(uri);
    let stale = Jwks {
        keys: vec![ec_key("old-kid")],
    };
    cache
        .store(&issuer_key(&provider, "jwks"), &stale, 3600)
        .await
        .unwrap();
    provider
}

/// A URL nothing listens on, so connecting to it is silence.
fn silent_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    url
}

#[tokio::test]
async fn test_fetch_jwks_uses_cache() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();

    let first = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache)
        .await
        .unwrap();
    assert!(!first.from_cache);
    assert_eq!(first.value.keys.len(), 1);
    let second = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache)
        .await
        .unwrap();
    assert!(second.from_cache);
}

/// Without `jwks_uri` the JWKS location comes from discovery, itself cached.
#[tokio::test]
async fn test_fetch_jwks_with_discovery() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": mock_server.uri(),
            "jwks_uri": format!("{}/.well-known/jwks", mock_server.uri())
        })))
        .expect(1)
        .mount(&mock_server)
        .await;
    mount_jwks(&mock_server, static_jwks_response()).await;

    let provider = Config {
        jwks_uri: None,
        ..build_test_provider_config(&mock_server.uri())
    };
    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();

    for _ in 0..2 {
        let jwks = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache)
            .await
            .unwrap();
        assert_eq!(jwks.value.keys.len(), 1);
    }
}

#[tokio::test]
async fn test_fetch_oidc_configuration_issuer_mismatch() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": "https://wrong-issuer.com",
            "jwks_uri": format!("{}/.well-known/jwks", mock_server.uri())
        })))
        .mount(&mock_server)
        .await;

    let provider = Config {
        jwks_uri: None,
        ..build_test_provider_config(&mock_server.uri())
    };
    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();

    let result = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache).await;
    assert!(
        matches!(&result, Err(Error::Unauthorized(msg)) if msg.contains("mismatch")),
        "{result:?}"
    );
    let cached = cache
        .retrieve::<OpenIdConfiguration>(&issuer_key(&provider, "config"))
        .await
        .unwrap();
    assert!(cached.is_none());
}

/// An issuer that says nothing is remembered for the cooldown, so a cold cache
/// during an outage costs one connection attempt rather than one per request.
#[tokio::test]
async fn test_fetch_jwks_silence_is_remembered_for_the_cooldown() {
    let provider = build_test_provider_config(&silent_url());
    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();

    let first = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache).await;
    assert!(
        matches!(&first, Err(Error::ProviderUnavailable(msg)) if msg.contains("Failed to fetch URL")),
        "{first:?}"
    );

    let second = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache).await;
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

    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();

    for _ in 0..2 {
        let result = fetch_jwks(&provider, &client, cache.as_ref(), JwksRead::PreferCache).await;
        assert!(
            matches!(&result, Err(Error::ProviderUnavailable(msg)) if msg.contains("HTTP 503")),
            "{result:?}"
        );
    }
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

    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();
    let provider = provider_with_stale_jwks(&mock_server.uri(), &cache).await;
    let token = make_token(&valid_claims(&mock_server.uri(), "test-audience"), KID);

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
    Mock::given(method("GET"))
        .and(path("/.well-known/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(static_jwks_response()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();
    let provider = provider_with_stale_jwks(&mock_server.uri(), &cache).await;

    let claims = valid_claims(&mock_server.uri(), "test-audience");
    for kid in ["attacker-kid-1", "attacker-kid-2"] {
        let token = make_token(&claims, kid);
        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;
        assert!(
            matches!(&result, Err(Error::Unauthorized(msg)) if msg.contains("No matching key")),
            "an unknown kid must be rejected, got {result:?}"
        );
    }
}

/// The token is signed ES256 and its key resolves, so only the allowlist
/// decides; an allowlist spanning two key families still admits one of them.
#[tokio::test]
async fn the_algorithm_allowlist_decides() {
    let mock_server = MockServer::start().await;
    mount_jwks(&mock_server, static_jwks_response()).await;
    let token = make_token(&valid_claims(&mock_server.uri(), "test-audience"), KID);
    let client = Client::new();

    for (allowed, accepted) in [
        (vec![Algorithm::RS256], false),
        (vec![Algorithm::RS256, Algorithm::ES256], true),
    ] {
        let provider = Config {
            allowed_algorithms: allowed,
            ..build_test_provider_config(&mock_server.uri())
        };
        let cache = CacheConfig::Memory.to_backend().unwrap();
        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;
        if accepted {
            assert_eq!(result.unwrap().claims["sub"], "unit-test-subject");
        } else {
            assert!(
                matches!(&result, Err(Error::Unauthorized(msg)) if msg.contains("InvalidAlgorithm")),
                "{result:?}"
            );
        }
    }
}

/// `decode` checks `iss` and `aud` only when the token carries them, so a
/// token omitting a pinned claim must still be refused.
#[tokio::test]
async fn a_token_missing_a_pinned_claim_is_rejected() {
    let mock_server = MockServer::start().await;
    mount_jwks(&mock_server, static_jwks_response()).await;
    let provider = build_test_provider_config(&mock_server.uri());
    let client = Client::new();
    let cache = CacheConfig::Memory.to_backend().unwrap();

    for pinned in ["aud", "iss"] {
        let mut claims = valid_claims(&mock_server.uri(), "test-audience");
        claims.remove(pinned);
        let token = make_token(&claims, KID);
        let result =
            validate_oidc_token("test-provider", &provider, &token, &client, cache.as_ref()).await;
        assert!(
            result.is_err(),
            "a token without `{pinned}` must be refused"
        );
    }
}

#[tokio::test]
async fn test_validate_oidc_token_invalid_jwt_format() {
    let provider = build_test_provider_config("https://issuer.example.com");
    let cache = CacheConfig::Memory.to_backend().unwrap();

    let result = validate_oidc_token(
        "test-provider",
        &provider,
        "not-a-valid-jwt",
        &Client::new(),
        cache.as_ref(),
    )
    .await;

    assert!(
        matches!(&result, Err(Error::Unauthorized(msg)) if msg.contains("Failed to decode JWT header")),
        "{result:?}"
    );
}

pub fn encoding_key() -> EncodingKey {
    EncodingKey::from_ec_pem(private_key_pem().as_bytes()).expect("generated test key must parse")
}

pub fn make_token(claims: &HashMap<String, Value>, kid: &str) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    encode(&header, claims, &encoding_key()).expect("token encoding must succeed")
}

pub fn valid_claims(issuer: &str, audience: &str) -> HashMap<String, Value> {
    let mut claims = HashMap::new();
    claims.insert("iss".to_string(), json!(issuer));
    claims.insert("sub".to_string(), json!("unit-test-subject"));
    claims.insert("aud".to_string(), json!(audience));
    claims.insert(
        "exp".to_string(),
        json!((Utc::now() + TimeDelta::hours(1)).timestamp()),
    );
    claims.insert("iat".to_string(), json!(Utc::now().timestamp()));
    claims
}

const ISSUER: &str = "https://issuer.example.com";

/// A provider verifying against a JWKS the test holds, with no clock skew so a
/// one-second-old `exp` or future `nbf` is refused.
fn test_provider(audience: Option<&str>, required_claims: &[&str]) -> Config {
    Config {
        jwks_uri: None,
        required_claims: required_claims.iter().map(ToString::to_string).collect(),
        required_audience: audience.map(str::to_string),
        clock_skew_tolerance: 0,
        ..build_test_provider_config(ISSUER)
    }
}

fn claims_with(changes: &[(&str, Value)]) -> HashMap<String, Value> {
    let mut claims = valid_claims(ISSUER, "any");
    for (name, value) in changes {
        claims.insert((*name).to_string(), value.clone());
    }
    claims
}

#[test]
fn verify_jwt_rejects_invalid_tokens() {
    let hour = TimeDelta::hours(1);
    let alt_key = EncodingKey::from_ec_pem(alt_private_key_pem().as_bytes()).unwrap();
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(KID.to_string());

    let cases = [
        (
            "expired",
            test_provider(None, &[]),
            make_token(
                &claims_with(&[("exp", json!((Utc::now() - hour).timestamp()))]),
                KID,
            ),
        ),
        (
            "not yet valid",
            test_provider(None, &[]),
            make_token(
                &claims_with(&[("nbf", json!((Utc::now() + hour).timestamp()))]),
                KID,
            ),
        ),
        (
            "wrong issuer",
            test_provider(None, &[]),
            make_token(
                &claims_with(&[("iss", json!("https://wrong-issuer.example.com"))]),
                KID,
            ),
        ),
        (
            "wrong audience",
            test_provider(Some("required-audience"), &[]),
            make_token(&claims_with(&[]), KID),
        ),
        (
            "signed by another key under our kid",
            test_provider(None, &[]),
            encode(&header, &claims_with(&[]), &alt_key).unwrap(),
        ),
        (
            "missing a required claim",
            test_provider(None, &["repository", "actor"]),
            make_token(&claims_with(&[("repository", json!("myorg/myapp"))]), KID),
        ),
        // A null claim carries no more identity than an absent one.
        (
            "required claim null",
            test_provider(None, &["repository"]),
            make_token(&claims_with(&[("repository", json!(null))]), KID),
        ),
    ];

    let jwks = Jwks {
        keys: vec![ec_key(KID)],
    };
    for (reason, provider, token) in cases {
        let header = decode_header(&token).unwrap();
        let result = verify_jwt_with_header(&token, &header, &jwks, "test-provider", &provider);
        assert!(
            matches!(result, Err(Error::Unauthorized(_))),
            "{reason}: {result:?}"
        );
    }
}

/// The key is picked by `kid` among several, the audience is unchecked when
/// the provider pins none, and every required claim present passes.
#[test]
fn verify_jwt_accepts_valid_tokens() {
    // A valid P-256 key under another kid, which must not be picked.
    let decoy = Jwk::Ec {
        key_use: Some("sig".to_string()),
        kid: Some("decoy-key".to_string()),
        alg: Some("ES256".to_string()),
        x: "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4".to_string(),
        y: "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM".to_string(),
    };
    let jwks = Jwks {
        keys: vec![decoy, ec_key(KID)],
    };

    for (provider, claims) in [
        (test_provider(Some("any"), &[]), claims_with(&[])),
        (test_provider(None, &[]), claims_with(&[])),
        (
            test_provider(None, &["repository", "actor"]),
            claims_with(&[
                ("repository", json!("myorg/myapp")),
                ("actor", json!("octocat")),
            ]),
        ),
    ] {
        let token = make_token(&claims, KID);
        let header = decode_header(&token).unwrap();
        let oidc =
            verify_jwt_with_header(&token, &header, &jwks, "test-provider", &provider).unwrap();
        assert_eq!(oidc.claims["sub"], "unit-test-subject");
    }
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
        issuer_bearer_token(&provider, jwks_url).unwrap().as_deref(),
        Some("first-token")
    );
    fs::write(&token_path, "second-token\n").unwrap();
    assert_eq!(
        issuer_bearer_token(&provider, jwks_url).unwrap().as_deref(),
        Some("second-token")
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
    let cache = CacheConfig::Memory.to_backend().unwrap();

    assert!(
        fetch_jwks(&anonymous, &client, cache.as_ref(), JwksRead::PreferCache)
            .await
            .is_err(),
        "the issuer serves its JWKS to authenticated callers only"
    );
    assert!(
        fetch_jwks(
            &authenticated,
            &client,
            cache.as_ref(),
            JwksRead::PreferCache
        )
        .await
        .is_ok()
    );
}
