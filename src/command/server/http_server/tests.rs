use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, Version,
    password_hash::{SaltString, rand_core::OsRng},
};
use base64::{Engine, prelude::BASE64_STANDARD};
use http_body_util::BodyExt;
use hyper::{
    Method, Request, StatusCode,
    header::{
        AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, HeaderValue, RETRY_AFTER, WWW_AUTHENTICATE,
    },
};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use serde_json::{Value, from_slice};
use tracing_subscriber::{layer::SubscriberExt, registry::Registry as TracingRegistry};

use angos_transport::ResponseBody;

use crate::{
    auth::PeerCertificate,
    command::server::{
        ServerContext,
        error::Error,
        handlers::{handle_get_token, handle_healthz, handle_metrics},
        http_server::{
            connection::{current_trace_id, inject_peer_certificate, method_label},
            dispatch::{authenticate_and_authorize, handle_unknown_route},
            error_response::{error_to_response, fallback_500},
        },
        server_context::tests::{
            TestConfigOptions, create_test_server_context_from_config,
            create_test_server_context_with,
        },
    },
    identity::{Action, ClientIdentity},
    metrics_provider,
    policy::{AccessMode, AccessPolicyConfig},
    registry,
    test_fixtures::configuration::load_config,
};

#[test]
fn test_error_to_response_unauthorized_with_request_id() {
    let error = Error::Unauthorized("Invalid credentials".to_string());
    let request_id = Some("req-123".to_string());

    let response = error_to_response(&error, request_id.as_ref(), None);

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(
        response.headers().get(WWW_AUTHENTICATE).unwrap(),
        r#"Basic realm="Angos", charset="UTF-8""#
    );
}

#[tokio::test]
async fn test_error_to_response_from_registry_error() {
    let registry_error = registry::Error::BlobUnknown;
    let error: Error = registry_error.into();
    let request_id = Some("req-blob".to_string());

    let response = error_to_response(&error, request_id.as_ref(), None);

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let (_, body) = response.into_parts();
    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };
    let json: Value = from_slice(&body_bytes).unwrap();
    assert_eq!(json["errors"][0]["code"], "BLOB_UNKNOWN");
}

/// The collector's writer backoff clears in about a second, so the refusal
/// tells the client when to come back instead of leaving it to guess.
#[test]
fn reclamation_in_progress_answers_503_with_retry_after() {
    let error: Error = registry::Error::ReclamationInProgress("reclaiming".to_string()).into();

    let response = error_to_response(&error, None, None);

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "1");
}

/// Only the reclamation refusal is retryable on a schedule, so no other error
/// may advertise one.
#[test]
fn other_errors_carry_no_retry_after() {
    let error: Error = registry::Error::Conflict("locked".to_string()).into();

    let response = error_to_response(&error, None, None);

    assert!(response.headers().get(RETRY_AFTER).is_none());
}

#[test]
fn test_error_to_response_custom_error() {
    let error = Error::Custom {
        status_code: StatusCode::BAD_GATEWAY,
        code: "UPSTREAM_ERROR".to_string(),
        msg: Some("Failed to connect".to_string()),
    };
    let request_id = Some("req-custom".to_string());

    let response = error_to_response(&error, request_id.as_ref(), None);

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

/// A 401 relayed from a pull-through upstream is that registry's refusal, so
/// answering it with our own realm costs the client a token round trip that
/// cannot change the outcome.
#[test]
fn only_our_own_denial_carries_the_bearer_challenge() {
    let challenge = HeaderValue::from_static(
        r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#,
    );
    let upstream = Error::Custom {
        status_code: StatusCode::UNAUTHORIZED,
        code: "UNAUTHORIZED".to_string(),
        msg: Some("upstream refused".to_string()),
    };

    let relayed = error_to_response(&upstream, None, Some(challenge.clone()));
    assert_eq!(relayed.status(), StatusCode::UNAUTHORIZED);
    assert!(relayed.headers().get(WWW_AUTHENTICATE).is_none());

    let ours = error_to_response(
        &Error::Unauthorized("no credentials".to_string()),
        None,
        Some(challenge.clone()),
    );
    assert_eq!(ours.headers().get(WWW_AUTHENTICATE), Some(&challenge));
}

#[test]
fn test_handle_healthz_success() {
    let response = handle_healthz().unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
}

#[tokio::test]
async fn test_handle_healthz_body_content() {
    use http_body_util::BodyExt;

    let response = handle_healthz().unwrap();
    let (_, body) = response.into_parts();

    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };

    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(body_str, r#"{"status":"ok"}"#);
}

#[test]
fn test_handle_metrics_success() {
    metrics_provider::init_for_tests();
    let response = handle_metrics().unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(CONTENT_TYPE).is_some());
}

#[tokio::test]
async fn test_handle_metrics_contains_metric_data() {
    use http_body_util::BodyExt;

    metrics_provider::init_for_tests();
    let response = handle_metrics().unwrap();
    let (parts, body) = response.into_parts();

    let content_type = parts.headers.get(CONTENT_TYPE).unwrap().to_str().unwrap();
    assert!(content_type.contains("text/plain") || content_type.contains("application/json"));

    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };

    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(!body_str.is_empty());
}

#[test]
fn test_error_to_response_all_error_types() {
    let errors = vec![
        (
            Error::Unauthorized("msg".to_string()),
            StatusCode::UNAUTHORIZED,
            true,
        ),
        (
            Error::NotFound("msg".to_string()),
            StatusCode::NOT_FOUND,
            false,
        ),
        (
            Error::BadRequest("msg".to_string()),
            StatusCode::BAD_REQUEST,
            false,
        ),
        (
            Error::Internal("msg".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
        ),
        (
            Error::RangeNotSatisfiable("msg".to_string()),
            StatusCode::RANGE_NOT_SATISFIABLE,
            false,
        ),
        (
            Error::Initialization("msg".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
        ),
        (
            Error::Execution("msg".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
        ),
    ];

    for (error, expected_status, should_have_www_authenticate) in errors {
        let response = error_to_response(&error, None, None);

        assert_eq!(response.status(), expected_status);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );

        if should_have_www_authenticate {
            assert!(response.headers().get(WWW_AUTHENTICATE).is_some());
        } else {
            assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
        }
    }
}

#[tokio::test]
async fn test_error_to_response_body_contains_error_message() {
    use http_body_util::BodyExt;

    let error = Error::BadRequest("Invalid manifest format".to_string());
    let request_id = Some("req-manifest".to_string());

    let response = error_to_response(&error, request_id.as_ref(), None);
    let (_, body) = response.into_parts();

    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };

    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(body_str.contains("errors"));
}

#[test]
fn test_error_to_response_with_empty_message() {
    let error = Error::Internal(String::new());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref(), None);

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
}

/// The router reports a malformed reference and a path it does not serve as the
/// same `None`, so the method decides the status: a read missed, a write was
/// malformed. OCI conformance fails the registry when a manifest `PUT` carrying
/// an unparseable reference answers anything but `400`.
/// An unmatched path is answered before authentication, so an anonymous client
/// reaches the metrics recording at line rate. hyper admits any RFC 9110 token
/// as a method and the registry never evicts a child, so every token outside
/// the served set has to collapse onto one label.
#[test]
fn the_method_label_holds_to_the_served_methods() {
    for method in [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::PUT,
        Method::PATCH,
        Method::DELETE,
        Method::OPTIONS,
    ] {
        assert_eq!(method_label(&method), method.as_str());
    }

    for raw in ["FOO000001", "FOO000002", "CONNECT", "TRACE"] {
        let method = Method::from_bytes(raw.as_bytes()).expect("a token is a valid method");
        assert_eq!(
            method_label(&method),
            "other",
            "{raw} must not get its own series"
        );
    }
}

#[test]
fn unknown_route_status_follows_the_method() {
    for (method, expected_not_found) in [
        (Method::GET, true),
        (Method::HEAD, true),
        (Method::PUT, false),
        (Method::POST, false),
        (Method::DELETE, false),
    ] {
        let request = Request::builder()
            .method(method.clone())
            .uri("/v2/conformance/test/manifests/sha256:not-a-digest")
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        let Err(error) = handle_unknown_route(&parts) else {
            panic!("{method} on an unmatched path must be rejected, not served");
        };
        assert_eq!(
            matches!(error, Error::NotFound(_)),
            expected_not_found,
            "{method} on an unmatched path returned the wrong status: {error:?}"
        );
    }
}

/// containerd probes the `OAuth2` form of the token endpoint, `POST /token`,
/// before the plain `GET /token` angos serves, and falls back to the plain form
/// only on 401, 404 or 405. The catch-all answers 400 for a non-read method, so
/// without this the probe ends the exchange and every `buildctl` push fails with
/// `unsupported: unsupported route: POST /token`.
#[test]
fn token_endpoint_refuses_an_unserved_method_with_405() {
    for method in [Method::POST, Method::PUT, Method::DELETE] {
        let request = Request::builder()
            .method(method.clone())
            .uri("/token")
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        let Err(error) = handle_unknown_route(&parts) else {
            panic!("{method} /token must be rejected, not served");
        };
        assert_eq!(
            error.status_code(),
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} /token must be a method-not-allowed, got {error:?}"
        );

        // The status alone is what containerd reads, but a 405 has to name the
        // method that would work.
        let response = error_to_response(&error, None, None);
        assert_eq!(
            response.headers().get(hyper::header::ALLOW).unwrap(),
            "GET",
            "a 405 must advertise the method the path serves"
        );
    }
}

/// The 405 above is the token endpoint's alone: every other unserved write path
/// keeps the 400 conformance requires of a malformed manifest `PUT`.
#[test]
fn the_405_does_not_leak_to_other_unserved_write_paths() {
    let request = Request::builder()
        .method(Method::POST)
        .uri("/tokens")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let Err(error) = handle_unknown_route(&parts) else {
        panic!("an unmatched path must be rejected, not served");
    };
    assert_eq!(
        error.status_code(),
        StatusCode::BAD_REQUEST,
        "only /token answers 405, got {error:?}"
    );
}

/// A registry serving the referrers API must answer an invalid request with
/// `400` and never with the `404` an unserved read path gets, whichever part of
/// the request is malformed.
#[test]
fn unknown_referrers_route_is_a_bad_request() {
    let digest = format!("sha256:{}", "a".repeat(64));
    for uri in [
        "/v2/conformance/test/referrers/not-a-digest".to_string(),
        format!("/v2/conformance/test/referrers/{digest}?artifactType=not-a-media-type"),
        format!("/v2/conformance/test/referrers/{digest}?n=abc"),
    ] {
        let request = Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        let Err(error) = handle_unknown_route(&parts) else {
            panic!("{uri} must be rejected, not served");
        };
        assert_eq!(
            error.status_code(),
            StatusCode::BAD_REQUEST,
            "{uri} must be a bad request, got {error:?}"
        );
    }
}

/// A namespace that does not parse is a miss on every route, referrers
/// included: only the digest and the filter carry the `400`.
#[test]
fn unknown_referrers_route_with_a_bad_namespace_stays_a_miss() {
    let request = Request::builder()
        .method(Method::GET)
        .uri("/v2/BAD-NAMESPACE/referrers/not-a-digest")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let Err(error) = handle_unknown_route(&parts) else {
        panic!("an unmatched path must be rejected, not served");
    };
    assert!(
        matches!(error, Error::NotFound(_)),
        "an invalid namespace must stay a miss, got {error:?}"
    );
}

#[tokio::test]
async fn test_authenticate_and_authorize_returns_client_identity() {
    let context = create_test_context_with_allow_policy().await;

    let request = Request::builder().uri("/v2/").body(()).unwrap();
    let (parts, ()) = request.into_parts();
    let route = Action::ApiVersion;

    let result = authenticate_and_authorize(&context, &route, &parts).await;

    let identity: ClientIdentity = result.unwrap();
    assert!(identity.username.is_none());
}

#[tokio::test]
async fn bad_basic_auth_returns_http_401() {
    metrics_provider::init_for_tests();
    let salt = SaltString::generate(OsRng);
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default());
    let password_hash = argon.hash_password(b"testpass", &salt).unwrap().to_string();
    let config = load_config(&format!(
        r#"
        [global.access_policy]
        default = "allow"
        rules = []

        [auth.identity.testuser]
        username = "testuser"
        password = "{password_hash}"
    "#
    ));
    let context = create_test_server_context_from_config(&config).await;
    let credentials = BASE64_STANDARD.encode("testuser:wrongpass");
    let request = Request::builder()
        .uri("/v2/")
        .header(AUTHORIZATION, format!("Basic {credentials}"))
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let error = authenticate_and_authorize(&context, &Action::ApiVersion, &parts)
        .await
        .unwrap_err();
    let response = error_to_response(&error, None, None);

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(WWW_AUTHENTICATE).unwrap(),
        r#"Basic realm="Angos", charset="UTF-8""#
    );
}

/// The token endpoint is a route like any other. It grants nothing on its own,
/// but an operator who wants to refuse issuance must be able to say so.
#[tokio::test]
async fn the_token_endpoint_is_gated_by_the_access_policy() {
    let config = load_config(
        r#"
        [global.access_policy]
        default = "deny"
        rules = []

        [auth.token_service]
        secret_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;
    let request = Request::builder().uri("/token").body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let error = authenticate_and_authorize(&context, &Action::Token, &parts)
        .await
        .unwrap_err();

    assert_eq!(
        error_to_response(&error, None, None).status(),
        StatusCode::UNAUTHORIZED
    );
}

async fn token_service_context() -> ServerContext {
    let config = load_config(
        r#"
        [global.access_policy]
        default = "allow"
        rules = []

        [auth.token_service]
        secret_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
    "#,
    );
    create_test_server_context_from_config(&config).await
}

/// Renewal would let a token outlive the credential it was minted from for as
/// long as the client keeps asking, so `ttl_secs` would bound nothing.
#[tokio::test]
async fn a_registry_token_cannot_be_exchanged_for_another() {
    let context = token_service_context().await;
    let identity = ClientIdentity {
        username: Some("ci-bot".to_string()),
        from_registry_token: true,
        ..ClientIdentity::default()
    };

    let Err(error) = handle_get_token(
        context.token_issuer().expect("the test context issues"),
        &identity,
    ) else {
        panic!("a registry token must not be renewable");
    };

    assert!(matches!(error, Error::Unauthorized(_)));
}

/// The body is a bearer credential, and an anonymous exchange is a plain 200
/// JSON GET that a shared cache would otherwise be free to store.
#[tokio::test]
async fn an_issued_token_is_never_cached() {
    let context = token_service_context().await;

    let response = handle_get_token(
        context.token_issuer().expect("the test context issues"),
        &ClientIdentity::default(),
    )
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
}

async fn create_test_context_with_allow_policy() -> ServerContext {
    create_test_server_context_with(TestConfigOptions {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        }),
        ..TestConfigOptions::default()
    })
    .await
}

#[test]
fn current_trace_id_returns_none_without_otel_layer() {
    // Active tracing subscriber without any OpenTelemetry layer: span has no
    // OTel context bridge, so current_trace_id must return None.
    let subscriber = TracingRegistry::default();
    let result = tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("test_span");
        current_trace_id(&span)
    });
    assert_eq!(result, None);
}

#[test]
fn current_trace_id_returns_hex_id_with_otel_layer() {
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer("angos-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

    let trace_id = tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("test_span");
        current_trace_id(&span)
    });

    let trace_id = trace_id.expect("OTel-equipped subscriber must yield a trace ID");
    assert_eq!(
        trace_id.len(),
        32,
        "W3C trace ID is 32 hex chars, got {trace_id:?}"
    );
    assert!(
        trace_id.chars().all(|c| c.is_ascii_hexdigit()),
        "trace ID must be lowercase hex, got {trace_id:?}"
    );
}

#[test]
fn current_trace_id_returns_none_for_disabled_span() {
    // tracing::Span::none() creates a permanently-disabled (no-op) span.
    // Without an entered span there is no OTel bridge context, so
    // current_trace_id must return None regardless of the subscriber.
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer("angos-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    let trace_id = tracing::subscriber::with_default(subscriber, || {
        let span = tracing::Span::none();
        current_trace_id(&span)
    });
    assert_eq!(
        trace_id, None,
        "disabled no-op span must not produce a trace ID"
    );
}

#[test]
fn current_trace_id_child_span_inherits_parent_trace_id() {
    // The reason current_trace_id exists: requests log a single trace ID across
    // their entire span tree. A child span entered while the parent is active
    // must report the parent's trace ID, not a fresh one.
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer("angos-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    let (parent_id, child_id) = tracing::subscriber::with_default(subscriber, || {
        let parent = tracing::info_span!("parent");
        let _enter = parent.enter();
        let parent_id = current_trace_id(&parent);
        let child = tracing::info_span!("child");
        let child_id = current_trace_id(&child);
        (parent_id, child_id)
    });
    let parent_id = parent_id.expect("parent span must produce a trace ID");
    let child_id = child_id.expect("child span must inherit a trace ID");
    assert_eq!(
        parent_id, child_id,
        "child span entered under an active parent must inherit its trace ID"
    );
}

#[test]
fn inject_peer_certificate_skips_extension_when_cert_is_none() {
    let mut request = Request::builder().uri("/").body(()).unwrap();
    inject_peer_certificate(&mut request, None);
    assert!(request.extensions().get::<PeerCertificate>().is_none());
}

#[test]
fn inject_peer_certificate_inserts_extension_for_valid_cert() {
    let cert = b"-----BEGIN CERTIFICATE-----\nMIIBIjANBgkq\n-----END CERTIFICATE-----\n".as_slice();
    let mut request = Request::builder().uri("/").body(()).unwrap();
    inject_peer_certificate(&mut request, Some(cert));
    let stored = request
        .extensions()
        .get::<PeerCertificate>()
        .expect("PeerCertificate extension must be present");
    assert_eq!(stored.0.as_ref(), cert);
}

#[test]
fn inject_peer_certificate_inserts_extension_for_empty_cert() {
    // Empty Some(data) is still Some, so the extension is inserted.
    // Validation happens later in the mTLS authenticator, not here.
    let mut request = Request::builder().uri("/").body(()).unwrap();
    inject_peer_certificate(&mut request, Some(&[]));
    let stored = request
        .extensions()
        .get::<PeerCertificate>()
        .expect("PeerCertificate extension must be present even for empty cert");
    assert!(stored.0.as_ref().is_empty());
}

#[test]
fn inject_peer_certificate_last_write_wins_when_called_twice() {
    // Extensions::insert replaces any existing value of the same type.
    let first = b"first-cert-data".as_slice();
    let second = b"second-cert-data".as_slice();
    let mut request = Request::builder().uri("/").body(()).unwrap();
    inject_peer_certificate(&mut request, Some(first));
    inject_peer_certificate(&mut request, Some(second));
    let stored = request.extensions().get::<PeerCertificate>().unwrap();
    assert_eq!(stored.0.as_ref(), second);
}

#[test]
fn fallback_500_returns_valid_500_text_plain_response() {
    // Directly verify the fallback helper used by error_to_response when the
    // hyper builder records an error (e.g. a header value containing a control
    // byte).  The helper must always produce a well-formed response regardless
    // of external state.
    let response = fallback_500();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/plain",
        "fallback must set Content-Type: text/plain"
    );
}

#[tokio::test]
async fn fallback_500_body_is_ascii_internal_server_error() {
    use http_body_util::BodyExt;

    let response = fallback_500();
    let (_, body) = response.into_parts();

    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };

    assert_eq!(
        body_bytes.as_ref(),
        b"Internal Server Error",
        "fallback body must be the static ASCII string 'Internal Server Error'"
    );
}
