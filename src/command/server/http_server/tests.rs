use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use http_body_util::BodyExt;
use hyper::header::{
    ALLOW, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, HeaderValue, WWW_AUTHENTICATE,
};
use hyper::{Method, Request, StatusCode};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::info_span;
use tracing::subscriber::with_default;
use tracing_opentelemetry::layer as otel_layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::Registry as TracingRegistry;

use angos_transport::ResponseBody;

use crate::command::server::ServerContext;
use crate::command::server::error::Error;
use crate::command::server::handlers::{handle_get_token, handle_healthz, handle_metrics};
use crate::command::server::http_server::connection::{current_trace_id, method_label};
use crate::command::server::http_server::dispatch::{
    authenticate_and_authorize, handle_unknown_route,
};
use crate::command::server::http_server::error_response::error_to_response;
use crate::command::server::server_context::tests::create_test_server_context_from_config;
use crate::identity::{Action, ClientIdentity};
use crate::metrics_provider;
use crate::test_fixtures::configuration::load_config;

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

#[tokio::test]
async fn test_handle_healthz_body_content() {
    let response = handle_healthz().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let ResponseBody::Fixed(body) = response.into_body() else {
        panic!("Expected Fixed body");
    };
    let body_bytes = body.collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes.as_ref(), br#"{"status":"ok"}"#);
}

#[tokio::test]
async fn test_handle_metrics_contains_metric_data() {
    metrics_provider::initialize_metrics();
    let response = handle_metrics().unwrap();
    let (parts, body) = response.into_parts();

    let content_type = parts.headers.get(CONTENT_TYPE).unwrap().to_str().unwrap();
    assert!(content_type.contains("text/plain") || content_type.contains("application/json"));

    let ResponseBody::Fixed(body) = body else {
        panic!("Expected Fixed body");
    };
    let body_bytes = body.collect().await.unwrap().to_bytes();
    assert!(!body_bytes.is_empty(), "the response body is empty");
}

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

/// The router reports a malformed reference and a path it does not serve as the
/// same `None`, so the method decides the status: a read missed, a write was
/// malformed, which OCI conformance requires of a manifest `PUT` carrying an
/// unparseable reference. Two exceptions: the referrers API answers an invalid
/// digest or filter `400` (a bad namespace stays a miss), and `/token` answers an
/// unserved method `405`, since containerd probes `POST /token` first and falls
/// back to `GET` only on 401, 404 or 405.
#[test]
fn unknown_route_status_follows_the_method_and_path() {
    let digest = format!("sha256:{}", "a".repeat(64));
    let manifest = "/v2/conformance/test/manifests/sha256:not-a-digest";
    for (method, uri, expected) in [
        (Method::GET, manifest.to_string(), StatusCode::NOT_FOUND),
        (Method::HEAD, manifest.to_string(), StatusCode::NOT_FOUND),
        (Method::PUT, manifest.to_string(), StatusCode::BAD_REQUEST),
        (Method::POST, manifest.to_string(), StatusCode::BAD_REQUEST),
        (
            Method::DELETE,
            manifest.to_string(),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/v2/conformance/test/referrers/not-a-digest".to_string(),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            format!("/v2/conformance/test/referrers/{digest}?artifactType=not-a-media-type"),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            format!("/v2/conformance/test/referrers/{digest}?n=abc"),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/v2/BAD-NAMESPACE/referrers/not-a-digest".to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            Method::POST,
            "/token".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            Method::PUT,
            "/token".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            Method::DELETE,
            "/token".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (Method::POST, "/tokens".to_string(), StatusCode::BAD_REQUEST),
    ] {
        let request = Request::builder()
            .method(method.clone())
            .uri(&uri)
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();

        let Err(error) = handle_unknown_route(&parts) else {
            panic!("{method} {uri} must be rejected, not served");
        };
        let response = error_to_response(&error, None, None);
        assert_eq!(response.status(), expected, "{method} {uri}: {error:?}");
        if expected == StatusCode::METHOD_NOT_ALLOWED {
            assert_eq!(
                response.headers().get(ALLOW).unwrap(),
                "GET",
                "a 405 must advertise the method the path serves"
            );
        }
    }
}

#[tokio::test]
async fn test_authenticate_and_authorize_returns_client_identity() {
    let config = load_config(
        r#"
        [global.access_policy]
        default = "allow"
        rules = []
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;

    let request = Request::builder().uri("/v2/").body(()).unwrap();
    let (parts, ()) = request.into_parts();
    let route = Action::ApiVersion;

    let result = authenticate_and_authorize(&context, &route, &parts).await;

    let identity: ClientIdentity = result.unwrap();
    assert!(identity.username.is_none());
}

#[tokio::test]
async fn bad_basic_auth_returns_http_401() {
    metrics_provider::initialize_metrics();
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default());
    let password_hash = argon.hash_password(b"testpass").unwrap().to_string();
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

#[test]
fn current_trace_id_returns_none_without_otel_layer() {
    // Active tracing subscriber without any OpenTelemetry layer: span has no
    // OTel context bridge, so current_trace_id must return None.
    let subscriber = TracingRegistry::default();
    let result = with_default(subscriber, || {
        let span = info_span!("test_span");
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
    let subscriber = TracingRegistry::default().with(otel_layer().with_tracer(tracer));

    let trace_id = with_default(subscriber, || {
        let span = info_span!("test_span");
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
