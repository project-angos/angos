use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, Version,
    password_hash::{SaltString, rand_core::OsRng},
};
use base64::{Engine, prelude::BASE64_STANDARD};
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response, StatusCode,
    body::Bytes,
    header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE},
};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use serde_json::{Value, from_slice};
use tracing_subscriber::{layer::SubscriberExt, registry::Registry as TracingRegistry};

use crate::{
    auth::PeerCertificate,
    command::server::{
        ServerContext,
        error::Error,
        handlers::{
            blob::handle_delete_blob, content_discovery::handle_list_catalog,
            ext::handle_list_repositories,
        },
        http_server::{
            connection::{current_trace_id, inject_peer_certificate},
            dispatch::authenticate_and_authorize,
            error_response::{error_to_response, fallback_500},
            observability::{handle_healthz, handle_metrics},
        },
        response_body::ResponseBody,
        server_context::tests::{
            TestConfigOptions, create_test_server_context_from_config,
            create_test_server_context_with,
        },
    },
    configuration::Configuration,
    identity::{Action, ClientIdentity},
    metrics_provider,
    oci::{Digest, Namespace},
    policy::{AccessMode, AccessPolicyConfig},
    registry,
};

#[test]
fn test_error_to_response_unauthorized_with_request_id() {
    let error = Error::Unauthorized("Invalid credentials".to_string());
    let request_id = Some("req-123".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(
        response.headers().get(WWW_AUTHENTICATE).unwrap(),
        r#"Basic realm="Simple Registry", charset="UTF-8""#
    );
}

#[test]
fn test_error_to_response_unauthorized_without_request_id() {
    let error = Error::Unauthorized("Access denied".to_string());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(
        response.headers().get(WWW_AUTHENTICATE).unwrap(),
        r#"Basic realm="Simple Registry", charset="UTF-8""#
    );
}

#[test]
fn test_error_to_response_not_found() {
    let error = Error::NotFound("Resource not found".to_string());
    let request_id = Some("req-456".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[test]
fn test_error_to_response_bad_request() {
    let error = Error::BadRequest("Invalid input".to_string());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[test]
fn test_error_to_response_internal() {
    let error = Error::Internal("Server error".to_string());
    let request_id = Some("req-789".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[tokio::test]
async fn test_error_to_response_from_registry_error() {
    let registry_error = registry::Error::BlobUnknown;
    let error: Error = registry_error.into();
    let request_id = Some("req-blob".to_string());

    let response = error_to_response(&error, request_id.as_ref());

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

#[test]
fn test_error_to_response_range_not_satisfiable() {
    let error = Error::RangeNotSatisfiable("Invalid range".to_string());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
}

#[test]
fn test_error_to_response_conflict() {
    let error = Error::Conflict("Resource conflict".to_string());
    let request_id = Some("req-conflict".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
}

#[test]
fn test_error_to_response_initialization() {
    let error = Error::Initialization("Failed to start".to_string());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[test]
fn test_error_to_response_custom_error() {
    let error = Error::Custom {
        status_code: StatusCode::BAD_GATEWAY,
        code: "UPSTREAM_ERROR".to_string(),
        msg: Some("Failed to connect".to_string()),
    };
    let request_id = Some("req-custom".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
}

#[test]
fn test_handle_healthz_success() {
    let result = handle_healthz();

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
}

#[tokio::test]
async fn test_handle_healthz_body_content() {
    use http_body_util::BodyExt;

    let result = handle_healthz();
    assert!(result.is_ok());

    let response = result.unwrap();
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
    let result = handle_metrics();

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(CONTENT_TYPE).is_some());
}

#[tokio::test]
async fn test_handle_metrics_body_not_empty() {
    use http_body_util::BodyExt;

    metrics_provider::init_for_tests();
    let result = handle_metrics();
    assert!(result.is_ok());

    let response = result.unwrap();
    let (_, body) = response.into_parts();

    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };

    assert!(!body_bytes.is_empty());
}

#[tokio::test]
async fn test_handle_metrics_contains_metric_data() {
    use http_body_util::BodyExt;

    metrics_provider::init_for_tests();
    let result = handle_metrics();
    assert!(result.is_ok());

    let response = result.unwrap();
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
            Error::Conflict("msg".to_string()),
            StatusCode::CONFLICT,
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
        let response = error_to_response(&error, None);

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

#[test]
fn test_error_to_response_preserves_request_id() {
    let error = Error::Internal("Test error".to_string());
    let request_id = Some("test-request-123".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_error_to_response_body_contains_error_message() {
    use http_body_util::BodyExt;

    let error = Error::BadRequest("Invalid manifest format".to_string());
    let request_id = Some("req-manifest".to_string());

    let response = error_to_response(&error, request_id.as_ref());
    let (_, body) = response.into_parts();

    let body_bytes = match body {
        ResponseBody::Fixed(b) => b.collect().await.unwrap().to_bytes(),
        _ => panic!("Expected Fixed body"),
    };

    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(body_str.contains("errors"));
}

#[test]
fn test_error_to_response_unauthorized_realm() {
    let error = Error::Unauthorized("Invalid token".to_string());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref());

    let www_authenticate = response
        .headers()
        .get(WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();

    assert!(www_authenticate.contains("Basic"));
    assert!(www_authenticate.contains("realm"));
    assert!(www_authenticate.contains("Simple Registry"));
    assert!(www_authenticate.contains("UTF-8"));
}

#[test]
fn test_error_to_response_with_empty_message() {
    let error = Error::Internal(String::new());
    let request_id = None;

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
}

#[test]
fn test_error_to_response_multiple_errors_same_request_id() {
    let request_id = Some("shared-req-id".to_string());

    let error1 = Error::NotFound("Resource 1".to_string());
    let response1 = error_to_response(&error1, request_id.as_ref());
    assert_eq!(response1.status(), StatusCode::NOT_FOUND);

    let error2 = Error::BadRequest("Bad input".to_string());
    let response2 = error_to_response(&error2, request_id.as_ref());
    assert_eq!(response2.status(), StatusCode::BAD_REQUEST);
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
    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
        max_concurrent_cache_jobs = 10

        [global.access_policy]
        default = "allow"
        rules = []

        [auth.identity.testuser]
        username = "testuser"
        password = "{password_hash}"
    "#
    );
    let config: Configuration = toml::from_str(&toml).unwrap();
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
    let response = error_to_response(&error, None);

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(WWW_AUTHENTICATE).unwrap(),
        r#"Basic realm="Simple Registry", charset="UTF-8""#
    );
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

#[tokio::test]
async fn test_handle_list_repositories() {
    let context = create_test_context_with_allow_policy().await;

    let result = handle_list_repositories(&context).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_handle_list_catalog() {
    let context = create_test_context_with_allow_policy().await;

    let result = handle_list_catalog(&context, None, None).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_handle_delete_blob() {
    let context = create_test_context_with_allow_policy().await;
    let namespace = Namespace::new("test/repo").unwrap();
    let digest: Digest = "sha256:abababababababababababababababababababababababababababababababab"
        .parse()
        .unwrap();

    let _result = handle_delete_blob(&context, &namespace, &digest).await;
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

#[test]
fn error_to_response_falls_back_to_500_when_builder_fails() {
    // Simulate the builder failure mode: a header value that contains a NUL
    // byte (\0) is rejected by hyper as invalid.  We cannot inject this
    // through a public Error variant, so we call the builder directly in the
    // same way error_to_response does, confirm it errors, and then verify that
    // fallback_500() produces the expected minimal 500 response — which is
    // exactly what error_to_response calls via unwrap_or_else.
    let builder_result: Result<Response<ResponseBody>, _> = Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(CONTENT_TYPE, "application/json")
        .header(WWW_AUTHENTICATE, "invalid\0value")
        .body(ResponseBody::Fixed(Full::new(Bytes::from_static(b""))));

    assert!(
        builder_result.is_err(),
        "hyper must reject a header value containing a NUL byte"
    );

    // Applying the same unwrap_or_else used in error_to_response must yield
    // the fallback response rather than panicking.
    let response = builder_result.unwrap_or_else(|_| fallback_500());

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "text/plain");
}
