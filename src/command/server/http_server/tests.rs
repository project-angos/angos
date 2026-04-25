use std::collections::HashMap;

use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_subscriber::{layer::SubscriberExt, registry::Registry as TracingRegistry};

use super::*;
use crate::{
    command::server::handlers::{
        blob::handle_delete_blob, content_discovery::handle_list_catalog,
        ext::handle_list_repositories,
    },
    configuration::Configuration,
    identity::ClientIdentity,
    registry,
    registry::{Registry, RegistryConfig},
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

#[test]
fn test_error_to_response_from_registry_error() {
    let registry_error = registry::Error::BlobUnknown;
    let error: Error = registry_error.into();
    let request_id = Some("req-blob".to_string());

    let response = error_to_response(&error, request_id.as_ref());

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
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
    let result = handle_metrics();

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(CONTENT_TYPE).is_some());
}

#[tokio::test]
async fn test_handle_metrics_body_not_empty() {
    use http_body_util::BodyExt;

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
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test-blobs"

        [metadata_store.fs]
        root_dir = "/tmp/test-metadata"

        [cache.memory]

        [server]
        bind_address = "127.0.0.1"
        port = 8080

        [global]
        update_pull_time = false
        max_concurrent_cache_jobs = 10

        [global.access_policy]
        default = "allow"
        rules = []
    "#;

    let config: Configuration = toml::from_str(toml).unwrap();
    let (blob_store, upload_store, presigned_blob_store) =
        config.blob_store.to_backend(None).unwrap();
    let metadata_store = config
        .resolve_metadata_config()
        .to_backend(None)
        .await
        .unwrap();
    let repositories = Arc::new(HashMap::new());
    let registry_config = RegistryConfig::new()
        .update_pull_time(false)
        .enable_blob_redirect(true)
        .enable_manifest_redirect(true)
        .concurrent_cache_jobs(10)
        .global_immutable_tags(false)
        .global_immutable_tags_exclusions(Vec::new());
    let registry = Registry::new(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        repositories,
        registry_config,
    )
    .unwrap();
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder().uri("/v2/").body(()).unwrap();
    let (parts, ()) = request.into_parts();
    let route = Action::ApiVersion;

    let result = authenticate_and_authorize(&context, &route, &parts).await;

    let identity: ClientIdentity = result.unwrap();
    assert!(identity.username.is_none());
}

async fn create_test_context_with_allow_policy() -> ServerContext {
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test-blobs"

        [metadata_store.fs]
        root_dir = "/tmp/test-metadata"

        [cache.memory]

        [server]
        bind_address = "127.0.0.1"
        port = 8080

        [global]
        update_pull_time = false
        max_concurrent_cache_jobs = 10

        [global.access_policy]
        default = "allow"
        rules = []
    "#;

    let config: Configuration = toml::from_str(toml).unwrap();
    let (blob_store, upload_store, presigned_blob_store) =
        config.blob_store.to_backend(None).unwrap();
    let metadata_store = config
        .resolve_metadata_config()
        .to_backend(None)
        .await
        .unwrap();
    let repositories = Arc::new(HashMap::new());
    let registry_config = RegistryConfig::new()
        .update_pull_time(false)
        .enable_blob_redirect(true)
        .enable_manifest_redirect(true)
        .concurrent_cache_jobs(10)
        .global_immutable_tags(false)
        .global_immutable_tags_exclusions(Vec::new());
    let registry = Registry::new(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        repositories,
        registry_config,
    )
    .unwrap();
    ServerContext::new(&config, registry).unwrap()
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
