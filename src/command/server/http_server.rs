use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{CONTENT_RANGE, CONTENT_TYPE, RANGE, WWW_AUTHENTICATE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use opentelemetry::trace::TraceContextExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::pin;
use tracing::{Span, debug, error, info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::command::server::auth::PeerCertificate;
use crate::command::server::error::Error;
use crate::command::server::request_ext::{HeaderExt, IntoAsyncRead};
use crate::command::server::response_body::ResponseBody;
use crate::command::server::{ServerContext, router, ui};
use crate::identity::{ClientIdentity, Route};
use crate::metrics_provider::{IN_FLIGHT_REQUESTS, METRICS_PROVIDER};
use crate::oci::{Digest, Namespace, Reference};

pub async fn serve_request<S>(
    stream: TokioIo<S>,
    context: Arc<ServerContext>,
    peer_certificate: Option<Vec<u8>>,
    timeouts: Arc<[Duration; 2]>,
    remote_address: std::net::SocketAddr,
) where
    S: Unpin + AsyncWrite + AsyncRead + Send + Debug + 'static,
{
    let conn = http1::Builder::new().serve_connection(
        stream,
        service_fn(move |mut request| {
            if let Some(ref cert_data) = peer_certificate {
                let peer_certificate = PeerCertificate(Arc::new(cert_data.clone()));
                request.extensions_mut().insert(peer_certificate);
            }
            request.extensions_mut().insert(remote_address);
            handle_request(Arc::clone(&context), request)
        }),
    );
    pin!(conn);

    IN_FLIGHT_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    METRICS_PROVIDER.metric_http_request_in_flight.set(
        i64::try_from(IN_FLIGHT_REQUESTS.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(i64::MAX),
    );

    for (iter, sleep_duration) in timeouts.iter().enumerate() {
        debug!("iter = {iter} sleep_duration = {sleep_duration:?}");
        tokio::select! {
            res = conn.as_mut() => {
                // Polling the connection returned a result.
                // In this case print either the successful or error result for the connection
                // and break out of the loop.
                match res {
                    Ok(()) => debug!("after polling conn, no error"),
                    Err(error) =>  debug!("error serving connection: {error}"),
                }
                break;
            }
            () = tokio::time::sleep(*sleep_duration) => {
                // tokio::time::sleep returned a result.
                // Call graceful_shutdown on the connection and continue the loop.
                debug!("iter = {iter} got timeout_interval, calling conn.graceful_shutdown");
                conn.as_mut().graceful_shutdown();
            }
        }
    }

    IN_FLIGHT_REQUESTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    METRICS_PROVIDER.metric_http_request_in_flight.set(
        i64::try_from(IN_FLIGHT_REQUESTS.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(i64::MAX),
    );
}

#[instrument(skip(context, request))]
async fn handle_request(
    context: Arc<ServerContext>,
    request: Request<Incoming>,
) -> Result<Response<ResponseBody>, Infallible> {
    let start_time = Instant::now();
    let method = request.method().to_owned();
    let path = request.uri().path().to_owned();
    let route_action = router::parse(request.method(), request.uri()).action_name();

    let trace_id = {
        let context = Span::current().context();
        let span = context.span();
        let span_context = span.span_context();
        if span_context.is_valid() {
            Some(span_context.trace_id().to_string())
        } else {
            None
        }
    };

    let response = match router(Arc::clone(&context), request).await {
        Ok(response) => response,
        Err(error) => error_to_response(&error, trace_id.as_ref()),
    };

    #[allow(clippy::cast_precision_loss)]
    let elapsed = start_time.elapsed().as_millis() as f64;
    let status = response.status();

    METRICS_PROVIDER
        .metric_http_request_total
        .with_label_values(&[method.as_str(), route_action, status.as_str()])
        .inc();
    METRICS_PROVIDER
        .metric_http_request_duration
        .with_label_values(&[method.as_str(), route_action])
        .observe(elapsed);

    let log = if let Some(trace_id) = trace_id {
        format!("{trace_id} {elapsed:?} - {status} {method} {path}")
    } else {
        format!("{elapsed:?} - {status} {method} {path}")
    };

    if status.is_server_error() {
        error!("{log}");
    } else {
        info!("{log}");
    }

    Ok(response)
}

#[instrument(skip(context, req))]
async fn router(
    context: Arc<ServerContext>,
    req: Request<Incoming>,
) -> Result<Response<ResponseBody>, Error> {
    let (parts, incoming) = req.into_parts();
    let route = router::parse(&parts.method, &parts.uri);

    let identity = authenticate_and_authorize(&context, &route, &parts).await?;
    dispatch_route(&context, route, &parts, incoming, &identity).await
}

#[instrument(skip(context, parts))]
async fn authenticate_and_authorize(
    context: &ServerContext,
    route: &Route<'_>,
    parts: &hyper::http::request::Parts,
) -> Result<ClientIdentity, Error> {
    let remote_address = parts.extensions.get::<std::net::SocketAddr>().copied();
    let identity = context.authenticate_request(parts, remote_address).await?;
    context.authorize_request(route, &identity, parts).await?;
    Ok(identity)
}

#[instrument(skip(context, parts, incoming, identity))]
async fn dispatch_route<'a>(
    context: &'a ServerContext,
    route: Route<'a>,
    parts: &'a hyper::http::request::Parts,
    incoming: Incoming,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    match route {
        Route::UiAsset { path } if context.enable_ui => ui::serve_asset(path),
        Route::UiConfig if context.enable_ui => handle_ui_config(context),
        Route::Unknown | Route::UiAsset { .. } | Route::UiConfig => handle_unknown_route(parts),
        Route::ApiVersion => Ok(context.registry.handle_get_api_version().await?),
        Route::StartUpload { namespace, digest } => {
            handle_start_upload(context, &namespace, digest, identity).await
        }
        Route::GetUpload { namespace, uuid } => {
            handle_get_upload(context, &namespace, uuid, identity).await
        }
        Route::PatchUpload { namespace, uuid } => {
            handle_patch_upload(context, parts, incoming, &namespace, uuid, identity).await
        }
        Route::PutUpload {
            namespace,
            uuid,
            digest,
        } => handle_put_upload(context, incoming, &namespace, uuid, digest, identity).await,
        Route::DeleteUpload { namespace, uuid } => {
            handle_delete_upload(context, &namespace, uuid, identity).await
        }
        Route::GetBlob { namespace, digest } => {
            handle_get_blob(context, parts, &namespace, digest, identity).await
        }
        Route::HeadBlob { namespace, digest } => {
            handle_head_blob(context, parts, &namespace, digest, identity).await
        }
        Route::DeleteBlob { namespace, digest } => {
            handle_delete_blob(context, &namespace, digest, identity).await
        }
        Route::GetManifest {
            namespace,
            reference,
        } => handle_get_manifest(context, parts, &namespace, reference, identity).await,
        Route::HeadManifest {
            namespace,
            reference,
        } => handle_head_manifest(context, parts, &namespace, reference, identity).await,
        Route::PutManifest {
            namespace,
            reference,
        } => handle_put_manifest(context, parts, incoming, &namespace, reference, identity).await,
        Route::DeleteManifest {
            namespace,
            reference,
        } => handle_delete_manifest(context, &namespace, reference, identity).await,
        Route::GetReferrer {
            namespace,
            digest,
            artifact_type,
        } => handle_get_referrer(context, &namespace, digest, artifact_type, identity).await,
        Route::ListCatalog { n, last } => handle_list_catalog(context, n, last, identity).await,
        Route::ListTags { namespace, n, last } => {
            handle_list_tags(context, &namespace, n, last, identity).await
        }
        Route::ListRevisions { namespace } => {
            handle_list_revisions(context, &namespace, identity).await
        }
        Route::ListUploads { namespace } => {
            handle_list_uploads(context, &namespace, identity).await
        }
        Route::ListRepositories => handle_list_repositories(context, identity).await,
        Route::ListNamespaces { repository } => {
            handle_list_namespaces(context, repository, identity).await
        }
        Route::Healthz => handle_healthz(),
        Route::Metrics => handle_metrics(),
    }
}

fn handle_unknown_route(
    parts: &hyper::http::request::Parts,
) -> Result<Response<ResponseBody>, Error> {
    if [Method::GET, Method::HEAD].contains(&parts.method) {
        let msg = format!("unknown route: {} {}", parts.method, parts.uri);
        Err(Error::NotFound(msg))
    } else {
        let msg = format!("unsupported route: {} {}", parts.method, parts.uri);
        Err(Error::BadRequest(msg))
    }
}

async fn handle_start_upload(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Option<Digest>,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_start_upload(namespace, digest)
        .await?)
}

async fn handle_get_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_get_upload(namespace, uuid).await?)
}

async fn handle_patch_upload(
    context: &ServerContext,
    parts: &hyper::http::request::Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let start_offset = parts.range(CONTENT_RANGE)?.map(|(start, _)| start);
    let body_stream = incoming.into_async_read();

    Ok(context
        .registry
        .handle_patch_upload(namespace, uuid, start_offset, body_stream)
        .await?)
}

async fn handle_put_upload(
    context: &ServerContext,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
    digest: Digest,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let body_stream = incoming.into_async_read();

    Ok(context
        .registry
        .handle_put_upload(namespace, uuid, &digest, body_stream)
        .await?)
}

async fn handle_delete_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_delete_upload(namespace, uuid)
        .await?)
}

async fn handle_get_blob(
    context: &ServerContext,
    parts: &hyper::http::request::Parts,
    namespace: &Namespace,
    digest: Digest,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let range = parts.range(RANGE)?;

    Ok(context
        .registry
        .handle_get_blob(namespace, &digest, &mime_types, range)
        .await?)
}

async fn handle_head_blob(
    context: &ServerContext,
    parts: &hyper::http::request::Parts,
    namespace: &Namespace,
    digest: Digest,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();

    Ok(context
        .registry
        .handle_head_blob(namespace, &digest, &mime_types)
        .await?)
}

async fn handle_delete_blob(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Digest,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_delete_blob(namespace, &digest)
        .await?)
}

async fn handle_get_manifest(
    context: &ServerContext,
    parts: &hyper::http::request::Parts,
    namespace: &Namespace,
    reference: Reference,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_tag_immutable = match &reference {
        Reference::Tag(tag) => context.is_tag_immutable(namespace, tag.as_str()),
        Reference::Digest(_) => false,
    };

    Ok(context
        .registry
        .handle_get_manifest(namespace, reference, &mime_types, is_tag_immutable)
        .await?)
}

async fn handle_head_manifest(
    context: &ServerContext,
    parts: &hyper::http::request::Parts,
    namespace: &Namespace,
    reference: Reference,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_tag_immutable = match &reference {
        Reference::Tag(tag) => context.is_tag_immutable(namespace, tag.as_str()),
        Reference::Digest(_) => false,
    };

    Ok(context
        .registry
        .handle_head_manifest(namespace, reference, &mime_types, is_tag_immutable)
        .await?)
}

async fn handle_put_manifest(
    context: &ServerContext,
    parts: &hyper::http::request::Parts,
    incoming: Incoming,
    namespace: &Namespace,
    reference: Reference,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_type = parts.get_header(CONTENT_TYPE).ok_or(Error::BadRequest(
        "No Content-Type header provided".to_string(),
    ))?;

    let body_stream = incoming.into_async_read();

    Ok(context
        .registry
        .handle_put_manifest(namespace, reference, mime_type, body_stream)
        .await?)
}

async fn handle_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_delete_manifest(namespace, reference)
        .await?)
}

async fn handle_get_referrer(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Digest,
    artifact_type: Option<String>,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_get_referrers(namespace, &digest, artifact_type)
        .await?)
}

async fn handle_list_catalog(
    context: &ServerContext,
    n: Option<u16>,
    last: Option<String>,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_catalog(n, last).await?)
}

async fn handle_list_tags(
    context: &ServerContext,
    namespace: &Namespace,
    n: Option<u16>,
    last: Option<String>,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_list_tags(namespace, n, last)
        .await?)
}

async fn handle_list_revisions(
    context: &ServerContext,
    namespace: &Namespace,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_revisions(namespace).await?)
}

async fn handle_list_uploads(
    context: &ServerContext,
    namespace: &Namespace,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_uploads(namespace).await?)
}

async fn handle_list_repositories(
    context: &ServerContext,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_repositories().await?)
}

async fn handle_list_namespaces(
    context: &ServerContext,
    repository: &str,
    _identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_namespaces(repository).await?)
}

fn handle_ui_config(context: &ServerContext) -> Result<Response<ResponseBody>, Error> {
    let config = serde_json::json!({
        "name": context.ui_name
    });
    let body = serde_json::to_string(&config)
        .map_err(|e| Error::Internal(format!("Failed to serialize UI config: {e}")))?;

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(ResponseBody::Fixed(Full::new(Bytes::from(body))))
        .map_err(|e| Error::Internal(format!("Failed to build UI config response: {e}")))
}

fn handle_healthz() -> Result<Response<ResponseBody>, Error> {
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(ResponseBody::Fixed(Full::new(Bytes::from(
            r#"{"status":"ok"}"#,
        ))));

    match response {
        Ok(resp) => Ok(resp),
        Err(e) => {
            let msg = format!("Failed to build healthz response: {e}");
            Err(Error::Internal(msg))
        }
    }
}

fn handle_metrics() -> Result<Response<ResponseBody>, Error> {
    let (content_type, metrics) = METRICS_PROVIDER.gather()?;
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .body(ResponseBody::Fixed(Full::new(Bytes::from(metrics))));

    match response {
        Ok(resp) => Ok(resp),
        Err(e) => {
            let msg = format!("Failed to build metrics response: {e}");
            Err(Error::Internal(msg))
        }
    }
}

pub fn error_to_response(error: &Error, request_id: Option<&String>) -> Response<ResponseBody> {
    let status = error.status_code();
    let body = error.as_json(request_id);

    let body = body.to_string();
    let body = Bytes::from(body);

    match error {
        Error::Unauthorized(_) => Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .header(
                WWW_AUTHENTICATE,
                r#"Basic realm="Simple Registry", charset="UTF-8""#,
            )
            .body(ResponseBody::Fixed(Full::new(body)))
            .unwrap(),
        _ => Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .body(ResponseBody::Fixed(Full::new(body)))
            .unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::configuration::Configuration;
    use crate::identity::ClientIdentity;
    use crate::registry;
    use crate::registry::{Registry, RegistryConfig};

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
            default_allow = true
            rules = []
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let blob_store = config.blob_store.to_backend().unwrap();
        let metadata_store = config.resolve_metadata_config().to_backend().unwrap();
        let repositories = Arc::new(HashMap::new());
        let registry_config = RegistryConfig::new()
            .update_pull_time(false)
            .enable_redirect(true)
            .concurrent_cache_jobs(10)
            .global_immutable_tags(false)
            .global_immutable_tags_exclusions(Vec::new());
        let registry =
            Registry::new(blob_store, metadata_store, repositories, registry_config).unwrap();
        let context = ServerContext::new(&config, registry).unwrap();

        let request = Request::builder().uri("/v2/").body(()).unwrap();
        let (parts, ()) = request.into_parts();
        let route = Route::ApiVersion;

        let result = authenticate_and_authorize(&context, &route, &parts).await;

        let identity: ClientIdentity = result.unwrap();
        assert!(identity.username.is_none());
    }

    fn create_test_context_with_allow_policy() -> ServerContext {
        use std::collections::HashMap;

        use crate::configuration::Configuration;
        use crate::registry::{Registry, RegistryConfig};

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
            default_allow = true
            rules = []
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let blob_store = config.blob_store.to_backend().unwrap();
        let metadata_store = config.resolve_metadata_config().to_backend().unwrap();
        let repositories = Arc::new(HashMap::new());
        let registry_config = RegistryConfig::new()
            .update_pull_time(false)
            .enable_redirect(true)
            .concurrent_cache_jobs(10)
            .global_immutable_tags(false)
            .global_immutable_tags_exclusions(Vec::new());
        let registry =
            Registry::new(blob_store, metadata_store, repositories, registry_config).unwrap();
        ServerContext::new(&config, registry).unwrap()
    }

    #[tokio::test]
    async fn test_handle_list_repositories_accepts_identity() {
        use crate::identity::ClientIdentity;

        let context = create_test_context_with_allow_policy();
        let identity = ClientIdentity::default();

        let result = handle_list_repositories(&context, &identity).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_list_catalog_accepts_identity() {
        use crate::identity::ClientIdentity;

        let context = create_test_context_with_allow_policy();
        let identity = ClientIdentity::default();

        let result = handle_list_catalog(&context, None, None, &identity).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_delete_blob_accepts_identity() {
        use crate::identity::ClientIdentity;

        let context = create_test_context_with_allow_policy();
        let identity = ClientIdentity::default();
        let namespace = Namespace::new("test/repo").unwrap();
        let digest: Digest =
            "sha256:abababababababababababababababababababababababababababababababab"
                .parse()
                .unwrap();

        let _result = handle_delete_blob(&context, &namespace, digest, &identity).await;
    }
}
