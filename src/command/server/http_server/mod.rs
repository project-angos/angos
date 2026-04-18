use std::{
    convert::Infallible,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE, WWW_AUTHENTICATE},
    http::request::Parts,
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use opentelemetry::trace::TraceContextExt;
use serde::Serialize;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    pin,
};
use tracing::{Span, debug, error, info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::{
    command::server::{
        ServerContext,
        auth::PeerCertificate,
        error::Error,
        request_ext::{HeaderExt, IntoAsyncRead},
        response_body::ResponseBody,
        router, ui,
    },
    event_webhook::event::{Event, EventActor, EventKind},
    identity::{ClientIdentity, Route},
    metrics_provider::{InFlightGuard, METRICS_PROVIDER},
    oci::{Digest, Namespace, Reference},
    registry::metadata_store::ConditionalCapabilities,
};

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

    let _in_flight_guard = InFlightGuard::new();

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
    parts: &Parts,
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
    parts: &'a Parts,
    incoming: Incoming,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    match route {
        Route::UiAsset { path } if context.enable_ui => ui::serve_asset(path),
        Route::UiConfig if context.enable_ui => handle_ui_config(context),
        Route::Unknown | Route::UiAsset { .. } | Route::UiConfig => handle_unknown_route(parts),
        Route::ApiVersion => Ok(context.registry.handle_get_api_version().await?),
        Route::StartUpload { namespace, digest } => {
            handle_start_upload(context, &namespace, digest).await
        }
        Route::GetUpload { namespace, uuid } => handle_get_upload(context, &namespace, uuid).await,
        Route::PatchUpload { namespace, uuid } => {
            handle_patch_upload(context, parts, incoming, &namespace, uuid).await
        }
        Route::PutUpload {
            namespace,
            uuid,
            digest,
        } => handle_put_upload(context, parts, incoming, &namespace, uuid, digest, identity).await,
        Route::DeleteUpload { namespace, uuid } => {
            handle_delete_upload(context, &namespace, uuid).await
        }
        Route::GetBlob { namespace, digest } => {
            handle_get_blob(context, parts, &namespace, digest).await
        }
        Route::HeadBlob { namespace, digest } => {
            handle_head_blob(context, parts, &namespace, digest).await
        }
        Route::DeleteBlob { namespace, digest } => {
            handle_delete_blob(context, &namespace, digest).await
        }
        Route::GetManifest {
            namespace,
            reference,
        } => handle_get_manifest(context, parts, &namespace, reference).await,
        Route::HeadManifest {
            namespace,
            reference,
        } => handle_head_manifest(context, parts, &namespace, reference).await,
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
        } => handle_get_referrer(context, &namespace, digest, artifact_type).await,
        Route::ListCatalog { n, last } => handle_list_catalog(context, n, last).await,
        Route::ListTags { namespace, n, last } => {
            handle_list_tags(context, &namespace, n, last).await
        }
        Route::ListRevisions { namespace } => handle_list_revisions(context, &namespace).await,
        Route::ListUploads { namespace } => handle_list_uploads(context, &namespace).await,
        Route::ListRepositories => handle_list_repositories(context).await,
        Route::ListNamespaces { repository } => handle_list_namespaces(context, repository).await,
        Route::Healthz => handle_healthz(),
        Route::Readyz => handle_readyz(context, parts).await,
        Route::Metrics => handle_metrics(),
    }
}

fn handle_unknown_route(parts: &Parts) -> Result<Response<ResponseBody>, Error> {
    if [Method::GET, Method::HEAD].contains(&parts.method) {
        let msg = format!("unknown route: {} {}", parts.method, parts.uri);
        Err(Error::NotFound(msg))
    } else {
        let msg = format!("unsupported route: {} {}", parts.method, parts.uri);
        Err(Error::BadRequest(msg))
    }
}

fn repository_name_for(context: &ServerContext, namespace: &Namespace) -> String {
    context
        .registry
        .get_repository_for_namespace(namespace)
        .map(|r| r.name.clone())
        .unwrap_or_default()
}

async fn handle_start_upload(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Option<Digest>,
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
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_get_upload(namespace, uuid).await?)
}

async fn handle_patch_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
) -> Result<Response<ResponseBody>, Error> {
    let start_offset = parts.range(CONTENT_RANGE)?.map(|(start, _)| start);
    let content_length: u64 = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_stream = incoming.into_async_read();

    Ok(context
        .registry
        .handle_patch_upload(namespace, uuid, start_offset, content_length, body_stream)
        .await?)
}

async fn handle_put_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
    digest: Digest,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let content_length: u64 = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_stream = incoming.into_async_read();

    let response = context
        .registry
        .handle_put_upload(namespace, uuid, &digest, content_length, body_stream)
        .await?;

    let event = Event::new(
        EventKind::BlobPush,
        namespace.to_string(),
        repository_name_for(context, namespace),
    )
    .digest(Some(digest.to_string()))
    .actor(Some(EventActor::from(identity.clone())));
    context.dispatch_event(&event).await?;

    Ok(response)
}

async fn handle_delete_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_delete_upload(namespace, uuid)
        .await?)
}

async fn handle_get_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
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
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
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
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_delete_blob(namespace, &digest)
        .await?)
}

async fn handle_get_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    Ok(context
        .registry
        .handle_get_manifest(namespace, reference, &mime_types, is_immutable)
        .await?)
}

async fn handle_head_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    Ok(context
        .registry
        .handle_head_manifest(namespace, reference, &mime_types, is_immutable)
        .await?)
}

async fn handle_put_manifest(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_type = parts.get_header(CONTENT_TYPE).ok_or(Error::BadRequest(
        "No Content-Type header provided".to_string(),
    ))?;

    let body_stream = incoming.into_async_read();

    let response = context
        .registry
        .handle_put_manifest(namespace, reference.clone(), mime_type, body_stream)
        .await?;

    let digest = response
        .headers()
        .get("Docker-Content-Digest")
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string);

    let repository = repository_name_for(context, namespace);
    let actor = Some(EventActor::from(identity.clone()));

    let manifest_event = Event::new(
        EventKind::ManifestPush,
        namespace.to_string(),
        repository.clone(),
    )
    .digest(digest.clone())
    .reference(Some(reference.to_string()))
    .actor(actor.clone());
    context.dispatch_event(&manifest_event).await?;

    if let Reference::Tag(tag) = &reference {
        let tag_event = Event::new(EventKind::TagCreate, namespace.to_string(), repository)
            .digest(digest)
            .reference(Some(reference.to_string()))
            .tag(Some(tag.clone()))
            .actor(actor);
        context.dispatch_event(&tag_event).await?;
    }

    Ok(response)
}

async fn handle_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let response = context
        .registry
        .handle_delete_manifest(namespace, reference.clone())
        .await?;

    let repository = repository_name_for(context, namespace);
    let actor = Some(EventActor::from(identity.clone()));

    let digest = match &reference {
        Reference::Digest(d) => Some(d.to_string()),
        Reference::Tag(_) => None,
    };

    let delete_event = Event::new(
        EventKind::ManifestDelete,
        namespace.to_string(),
        repository.clone(),
    )
    .digest(digest.clone())
    .reference(Some(reference.to_string()))
    .actor(actor.clone());
    context.dispatch_event(&delete_event).await?;

    if let Reference::Tag(tag) = &reference {
        let tag_event = Event::new(EventKind::TagDelete, namespace.to_string(), repository)
            .digest(digest)
            .reference(Some(reference.to_string()))
            .tag(Some(tag.clone()))
            .actor(actor);
        context.dispatch_event(&tag_event).await?;
    }

    Ok(response)
}

async fn handle_get_referrer(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Digest,
    artifact_type: Option<String>,
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
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_catalog(n, last).await?)
}

async fn handle_list_tags(
    context: &ServerContext,
    namespace: &Namespace,
    n: Option<u16>,
    last: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context
        .registry
        .handle_list_tags(namespace, n, last)
        .await?)
}

async fn handle_list_revisions(
    context: &ServerContext,
    namespace: &Namespace,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_revisions(namespace).await?)
}

async fn handle_list_uploads(
    context: &ServerContext,
    namespace: &Namespace,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_uploads(namespace).await?)
}

async fn handle_list_repositories(
    context: &ServerContext,
) -> Result<Response<ResponseBody>, Error> {
    Ok(context.registry.handle_list_repositories().await?)
}

async fn handle_list_namespaces(
    context: &ServerContext,
    repository: &str,
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

async fn handle_readyz(
    context: &ServerContext,
    parts: &Parts,
) -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct ReadyResponse<'a> {
        status: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        conditional: Option<&'a ConditionalCapabilities>,
    }

    #[derive(Serialize)]
    struct NotReadyResponse {
        status: &'static str,
        error: String,
    }

    let verbose = parts
        .uri
        .query()
        .is_some_and(|q| q.contains("verbose=true"));

    match context.registry.check_ready().await {
        Ok(conditional) => {
            let payload = ReadyResponse {
                status: "ready",
                conditional: if verbose { conditional.as_ref() } else { None },
            };
            let body = serde_json::to_string(&payload).map_err(|e| {
                Error::Internal(format!("Failed to serialize readyz response: {e}"))
            })?;
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .body(ResponseBody::Fixed(Full::new(Bytes::from(body))))
                .map_err(|e| Error::Internal(format!("Failed to build readyz response: {e}")))
        }
        Err(e) => {
            let payload = NotReadyResponse {
                status: "not_ready",
                error: e.to_string(),
            };
            let body = serde_json::to_string(&payload).map_err(|e| {
                Error::Internal(format!("Failed to serialize readyz response: {e}"))
            })?;
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(CONTENT_TYPE, "application/json")
                .body(ResponseBody::Fixed(Full::new(Bytes::from(body))))
                .map_err(|e| Error::Internal(format!("Failed to build readyz response: {e}")))
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
mod tests;
