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
    auth::PeerCertificate,
    command::server::{
        ServerContext,
        error::Error,
        handlers,
        request_ext::{HeaderExt, IntoAsyncRead},
        response_body::ResponseBody,
        router, ui,
    },
    event_webhook::event::Event,
    identity::{Action, ClientIdentity},
    metrics_provider::{InFlightGuard, METRICS_PROVIDER},
    oci::{Digest, Namespace, Reference},
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
            inject_peer_certificate(&mut request, peer_certificate.as_deref());
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
                match res {
                    Ok(()) => debug!("after polling conn, no error"),
                    Err(error) =>  debug!("error serving connection: {error}"),
                }
                break;
            }
            () = tokio::time::sleep(*sleep_duration) => {
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
    let route_action = router::parse(request.method(), request.uri())
        .as_ref()
        .map_or("unknown", Action::action_name);

    let trace_id = current_trace_id(&Span::current());

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

/// Extracts the `OpenTelemetry` trace ID from the given tracing span.
///
/// Returns `None` if the span has no `OpenTelemetry` bridge context (e.g. the
/// span is disabled, or no `OpenTelemetry` layer is registered with the subscriber).
fn current_trace_id(span: &Span) -> Option<String> {
    let context = span.context();
    let otel_span = context.span();
    let span_context = otel_span.span_context();
    if span_context.is_valid() {
        Some(span_context.trace_id().to_string())
    } else {
        None
    }
}

#[instrument(skip(context, req))]
async fn router(
    context: Arc<ServerContext>,
    req: Request<Incoming>,
) -> Result<Response<ResponseBody>, Error> {
    let (parts, incoming) = req.into_parts();
    let Some(action) = router::parse(&parts.method, &parts.uri) else {
        return handle_unknown_route(&parts);
    };

    let identity = authenticate_and_authorize(&context, &action, &parts).await?;
    dispatch_route(&context, action, &parts, incoming, &identity).await
}

#[instrument(skip(context, parts))]
async fn authenticate_and_authorize(
    context: &ServerContext,
    route: &Action,
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
    route: Action,
    parts: &'a Parts,
    incoming: Incoming,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    match route {
        Action::UiAsset { path } if context.enable_ui => ui::serve_asset(&path),
        Action::UiConfig if context.enable_ui => handle_ui_config(context),
        Action::UiAsset { .. } | Action::UiConfig => handle_unknown_route(parts),
        Action::ApiVersion => Ok(handlers::version::handle_get_api_version()?),
        Action::StartUpload { namespace, digest } => {
            handlers::upload::handle_start_upload(context, &namespace, digest).await
        }
        Action::GetUpload { namespace, uuid } => {
            handlers::upload::handle_get_upload(context, &namespace, uuid).await
        }
        Action::PatchUpload { namespace, uuid } => {
            handle_patch_upload(context, parts, incoming, &namespace, uuid).await
        }
        Action::PutUpload {
            namespace,
            uuid,
            digest,
        } => handle_put_upload(context, parts, incoming, &namespace, uuid, digest, identity).await,
        Action::DeleteUpload { namespace, uuid } => {
            handlers::upload::handle_delete_upload(context, &namespace, uuid).await
        }
        Action::GetBlob { namespace, digest } => {
            handle_get_blob(context, parts, &namespace, digest).await
        }
        Action::HeadBlob { namespace, digest } => {
            handle_head_blob(context, parts, &namespace, digest).await
        }
        Action::DeleteBlob { namespace, digest } => {
            handlers::blob::handle_delete_blob(context, &namespace, &digest).await
        }
        Action::GetManifest {
            namespace,
            reference,
        } => handle_get_manifest(context, parts, &namespace, reference).await,
        Action::HeadManifest {
            namespace,
            reference,
        } => handle_head_manifest(context, parts, &namespace, reference).await,
        Action::PutManifest {
            namespace,
            reference,
        } => handle_put_manifest(context, parts, incoming, &namespace, reference, identity).await,
        Action::DeleteManifest {
            namespace,
            reference,
        } => handle_delete_manifest(context, &namespace, reference, identity).await,
        Action::GetReferrer {
            namespace,
            digest,
            artifact_type,
        } => {
            handlers::content_discovery::handle_get_referrers(
                context,
                &namespace,
                &digest,
                artifact_type,
            )
            .await
        }
        Action::ListCatalog { n, last } => {
            handlers::content_discovery::handle_list_catalog(context, n, last).await
        }
        Action::ListTags { namespace, n, last } => {
            handlers::content_discovery::handle_list_tags(context, &namespace, n, last).await
        }
        Action::ListRevisions { namespace } => {
            handlers::ext::handle_list_revisions(context, &namespace).await
        }
        Action::ListUploads { namespace } => {
            handlers::ext::handle_list_uploads(context, &namespace).await
        }
        Action::ListRepositories => handlers::ext::handle_list_repositories(context).await,
        Action::ListNamespaces { repository } => {
            handlers::ext::handle_list_namespaces(context, &repository).await
        }
        Action::Healthz => handle_healthz(),
        Action::Readyz => handle_readyz(context).await,
        Action::Metrics => handle_metrics(),
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

/// Attaches the peer TLS certificate (if present) as a request extension so
/// downstream authenticators can inspect it.
fn inject_peer_certificate(request: &mut Request<Incoming>, cert_data: Option<&[u8]>) {
    if let Some(cert_data) = cert_data {
        let peer_certificate = PeerCertificate(Arc::new(cert_data.to_vec()));
        request.extensions_mut().insert(peer_certificate);
    }
}

/// Dispatches events produced by a handler through the server's event
/// dispatcher. Handlers are responsible for building events; the HTTP layer
/// only forwards them.
async fn dispatch_events(context: &ServerContext, events: Vec<Event>) -> Result<(), Error> {
    for event in events {
        context.dispatch_event(&event).await?;
    }
    Ok(())
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

    handlers::upload::handle_patch_upload(
        context,
        namespace,
        uuid,
        start_offset,
        content_length,
        body_stream,
    )
    .await
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

    let (response, events) = handlers::upload::handle_put_upload(
        context,
        namespace,
        uuid,
        &digest,
        content_length,
        body_stream,
        identity,
    )
    .await?;

    dispatch_events(context, events).await?;
    Ok(response)
}

async fn handle_get_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let range = parts.range(RANGE)?;

    handlers::blob::handle_get_blob(context, namespace, &digest, &mime_types, range).await
}

async fn handle_head_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();

    handlers::blob::handle_head_blob(context, namespace, &digest, &mime_types).await
}

async fn handle_get_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handlers::manifest::handle_get_manifest(
        context,
        namespace,
        reference,
        &mime_types,
        is_immutable,
    )
    .await
}

async fn handle_head_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handlers::manifest::handle_head_manifest(
        context,
        namespace,
        reference,
        &mime_types,
        is_immutable,
    )
    .await
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

    let (response, events) = handlers::manifest::handle_put_manifest(
        context,
        namespace,
        reference,
        mime_type,
        body_stream,
        identity,
    )
    .await?;

    dispatch_events(context, events).await?;
    Ok(response)
}

async fn handle_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let (response, events) =
        handlers::manifest::handle_delete_manifest(context, namespace, reference, identity).await?;

    dispatch_events(context, events).await?;
    Ok(response)
}

fn json_response(
    status: StatusCode,
    body: impl Serialize,
) -> Result<Response<ResponseBody>, Error> {
    let json = serde_json::to_string(&body)?;
    Ok(Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(ResponseBody::Fixed(Full::new(Bytes::from(json))))?)
}

fn handle_ui_config(context: &ServerContext) -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct UiConfigResponse<'a> {
        name: &'a str,
    }
    json_response(
        StatusCode::OK,
        &UiConfigResponse {
            name: &context.ui_name,
        },
    )
}

fn handle_healthz() -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct HealthResponse {
        status: &'static str,
    }
    json_response(StatusCode::OK, &HealthResponse { status: "ok" })
}

async fn handle_readyz(context: &ServerContext) -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct ReadyResponse {
        status: &'static str,
    }

    #[derive(Serialize)]
    struct NotReadyResponse {
        status: &'static str,
        error: String,
    }

    match context.registry.check_ready().await {
        Ok(()) => json_response(StatusCode::OK, &ReadyResponse { status: "ready" }),
        Err(e) => {
            let payload = NotReadyResponse {
                status: "not_ready",
                error: e.to_string(),
            };
            json_response(StatusCode::SERVICE_UNAVAILABLE, &payload)
        }
    }
}

fn handle_metrics() -> Result<Response<ResponseBody>, Error> {
    let (content_type, metrics) = METRICS_PROVIDER.gather()?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .body(ResponseBody::Fixed(Full::new(Bytes::from(metrics))))?)
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
