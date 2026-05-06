use std::{
    convert::Infallible,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use opentelemetry::trace::TraceContextExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    pin,
};
use tracing::{Span, debug, error, info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::{dispatch::dispatch_request, error_to_response};
use crate::{
    auth::PeerCertificate,
    command::server::{ServerContext, response_body::ResponseBody, router},
    identity::Action,
    metrics_provider::{InFlightGuard, metrics_provider},
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
    let action = router::parse(request.method(), request.uri());
    let route_action = action.as_ref().map_or("unknown", Action::action_name);

    let trace_id = current_trace_id(&Span::current());

    let response = match dispatch_request(Arc::clone(&context), request, action).await {
        Ok(response) => response,
        Err(error) => error_to_response(&error, trace_id.as_ref()),
    };

    #[allow(clippy::cast_precision_loss)]
    let elapsed = start_time.elapsed().as_millis() as f64;
    let status = response.status();

    metrics_provider()
        .metric_http_request_total
        .with_label_values(&[method.as_str(), route_action, status.as_str()])
        .inc();
    metrics_provider()
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
pub fn current_trace_id(span: &Span) -> Option<String> {
    let context = span.context();
    let otel_span = context.span();
    let span_context = otel_span.span_context();
    if span_context.is_valid() {
        Some(span_context.trace_id().to_string())
    } else {
        None
    }
}

/// Attaches the peer TLS certificate (if present) as a request extension so
/// downstream authenticators can inspect it.
pub fn inject_peer_certificate<B>(request: &mut Request<B>, cert_data: Option<&[u8]>) {
    if let Some(cert_data) = cert_data {
        let peer_certificate = PeerCertificate(Arc::new(cert_data.to_vec()));
        request.extensions_mut().insert(peer_certificate);
    }
}
