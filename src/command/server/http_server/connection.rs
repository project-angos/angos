use std::{
    convert::Infallible,
    fmt::Debug,
    future::Future,
    net::SocketAddr,
    pin::Pin,
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
    command::server::{
        ServerContext, error::Error as ServerError, response_body::ResponseBody, router,
    },
    identity::Action,
    metrics_provider::{InFlightGuard, metrics_provider},
    timing::elapsed_ms,
};

type DispatchFuture =
    Pin<Box<dyn Future<Output = Result<Response<ResponseBody>, ServerError>> + Send + 'static>>;

pub async fn serve_request<S>(
    stream: TokioIo<S>,
    context: Arc<ServerContext>,
    peer_certificate: Option<Vec<u8>>,
    timeouts: Arc<[Duration; 2]>,
    remote_address: SocketAddr,
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

    let [query_timeout, grace_period] = *timeouts;

    // Phase 1: serve until the connection finishes or its query timeout (a wall-clock
    // deadline from connection start, not an activity-reset watchdog) fires. On
    // timeout, signal a graceful shutdown so the connection stops accepting new
    // requests on the keepalive but still drains the in-flight one.
    tokio::select! {
        res = conn.as_mut() => {
            match res {
                Ok(()) => debug!("connection completed before query timeout"),
                Err(error) => debug!("connection error before query timeout: {error}"),
            }
            return;
        }
        () = tokio::time::sleep(query_timeout) => {
            debug!("query timeout reached, signalling graceful shutdown");
            conn.as_mut().graceful_shutdown();
        }
    }

    // Phase 2: graceful shutdown — wait for the connection to drain, or for the
    // grace period to elapse and drop the connection.
    tokio::select! {
        res = conn.as_mut() => {
            match res {
                Ok(()) => debug!("connection drained after graceful shutdown"),
                Err(error) => debug!("connection error during graceful shutdown: {error}"),
            }
        }
        () = tokio::time::sleep(grace_period) => {
            debug!("grace period expired, dropping connection");
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

    let dispatch: DispatchFuture =
        Box::pin(dispatch_request(Arc::clone(&context), request, action));
    let response = match dispatch.await {
        Ok(response) => response,
        Err(error) => error_to_response(&error, trace_id.as_ref()),
    };

    let elapsed = elapsed_ms(start_time);
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
        error!(error = %error_for_log(&response), "{log}");
    } else {
        info!("{log}");
    }

    Ok(response)
}

fn error_for_log(response: &Response<ResponseBody>) -> &str {
    response
        .extensions()
        .get::<String>()
        .map_or("unknown", String::as_str)
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
