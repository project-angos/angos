use std::{
    convert::Infallible, fmt::Debug, future::Future, net::SocketAddr, pin::Pin, sync::Arc,
    time::Instant,
};

use arc_swap::ArcSwap;
use hyper::{
    Method, Request, Response, body::Incoming, header::HeaderValue, server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use opentelemetry::trace::TraceContextExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    pin,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{Span, debug, error, info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use angos_oci::header::OCI_NAMESPACE;
use angos_transport::ResponseBody;

use crate::{
    auth::PeerCertificate,
    command::server::{
        ServerContext,
        error::Error as ServerError,
        http_server::{dispatch::dispatch_request, error_to_response},
        listeners::RequestTimeouts,
        router,
    },
    identity::RequestScheme,
    metrics_provider::{InFlightGuard, metrics_provider},
};

/// Boxed, type-erased dispatch future. The erasure is load-bearing, not an
/// optimization: with the concrete future type inlined, `tokio::spawn` fails
/// on it with an E0275 trait-solver overflow.
type DispatchFuture =
    Pin<Box<dyn Future<Output = Result<Response<ResponseBody>, ServerError>> + Send + 'static>>;

/// Serve one connection until it closes, its query timeout fires, or the
/// server shuts down. The last two both let the exchange in flight finish,
/// they only stop the keepalive from taking another request.
pub async fn serve_request<S>(
    stream: TokioIo<S>,
    context: Arc<ArcSwap<ServerContext>>,
    peer_certificate: Option<Vec<u8>>,
    timeouts: Arc<RequestTimeouts>,
    remote_address: SocketAddr,
    scheme: RequestScheme,
    shutdown: CancellationToken,
) where
    S: Unpin + AsyncWrite + AsyncRead + Send + Debug + 'static,
{
    // A timer arms hyper's 30-second `header_read_timeout`, which is inert with
    // no timer installed: a client dribbling one header byte at a time would
    // otherwise hold the connection for the whole `query_timeout`. It also
    // closes an idle keep-alive after the same 30 seconds.
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .serve_connection(
            stream,
            service_fn(move |mut request| {
                inject_peer_certificate(&mut request, peer_certificate.as_deref());
                request.extensions_mut().insert(remote_address);
                request.extensions_mut().insert(scheme);
                handle_request(context.load_full(), request)
            }),
        );
    pin!(conn);

    let _in_flight_guard = InFlightGuard::new();

    let query_timeout = timeouts.query;
    let grace_period = timeouts.grace;

    // Phase 1: serve until the connection finishes, its query timeout (a
    // wall-clock deadline from connection start, not an activity-reset
    // watchdog) fires, or the server shuts down. Either signal starts a
    // graceful shutdown, so the connection stops taking new requests on the
    // keepalive but still drains the in-flight one.
    tokio::select! {
        res = conn.as_mut() => {
            match res {
                Ok(()) => debug!("connection completed before query timeout"),
                Err(error) => debug!("connection error before query timeout: {error}"),
            }
            return;
        }
        () = sleep(query_timeout) => {
            debug!("query timeout reached, signalling graceful shutdown");
            conn.as_mut().graceful_shutdown();
        }
        () = shutdown.cancelled() => {
            debug!("server shutting down, letting the connection finish its exchange");
            conn.as_mut().graceful_shutdown();
        }
    }

    // Phase 2: graceful shutdown. Wait for the connection to drain, or for the
    // grace period to elapse and drop the connection.
    tokio::select! {
        res = conn.as_mut() => {
            match res {
                Ok(()) => debug!("connection drained after graceful shutdown"),
                Err(error) => debug!("connection error during graceful shutdown: {error}"),
            }
        }
        () = sleep(grace_period) => {
            debug!("grace period expired, dropping connection");
        }
    }
}

#[instrument(skip(context, request))]
async fn handle_request(
    context: Arc<ServerContext>,
    mut request: Request<Incoming>,
) -> Result<Response<ResponseBody>, Infallible> {
    let start_time = Instant::now();
    let method = request.method().to_owned();
    let path = request.uri().path().to_owned();

    // Resolve the client's scheme once, honouring a trusted proxy's
    // `X-Forwarded-Proto`, and overwrite the listener's raw scheme. The bearer
    // realm and the auth webhook both read this one extension.
    let scheme = context.resolve_scheme(&request);
    request.extensions_mut().insert(scheme);
    let mut route = router::parse(request.method(), request.uri());
    // A mirroring client names the registry it believes it is addressing in
    // `?ns=`; serving it from the repository mirroring that namespace is what
    // lets such a client use angos without prefixing the paths it requests.
    let proxy_namespace = context.apply_proxy_namespace(route.as_mut(), request.uri());
    // Labels both HTTP metrics below.
    let route_label = route
        .as_ref()
        .map_or("unknown", router::Route::metric_label);

    let trace_id = current_trace_id(&Span::current());
    // Captured before the request moves into the dispatch future, since the realm
    // may be derived from the request's own host. A denial on the token endpoint
    // itself is not challenged: the client would be sent to fetch a token from
    // the endpoint that just refused it.
    let challenge_origin = (!matches!(route, Some(router::Route::Token)))
        .then(|| context.challenge_origin(&request))
        .flatten();

    // A `?ns=` that cannot be applied fails the request instead of dispatching
    // it against the namespace the client spelled out.
    let dispatched = match proxy_namespace {
        Ok(served) => {
            let dispatch: DispatchFuture =
                Box::pin(dispatch_request(Arc::clone(&context), request, route));
            dispatch.await.map(|response| (response, served))
        }
        Err(error) => Err(error),
    };
    let (mut response, served_namespace) = match dispatched {
        Ok(served) => served,
        Err(error) => {
            let challenge =
                challenge_origin.and_then(|(scheme, host)| context.bearer_challenge(scheme, &host));
            (
                error_to_response(&error, trace_id.as_ref(), challenge),
                None,
            )
        }
    };

    // Echoed only when the parameter selected a repository, so its absence
    // tells a client the namespace it named had no effect.
    if let Some(value) = served_namespace.and_then(|ns| HeaderValue::try_from(ns).ok()) {
        response.headers_mut().insert(OCI_NAMESPACE, value);
    }

    // `as_secs_f64()` rather than `as_millis() as f64`: the latter trips the
    // precision-loss lint, and the conversion is lossless at request scale.
    let elapsed = start_time.elapsed().as_secs_f64() * 1000.0;
    let status = response.status();

    metrics_provider()
        .metric_http_request_total
        .with_label_values(&[method_label(&method), route_label, status.as_str()])
        .inc();
    metrics_provider()
        .metric_http_request_duration
        .with_label_values(&[method_label(&method), route_label])
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

/// The `method` label of the HTTP metrics, held to the methods routes are
/// served on. Any other token is one a client chose, and a metric child lives
/// as long as the process, so labelling it verbatim would let one unauthorized
/// client add a counter and a histogram series per request it sends.
pub fn method_label(method: &Method) -> &str {
    if matches!(
        *method,
        Method::GET
            | Method::HEAD
            | Method::POST
            | Method::PUT
            | Method::PATCH
            | Method::DELETE
            | Method::OPTIONS
    ) {
        method.as_str()
    } else {
        "other"
    }
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
