use std::sync::{OnceLock, atomic::AtomicU64};

use prometheus::{
    Encoder, HistogramVec, IntCounterVec, IntGauge, Registry as PrometheusRegistry, TextEncoder,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry,
};
use tracing::error;

use crate::registry::Error;

pub static IN_FLIGHT_REQUESTS: AtomicU64 = AtomicU64::new(0);

static METRICS: OnceLock<MetricsProvider> = OnceLock::new();

/// Initializes the metrics provider at startup.
///
/// Must be called once before any code that records a metric runs. Returns
/// `Err` if metric registration fails or if called more than once.
pub fn initialize_metrics() -> Result<(), Error> {
    let provider = MetricsProvider::new()?;
    METRICS
        .set(provider)
        .map_err(|_| Error::Initialization("metrics provider already initialized".to_string()))
}

/// Returns a reference to the initialized metrics provider.
///
/// # Panics
///
/// Panics if `initialize_metrics()` has not been called. This is a programmer
/// error — all code paths that record metrics run after startup initialization.
pub fn metrics_provider() -> &'static MetricsProvider {
    METRICS
        .get()
        .expect("initialize_metrics() must be called at startup before any metric is recorded")
}

pub struct InFlightGuard;

impl InFlightGuard {
    pub fn new() -> Self {
        IN_FLIGHT_REQUESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self::update_gauge();
        Self
    }

    fn update_gauge() {
        metrics_provider().metric_http_request_in_flight.set(
            i64::try_from(IN_FLIGHT_REQUESTS.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(i64::MAX),
        );
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        IN_FLIGHT_REQUESTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Self::update_gauge();
    }
}

pub struct MetricsProvider {
    registry: PrometheusRegistry,
    pub metric_http_request_total: IntCounterVec,
    pub metric_http_request_duration: HistogramVec,
    pub metric_http_request_in_flight: IntGauge,
    pub auth_attempts: IntCounterVec,
    pub lock_acquisition_duration: HistogramVec,
    pub lock_acquisitions: IntCounterVec,
    pub lock_retries: IntCounterVec,
    pub lock_invalidations: IntCounterVec,
    pub lock_recoveries: IntCounterVec,
}

struct HttpMetrics {
    total: IntCounterVec,
    duration: HistogramVec,
    in_flight: IntGauge,
}

fn register_http_metrics(registry: &PrometheusRegistry) -> Result<HttpMetrics, Error> {
    let total = register_int_counter_vec_with_registry!(
        "http_requests_total",
        "Total number of HTTP requests made.",
        &["method", "route", "status"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create http_requests_total metric: {error}");
        Error::Initialization(String::from("Unable to create http_requests_total metric"))
    })?;

    let duration = register_histogram_vec_with_registry!(
        "http_request_duration_ms",
        "The HTTP request latencies in milliseconds.",
        &["method", "route"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create http_request_duration metric: {error}");
        Error::Initialization(String::from(
            "Unable to create http_request_duration metric",
        ))
    })?;

    let in_flight = register_int_gauge_with_registry!(
        "http_requests_in_flight",
        "The current number of in-flight HTTP requests.",
        registry
    )
    .map_err(|error| {
        error!("Unable to create http_requests_in_flight metric: {error}");
        Error::Initialization(String::from(
            "Unable to create http_requests_in_flight metric",
        ))
    })?;

    Ok(HttpMetrics {
        total,
        duration,
        in_flight,
    })
}

struct LockAndAuthMetrics {
    auth_attempts: IntCounterVec,
    lock_acquisition_duration: HistogramVec,
    lock_acquisitions: IntCounterVec,
    lock_retries: IntCounterVec,
    lock_invalidations: IntCounterVec,
    lock_recoveries: IntCounterVec,
}

fn register_lock_and_auth_metrics(
    registry: &PrometheusRegistry,
) -> Result<LockAndAuthMetrics, Error> {
    let auth_attempts = register_int_counter_vec_with_registry!(
        "auth_attempts_total",
        "Total number of authentication attempts",
        &["method", "result"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create auth_attempts_total metric: {error}");
        Error::Initialization(String::from("Unable to create auth_attempts_total metric"))
    })?;

    let lock_acquisition_duration = register_histogram_vec_with_registry!(
        "lock_acquisition_duration_ms",
        "Lock acquisition duration in milliseconds",
        &["backend"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create lock_acquisition_duration_ms metric: {error}");
        Error::Initialization(String::from(
            "Unable to create lock_acquisition_duration_ms metric",
        ))
    })?;

    let lock_acquisitions = register_int_counter_vec_with_registry!(
        "lock_acquisitions_total",
        "Total lock acquisition attempts",
        &["backend", "result"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create lock_acquisitions_total metric: {error}");
        Error::Initialization(String::from(
            "Unable to create lock_acquisitions_total metric",
        ))
    })?;

    let lock_retries = register_int_counter_vec_with_registry!(
        "lock_retries_total",
        "Total lock acquisition retries",
        &["backend"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create lock_retries_total metric: {error}");
        Error::Initialization(String::from("Unable to create lock_retries_total metric"))
    })?;

    let lock_invalidations = register_int_counter_vec_with_registry!(
        "lock_invalidations_total",
        "Total lock invalidations",
        &["backend", "reason"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create lock_invalidations_total metric: {error}");
        Error::Initialization(String::from(
            "Unable to create lock_invalidations_total metric",
        ))
    })?;

    let lock_recoveries = register_int_counter_vec_with_registry!(
        "lock_recoveries_total",
        "Total stale lock recovery attempts",
        &["backend", "result"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create lock_recoveries_total metric: {error}");
        Error::Initialization(String::from(
            "Unable to create lock_recoveries_total metric",
        ))
    })?;

    Ok(LockAndAuthMetrics {
        auth_attempts,
        lock_acquisition_duration,
        lock_acquisitions,
        lock_retries,
        lock_invalidations,
        lock_recoveries,
    })
}

impl MetricsProvider {
    pub fn new() -> Result<Self, Error> {
        let registry = PrometheusRegistry::new();
        let http = register_http_metrics(&registry)?;
        let lock_auth = register_lock_and_auth_metrics(&registry)?;

        Ok(Self {
            registry,
            metric_http_request_total: http.total,
            metric_http_request_duration: http.duration,
            metric_http_request_in_flight: http.in_flight,
            auth_attempts: lock_auth.auth_attempts,
            lock_acquisition_duration: lock_auth.lock_acquisition_duration,
            lock_acquisitions: lock_auth.lock_acquisitions,
            lock_retries: lock_auth.lock_retries,
            lock_invalidations: lock_auth.lock_invalidations,
            lock_recoveries: lock_auth.lock_recoveries,
        })
    }

    pub fn gather(&self) -> Result<(String, Vec<u8>), Error> {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|error| Error::Internal(format!("Unable to encode metrics: {error}")))?;
        Ok((encoder.format_type().to_string(), buffer))
    }
}

#[cfg(test)]
pub(crate) fn init_for_tests() {
    // Ignore the already-initialized error — tests run in parallel and share one process.
    let _ = initialize_metrics();
}
