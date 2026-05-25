use std::sync::OnceLock;

use prometheus::{
    Encoder, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Registry as PrometheusRegistry,
    TextEncoder, register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry,
};
use tracing::error;

use crate::registry::Error;

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
        metrics_provider().metric_http_request_in_flight.inc();
        Self
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        metrics_provider().metric_http_request_in_flight.dec();
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
    pub job_queue_pending: IntGaugeVec,
    pub job_queue_enqueued_total: IntCounterVec,
    pub job_queue_enqueue_failures_total: IntCounterVec,
}

/// Map a Prometheus registration failure to an `Error::Initialization`,
/// emitting a `tracing::error` with the metric name first.
fn register_err(name: &'static str) -> impl FnOnce(prometheus::Error) -> Error {
    move |error| {
        error!("Unable to create {name} metric: {error}");
        Error::Initialization(format!("Unable to create {name} metric"))
    }
}

impl MetricsProvider {
    pub fn new() -> Result<Self, Error> {
        let registry = PrometheusRegistry::new();

        let metric_http_request_total = Self::build_http_request_total(&registry)?;
        let metric_http_request_duration = Self::build_http_request_duration(&registry)?;
        let metric_http_request_in_flight = Self::build_http_request_in_flight(&registry)?;
        let auth_attempts = Self::build_auth_attempts(&registry)?;
        let lock_acquisition_duration = Self::build_lock_acquisition_duration(&registry)?;
        let lock_acquisitions = Self::build_lock_acquisitions(&registry)?;
        let lock_retries = Self::build_lock_retries(&registry)?;
        let lock_invalidations = Self::build_lock_invalidations(&registry)?;
        let lock_recoveries = Self::build_lock_recoveries(&registry)?;
        let job_queue_pending = Self::build_job_queue_pending(&registry)?;
        let job_queue_enqueued_total = Self::build_job_queue_enqueued_total(&registry)?;
        let job_queue_enqueue_failures_total =
            Self::build_job_queue_enqueue_failures_total(&registry)?;

        Ok(Self {
            registry,
            metric_http_request_total,
            metric_http_request_duration,
            metric_http_request_in_flight,
            auth_attempts,
            lock_acquisition_duration,
            lock_acquisitions,
            lock_retries,
            lock_invalidations,
            lock_recoveries,
            job_queue_pending,
            job_queue_enqueued_total,
            job_queue_enqueue_failures_total,
        })
    }

    fn build_http_request_total(registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "http_requests_total",
            "Total number of HTTP requests made.",
            &["method", "route", "status"],
            registry
        )
        .map_err(register_err("http_requests_total"))
    }

    fn build_http_request_duration(registry: &PrometheusRegistry) -> Result<HistogramVec, Error> {
        register_histogram_vec_with_registry!(
            "http_request_duration_ms",
            "The HTTP request latencies in milliseconds.",
            &["method", "route"],
            registry
        )
        .map_err(register_err("http_request_duration_ms"))
    }

    fn build_http_request_in_flight(registry: &PrometheusRegistry) -> Result<IntGauge, Error> {
        register_int_gauge_with_registry!(
            "http_requests_in_flight",
            "The current number of in-flight HTTP requests.",
            registry
        )
        .map_err(register_err("http_requests_in_flight"))
    }

    fn build_auth_attempts(registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "auth_attempts_total",
            "Total number of authentication attempts",
            &["method", "result"],
            registry
        )
        .map_err(register_err("auth_attempts_total"))
    }

    fn build_lock_acquisition_duration(
        registry: &PrometheusRegistry,
    ) -> Result<HistogramVec, Error> {
        register_histogram_vec_with_registry!(
            "lock_acquisition_duration_ms",
            "Lock acquisition duration in milliseconds",
            &["backend"],
            registry
        )
        .map_err(register_err("lock_acquisition_duration_ms"))
    }

    fn build_lock_acquisitions(registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "lock_acquisitions_total",
            "Total lock acquisition attempts",
            &["backend", "result"],
            registry
        )
        .map_err(register_err("lock_acquisitions_total"))
    }

    fn build_lock_retries(registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "lock_retries_total",
            "Total lock acquisition retries",
            &["backend"],
            registry
        )
        .map_err(register_err("lock_retries_total"))
    }

    fn build_lock_invalidations(registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "lock_invalidations_total",
            "Total lock invalidations",
            &["backend", "reason"],
            registry
        )
        .map_err(register_err("lock_invalidations_total"))
    }

    fn build_lock_recoveries(registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "lock_recoveries_total",
            "Total stale lock recovery attempts",
            &["backend", "result"],
            registry
        )
        .map_err(register_err("lock_recoveries_total"))
    }

    fn build_job_queue_pending(registry: &PrometheusRegistry) -> Result<IntGaugeVec, Error> {
        register_int_gauge_vec_with_registry!(
            "angos_job_queue_pending",
            "Number of jobs currently pending in the queue",
            &["queue"],
            registry
        )
        .map_err(register_err("angos_job_queue_pending"))
    }

    fn build_job_queue_enqueued_total(
        registry: &PrometheusRegistry,
    ) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "angos_job_queue_enqueued_total",
            "Total jobs submitted to the queue",
            &["queue", "dedup"],
            registry
        )
        .map_err(register_err("angos_job_queue_enqueued_total"))
    }

    fn build_job_queue_enqueue_failures_total(
        registry: &PrometheusRegistry,
    ) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(
            "angos_job_queue_enqueue_failures_total",
            "Total enqueue attempts that did not land on the queue (envelope build or storage error)",
            &["queue"],
            registry
        )
        .map_err(register_err("angos_job_queue_enqueue_failures_total"))
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
pub fn init_for_tests() {
    // Ignore the already-initialized error — tests run in parallel and share one process.
    let _ = initialize_metrics();
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Mutex, PoisonError},
        thread,
    };

    use super::*;

    // Serializes all tests that touch the in-flight gauge so they cannot observe
    // each other's intermediate values.
    static IN_FLIGHT_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn new_registers_all_metrics_and_gather_succeeds() {
        let provider = MetricsProvider::new().expect("MetricsProvider::new must succeed");
        let (content_type, payload) = provider.gather().expect("gather must succeed");

        assert!(
            content_type.starts_with("text/plain"),
            "content type must start with text/plain, got: {content_type}"
        );
        assert!(
            content_type.contains("version="),
            "content type must include Prometheus exposition version, got: {content_type}"
        );

        let text = String::from_utf8(payload).expect("gather output must be valid UTF-8");
        assert!(
            text.contains("http_requests_in_flight"),
            "gathered output must include http_requests_in_flight gauge"
        );
    }

    #[test]
    fn gather_emits_recorded_counter_values() {
        let provider = MetricsProvider::new().expect("MetricsProvider::new must succeed");

        // Increment the same label combination twice so the counter reaches 2.
        let labels = ["GET", "/v2/", "200"];
        provider
            .metric_http_request_total
            .with_label_values(&labels)
            .inc_by(2);

        let (_, payload) = provider.gather().expect("gather must succeed");
        let text = String::from_utf8(payload).expect("gather output must be valid UTF-8");

        assert!(
            text.contains("http_requests_total"),
            "gathered output must include http_requests_total"
        );
        assert!(
            text.contains("GET"),
            "gathered output must include the method label value"
        );
        assert!(
            text.contains("200"),
            "gathered output must include the status label value"
        );
        // The Prometheus text format ends a sample line with "} <value>\n".
        assert!(
            text.contains("} 2"),
            "gathered output must contain a sample with value 2, output:\n{text}"
        );
    }

    #[test]
    fn in_flight_guard_increments_on_new_and_decrements_on_drop() {
        let _lock = IN_FLIGHT_TEST_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        init_for_tests();

        let gauge = &metrics_provider().metric_http_request_in_flight;
        let baseline = gauge.get();

        let outer = InFlightGuard::new();
        assert_eq!(
            gauge.get(),
            baseline + 1,
            "gauge must be baseline+1 after outer guard created"
        );

        let inner = InFlightGuard::new();
        assert_eq!(
            gauge.get(),
            baseline + 2,
            "gauge must be baseline+2 after inner guard created"
        );

        drop(inner);
        assert_eq!(
            gauge.get(),
            baseline + 1,
            "gauge must return to baseline+1 after inner guard dropped"
        );

        drop(outer);
        assert_eq!(
            gauge.get(),
            baseline,
            "gauge must return to baseline after outer guard dropped"
        );
    }

    #[test]
    fn in_flight_guard_concurrent_invariant() {
        const THREADS: usize = 32;
        const ITERATIONS: usize = 50;

        let _lock = IN_FLIGHT_TEST_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        init_for_tests();

        let baseline = metrics_provider().metric_http_request_in_flight.get();

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                thread::spawn(|| {
                    for _ in 0..ITERATIONS {
                        let _g = InFlightGuard::new();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("worker thread must not panic");
        }

        assert_eq!(
            metrics_provider().metric_http_request_in_flight.get(),
            baseline,
            "gauge must return to baseline after all guards are dropped"
        );
    }

    #[test]
    fn initialize_metrics_rejects_double_init() {
        // Ensure the global is set, then call initialize_metrics() again.
        init_for_tests();
        let result = initialize_metrics();
        assert!(
            matches!(result, Err(Error::Initialization(_))),
            "second initialize_metrics call must return Err(Initialization), got: {result:?}"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already initialized"),
            "error message must mention 'already initialized', got: {err_msg}"
        );
    }
}
