use std::process::exit;
use std::sync::LazyLock;

use prometheus::{
    DEFAULT_BUCKETS, Encoder, Error as PrometheusError, HistogramVec, IntCounterVec, IntGauge,
    IntGaugeVec, Registry as PrometheusRegistry, TextEncoder, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry,
};
use tracing::error;

/// Millisecond buckets, from a cached HEAD to a multi-gigabyte blob transfer.
/// The crate default ends at 10, which as milliseconds would pin every quantile there.
const HTTP_DURATION_BUCKETS_MS: [f64; 14] = [
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
    60_000.0,
];

/// Errors raised while initializing or serving the metrics registry.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Initialization(String),
    #[error("unable to encode metrics: {0}")]
    Encode(String),
}

static METRICS: LazyLock<MetricsProvider> = LazyLock::new(|| {
    match MetricsProvider::new() {
        Ok(provider) => provider,
        // Only a duplicate metric registration (a programmer error any
        // metric-recording test catches) lands here; fail as fast as the old
        // startup initialization did.
        Err(e) => {
            eprintln!("Failed to initialize metrics provider: {e}");
            exit(1);
        }
    }
});

/// Initializes the metrics provider. Recording self-initializes on first
/// use; calling this at startup only front-loads the registration work.
pub fn initialize_metrics() {
    LazyLock::force(&METRICS);
}

/// Returns the process-wide metrics provider, initializing it on first use.
pub fn metrics_provider() -> &'static MetricsProvider {
    &METRICS
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
    pub webhook_auth_requests: IntCounterVec,
    pub webhook_auth_duration: HistogramVec,
    pub event_webhook_deliveries: IntCounterVec,
    pub event_webhook_delivery_duration: HistogramVec,
    pub job_queue_pending: IntGaugeVec,
    pub job_queue_failed: IntGaugeVec,
    pub job_queue_enqueued_total: IntCounterVec,
    pub job_queue_enqueue_failures_total: IntCounterVec,
    pub replication_push_total: IntCounterVec,
    pub replication_last_success_timestamp: IntGaugeVec,
    pub replication_reconcile_total: IntCounterVec,
    pub pull_through_total: IntCounterVec,
}

/// Map a Prometheus registration failure to an `Error::Initialization`,
/// emitting a `tracing::error` with the metric name first.
fn register_err(name: &'static str) -> impl FnOnce(PrometheusError) -> Error {
    move |error| {
        error!("Unable to create {name} metric: {error}");
        Error::Initialization(format!("Unable to create {name} metric"))
    }
}

/// A metric's name, help text and label names.
struct MetricSpec {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
}

const HTTP_REQUESTS_TOTAL: MetricSpec = MetricSpec {
    name: "http_requests_total",
    help: "Total number of HTTP requests made.",
    labels: &["method", "route", "status"],
};
const HTTP_REQUEST_DURATION: MetricSpec = MetricSpec {
    name: "http_request_duration_ms",
    help: "The HTTP request latencies in milliseconds.",
    labels: &["method", "route"],
};
const HTTP_REQUESTS_IN_FLIGHT: MetricSpec = MetricSpec {
    name: "http_requests_in_flight",
    help: "The current number of in-flight HTTP requests.",
    labels: &[],
};
const AUTH_ATTEMPTS: MetricSpec = MetricSpec {
    name: "auth_attempts_total",
    help: "Total number of authentication attempts",
    labels: &["method", "result"],
};
const WEBHOOK_AUTH_REQUESTS: MetricSpec = MetricSpec {
    name: "webhook_authorization_requests_total",
    help: "Total webhook authorization requests",
    labels: &["webhook", "result"],
};
const WEBHOOK_AUTH_DURATION: MetricSpec = MetricSpec {
    name: "webhook_authorization_duration_seconds",
    help: "Webhook authorization request duration",
    labels: &["webhook"],
};
const EVENT_WEBHOOK_DELIVERIES: MetricSpec = MetricSpec {
    name: "event_webhook_deliveries_total",
    help: "Total event webhook deliveries",
    labels: &["webhook", "event", "result"],
};
const EVENT_WEBHOOK_DELIVERY_DURATION: MetricSpec = MetricSpec {
    name: "event_webhook_delivery_duration_seconds",
    help: "Event webhook delivery duration",
    labels: &["webhook", "event"],
};
const JOB_QUEUE_PENDING: MetricSpec = MetricSpec {
    name: "angos_job_queue_pending",
    help: "Number of jobs currently pending in the queue",
    labels: &["queue"],
};
const JOB_QUEUE_FAILED: MetricSpec = MetricSpec {
    name: "angos_job_queue_failed",
    help: "Number of dead-lettered jobs currently in the queue",
    labels: &["queue"],
};
const JOB_QUEUE_ENQUEUED: MetricSpec = MetricSpec {
    name: "angos_job_queue_enqueued_total",
    help: "Total jobs submitted to the queue",
    labels: &["queue", "dedup"],
};
const JOB_QUEUE_ENQUEUE_FAILURES: MetricSpec = MetricSpec {
    name: "angos_job_queue_enqueue_failures_total",
    help: "Total enqueue attempts that did not land on the queue (envelope build or storage error)",
    labels: &["queue"],
};
const REPLICATION_PUSH: MetricSpec = MetricSpec {
    name: "angos_replication_push_total",
    help: "Total replication pushes to a downstream, by outcome (pushed, converged, superseded, failed)",
    labels: &["downstream", "outcome"],
};
const REPLICATION_LAST_SUCCESS: MetricSpec = MetricSpec {
    name: "angos_replication_last_success_timestamp_seconds",
    help: "Unix timestamp (seconds) of the last successful or superseded replication push per downstream",
    labels: &["downstream"],
};
const REPLICATION_RECONCILE: MetricSpec = MetricSpec {
    name: "angos_replication_reconcile_total",
    help: "Total replication reconcile enqueues emitted by the scrub checker, by outcome",
    labels: &["outcome"],
};
const PULL_THROUGH: MetricSpec = MetricSpec {
    name: "angos_pull_through_total",
    help: "Total pull-through cache lookups, by repository, content kind (manifest, blob) and outcome (hit, miss, refresh, stale)",
    labels: &["repository", "kind", "outcome"],
};

impl MetricSpec {
    fn counter_vec(&self, registry: &PrometheusRegistry) -> Result<IntCounterVec, Error> {
        register_int_counter_vec_with_registry!(self.name, self.help, self.labels, registry)
            .map_err(register_err(self.name))
    }

    fn gauge_vec(&self, registry: &PrometheusRegistry) -> Result<IntGaugeVec, Error> {
        register_int_gauge_vec_with_registry!(self.name, self.help, self.labels, registry)
            .map_err(register_err(self.name))
    }

    fn gauge(&self, registry: &PrometheusRegistry) -> Result<IntGauge, Error> {
        register_int_gauge_with_registry!(self.name, self.help, registry)
            .map_err(register_err(self.name))
    }

    fn histogram_vec(
        &self,
        registry: &PrometheusRegistry,
        buckets: &[f64],
    ) -> Result<HistogramVec, Error> {
        register_histogram_vec_with_registry!(
            self.name,
            self.help,
            self.labels,
            buckets.to_vec(),
            registry
        )
        .map_err(register_err(self.name))
    }
}

impl MetricsProvider {
    pub fn new() -> Result<Self, Error> {
        let registry = PrometheusRegistry::new();
        Ok(Self {
            metric_http_request_total: HTTP_REQUESTS_TOTAL.counter_vec(&registry)?,
            metric_http_request_duration: HTTP_REQUEST_DURATION
                .histogram_vec(&registry, &HTTP_DURATION_BUCKETS_MS)?,
            metric_http_request_in_flight: HTTP_REQUESTS_IN_FLIGHT.gauge(&registry)?,
            auth_attempts: AUTH_ATTEMPTS.counter_vec(&registry)?,
            webhook_auth_requests: WEBHOOK_AUTH_REQUESTS.counter_vec(&registry)?,
            webhook_auth_duration: WEBHOOK_AUTH_DURATION
                .histogram_vec(&registry, DEFAULT_BUCKETS)?,
            event_webhook_deliveries: EVENT_WEBHOOK_DELIVERIES.counter_vec(&registry)?,
            event_webhook_delivery_duration: EVENT_WEBHOOK_DELIVERY_DURATION
                .histogram_vec(&registry, DEFAULT_BUCKETS)?,
            job_queue_pending: JOB_QUEUE_PENDING.gauge_vec(&registry)?,
            job_queue_failed: JOB_QUEUE_FAILED.gauge_vec(&registry)?,
            job_queue_enqueued_total: JOB_QUEUE_ENQUEUED.counter_vec(&registry)?,
            job_queue_enqueue_failures_total: JOB_QUEUE_ENQUEUE_FAILURES.counter_vec(&registry)?,
            replication_push_total: REPLICATION_PUSH.counter_vec(&registry)?,
            replication_last_success_timestamp: REPLICATION_LAST_SUCCESS.gauge_vec(&registry)?,
            replication_reconcile_total: REPLICATION_RECONCILE.counter_vec(&registry)?,
            pull_through_total: PULL_THROUGH.counter_vec(&registry)?,
            registry,
        })
    }

    /// Encodes every metric in the Prometheus text format ([`prometheus::TEXT_FORMAT`]).
    pub fn gather(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = vec![];
        let metric_families = self.registry.gather();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .map_err(|error| Error::Encode(error.to_string()))?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, PoisonError};
    use std::thread;

    use super::*;

    // Serializes all tests that touch the in-flight gauge so they cannot observe
    // each other's intermediate values.
    static IN_FLIGHT_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn new_registers_all_metrics_and_gather_succeeds() {
        let provider = MetricsProvider::new().expect("MetricsProvider::new must succeed");
        let payload = provider.gather().expect("gather must succeed");
        let text = String::from_utf8(payload).expect("gather output must be valid UTF-8");
        assert!(
            text.contains("http_requests_in_flight"),
            "gathered output lacks the gauge: {text}"
        );
    }

    #[test]
    fn http_duration_buckets_resolve_a_request_slower_than_ten_milliseconds() {
        let provider = MetricsProvider::new().expect("MetricsProvider::new must succeed");
        provider
            .metric_http_request_duration
            .with_label_values(&["GET", "get-manifest"])
            .observe(40.0);

        let payload = provider.gather().expect("gather must succeed");
        let text = String::from_utf8(payload).expect("gather output must be valid UTF-8");

        assert!(
            text.contains("route=\"get-manifest\",le=\"25\"} 0"),
            "a 40 ms observation must fall outside the 25 ms bucket, output:\n{text}"
        );
        assert!(
            text.contains("route=\"get-manifest\",le=\"50\"} 1"),
            "a 40 ms observation must fall inside the 50 ms bucket, output:\n{text}"
        );
    }

    #[test]
    fn in_flight_guard_increments_on_new_and_decrements_on_drop() {
        let _lock = IN_FLIGHT_TEST_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        initialize_metrics();

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
        initialize_metrics();

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
}
