//! Metrics hook for lock backends.
//!
//! The engine crate cannot depend on the registry's prometheus-backed
//! `metrics_provider`, so it exposes a [`LockMetrics`] trait with no-op
//! default impls and a global slot the host (registry) sets at startup. Each
//! backend calls into [`lock_metrics()`] at every observation point; with no
//! installed sink the calls are zero-cost.
//!
//! The five observation points mirror the prometheus shape:
//! - `lock_acquisition_duration_ms` (histogram, labels: `backend`)
//! - `lock_acquisitions_total` (counter, labels: `backend`, `outcome`)
//! - `lock_invalidations_total` (counter, labels: `backend`, `reason`)
//! - `lock_retries_total` (counter, labels: `backend`)
//! - `lock_recoveries_total` (counter, labels: `backend`, `outcome`)

use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

/// Sink for lock-backend observations.
///
/// Every method defaults to a no-op so impls can override only the events
/// they care about and so backends never need to branch on `Option`.
pub trait LockMetrics: Send + Sync {
    fn observe_acquisition_duration(&self, _backend: &str, _ms: f64) {}
    fn record_acquisition(&self, _backend: &str, _outcome: &str) {}
    fn record_invalidation(&self, _backend: &str, _reason: &str) {}
    fn record_retry(&self, _backend: &str) {}
    fn record_recovery(&self, _backend: &str, _outcome: &str) {}
}

/// Default sink: every method is a no-op.
pub struct NoopMetrics;

impl LockMetrics for NoopMetrics {}

static METRICS: OnceLock<Arc<dyn LockMetrics>> = OnceLock::new();
static NOOP: NoopMetrics = NoopMetrics;

/// Install the process-wide [`LockMetrics`] sink.
///
/// Intended to be called once during host startup, before any lock backend
/// is constructed. Subsequent calls are ignored and return `Err` so the host
/// can detect double-initialisation.
///
/// # Errors
///
/// Returns the supplied `Arc` back when a sink is already installed.
pub fn set_lock_metrics(sink: Arc<dyn LockMetrics>) -> Result<(), Arc<dyn LockMetrics>> {
    METRICS.set(sink)
}

/// Return the installed sink, or the no-op default.
///
/// Callers receive `&'static dyn LockMetrics` and can record observations
/// unconditionally; with no installed sink the calls are no-ops.
pub fn lock_metrics() -> &'static dyn LockMetrics {
    match METRICS.get() {
        Some(sink) => sink.as_ref(),
        None => &NOOP,
    }
}

/// Milliseconds elapsed since `start`, suitable for histogram observations.
///
/// Uses `Duration::as_secs_f64()` to avoid the precision-loss lint that
/// `Duration::as_millis() as f64` triggers; lossless at per-request scales.
#[must_use]
pub fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
