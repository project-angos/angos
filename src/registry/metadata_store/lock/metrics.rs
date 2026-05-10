use prometheus::{
    HistogramVec, IntCounterVec, Registry as PrometheusRegistry,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
};
use tracing::error;

use crate::registry::Error;

pub struct LockMetrics {
    pub acquisition_duration: HistogramVec,
    pub acquisitions: IntCounterVec,
    pub retries: IntCounterVec,
    pub invalidations: IntCounterVec,
    pub recoveries: IntCounterVec,
}

pub fn register(registry: &PrometheusRegistry) -> Result<LockMetrics, Error> {
    let acquisition_duration = register_histogram_vec_with_registry!(
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

    let acquisitions = register_int_counter_vec_with_registry!(
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

    let retries = register_int_counter_vec_with_registry!(
        "lock_retries_total",
        "Total lock acquisition retries",
        &["backend"],
        registry
    )
    .map_err(|error| {
        error!("Unable to create lock_retries_total metric: {error}");
        Error::Initialization(String::from("Unable to create lock_retries_total metric"))
    })?;

    let invalidations = register_int_counter_vec_with_registry!(
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

    let recoveries = register_int_counter_vec_with_registry!(
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

    Ok(LockMetrics {
        acquisition_duration,
        acquisitions,
        retries,
        invalidations,
        recoveries,
    })
}

pub fn lock_metrics() -> &'static LockMetrics {
    &crate::metrics_provider::metrics_provider().locks
}
