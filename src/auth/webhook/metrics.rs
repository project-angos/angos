use std::sync::LazyLock;

use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};

pub static WEBHOOK_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "webhook_authorization_requests_total",
        "Total webhook authorization requests",
        &["webhook", "result"]
    )
    .unwrap()
});

pub static WEBHOOK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "webhook_authorization_duration_seconds",
        "Webhook authorization request duration",
        &["webhook"]
    )
    .unwrap()
});
