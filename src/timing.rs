use std::time::Instant;

/// Returns the milliseconds elapsed since `start` as `f64`, suitable for
/// Prometheus histogram observations. Uses `Duration::as_secs_f64()` to avoid
/// the precision-loss lint that `Duration::as_millis() as f64` would trigger;
/// the conversion is lossless within scales relevant to per-request timing.
pub fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
