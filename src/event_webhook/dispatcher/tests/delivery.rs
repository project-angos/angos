use std::time::Duration;

use crate::event_webhook::dispatcher::{backoff_for_attempt, format_retry_failure};

#[test]
fn backoff_grows_exponentially() {
    assert_eq!(backoff_for_attempt(1), Duration::from_millis(100));
    assert_eq!(backoff_for_attempt(2), Duration::from_millis(200));
    assert_eq!(backoff_for_attempt(3), Duration::from_millis(400));
}

#[test]
fn backoff_saturates_for_huge_attempt() {
    assert_eq!(backoff_for_attempt(100), Duration::from_millis(u64::MAX));
}

#[test]
fn format_retry_failure_one_attempt_identical_error() {
    assert_eq!(
        format_retry_failure(1, Some("connection refused"), Some("connection refused")),
        "after 1 attempt(s): connection refused"
    );
}

#[test]
fn format_retry_failure_three_attempts_identical_error() {
    assert_eq!(
        format_retry_failure(3, Some("connection refused"), Some("connection refused")),
        "after 3 attempt(s): connection refused"
    );
}

#[test]
fn format_retry_failure_three_attempts_different_errors() {
    assert_eq!(
        format_retry_failure(3, Some("timeout"), Some("503 Service Unavailable")),
        "after 3 attempt(s); first error: timeout; last error: 503 Service Unavailable"
    );
}

#[test]
fn format_retry_failure_none_is_defensive_unknown() {
    assert_eq!(
        format_retry_failure(0, None, None),
        "after 0 attempt(s): unknown error"
    );
}
