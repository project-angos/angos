use std::{
    io::Error,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::warn;

const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
const CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 10;

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    consecutive_failures: Arc<AtomicU32>,
    opened_at_epoch_secs: Arc<AtomicU64>,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            opened_at_epoch_secs: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn check(&self) -> Result<(), Error> {
        let failures = self.consecutive_failures.load(Ordering::Acquire);
        if failures < CIRCUIT_BREAKER_THRESHOLD {
            return Ok(());
        }
        let opened_at = self.opened_at_epoch_secs.load(Ordering::Acquire);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.saturating_sub(opened_at) >= CIRCUIT_BREAKER_COOLDOWN_SECS {
            return Ok(());
        }
        Err(Error::other(format!(
            "circuit breaker open: {failures} consecutive failures, \
             cooling down for {CIRCUIT_BREAKER_COOLDOWN_SECS}s"
        )))
    }

    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.opened_at_epoch_secs.store(0, Ordering::Release);
    }

    pub fn record_failure(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Store `opened_at` BEFORE publishing the new failure count. Any concurrent
        // `check()` that reads `consecutive_failures >= THRESHOLD` via Acquire is then
        // guaranteed (release-acquire pair via the fetch_add) to read at least this
        // `opened_at` value, eliminating the window where the count appears open but
        // `opened_at` is still zero. Writing on every failure also re-arms the cooldown
        // when a request slipped through after the previous cooldown expired and failed.
        self.opened_at_epoch_secs.store(now, Ordering::Release);
        let prev = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == CIRCUIT_BREAKER_THRESHOLD {
            warn!(
                threshold = CIRCUIT_BREAKER_THRESHOLD,
                cooldown_secs = CIRCUIT_BREAKER_COOLDOWN_SECS,
                "Circuit breaker opened after {CIRCUIT_BREAKER_THRESHOLD} consecutive failures"
            );
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn check_passes_when_below_threshold() {
        let cb = CircuitBreaker::new();
        for _ in 0..(CIRCUIT_BREAKER_THRESHOLD - 1) {
            cb.record_failure();
        }
        assert!(cb.check().is_ok());
    }

    #[test]
    fn check_fails_when_threshold_crossed() {
        let cb = CircuitBreaker::new();
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            cb.record_failure();
        }
        assert!(cb.check().is_err());
    }

    #[test]
    fn record_success_clears_opened_at_and_failure_count() {
        let cb = CircuitBreaker::new();
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            cb.record_failure();
        }
        assert!(cb.check().is_err());

        cb.record_success();

        assert_eq!(cb.consecutive_failures.load(Ordering::Acquire), 0);
        assert_eq!(cb.opened_at_epoch_secs.load(Ordering::Acquire), 0);
        assert!(cb.check().is_ok());
    }

    #[test]
    fn record_failure_after_cooldown_re_arms_opened_at() {
        let cb = CircuitBreaker::new();
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            cb.record_failure();
        }
        let initial_opened_at = cb.opened_at_epoch_secs.load(Ordering::Acquire);

        // Simulate cooldown elapse by rewinding `opened_at` past the cooldown window.
        cb.opened_at_epoch_secs.store(
            initial_opened_at.saturating_sub(CIRCUIT_BREAKER_COOLDOWN_SECS + 1),
            Ordering::Release,
        );
        assert!(
            cb.check().is_ok(),
            "after cooldown elapses, check should pass"
        );

        // A subsequent failure must re-arm `opened_at` to a fresh timestamp,
        // re-opening the breaker for another cooldown window.
        cb.record_failure();
        let re_armed = cb.opened_at_epoch_secs.load(Ordering::Acquire);
        assert!(
            re_armed >= initial_opened_at,
            "post-cooldown failure must re-arm opened_at: was {initial_opened_at}, now {re_armed}"
        );
        assert!(
            cb.check().is_err(),
            "breaker must re-open after a post-cooldown failure"
        );
    }

    #[test]
    fn concurrent_failures_publish_opened_at_before_count() {
        // Drive many threads to call `record_failure` concurrently, then verify that any
        // `check()` observation that sees `failures >= THRESHOLD` ALSO sees `opened_at != 0`.
        // Without the release-before-publish ordering, `check()` could see failures=5 but
        // opened_at=0 and incorrectly return Ok.
        for _ in 0..100 {
            let cb = CircuitBreaker::new();
            let cb_clone = cb.clone();
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let cb = cb_clone.clone();
                    thread::spawn(move || cb.record_failure())
                })
                .collect();

            // Hammer check() concurrently with the failures.
            let cb_checker = cb.clone();
            let checker = thread::spawn(move || {
                let mut observations = Vec::new();
                for _ in 0..1000 {
                    let failures = cb_checker.consecutive_failures.load(Ordering::Acquire);
                    let opened_at = cb_checker.opened_at_epoch_secs.load(Ordering::Acquire);
                    observations.push((failures, opened_at));
                }
                observations
            });

            for h in handles {
                h.join().unwrap();
            }
            let observations = checker.join().unwrap();

            // Invariant: whenever a checker thread sees failures >= THRESHOLD,
            // opened_at must be non-zero (i.e., set by some prior record_failure).
            for (failures, opened_at) in observations {
                if failures >= CIRCUIT_BREAKER_THRESHOLD {
                    assert!(
                        opened_at != 0,
                        "race observed: failures={failures} but opened_at=0"
                    );
                }
            }
        }
    }
}
