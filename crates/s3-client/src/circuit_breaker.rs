use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::warn;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CircuitBreakerError {
    pub failures: u32,
    pub cooldown_secs: u64,
}

impl fmt::Display for CircuitBreakerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "circuit breaker open: {} consecutive failures, cooling down for {}s",
            self.failures, self.cooldown_secs
        )
    }
}

impl std::error::Error for CircuitBreakerError {}

impl From<CircuitBreakerError> for io::Error {
    fn from(e: CircuitBreakerError) -> Self {
        io::Error::other(e.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    consecutive_failures: Arc<AtomicU32>,
    opened_at_epoch_secs: Arc<AtomicU64>,
    threshold: u32,
    cooldown_secs: u64,
}

impl CircuitBreaker {
    /// Build a breaker that opens after `threshold` consecutive failures and
    /// stays open for `cooldown_secs` before admitting a half-open probe.
    pub fn new(threshold: u32, cooldown_secs: u64) -> Self {
        Self {
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            opened_at_epoch_secs: Arc::new(AtomicU64::new(0)),
            threshold,
            cooldown_secs,
        }
    }

    /// Gate a request against the breaker.
    ///
    /// Returns `Ok(())` while closed (below the failure threshold). Once open,
    /// requests are rejected until the cooldown elapses, after which the breaker
    /// goes *half-open*: exactly one probe request is admitted per cooldown
    /// window. The probe's outcome (`record_success` closes the breaker,
    /// `record_failure` re-arms it) decides what happens next; concurrent
    /// callers during the probe are rejected, avoiding a thundering herd against
    /// a backend that may still be down.
    pub fn check(&self) -> Result<(), CircuitBreakerError> {
        let failures = self.consecutive_failures.load(Ordering::Acquire);
        if failures < self.threshold {
            return Ok(());
        }
        let opened_at = self.opened_at_epoch_secs.load(Ordering::Acquire);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let rejected = || CircuitBreakerError {
            failures,
            cooldown_secs: self.cooldown_secs,
        };
        if now.saturating_sub(opened_at) < self.cooldown_secs {
            return Err(rejected());
        }
        // Half-open: the cooldown has elapsed. Admit a single probe per window
        // instead of releasing every waiting caller at once. The caller that
        // wins the CAS advances `opened_at` to now and becomes the probe;
        // concurrent callers either lose the CAS or read the advanced timestamp
        // (cooldown no longer elapsed) and are rejected. A probe that never
        // reports a result is superseded by the next caller after another
        // cooldown, so the breaker cannot wedge half-open.
        if self
            .opened_at_epoch_secs
            .compare_exchange(opened_at, now, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Ok(())
        } else {
            Err(rejected())
        }
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
        if prev + 1 == self.threshold {
            warn!(
                threshold = self.threshold,
                cooldown_secs = self.cooldown_secs,
                "Circuit breaker opened after {} consecutive failures",
                self.threshold
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    const THRESHOLD: u32 = 5;
    const COOLDOWN_SECS: u64 = 10;

    fn breaker() -> CircuitBreaker {
        CircuitBreaker::new(THRESHOLD, COOLDOWN_SECS)
    }

    #[test]
    fn new_breaker_starts_closed() {
        let cb = breaker();
        assert_eq!(cb.consecutive_failures.load(Ordering::Acquire), 0);
        assert_eq!(cb.opened_at_epoch_secs.load(Ordering::Acquire), 0);
        assert!(cb.check().is_ok());
    }

    #[test]
    fn open_error_message_contains_expected_wording() {
        let cb = breaker();
        for _ in 0..THRESHOLD {
            cb.record_failure();
        }
        let err = cb.check().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("circuit breaker open"),
            "error must mention 'circuit breaker open', got: {msg}"
        );
        assert!(
            msg.contains(&THRESHOLD.to_string()),
            "error must include failure count, got: {msg}"
        );
        assert!(
            msg.contains(&COOLDOWN_SECS.to_string()),
            "error must include cooldown seconds, got: {msg}"
        );
    }

    #[test]
    fn open_error_carries_typed_failure_count_and_cooldown() {
        let cb = breaker();
        for _ in 0..THRESHOLD {
            cb.record_failure();
        }
        let err = cb.check().unwrap_err();
        assert_eq!(err.failures, THRESHOLD);
        assert_eq!(err.cooldown_secs, COOLDOWN_SECS);
    }

    #[test]
    fn success_mid_sequence_resets_failure_count() {
        let cb = breaker();
        for _ in 0..(THRESHOLD - 1) {
            cb.record_failure();
        }
        cb.record_success();

        // After the reset, THRESHOLD - 1 additional failures must not open the breaker.
        for _ in 0..(THRESHOLD - 1) {
            cb.record_failure();
        }
        assert!(
            cb.check().is_ok(),
            "THRESHOLD-1 failures after a success must not open the breaker"
        );

        // One more failure crosses the threshold.
        cb.record_failure();
        assert!(
            cb.check().is_err(),
            "reaching THRESHOLD failures after a mid-sequence reset must open the breaker"
        );
    }

    #[test]
    fn post_cooldown_success_fully_clears_state() {
        let cb = breaker();
        for _ in 0..THRESHOLD {
            cb.record_failure();
        }
        // Simulate cooldown elapse by rewinding `opened_at`.
        cb.opened_at_epoch_secs.store(
            cb.opened_at_epoch_secs
                .load(Ordering::Acquire)
                .saturating_sub(COOLDOWN_SECS + 1),
            Ordering::Release,
        );
        assert!(cb.check().is_ok(), "breaker should pass after cooldown");

        cb.record_success();

        assert_eq!(
            cb.consecutive_failures.load(Ordering::Acquire),
            0,
            "record_success must zero consecutive_failures"
        );
        assert_eq!(
            cb.opened_at_epoch_secs.load(Ordering::Acquire),
            0,
            "record_success must zero opened_at_epoch_secs"
        );
        assert!(
            cb.check().is_ok(),
            "breaker must pass after success fully clears state"
        );
    }

    #[test]
    fn check_passes_when_below_threshold() {
        let cb = breaker();
        for _ in 0..(THRESHOLD - 1) {
            cb.record_failure();
        }
        assert!(cb.check().is_ok());
    }

    #[test]
    fn record_success_clears_opened_at_and_failure_count() {
        let cb = breaker();
        for _ in 0..THRESHOLD {
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
        let cb = breaker();
        for _ in 0..THRESHOLD {
            cb.record_failure();
        }
        let initial_opened_at = cb.opened_at_epoch_secs.load(Ordering::Acquire);

        // Simulate cooldown elapse by rewinding `opened_at` past the cooldown window.
        cb.opened_at_epoch_secs.store(
            initial_opened_at.saturating_sub(COOLDOWN_SECS + 1),
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
            let cb = breaker();
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
                if failures >= THRESHOLD {
                    assert!(
                        opened_at != 0,
                        "race observed: failures={failures} but opened_at=0"
                    );
                }
            }
        }
    }

    /// Helper: open the breaker and rewind `opened_at` so the cooldown reads as
    /// elapsed, leaving the breaker half-open ready to admit one probe.
    fn open_and_elapse_cooldown() -> CircuitBreaker {
        let cb = breaker();
        for _ in 0..THRESHOLD {
            cb.record_failure();
        }
        cb.opened_at_epoch_secs.store(
            cb.opened_at_epoch_secs
                .load(Ordering::Acquire)
                .saturating_sub(COOLDOWN_SECS + 1),
            Ordering::Release,
        );
        cb
    }

    #[test]
    fn half_open_rejects_second_caller_until_next_cooldown() {
        let cb = open_and_elapse_cooldown();
        assert!(
            cb.check().is_ok(),
            "first caller after cooldown is admitted as the probe"
        );
        assert!(
            cb.check().is_err(),
            "a second caller in the same half-open window must be rejected (no herd)"
        );
    }

    #[test]
    fn half_open_admits_exactly_one_concurrent_probe() {
        // Many callers race check() at the moment the cooldown elapses; exactly
        // one must be admitted (the probe), the rest rejected.
        for _ in 0..100 {
            let cb = open_and_elapse_cooldown();
            let admitted = Arc::new(AtomicU32::new(0));
            let handles: Vec<_> = (0..32)
                .map(|_| {
                    let cb = cb.clone();
                    let admitted = Arc::clone(&admitted);
                    thread::spawn(move || {
                        if cb.check().is_ok() {
                            admitted.fetch_add(1, Ordering::AcqRel);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(
                admitted.load(Ordering::Acquire),
                1,
                "exactly one probe must be admitted per cooldown window"
            );
        }
    }

    #[test]
    fn half_open_probe_failure_re_arms_then_next_window_admits_again() {
        let cb = open_and_elapse_cooldown();
        assert!(cb.check().is_ok(), "probe admitted");

        // The probe fails: the breaker must re-arm and reject further callers.
        cb.record_failure();
        assert!(
            cb.check().is_err(),
            "after a failed probe the breaker re-opens for another cooldown"
        );

        // Once the new cooldown elapses, a fresh probe is admitted.
        cb.opened_at_epoch_secs.store(
            cb.opened_at_epoch_secs
                .load(Ordering::Acquire)
                .saturating_sub(COOLDOWN_SECS + 1),
            Ordering::Release,
        );
        assert!(
            cb.check().is_ok(),
            "a fresh probe is admitted after the next cooldown elapses"
        );
    }
}
