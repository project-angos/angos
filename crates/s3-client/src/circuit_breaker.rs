use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use tracing::warn;

use crate::Error;

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    consecutive_failures: Arc<AtomicU32>,
    /// Seconds since [`Self::start`] at which the breaker last opened. Measured
    /// against a monotonic baseline rather than the wall clock, which a step
    /// backwards would stretch the cooldown by and a step forwards would skip.
    opened_at_secs: Arc<AtomicU64>,
    start: Instant,
    threshold: u32,
    cooldown_secs: u64,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown_secs: u64) -> Self {
        Self {
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            opened_at_secs: Arc::new(AtomicU64::new(0)),
            start: Instant::now(),
            threshold,
            cooldown_secs,
        }
    }

    /// Once open, rejects requests until the cooldown elapses, then admits
    /// exactly one half-open probe per cooldown window so a backend that may
    /// still be down is not hit by a thundering herd.
    pub fn check(&self) -> Result<(), Error> {
        let failures = self.consecutive_failures.load(Ordering::Acquire);
        if failures < self.threshold {
            return Ok(());
        }
        let opened_at = self.opened_at_secs.load(Ordering::Acquire);
        let now = self.start.elapsed().as_secs();
        let rejected = || {
            Error::Io(format!(
                "circuit breaker open: {failures} consecutive failures, cooling down for {}s",
                self.cooldown_secs
            ))
        };
        if now.saturating_sub(opened_at) < self.cooldown_secs {
            return Err(rejected());
        }
        // The CAS winner advances `opened_at` and becomes the probe; a probe
        // that never reports is superseded after another cooldown, so the
        // breaker cannot wedge half-open.
        if self
            .opened_at_secs
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
        self.opened_at_secs.store(0, Ordering::Release);
    }

    pub fn record_failure(&self) {
        let now = self.start.elapsed().as_secs();
        // Stored before the count is published, so a `check()` that sees the
        // threshold reached also sees this `opened_at`; writing it on every
        // failure also re-arms the cooldown after a failed probe.
        self.opened_at_secs.store(now, Ordering::Release);
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
    use std::time::Duration;

    use super::*;

    const THRESHOLD: u32 = 5;
    const COOLDOWN_SECS: u64 = 10;

    /// The baseline sits far enough in the past that a test can rewind
    /// `opened_at` by a whole cooldown to expire it, which a breaker built at
    /// `Instant::now()` has no room for.
    fn breaker() -> CircuitBreaker {
        let elapsed = Duration::from_secs(COOLDOWN_SECS * 10);
        CircuitBreaker {
            start: Instant::now()
                .checked_sub(elapsed)
                .unwrap_or_else(Instant::now),
            ..CircuitBreaker::new(THRESHOLD, COOLDOWN_SECS)
        }
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
        let cb = open_and_elapse_cooldown();
        assert!(cb.check().is_ok(), "breaker should pass after cooldown");

        cb.record_success();

        assert!(
            cb.check().is_ok(),
            "breaker must pass after success fully clears state"
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
                    let opened_at = cb_checker.opened_at_secs.load(Ordering::Acquire);
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

    /// Open the breaker and rewind `opened_at` so the cooldown reads as
    /// elapsed, leaving the breaker half-open ready to admit one probe.
    fn open_and_elapse_cooldown() -> CircuitBreaker {
        let cb = breaker();
        for _ in 0..THRESHOLD {
            cb.record_failure();
        }
        cb.opened_at_secs.store(
            cb.opened_at_secs
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
        assert!(
            cb.check().is_ok(),
            "the first caller after cooldown is the probe"
        );

        // The probe fails: the breaker must re-arm and reject further callers.
        cb.record_failure();
        assert!(
            cb.check().is_err(),
            "after a failed probe the breaker re-opens for another cooldown"
        );

        // Once the new cooldown elapses, a fresh probe is admitted.
        cb.opened_at_secs.store(
            cb.opened_at_secs
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
