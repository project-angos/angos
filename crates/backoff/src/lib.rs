//! Shared retry-backoff policy: exponential (or constant) growth, capped, with
//! optional pseudo-random jitter.
//!
//! [`Backoff`] is a small, copyable value that maps a zero-indexed retry
//! `attempt` to a [`Duration`]. It is pure and synchronous; the caller awaits
//! its own sleep on the returned delay, so the crate carries no async runtime
//! dependency and is usable from any subsystem.
//!
//! The delay for `attempt` is `min(base * multiplier^attempt, max)`. With jitter
//! enabled a random amount in `[0, delay)` is added, decorrelating concurrent
//! retries against a shared resource. All arithmetic saturates, so no `attempt`
//! value can overflow or panic.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// An exponential (or constant) backoff policy.
///
/// Construct with [`Backoff::exponential`], [`Backoff::constant`], or
/// [`Backoff::new`] for an explicit multiplier, optionally enable jitter with
/// [`Backoff::with_jitter`], then read delays with [`Backoff::delay`] (jittered)
/// or [`Backoff::step`] (deterministic).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    multiplier: u32,
    jitter: bool,
}

impl Backoff {
    /// A policy growing by `multiplier` per attempt, with `jitter` on or off.
    #[must_use]
    pub const fn new(base: Duration, max: Duration, multiplier: u32, jitter: bool) -> Self {
        Self {
            base,
            max,
            multiplier,
            jitter,
        }
    }

    /// An exponential policy doubling from `base` up to `max`, without jitter.
    #[must_use]
    pub const fn exponential(base: Duration, max: Duration) -> Self {
        Self::new(base, max, 2, false)
    }

    /// A constant policy whose delay is always `delay` before jitter.
    #[must_use]
    pub const fn constant(delay: Duration) -> Self {
        Self::new(delay, delay, 1, false)
    }

    /// Return the same policy with jitter enabled: each delay gains a random
    /// amount in `[0, delay)`, so its realized value lands in `[delay, 2*delay)`.
    #[must_use]
    pub const fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    /// The deterministic, pre-jitter delay for a zero-indexed `attempt`:
    /// `min(base * multiplier^attempt, max)`.
    #[must_use]
    pub fn step(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.checked_pow(attempt).unwrap_or(u32::MAX);
        self.base.saturating_mul(factor).min(self.max)
    }

    /// The delay to wait before the given zero-indexed retry `attempt`,
    /// including jitter when enabled. `attempt` 0 is the delay before the first
    /// retry.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        let step = self.step(attempt);
        if self.jitter {
            step.saturating_add(Duration::from_millis(jitter_below(delay_millis(step))))
        } else {
            step
        }
    }
}

fn delay_millis(delay: Duration) -> u64 {
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

/// A non-cryptographic pseudo-random value in `[0, max_exclusive)`, for retry
/// jitter only. Returns 0 when `max_exclusive` is 0.
///
/// This avoids any RNG dependency by reading a freshly-seeded [`RandomState`]'s
/// hasher state as an entropy source. The standard library does not guarantee
/// the seed's quality or distribution, so this must never be relied on for
/// anything beyond spreading out contended retries; correctness never depends
/// on the value.
#[must_use]
pub fn jitter_below(max_exclusive: u64) -> u64 {
    if max_exclusive == 0 {
        return 0;
    }
    RandomState::new().build_hasher().finish() % max_exclusive
}

#[cfg(test)]
mod tests {
    use super::Backoff;
    use std::time::Duration;

    #[test]
    fn exponential_doubles_until_capped() {
        let backoff = Backoff::exponential(Duration::from_secs(1), Duration::from_mins(1));
        assert_eq!(backoff.delay(0), Duration::from_secs(1));
        assert_eq!(backoff.delay(1), Duration::from_secs(2));
        assert_eq!(backoff.delay(2), Duration::from_secs(4));
        assert_eq!(backoff.delay(5), Duration::from_secs(32));
        assert_eq!(backoff.delay(6), Duration::from_mins(1));
        assert_eq!(backoff.delay(7), Duration::from_mins(1));
    }

    #[test]
    fn job_retry_formula_matches_one_minute_base_ten_minute_cap() {
        let backoff = Backoff::exponential(Duration::from_mins(1), Duration::from_mins(10));
        assert_eq!(backoff.step(0), Duration::from_mins(1));
        assert_eq!(backoff.step(3), Duration::from_mins(8));
        assert_eq!(backoff.step(4), Duration::from_mins(10));
        assert_eq!(backoff.step(10), Duration::from_mins(10));
    }

    #[test]
    fn cap_applies_when_base_already_exceeds_it() {
        let backoff = Backoff::exponential(Duration::from_mins(2), Duration::from_mins(1));
        assert_eq!(backoff.delay(0), Duration::from_mins(1));
        assert_eq!(backoff.delay(10), Duration::from_mins(1));
    }

    #[test]
    fn step_saturates_without_panicking() {
        let backoff = Backoff::exponential(Duration::MAX, Duration::from_mins(1));
        assert_eq!(backoff.step(6), Duration::from_mins(1));
        assert_eq!(backoff.step(u32::MAX), Duration::from_mins(1));
    }

    #[test]
    fn constant_policy_ignores_attempt() {
        let backoff = Backoff::constant(Duration::from_millis(25));
        assert_eq!(backoff.step(0), Duration::from_millis(25));
        assert_eq!(backoff.step(9), Duration::from_millis(25));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let backoff =
            Backoff::exponential(Duration::from_millis(100), Duration::from_secs(1)).with_jitter();
        for attempt in 0..8 {
            let step = backoff.step(attempt);
            let delay = backoff.delay(attempt);
            assert!(delay >= step);
            assert!(delay < step + step + Duration::from_millis(1));
        }
    }

    #[test]
    fn constant_jitter_stays_within_bounds() {
        let backoff = Backoff::constant(Duration::from_millis(25)).with_jitter();
        for _ in 0..256 {
            let delay = backoff.delay(0);
            assert!(delay >= Duration::from_millis(25));
            assert!(delay < Duration::from_millis(50));
        }
    }

    #[test]
    fn no_jitter_is_deterministic() {
        let backoff = Backoff::exponential(Duration::from_millis(100), Duration::from_mins(1));
        assert_eq!(backoff.delay(3), backoff.step(3));
    }
}
