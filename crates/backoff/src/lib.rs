//! Retry backoff: `min(base * 2^attempt, max)`, saturating, with optional
//! jitter. Pure and synchronous; the caller sleeps on the returned delay.

use std::hash::{BuildHasher, Hasher, RandomState};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    jitter: bool,
}

impl Backoff {
    /// Doubles from `base` up to `max`, without jitter.
    #[must_use]
    pub const fn exponential(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            jitter: false,
        }
    }

    /// Adds a random amount in `[0, delay)` to each delay, decorrelating
    /// concurrent retries against a shared resource.
    #[must_use]
    pub const fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    /// The delay before the zero-indexed retry `attempt`.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        let factor = 2u32.checked_pow(attempt).unwrap_or(u32::MAX);
        let step = self.base.saturating_mul(factor).min(self.max);
        if self.jitter {
            step.saturating_add(Duration::from_millis(jitter_below(
                u64::try_from(step.as_millis()).unwrap_or(u64::MAX),
            )))
        } else {
            step
        }
    }
}

/// A pseudo-random value in `[0, max_exclusive)`, 0 when `max_exclusive` is 0.
///
/// Reads a freshly seeded [`RandomState`] as entropy to avoid an RNG
/// dependency. Its quality is unspecified, so use it only to spread out
/// contended retries, never where correctness depends on the value.
#[must_use]
pub fn jitter_below(max_exclusive: u64) -> u64 {
    if max_exclusive == 0 {
        return 0;
    }
    RandomState::new().build_hasher().finish() % max_exclusive
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Backoff;

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
    fn cap_applies_when_base_already_exceeds_it() {
        let backoff = Backoff::exponential(Duration::from_mins(2), Duration::from_mins(1));
        assert_eq!(backoff.delay(0), Duration::from_mins(1));
        assert_eq!(backoff.delay(10), Duration::from_mins(1));
    }

    #[test]
    fn delay_saturates_without_panicking() {
        let backoff = Backoff::exponential(Duration::MAX, Duration::from_mins(1));
        assert_eq!(backoff.delay(6), Duration::from_mins(1));
        assert_eq!(backoff.delay(u32::MAX), Duration::from_mins(1));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let plain = Backoff::exponential(Duration::from_millis(100), Duration::from_secs(1));
        let jittered = plain.with_jitter();
        for attempt in 0..8 {
            let step = plain.delay(attempt);
            let delay = jittered.delay(attempt);
            assert!(delay >= step);
            assert!(delay < step + step + Duration::from_millis(1));
        }
    }
}
