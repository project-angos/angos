//! Wall-clock abstraction for time-dependent policy evaluation.
//!
//! Policies that consult the current time (e.g. retention rules with
//! `image.pushed_at > now() - days(30)`) take a [`Clock`] so that tests can
//! inject a deterministic timestamp instead of reading the system clock.

use chrono::{DateTime, Utc};

/// A source of the current wall-clock time.
///
/// `Send + Sync` are required because the clock is captured by closures
/// registered with `cel-interpreter` and shared across threads.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Production clock backed by the system wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
