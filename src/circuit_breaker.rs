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
    }

    pub fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == CIRCUIT_BREAKER_THRESHOLD {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.opened_at_epoch_secs.store(now, Ordering::Release);
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
