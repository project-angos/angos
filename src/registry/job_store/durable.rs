use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::{
    select,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    metrics_provider::metrics_provider,
    registry::job_store::{
        Error, JobEnvelope, JobQueue, JobStore, MAX_SCAN,
        lease::{LeaseBackend, LeaseGuard},
        parse_not_before,
    },
};

/// A job claimed by a worker, ready to execute. `lease` keeps the lease
/// alive via an internal heartbeat and releases it on `complete`/`fail`
/// (or `Drop`); `storage_key` identifies the pending file the envelope was
/// loaded from so `complete`/`fail` can delete or rewrite that file.
pub struct ClaimedJob {
    pub envelope: JobEnvelope,
    pub storage_key: String,
    pub lease: LeaseGuard,
}

pub enum FailOutcome {
    Retried { next_at: DateTime<Utc> },
    MovedToDeadLetter,
}

/// Outcome of a single `claim_one` attempt. `claimed` is `Some` when a job was
/// leased; otherwise `next_ready` carries the soonest `not_before` observed
/// across the scan so the caller can sleep until then rather than polling at
/// full cadence through unchanged backed-off envelopes.
pub struct ClaimOutcome {
    pub claimed: Option<ClaimedJob>,
    pub next_ready: Option<DateTime<Utc>>,
}

impl ClaimOutcome {
    /// How long the caller should idle before the next `claim_one` attempt.
    /// When only backed-off envelopes were seen, the sleep extends to the
    /// soonest `not_before`, clamped to `[poll_interval, max(poll_interval, 1 min)]`
    /// so the worker stops re-reading unchanged envelopes every tick while
    /// still picking up newly-enqueued ready jobs promptly.
    pub fn idle_sleep(&self, poll_interval: Duration) -> Duration {
        let max_sleep = poll_interval.max(Duration::from_mins(1));
        self.next_ready.map_or(poll_interval, |t| {
            (t - Utc::now())
                .to_std()
                .unwrap_or_default()
                .clamp(poll_interval, max_sleep)
        })
    }
}

/// Producer-side `JobQueue` backed by a `JobStore`. `enqueue` performs a
/// best-effort dedup scan; the per-key lease handles correctness.
pub struct DurableJobQueue {
    store: Arc<dyn JobStore>,
}

impl DurableJobQueue {
    pub fn new(store: Arc<dyn JobStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl JobQueue for DurableJobQueue {
    async fn enqueue(&self, envelope: JobEnvelope) -> Result<(), Error> {
        // Fast path: an index already exists for this lock_key (and points
        // at a still-pending file — `find_pending_with_lock_key` self-heals
        // orphans). No need to write anything.
        if self
            .store
            .find_pending_with_lock_key(&envelope.queue, &envelope.lock_key)
            .await
            .unwrap_or(false)
        {
            metrics_provider()
                .job_queue_enqueued_total
                .with_label_values(&[envelope.queue.as_str(), "hit"])
                .inc();
            return Ok(());
        }

        // Slow path: write the pending file then try to atomically claim the
        // lock-key index. The CAS-create resolves enqueue races between
        // replicas: two replicas may both observe no index and both write
        // their pending files, but only one wins the claim. The loser then
        // cleans up its pending file so the queue doesn't carry a spurious
        // duplicate.
        let storage_key = self
            .store
            .put_pending(&envelope.queue, &envelope, Utc::now())
            .await?;

        let won = self
            .store
            .try_claim_lock_key(&envelope.queue, &envelope.lock_key, &storage_key)
            .await?;

        let dedup = if won { "miss" } else { "hit" };
        metrics_provider()
            .job_queue_enqueued_total
            .with_label_values(&[envelope.queue.as_str(), dedup])
            .inc();

        if !won
            && let Err(e) = self
                .store
                .remove_pending(&envelope.queue, &storage_key)
                .await
        {
            warn!(
                lock_key = envelope.lock_key.as_str(),
                error = %e,
                "Failed to clean up duplicate pending after enqueue race"
            );
        }

        Ok(())
    }
}

pub struct DurableJobConsumer {
    store: Arc<dyn JobStore>,
    leases: Arc<dyn LeaseBackend>,
    worker_id: String,
    lease_ttl_secs: u64,
}

impl DurableJobConsumer {
    pub fn new(
        store: Arc<dyn JobStore>,
        leases: Arc<dyn LeaseBackend>,
        lease_ttl_secs: u64,
        worker_id: String,
    ) -> Self {
        Self {
            store,
            leases,
            worker_id,
            lease_ttl_secs,
        }
    }

    /// Claim the next available job from `queue`. Walks pending storage keys
    /// in ascending order (`list_pending` returns them sorted by the hex
    /// unix-millis prefix, i.e. by `not_before`). Stops at the first key whose
    /// prefix is in the future without reading its body — the prefix is the
    /// authoritative readiness signal. When no claim is made, `next_ready`
    /// carries that first future instant so the caller can sleep until then.
    pub async fn claim_one(&self, queue: &str) -> Result<ClaimOutcome, Error> {
        let now = Utc::now();
        let mut next_ready: Option<DateTime<Utc>> = None;
        for storage_key in self.store.list_pending(queue, MAX_SCAN).await? {
            // Read the readiness time off the filename; never GET the body for
            // a backed-off entry. A missing/malformed prefix falls through to
            // the body read so legacy keys (if any) still work.
            if let Some(not_before) = parse_not_before(&storage_key)
                && not_before > now
            {
                next_ready = Some(next_ready.map_or(not_before, |t| t.min(not_before)));
                break;
            }
            let envelope = match self.store.read_pending(queue, &storage_key).await {
                Ok(e) => e,
                Err(Error::NotFound) => continue,
                Err(e) => return Err(e),
            };
            if let Some(lease) = self
                .leases
                .try_acquire(&envelope.lock_key, &self.worker_id, self.lease_ttl_secs)
                .await?
            {
                return Ok(ClaimOutcome {
                    claimed: Some(ClaimedJob {
                        envelope,
                        storage_key,
                        lease,
                    }),
                    next_ready: None,
                });
            }
        }
        Ok(ClaimOutcome {
            claimed: None,
            next_ready,
        })
    }

    pub async fn complete(&self, claimed: ClaimedJob) -> Result<(), Error> {
        // The lease drops first, then the pending entry. Between the two,
        // another worker may briefly observe the pending entry and claim a
        // fresh lease — `JobHandler` implementations MUST therefore be
        // idempotent. A crash between the two ops is equivalent and is
        // covered by the same idempotency contract.
        let ClaimedJob {
            envelope,
            storage_key,
            lease,
        } = claimed;
        lease.release().await;
        self.store
            .remove_pending(&envelope.queue, &storage_key)
            .await?;
        // Drop the dedup index *after* the pending file is gone. A concurrent
        // retry that already re-pointed the index at a fresher envelope is
        // detected by the conditional check inside the backend.
        self.store
            .remove_lock_key_index_if_matches(&envelope.queue, &envelope.lock_key, &storage_key)
            .await
    }

    /// Record a failure. The job is either re-queued with backoff or moved to
    /// the dead-letter store when its retry budget is exhausted. On retry the
    /// envelope is rewritten to a *new* storage key encoding the bumped
    /// `not_before`; the previous key is deleted afterwards. A crash between
    /// the two writes re-runs the previous envelope at its old (already
    /// elapsed) `not_before`, which the handler-side idempotency contract
    /// already covers.
    pub async fn fail(&self, claimed: ClaimedJob, err: &str) -> Result<FailOutcome, Error> {
        let ClaimedJob {
            envelope,
            storage_key,
            lease,
        } = claimed;
        let new_attempts = envelope.attempts.saturating_add(1);

        if new_attempts >= envelope.max_attempts {
            lease.release().await;
            self.store
                .move_to_failed(&envelope.queue, &storage_key, &envelope, err)
                .await?;
            return Ok(FailOutcome::MovedToDeadLetter);
        }

        let delay = backoff(new_attempts);
        let next_at = Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default();

        let updated = JobEnvelope {
            attempts: new_attempts,
            ..envelope
        };

        // The worker holds the lease for `lock_key`, so it is the unique
        // writer here — `refresh_lock_key_index` is unconditional. Producers
        // racing this retry will dedup against the new storage_key once we
        // publish it, or against the old one before we do (both are correct).
        let new_storage_key = self
            .store
            .put_pending(&updated.queue, &updated, next_at)
            .await?;
        self.store
            .refresh_lock_key_index(&updated.queue, &updated.lock_key, &new_storage_key)
            .await?;
        self.store
            .remove_pending(&updated.queue, &storage_key)
            .await?;
        lease.release().await;

        Ok(FailOutcome::Retried { next_at })
    }
}

/// Exponential backoff for retry delays: `min(1 min * 2^n, 10 min)`. `n = 0`
/// means "first failure". The shift is bounded so it cannot overflow.
pub fn backoff(n: u32) -> Duration {
    Duration::from_mins(1 << n.min(4)).min(Duration::from_mins(10))
}

/// Refresh `angos_job_queue_pending` for `queue` on every `period` tick,
/// until `shutdown` is cancelled. Uses `tokio::time::interval` so the cadence
/// stays fixed across slow `count_pending` calls; missed ticks are coalesced
/// rather than catching up.
///
/// `ready_horizon_secs` is forwarded to `count_pending`: only envelopes ready
/// within that window contribute to the gauge.
pub async fn pending_refresh_loop(
    store: Arc<dyn JobStore>,
    queue: String,
    period: Duration,
    ready_horizon_secs: u64,
    shutdown: CancellationToken,
) {
    let mut timer = interval(period);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the immediate first tick so the first refresh happens after `period`.
    timer.tick().await;

    loop {
        select! {
            () = shutdown.cancelled() => return,
            _ = timer.tick() => {}
        }
        match store.count_pending(&queue, ready_horizon_secs).await {
            Ok(count) => {
                metrics_provider()
                    .job_queue_pending
                    .with_label_values(&[queue.as_str()])
                    .set(i64::try_from(count).unwrap_or(i64::MAX));
            }
            Err(e) => debug!(queue = %queue, error = %e, "Failed to refresh pending gauge"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::backoff;

    #[test]
    fn backoff_doubles_each_attempt_then_caps_at_ten_minutes() {
        assert_eq!(backoff(0), Duration::from_mins(1));
        assert_eq!(backoff(1), Duration::from_mins(2));
        assert_eq!(backoff(3), Duration::from_mins(8));
        assert_eq!(backoff(4), Duration::from_mins(10));
        assert_eq!(backoff(100), Duration::from_mins(10));
    }
}
