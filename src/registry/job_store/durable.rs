use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::{
    select, spawn,
    task::JoinHandle,
    time::{MissedTickBehavior, interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    metrics_provider::metrics_provider,
    registry::job_store::{Error, JobEnvelope, JobQueue, JobStore, MAX_SCAN},
};

/// Consecutive heartbeat failures tolerated before the lease is treated as
/// lost. Combined with the `ttl / 3` tick this gives roughly one TTL window of
/// grace before the worker bails out.
const MAX_HEARTBEAT_FAILURES: u32 = 3;

/// A job claimed by a worker, ready to execute. `lease_token` is an
/// opaque backend-specific identifier used to refresh or release the lease.
#[derive(Debug)]
pub struct ClaimedJob {
    pub envelope: JobEnvelope,
    pub lease_token: String,
}

pub enum FailOutcome {
    Retried { next_at: DateTime<Utc> },
    MovedToDeadLetter,
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
        let already_pending = self
            .store
            .find_pending_with_lock_key(&envelope.queue, &envelope.lock_key)
            .await
            .unwrap_or(false);

        let dedup = if already_pending { "hit" } else { "miss" };
        metrics_provider()
            .job_queue_enqueued_total
            .with_label_values(&[envelope.queue.as_str(), dedup])
            .inc();

        // The pending gauge is refreshed by the server's ticker; updating it
        // here would issue a LIST on every miss, which is expensive on S3.
        if already_pending {
            return Ok(());
        }
        self.store
            .put_pending(&envelope.queue, &envelope.id, &envelope)
            .await
    }
}

pub struct DurableJobConsumer {
    store: Arc<dyn JobStore>,
    worker_id: String,
    lease_ttl_secs: u64,
}

impl DurableJobConsumer {
    pub fn new(store: Arc<dyn JobStore>, lease_ttl_secs: u64, worker_id: String) -> Self {
        Self {
            store,
            worker_id,
            lease_ttl_secs,
        }
    }

    /// Claim the next available job from `queue`. Walks pending envelopes in
    /// ascending ULID order (oldest first), skips any still in backoff, then
    /// creates the lease atomically. Returns `None` when no candidate is
    /// ready or every ready one is leased by another worker.
    pub async fn claim_one(&self, queue: &str) -> Result<Option<ClaimedJob>, Error> {
        let now = Utc::now();
        for id in self.store.list_pending(queue, MAX_SCAN).await? {
            let envelope = match self.store.read_pending(queue, &id).await {
                Ok(e) => e,
                Err(Error::NotFound) => continue,
                Err(e) => return Err(e),
            };
            if envelope.not_before > now {
                continue;
            }
            if let Some(token) = self
                .store
                .try_create_lease(&envelope.lock_key, &self.worker_id, self.lease_ttl_secs)
                .await?
            {
                return Ok(Some(ClaimedJob {
                    envelope,
                    lease_token: token,
                }));
            }
        }
        Ok(None)
    }

    /// Spawn a heartbeat task that refreshes the lease for `claimed` every
    /// `ttl_secs / 3` seconds. After [`MAX_HEARTBEAT_FAILURES`] consecutive
    /// failures the task cancels `lease_lost` so the main worker loop can
    /// abort the job. The caller must cancel `lease_lost` on normal completion
    /// to stop the heartbeat.
    pub fn spawn_heartbeat(
        &self,
        claimed: &ClaimedJob,
        lease_lost: CancellationToken,
    ) -> JoinHandle<()> {
        let store = self.store.clone();
        let worker_id = self.worker_id.clone();
        let ttl_secs = self.lease_ttl_secs;
        let lock_key = claimed.envelope.lock_key.clone();
        let mut current_token = claimed.lease_token.clone();
        let tick = Duration::from_secs((ttl_secs / 3).max(1));

        spawn(async move {
            // `interval` fires immediately on first tick; consume it so the
            // first heartbeat happens after `tick`, not at t=0.
            let mut timer = interval(tick);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            timer.tick().await;

            let mut consecutive_failures: u32 = 0;
            loop {
                select! {
                    () = lease_lost.cancelled() => return,
                    _ = timer.tick() => {}
                }
                match store
                    .heartbeat_lease(&lock_key, &current_token, &worker_id, ttl_secs)
                    .await
                {
                    Ok(new_token) => {
                        consecutive_failures = 0;
                        current_token = new_token;
                    }
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        warn!(lock_key, error = %e, consecutive_failures, "Lease heartbeat failed");
                        if consecutive_failures >= MAX_HEARTBEAT_FAILURES {
                            warn!(lock_key, "Too many heartbeat failures, marking lease lost");
                            lease_lost.cancel();
                            return;
                        }
                    }
                }
            }
        })
    }

    pub async fn complete(&self, claimed: ClaimedJob) -> Result<(), Error> {
        // Order matters: remove the lease first so another worker doesn't see
        // a dangling pending entry, then remove pending. A crash between the
        // two is safe — the next worker re-checks blob ownership.
        self.store
            .remove_lease(&claimed.envelope.lock_key, &claimed.lease_token)
            .await?;
        self.store
            .remove_pending(&claimed.envelope.queue, &claimed.envelope.id)
            .await
    }

    /// Record a failure. The job is either re-queued with backoff or moved to
    /// the dead-letter store when its retry budget is exhausted.
    pub async fn fail(&self, claimed: ClaimedJob, err: &str) -> Result<FailOutcome, Error> {
        let ClaimedJob {
            envelope,
            lease_token,
        } = claimed;
        let new_attempts = envelope.attempts.saturating_add(1);

        if new_attempts >= envelope.max_attempts {
            self.store
                .remove_lease(&envelope.lock_key, &lease_token)
                .await?;
            self.store
                .move_to_failed(&envelope.queue, &envelope.id, &envelope, err)
                .await?;
            return Ok(FailOutcome::MovedToDeadLetter);
        }

        let delay = backoff(new_attempts);
        let next_at = Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default();

        let updated = JobEnvelope {
            attempts: new_attempts,
            not_before: next_at,
            ..envelope
        };

        self.store
            .put_pending(&updated.queue, &updated.id, &updated)
            .await?;
        self.store
            .remove_lease(&updated.lock_key, &lease_token)
            .await?;

        Ok(FailOutcome::Retried { next_at })
    }
}

/// Exponential backoff for retry delays: `min(60s * 2^n, 1h)`. `n = 0` means
/// "first failure". Capped at 1 h so the shift cannot overflow.
pub fn backoff(n: u32) -> Duration {
    Duration::from_mins(1 << n.min(6)).min(Duration::from_hours(1))
}

/// Refresh `angos_job_queue_pending` for `queue` on every `interval` tick,
/// until `shutdown` is cancelled.
pub async fn pending_refresh_loop(
    store: Arc<dyn JobStore>,
    queue: String,
    interval: Duration,
    shutdown: CancellationToken,
) {
    loop {
        select! {
            () = shutdown.cancelled() => return,
            () = sleep(interval) => match store.count_pending(&queue).await {
                Ok(count) => {
                    metrics_provider()
                        .job_queue_pending
                        .with_label_values(&[queue.as_str()])
                        .set(i64::try_from(count).unwrap_or(i64::MAX));
                }
                Err(e) => debug!(queue = %queue, error = %e, "Failed to refresh pending gauge"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::backoff;

    #[test]
    fn backoff_doubles_each_attempt_then_caps_at_one_hour() {
        assert_eq!(backoff(0), Duration::from_mins(1));
        assert_eq!(backoff(1), Duration::from_mins(2));
        assert_eq!(backoff(6), Duration::from_hours(1));
        assert_eq!(backoff(100), Duration::from_hours(1));
    }
}
