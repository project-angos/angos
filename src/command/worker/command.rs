use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use argh::FromArgs;
use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use tokio::{
    select,
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap::{self, Error},
        worker::runner::execute_one,
    },
    configuration::{Configuration, listeners::ServerTlsConfig, watcher::ConfigNotifier},
    registry::{
        cache_job_handler::{CACHE_QUEUE, CacheJobHandler},
        job_store::{self, JobHandler, JobStore},
    },
};

fn default_queue() -> String {
    CACHE_QUEUE.to_string()
}

/// Cap on the exponential backoff applied after consecutive `claim_one`
/// errors. A worker pointed at a broken storage backend will idle here
/// rather than hammer it at 1Hz; recovery is bounded by this ceiling.
const CLAIM_ERROR_BACKOFF_CAP: Duration = Duration::from_mins(1);

/// Backoff after `n` consecutive `claim_one` errors: `poll_interval` doubled
/// `n` times, capped at [`CLAIM_ERROR_BACKOFF_CAP`]. Shift is bounded so even
/// pathological `poll_interval × multiplier` calculations can't overflow
/// before the cap clamps them down.
fn claim_error_backoff(poll_interval: Duration, n: u32) -> Duration {
    let multiplier = 1u32 << n.min(6);
    poll_interval
        .saturating_mul(multiplier)
        .min(CLAIM_ERROR_BACKOFF_CAP)
}

/// Process durable background jobs.
#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "worker",
    description = "Process durable background jobs"
)]
pub struct Options {
    /// queue to drain (default: "cache")
    #[argh(option, default = "default_queue()")]
    pub queue: String,
    /// idle poll interval when the queue is empty
    #[argh(option, default = "HumanDuration::from(Duration::from_secs(1))")]
    pub poll_interval: HumanDuration,
}

/// Hot-reloadable worker subcommand. `consumer` and `handler` are swapped
/// atomically on configuration reload; in-flight jobs always finish on the
/// components they started with.
pub struct Command {
    inner: Arc<ArcSwap<Components>>,
    queue: String,
    poll_interval: Duration,
    concurrency: NonZeroUsize,
    shutdown: CancellationToken,
    workers: TaskTracker,
    /// Cancellation token tied to the transactional-engine recovery loop and
    /// body janitor. Fired on shutdown to stop both background tasks.
    engine_maintenance: CancellationToken,
}

struct Components {
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let engine_maintenance = CancellationToken::new();
        Ok(Self {
            inner: Arc::new(ArcSwap::from_pointee(
                Components::build(config, Some(engine_maintenance.clone())).await?,
            )),
            queue: options.queue.clone(),
            poll_interval: *options.poll_interval,
            concurrency: config.global.max_concurrent_cache_jobs,
            shutdown: CancellationToken::new(),
            workers: TaskTracker::new(),
            engine_maintenance,
        })
    }

    /// Cancel the worker pool and wait up to `grace` for tasks to finish.
    /// Surviving tasks are dropped on process exit; the durable queue's lease
    /// TTL guarantees re-claim by another worker.
    pub async fn shutdown_with_timeout(&self, grace: Duration) {
        self.shutdown.cancel();
        self.workers.close();
        if timeout(grace, self.workers.wait()).await.is_err() {
            warn!("Worker pool did not drain within shutdown grace period");
        }
        // Stop the transactional-engine maintenance tasks alongside the worker
        // pool so they don't outlive the process's storage handles.
        self.engine_maintenance.cancel();
    }

    /// Spawn `concurrency` claim-loop tasks and wait for them to finish.
    /// Returns when every worker observes the shutdown signal and exits.
    pub async fn run(&self) {
        for _ in 0..self.concurrency.get() {
            let inner = Arc::clone(&self.inner);
            let queue = self.queue.clone();
            let poll_interval = self.poll_interval;
            let shutdown = self.shutdown.clone();
            self.workers.spawn(async move {
                worker_loop(inner, queue, poll_interval, shutdown).await;
            });
        }
        self.workers.close();
        self.workers.wait().await;
    }
}

/// Single claim-loop task. Owns its own exponential-backoff counter so a
/// transient backend outage doesn't make every worker hammer the storage
/// in lockstep.
async fn worker_loop(
    inner: Arc<ArcSwap<Components>>,
    queue: String,
    poll_interval: Duration,
    shutdown: CancellationToken,
) {
    let mut consecutive_claim_errors: u32 = 0;
    loop {
        let snapshot = inner.load_full();
        select! {
            () = shutdown.cancelled() => {
                debug!("Worker poll loop stopping");
                return;
            }
            result = snapshot.consumer.claim_one(&queue) => match result {
                Err(e) => {
                    let backoff = claim_error_backoff(poll_interval, consecutive_claim_errors);
                    consecutive_claim_errors = consecutive_claim_errors.saturating_add(1);
                    error!(
                        error = %e,
                        consecutive_failures = consecutive_claim_errors,
                        backoff_secs = backoff.as_secs(),
                        "claim_one failed; backing off",
                    );
                    sleep(backoff).await;
                }
                Ok(outcome) => {
                    consecutive_claim_errors = 0;
                    match outcome.claimed {
                        None => sleep(outcome.idle_sleep(poll_interval)).await,
                        Some(claimed) => {
                            execute_one(
                                snapshot.consumer.as_ref(),
                                snapshot.handler.as_ref(),
                                claimed,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ConfigNotifier for Command {
    async fn notify_config_change(&self, config: &Configuration) {
        // The engine maintenance tasks were spawned by the initial bootstrap;
        // hot reloads do not respawn them.
        match Components::build(config, None).await {
            Ok(components) => self.inner.store(Arc::new(components)),
            Err(e) => error!("Failed to apply worker configuration: {e}"),
        }
    }

    fn notify_tls_config_change(&self, _tls: &ServerTlsConfig) {
        // The worker has no TLS listener.
    }
}

impl Components {
    async fn build(
        config: &Configuration,
        engine_maintenance: Option<CancellationToken>,
    ) -> Result<Self, Error> {
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let blob_backend = std::sync::Arc::new(config.blob_store.build_backend()?);
        let (metadata_store, _) =
            bootstrap::metadata_store(&config.resolve_registry_storage(), &auth_cache).await?;
        let repositories = bootstrap::repositories(
            &config.repository,
            &auth_cache,
            config.global.max_manifest_size_bytes(),
        )
        .await?;

        let _jq_config = config.global.job_queue.as_ref().ok_or_else(|| {
            bootstrap::Error::JobQueue(job_store::Error::Initialization(
                "[global.job_queue] is required for the worker subcommand".to_string(),
            ))
        })?;

        let worker_id = Uuid::new_v4().to_string();
        let storage_config = config.resolve_registry_storage();
        let storage = storage_config.to_handles().await?;

        // Spawn the engine maintenance loops once per worker process so any
        // crashed-mid-Apply transactions are recovered and orphan body
        // staging is reaped.
        if let Some(token) = engine_maintenance {
            bootstrap::spawn_engine_maintenance(storage.store.clone(), &storage.executor, token);
        }

        let consumer = Arc::new(JobStore::new(storage.store, storage.executor, worker_id));
        let handler: Arc<dyn JobHandler> = Arc::new(CacheJobHandler::new(
            repositories,
            blob_backend,
            metadata_store,
        ));

        Ok(Self { consumer, handler })
    }
}

#[cfg(test)]
mod tests {
    use super::{CLAIM_ERROR_BACKOFF_CAP, claim_error_backoff};
    use std::time::Duration;

    #[test]
    fn backoff_doubles_with_consecutive_failures() {
        let poll = Duration::from_secs(1);
        assert_eq!(claim_error_backoff(poll, 0), Duration::from_secs(1));
        assert_eq!(claim_error_backoff(poll, 1), Duration::from_secs(2));
        assert_eq!(claim_error_backoff(poll, 2), Duration::from_secs(4));
        assert_eq!(claim_error_backoff(poll, 5), Duration::from_secs(32));
    }

    #[test]
    fn backoff_caps_at_one_minute() {
        let poll = Duration::from_secs(1);
        // 1s × 2^6 = 64s → capped to 60s.
        assert_eq!(claim_error_backoff(poll, 6), CLAIM_ERROR_BACKOFF_CAP);
        // Shift is also clamped, so extreme `n` doesn't overflow.
        assert_eq!(claim_error_backoff(poll, u32::MAX), CLAIM_ERROR_BACKOFF_CAP);
    }

    #[test]
    fn backoff_caps_when_poll_interval_already_exceeds_ceiling() {
        // A misconfigured `poll_interval` larger than the cap still returns
        // the cap, not an even-larger value.
        let huge_poll = Duration::from_mins(2);
        assert_eq!(claim_error_backoff(huge_poll, 0), CLAIM_ERROR_BACKOFF_CAP);
        assert_eq!(claim_error_backoff(huge_poll, 10), CLAIM_ERROR_BACKOFF_CAP);
    }

    #[test]
    fn backoff_saturates_on_arithmetic_overflow() {
        // `Duration::saturating_mul` clamps at `Duration::MAX`; the outer `min`
        // then snaps down to the cap. No panic.
        let max_poll = Duration::MAX;
        assert_eq!(claim_error_backoff(max_poll, 6), CLAIM_ERROR_BACKOFF_CAP);
    }
}
