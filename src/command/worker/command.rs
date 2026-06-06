use std::{collections::HashSet, num::NonZeroUsize, sync::Arc, time::Duration};

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

use angos_tx_engine::store::Store;

use crate::{
    command::{
        bootstrap::{self, Error},
        worker::runner::execute_one,
    },
    configuration::{Configuration, listeners::ServerTlsConfig, watcher::ConfigNotifier},
    registry::{
        blob_store::BlobStore,
        cache_job_handler::{CACHE_QUEUE, CacheJobHandler},
        job_store::{self, JobHandler, JobStore},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::{REPLICATION_QUEUE, ReplicationJobHandler},
};

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
    /// queue to drain; repeatable, e.g. `--queue cache --queue replication`.
    /// Defaults to draining both the "cache" and "replication" queues, each on
    /// its own worker pool.
    #[argh(option)]
    pub queue: Vec<String>,
    /// idle poll interval when the queue is empty
    #[argh(option, default = "HumanDuration::from(Duration::from_secs(1))")]
    pub poll_interval: HumanDuration,
}

/// Hot-reloadable worker subcommand draining one or more queues, each on its own
/// worker pool. A queue's `consumer` and `handler` are swapped atomically on
/// configuration reload; in-flight jobs always finish on the components they
/// started with.
pub struct Command {
    queues: Vec<QueueRunner>,
    poll_interval: Duration,
    shutdown: CancellationToken,
    workers: TaskTracker,
    /// Cancellation token tied to the transactional-engine recovery loop and
    /// body janitor. Fired on shutdown to stop both background tasks.
    engine_maintenance: CancellationToken,
}

/// One drained queue: its hot-swappable components plus the worker concurrency
/// for that queue.
struct QueueRunner {
    inner: Arc<ArcSwap<Components>>,
    queue: String,
    concurrency: NonZeroUsize,
}

struct Components {
    consumer: Arc<JobStore>,
    handler: Arc<dyn JobHandler>,
}

/// Worker concurrency for `queue`: the replication queue uses
/// `max_concurrent_replication_jobs`; every other queue (e.g. `cache`) uses
/// `max_concurrent_cache_jobs`.
fn queue_concurrency(config: &Configuration, queue: &str) -> NonZeroUsize {
    if queue == REPLICATION_QUEUE {
        config.global.max_concurrent_replication_jobs
    } else {
        config.global.max_concurrent_cache_jobs
    }
}

/// The queues to drain: the repeatable `--queue` values de-duplicated, or --
/// when none is passed -- both the `cache` and `replication` queues. Order is
/// irrelevant; each queue runs an independent worker pool.
fn resolve_queues(requested: &[String]) -> Vec<String> {
    if requested.is_empty() {
        return vec![CACHE_QUEUE.to_string(), REPLICATION_QUEUE.to_string()];
    }
    requested
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let engine_maintenance = CancellationToken::new();
        // The expensive, queue-independent context (storage, metadata/blob
        // stores, repositories, engine maintenance) is built ONCE and shared by
        // every drained queue; only the handler and a `JobStore` consumer differ
        // per queue.
        let context = WorkerContext::build(config, Some(engine_maintenance.clone())).await?;

        let mut queues = Vec::new();
        for queue in resolve_queues(&options.queue) {
            let concurrency = queue_concurrency(config, &queue);
            let components = context.components_for(&queue)?;
            queues.push(QueueRunner {
                inner: Arc::new(ArcSwap::from_pointee(components)),
                queue,
                concurrency,
            });
        }

        Ok(Self {
            queues,
            poll_interval: *options.poll_interval,
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

    /// Spawn `concurrency` claim-loop tasks per drained queue and wait for them
    /// to finish. Returns when every worker observes the shutdown signal and
    /// exits.
    pub async fn run(&self) {
        for runner in &self.queues {
            for _ in 0..runner.concurrency.get() {
                let inner = Arc::clone(&runner.inner);
                let queue = runner.queue.clone();
                let poll_interval = self.poll_interval;
                let shutdown = self.shutdown.clone();
                self.workers.spawn(async move {
                    worker_loop(inner, queue, poll_interval, shutdown).await;
                });
            }
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
        let context = match WorkerContext::build(config, None).await {
            Ok(context) => context,
            Err(e) => {
                error!("Failed to rebuild worker context on reload: {e}");
                return;
            }
        };
        for runner in &self.queues {
            match context.components_for(&runner.queue) {
                Ok(components) => runner.inner.store(Arc::new(components)),
                Err(e) => error!(
                    "Failed to apply worker configuration for queue '{}': {e}",
                    runner.queue
                ),
            }
        }
    }

    fn notify_tls_config_change(&self, _tls: &ServerTlsConfig) {
        // The worker has no TLS listener.
    }
}

/// Queue-independent worker resources, built once and shared across every
/// drained queue. Cloning the inner `Arc`s into each queue's handler/consumer is
/// cheap, so draining N queues does not rebuild the metadata store, blob store,
/// repositories, or storage N times.
struct WorkerContext {
    storage: Arc<Store>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    repositories: Arc<RepositoryResolver>,
}

impl WorkerContext {
    async fn build(
        config: &Configuration,
        engine_maintenance: Option<CancellationToken>,
    ) -> Result<Self, Error> {
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let blob_store = std::sync::Arc::new(config.blob_store.build_backend()?);
        let metadata_store =
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

        let storage = config.resolve_registry_storage().build_store().await?;

        // Spawn the engine maintenance loops once per worker process so any
        // crashed-mid-Apply transactions are recovered and orphan body
        // staging is reaped. The recovery loop is backend-wide, so one instance
        // covers every queue this process drains.
        if let Some(token) = engine_maintenance {
            bootstrap::spawn_engine_maintenance(&storage, token);
        }

        Ok(Self {
            storage,
            blob_store,
            metadata_store,
            repositories,
        })
    }

    /// Build the [`Components`] for one queue: a fresh `JobStore` consumer over
    /// the shared storage plus the handler bound to that queue. The replication
    /// queue reads local manifest/blob bytes and pushes them to each downstream
    /// `RegistryClient`; every other queue (e.g. `cache`) fills blobs from
    /// upstreams.
    fn components_for(&self, queue: &str) -> Result<Components, Error> {
        let consumer = Arc::new(JobStore::new(
            self.storage.clone(),
            Uuid::new_v4().to_string(),
        ));
        let handler: Arc<dyn JobHandler> = if queue == REPLICATION_QUEUE {
            Arc::new(
                ReplicationJobHandler::builder()
                    .resolver(self.repositories.clone())
                    .blob_store(self.blob_store.clone())
                    .metadata_store(self.metadata_store.clone())
                    .build()
                    .map_err(bootstrap::Error::JobQueue)?,
            )
        } else {
            Arc::new(CacheJobHandler::new(
                self.repositories.clone(),
                self.blob_store.clone(),
                self.metadata_store.clone(),
            ))
        };

        Ok(Components { consumer, handler })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, collections::HashSet, sync::Arc, time::Duration};

    use tempfile::TempDir;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use super::{CLAIM_ERROR_BACKOFF_CAP, WorkerContext, claim_error_backoff, resolve_queues};
    use crate::{
        registry::{
            blob_store::BlobStore,
            cache_job_handler::{CACHE_FETCH_BLOB_KIND, CACHE_QUEUE},
            job_store::JobEnvelope,
            metadata_store::MetadataStore,
            repository_resolver::RepositoryResolver,
            test_utils::{build_store, build_test_fs_executor},
        },
        replication::{REPLICATION_PUSH_MANIFEST_KIND, REPLICATION_QUEUE},
    };

    #[test]
    fn resolve_queues_defaults_to_cache_and_replication() {
        assert_eq!(
            resolve_queues(&[]),
            vec![CACHE_QUEUE.to_string(), REPLICATION_QUEUE.to_string()]
        );
    }

    #[test]
    fn resolve_queues_dedups_requested_queues() {
        let resolved: HashSet<String> = resolve_queues(&[
            "replication".to_string(),
            "cache".to_string(),
            "replication".to_string(),
        ])
        .into_iter()
        .collect();
        assert_eq!(
            resolved,
            HashSet::from(["replication".to_string(), "cache".to_string()])
        );
    }

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

    /// Builds a `WorkerContext` over FS-backed test stores by constructing the
    /// struct literal DIRECTLY (same-module private access), bypassing
    /// `WorkerContext::build` (which requires a `[global.job_queue]` config and
    /// would trip `validate_durable_queue_lock`). The `TempDir` is returned so
    /// the on-disk store outlives the context.
    fn worker_context() -> (WorkerContext, TempDir) {
        crate::metrics_provider::init_for_tests();
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let storage = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(storage.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(storage.clone()).build().unwrap());
        let repositories =
            Arc::new(RepositoryResolver::new(Arc::new(HashMap::new())).unwrap());

        let context = WorkerContext {
            storage,
            blob_store,
            metadata_store,
            repositories,
        };
        (context, dir)
    }

    /// `components_for` binds the queue to its handler: the replication queue gets
    /// a `ReplicationJobHandler`, every other queue (here `cache`) gets a
    /// `CacheJobHandler`. The handler is an opaque `Arc<dyn JobHandler>` that
    /// can't be downcast, so the binding is asserted BEHAVIOURALLY through each
    /// handler's kind-dispatch: both handlers reject a foreign job kind with an
    /// "unsupported job kind" error at the very first step of `execute`, before
    /// touching the payload or any backend — so a wrong binding (a cache handler
    /// where a replication handler was expected, or vice-versa) would NOT reject
    /// the foreign kind and the assertion would fail.
    #[tokio::test]
    async fn components_for_binds_replication_queue_to_replication_handler() {
        let (context, _dir) = worker_context();

        // A `cache.fetch_blob` envelope handed to the REPLICATION queue's handler
        // must be rejected as an unsupported kind: only a `ReplicationJobHandler`
        // rejects this kind; a `CacheJobHandler` would accept it.
        let cache_envelope =
            JobEnvelope::new(CACHE_QUEUE, CACHE_FETCH_BLOB_KIND, "lock", &serde_json::json!({}))
                .unwrap();
        let replication_handler = context.components_for(REPLICATION_QUEUE).unwrap().handler;
        let err = replication_handler
            .execute(&cache_envelope)
            .await
            .expect_err("replication handler must reject a cache job kind");
        assert!(
            err.to_string().contains("unsupported job kind"),
            "replication queue bound to the wrong handler: {err}"
        );
    }

    #[tokio::test]
    async fn components_for_binds_other_queue_to_cache_handler() {
        let (context, _dir) = worker_context();

        // A `replication.push_manifest` envelope handed to the CACHE queue's
        // handler must be rejected as an unsupported kind: only a
        // `CacheJobHandler` rejects this kind; a `ReplicationJobHandler` would
        // accept it.
        let replication_envelope = JobEnvelope::new(
            REPLICATION_QUEUE,
            REPLICATION_PUSH_MANIFEST_KIND,
            "lock",
            &serde_json::json!({}),
        )
        .unwrap();
        let cache_handler = context.components_for(CACHE_QUEUE).unwrap().handler;
        let err = cache_handler
            .execute(&replication_envelope)
            .await
            .expect_err("cache handler must reject a replication job kind");
        assert!(
            err.to_string().contains("unsupported job kind"),
            "cache queue bound to the wrong handler: {err}"
        );

        // An arbitrary non-replication queue name also resolves to the cache
        // handler (the `else` branch), confirming the binding is "replication
        // queue => replication handler, EVERYTHING ELSE => cache handler".
        let other = context.components_for("some-other-queue").unwrap().handler;
        let err = other
            .execute(&replication_envelope)
            .await
            .expect_err("a non-replication queue must bind the cache handler");
        assert!(
            err.to_string().contains("unsupported job kind"),
            "a non-replication queue did not bind the cache handler: {err}"
        );
    }

    /// `components_for` mints a FRESH `JobStore` consumer per call, each with its
    /// own worker id (a fresh `Uuid`). The struct field is private, but two
    /// consumers over the same shared storage are distinct `Arc`s — asserting the
    /// pointers differ pins that the consumer is not accidentally shared/cached
    /// across queues (each queue must drain on its own consumer).
    #[tokio::test]
    async fn components_for_mints_a_fresh_consumer_per_call() {
        let (context, _dir) = worker_context();
        let a = context.components_for(CACHE_QUEUE).unwrap().consumer;
        let b = context.components_for(REPLICATION_QUEUE).unwrap().consumer;
        assert!(
            !Arc::ptr_eq(&a, &b),
            "each queue must get its own JobStore consumer"
        );
    }
}
