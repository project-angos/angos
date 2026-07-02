//! Shared construction and run tail for the storage-maintenance subcommands
//! `scrub`, `policy`, and `replication`: bootstrap the same store handles,
//! assemble the same [`Ctx`], build the in-process [`ReplicationDrain`], and end
//! with [`report_run`].

use std::{num::NonZeroUsize, sync::Arc};

use angos_tx_engine::lock::{LockSession, LockStrategy};
use futures_util::future::join_all;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap,
        scrub::{
            Error, Options,
            check::NamespaceChecker,
            context::{Ctx, MAX_FANOUT, ScrubFlags},
            executor::{ActionSink, CancelSink, DryRunSink, Executor, GatedSink},
            report::{self, ActionTally, Findings},
            scrub_lock, setup,
        },
        worker::runner::{JobRunOutcome, run_once},
    },
    configuration::{Configuration, RegistryStorageConfig},
    registry::{
        blob_store::BlobStore,
        job_store::{JobHandler, JobStore, Queue},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationJobHandler,
};

/// The store handles `scrub` / `policy` / `replication` each need.
pub struct Stores {
    pub blob_backend: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub repositories: Arc<RepositoryResolver>,
}

/// Build the shared store handles from the loaded configuration.
pub async fn bootstrap_stores(config: &Configuration) -> Result<Stores, Error> {
    let auth_cache = bootstrap::auth_cache(&config.cache)?;
    let blob_backend = Arc::new(config.blob_store.build_backend()?);
    let metadata_store =
        bootstrap::metadata_store(&config.resolve_registry_storage(), &auth_cache).await?;
    let repositories = bootstrap::repositories(
        &config.repository,
        &auth_cache,
        config.global.max_manifest_size_bytes(),
    )
    .await?;
    Ok(Stores {
        blob_backend,
        metadata_store,
        repositories,
    })
}

/// Resolve the metadata store's lock strategy and the registry identity string
/// that scopes the in-process memory lock map. `Inherit` must already have
/// resolved to a concrete backend; reaching it here is a hard initialization
/// error.
pub fn resolve_lock_strategy(config: &Configuration) -> Result<(LockStrategy, String), Error> {
    match config.resolve_registry_storage() {
        RegistryStorageConfig::FS(fs) => {
            let id = format!("fs:{}", fs.root_dir);
            Ok((fs.lock_strategy, id))
        }
        RegistryStorageConfig::S3(s3) => {
            let id = format!("s3:{}/{}", s3.connection.bucket, s3.connection.key_prefix);
            Ok((s3.lock_strategy, id))
        }
        RegistryStorageConfig::Inherit => Err(Error::Initialization(
            "[metadata_store] did not resolve to a concrete backend before maintenance lock setup"
                .into(),
        )),
    }
}

/// Refuse to mutate on the memory lock strategy: it gives zero cross-process
/// exclusion, so a mutating maintenance run is unsafe. Report-only runs pass.
pub fn refuse_memory_mutation(lock_strategy: &LockStrategy, mutating: bool) -> Result<(), Error> {
    if mutating && matches!(lock_strategy, LockStrategy::Memory) {
        return Err(Error::Initialization(
            "lock_strategy = memory gives zero cross-process exclusion, so a mutating \
             maintenance run is unsafe; switch the metadata store's lock_strategy to s3 or \
             redis, or run report-only"
                .into(),
        ));
    }
    Ok(())
}

/// A held maintenance lock paired with the task forwarding its cancellation
/// into the run token. [`finish`](Self::finish) aborts the forwarder before
/// releasing, so releasing the lock can never mark a completed run cancelled.
pub struct MaintenanceLock {
    session: LockSession,
    forwarder: JoinHandle<()>,
}

impl MaintenanceLock {
    /// Abort the cancellation forwarder, then release the lock.
    pub async fn finish(self) {
        self.forwarder.abort();
        self.session.release().await;
    }
}

/// Acquire the single-instance maintenance lock and spawn the task that cancels
/// `run_cancel` when the lock is lost or its max hold elapses. Refuses when
/// another maintenance command holds the lock.
pub async fn acquire_lock(
    lock_strategy: &LockStrategy,
    metadata_store: &MetadataStore,
    registry_id: &str,
    max_hold_secs: u64,
    run_cancel: CancellationToken,
) -> Result<MaintenanceLock, Error> {
    let session =
        scrub_lock::acquire(lock_strategy, metadata_store, registry_id, max_hold_secs).await?;
    let lock_cancel = session.cancellation();
    let forwarder = tokio::spawn(async move {
        lock_cancel.cancelled().await;
        warn!("maintenance lock lost or max hold elapsed; cancelling the run");
        run_cancel.cancel();
    });
    Ok(MaintenanceLock { session, forwarder })
}

/// Assemble the shared [`Ctx`]: clamp the fan-out, parse the flag gates, and
/// build the sink chain `CancelSink -> GatedSink -> CountingSink -> inner`
/// (the gate layer is skipped when both gates are closed, keeping report-only
/// accounting on the plain counting sink). `prune_unknown_supported` is `true`
/// only for `scrub`; `gates.any()` drives the summary's "would" vs "done" verb
/// and `cancel` is the run-level token.
#[allow(clippy::too_many_arguments)]
pub fn build_ctx(
    metadata_store: Arc<MetadataStore>,
    inner: Box<dyn ActionSink + Send>,
    scrub_opts: &Options,
    config: &Configuration,
    findings: Findings,
    prune_unknown_supported: bool,
    gates: SinkGates,
    cancel: CancellationToken,
) -> Arc<Ctx> {
    let configured = config.global.max_concurrent_scrub_tasks.get();
    let fanout = configured.min(MAX_FANOUT);
    if configured > MAX_FANOUT {
        warn!(
            "max_concurrent_scrub_tasks = {configured} exceeds the fan-out ceiling of {MAX_FANOUT}; clamping to {MAX_FANOUT}"
        );
    }
    let opts = ScrubFlags::from_options(scrub_opts, fanout, gates.any());
    let tally = Arc::new(ActionTally::default());
    let mut sink = report::counting_sink(inner, tally.clone());
    if gates.any() {
        sink = Box::new(GatedSink::new(
            sink,
            gates.policy_mutate,
            gates.gc_mutate,
            tally.clone(),
        ));
    }
    let sink: Box<dyn ActionSink + Send> = Box::new(CancelSink::new(sink, cancel.clone()));
    Arc::new(Ctx {
        metadata_store,
        sink: Arc::new(Mutex::new(sink)),
        opts,
        tally,
        findings,
        prune_unknown_supported,
        cancel,
    })
}

/// The per-category mutate gates for one run's action sink.
#[derive(Clone, Copy)]
pub struct SinkGates {
    /// Policy actions (retention deletes, replication enqueues) mutate.
    pub policy_mutate: bool,
    /// GC/repair actions mutate (scrub's `--commit`).
    pub gc_mutate: bool,
}

impl SinkGates {
    /// Whether any category mutates (the run is not report-only).
    pub fn any(self) -> bool {
        self.policy_mutate || self.gc_mutate
    }
}

/// What a single-metadata-node maintenance command (`policy`, `replication`)
/// needs to run. `replication_drain` is `Some` only when [`prepare`] wired it.
pub struct Prepared {
    pub ctx: Arc<Ctx>,
    pub namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    pub replication_drain: Option<ReplicationDrain>,
    /// The metadata store's lock strategy; a mutating run acquires the shared
    /// maintenance lock from it and memory-strategy mutation is refused.
    pub lock_strategy: LockStrategy,
    /// Registry identity scoping the in-process memory lock map.
    pub registry_id: String,
    /// Operator ceiling on one run's lock hold (`maintenance_lock_max_hold_secs`).
    pub max_hold_secs: u64,
    /// Run-level cancellation token baked into the `Ctx` sink chain; the lock
    /// session's cancellation forwards into it.
    pub run_cancel: CancellationToken,
}

/// Consumer queue and handler for draining reconcile-enqueued replication jobs
/// in-process. `concurrency` bounds the parallel claim loops.
pub struct ReplicationDrain {
    consumer: Arc<JobStore>,
    handler: Box<dyn JobHandler>,
    concurrency: NonZeroUsize,
}

impl ReplicationDrain {
    /// Builds the consumer queue and handler for the in-process drain.
    /// Reconcile-enqueued pushes carry `source_ts = None`; the handler
    /// re-derives it from the tag's `created_at`, preserving last-writer-wins.
    fn new(
        consumer: Arc<JobStore>,
        blob_store: &Arc<BlobStore>,
        metadata_store: &Arc<MetadataStore>,
        resolver: &Arc<RepositoryResolver>,
        concurrency: NonZeroUsize,
    ) -> Self {
        let handler = ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        );
        Self {
            consumer,
            handler: Box::new(handler),
            concurrency,
        }
    }

    /// Drains reconcile-enqueued replication jobs across `concurrency` claim
    /// loops. A loop ends when no claimable job remains (jobs already backed
    /// off to a future time are intentionally not awaited) or when `cancel`
    /// fires between claims. Returns the number of jobs that failed their push
    /// (retried or dead-lettered) so the caller can mark a run that did not
    /// converge as degraded; a job that only reschedules for retry is not
    /// re-claimed within the same drain, so it is counted once.
    pub async fn drain(self, cancel: CancellationToken) -> u64 {
        info!(
            "Draining enqueued replication jobs to convergence ({} concurrent)",
            self.concurrency
        );
        let loops = (0..self.concurrency.get()).map(|_| async {
            let mut processed: u64 = 0;
            let mut failed: u64 = 0;
            loop {
                if cancel.is_cancelled() {
                    warn!("replication drain cancelled; stopping this claim loop");
                    break;
                }
                match run_once(&self.consumer, self.handler.as_ref(), Queue::Replication).await {
                    Ok(Some(JobRunOutcome::Succeeded)) => processed += 1,
                    Ok(Some(JobRunOutcome::Failed)) => failed += 1,
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Failed to claim a replication job during drain: {e}");
                        break;
                    }
                }
            }
            (processed, failed)
        });
        let (processed, failed) = join_all(loops)
            .await
            .into_iter()
            .fold((0u64, 0u64), |(p, f), (lp, lf)| (p + lp, f + lf));
        info!("Replication drain complete: {processed} job(s) processed, {failed} failed");
        failed
    }
}

#[cfg(test)]
impl ReplicationDrain {
    /// Test-only: build a drain directly over a consumer and handler, bypassing
    /// the replication handler wiring so a failing handler can be substituted.
    pub fn from_parts(
        consumer: Arc<JobStore>,
        handler: Box<dyn JobHandler>,
        concurrency: NonZeroUsize,
    ) -> Self {
        Self {
            consumer,
            handler,
            concurrency,
        }
    }
}

/// Build the inner action sink and optional replication drain. When both gates
/// are closed the sink is a `DryRunSink` (classify + tally only) with no drain;
/// otherwise it is an `Executor` over a fresh per-run `JobStore` tagged with
/// `worker_tag` (a structured-log id, not a queue name). The per-category
/// gating itself lives in the [`GatedSink`] wrapped by [`build_ctx`].
///
/// One `Arc<JobStore>` serves as both producer and consumer; the drain is
/// wired only when `with_drain` is set and policy actions mutate.
pub fn build_sink_and_drain(
    blob_backend: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    repositories: &Arc<RepositoryResolver>,
    gates: SinkGates,
    worker_tag: &str,
    with_drain: bool,
    config: &Configuration,
) -> (Box<dyn ActionSink + Send>, Option<ReplicationDrain>) {
    if !gates.any() {
        info!("Report-only mode: no changes will be made to the storage");
        return (Box::new(DryRunSink), None);
    }

    let job_store = Arc::new(JobStore::new(
        metadata_store.store_arc(),
        format!("{worker_tag}-{}", Uuid::new_v4()),
    ));
    let executor = Executor::new(
        blob_backend.clone(),
        metadata_store.clone(),
        job_store.clone(),
    );
    let drain = (with_drain && gates.policy_mutate).then(|| {
        ReplicationDrain::new(
            job_store,
            blob_backend,
            metadata_store,
            repositories,
            config.global.max_concurrent_replication_jobs,
        )
    });
    (Box::new(executor), drain)
}

/// Bootstrap the stores, build the namespace checkers for `scrub_opts`, and wrap
/// the sink in the shared `Ctx`. `with_drain` is forwarded to
/// [`build_sink_and_drain`]: `replication` passes `true`, `policy` `false`.
pub async fn prepare(
    config: &Configuration,
    scrub_opts: &Options,
    worker_tag: &str,
    with_drain: bool,
) -> Result<Prepared, Error> {
    let (lock_strategy, registry_id) = resolve_lock_strategy(config)?;
    let Stores {
        blob_backend,
        metadata_store,
        repositories,
    } = bootstrap_stores(config).await?;

    let findings = Findings::default();
    let namespace_checkers = setup::namespace_checkers(
        scrub_opts,
        config,
        &blob_backend,
        &metadata_store,
        &repositories,
    )?;

    // Policy and replication have no `--commit`; their policy gate is
    // `!--dry-run` and their GC gate is always closed.
    let gates = SinkGates {
        policy_mutate: !scrub_opts.dry_run,
        gc_mutate: false,
    };
    let (inner, replication_drain) = build_sink_and_drain(
        &blob_backend,
        &metadata_store,
        &repositories,
        gates,
        worker_tag,
        with_drain,
        config,
    );

    let run_cancel = CancellationToken::new();
    let ctx = build_ctx(
        metadata_store,
        inner,
        scrub_opts,
        config,
        findings,
        false,
        gates,
        run_cancel.clone(),
    );
    Ok(Prepared {
        ctx,
        namespace_checkers,
        replication_drain,
        lock_strategy,
        registry_id,
        max_hold_secs: config.global.maintenance_lock_max_hold_secs.get(),
        run_cancel,
    })
}

/// End-of-run summary shared by every maintenance command: flush accumulated
/// access times, snapshot the findings, and log the per-category summary.
pub async fn report_run(ctx: &Arc<Ctx>, command: &str) {
    ctx.metadata_store.flush_access_times().await;
    let findings = ctx.findings.snapshot().await;
    report::log_summary(
        command,
        ctx.opts.action_mutate,
        ctx.prune_unknown_supported,
        &ctx.tally,
        &findings,
    );
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tempfile::TempDir;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::{executor::build_executor, store::Store, transaction::Transaction};

    use super::*;
    use crate::{
        metrics_provider,
        registry::job_store::{Error as JobError, JobEnvelope},
    };

    /// A handler that always fails, so the drain must record each attempt as a
    /// failed push rather than converged work.
    struct FailingHandler;

    #[async_trait]
    impl JobHandler for FailingHandler {
        async fn execute(&self, _envelope: &JobEnvelope) -> Result<Transaction, JobError> {
            Err(JobError::Initialization("push failed".into()))
        }
    }

    fn job_store(dir: &TempDir) -> Arc<JobStore> {
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(dir.path().to_str().expect("valid path")).build());
        let executor =
            build_executor(object.clone(), None, LockStrategy::Memory, None, false, false)
                .expect("build executor");
        let facade = Arc::new(Store::builder(object, executor).build());
        Arc::new(JobStore::new(facade, "test-drain"))
    }

    /// A drain whose every push fails returns a non-zero failure count instead of
    /// silently reporting the jobs as processed. This is the signal the one-shot
    /// replication command folds into its degraded verdict.
    #[tokio::test]
    async fn drain_counts_failed_pushes() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = job_store(&dir);

        store
            .enqueue(
                JobEnvelope::new(
                    Queue::Replication,
                    "test.replicate",
                    "replication.ns:sha256:aabbcc",
                    &(),
                )
                .expect("envelope"),
            )
            .await
            .expect("enqueue");

        let drain = ReplicationDrain::from_parts(
            store,
            Box::new(FailingHandler),
            NonZeroUsize::new(1).expect("nonzero"),
        );
        let failed = drain.drain(CancellationToken::new()).await;

        assert!(
            failed >= 1,
            "a push that fails must be counted as a drain failure, not silently processed \
             (got {failed})"
        );
    }
}
