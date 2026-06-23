//! Shared construction for the storage-maintenance subcommands `scrub`,
//! `policy`, and `replication`. All three bootstrap the same store handles and
//! assemble the same scrub-DAG [`Ctx`] before running, so those two steps live
//! here, beside the three commands, rather than inside any one of them.
//!
//! [`build_ctx`] reaches into the scrub engine (`scrub::context`, `scrub::report`)
//! because the `Ctx` it returns is that engine's type; the low-level
//! [`bootstrap`](crate::command::bootstrap) module stays free of that dependency.
//!
//! It also owns the in-process [`ReplicationDrain`] that `scrub --replicate` and
//! `angos replication` drain their reconcile-enqueued pushes through, since
//! [`build_sink_and_drain`] is where the drain is built.

use std::{num::NonZeroUsize, sync::Arc};

use futures_util::future::join_all;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap,
        scrub::{
            Error, Options,
            check::NamespaceChecker,
            context::{Ctx, MAX_FANOUT, ScrubFlags},
            executor::{ActionSink, DryRunSink, Executor},
            report::{self, ActionTally, Findings},
            setup,
        },
        worker::runner::run_once,
    },
    configuration::Configuration,
    registry::{
        blob_store::BlobStore,
        job_store::{JobHandler, JobStore, Queue},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationJobHandler,
};

/// The three store handles `scrub` / `policy` / `replication` each need. Returned
/// from [`bootstrap_stores`] so the identical bootstrap runs once and is
/// destructured by every `Command::new`.
pub struct Stores {
    pub blob_backend: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub repositories: Arc<RepositoryResolver>,
}

/// Build the shared store handles from the loaded configuration. The auth cache
/// stays internal: it only feeds the metadata-store and repository builders.
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

/// Assemble the shared [`Ctx`] from the post-sink pieces every `Command::new`
/// builds the same way: clamp the fan-out, parse the flag gates, wrap `inner` in
/// the counting sink that feeds a fresh tally, and box the sink behind the shared
/// `Mutex`. `prune_unknown_supported` is `true` only for `scrub`.
pub fn build_ctx(
    metadata_store: Arc<MetadataStore>,
    inner: Box<dyn ActionSink + Send>,
    scrub_opts: &Options,
    config: &Configuration,
    findings: Findings,
    prune_unknown_supported: bool,
) -> Arc<Ctx> {
    let configured = config.global.max_concurrent_scrub_tasks.get();
    let fanout = configured.min(MAX_FANOUT);
    if configured > MAX_FANOUT {
        warn!(
            "max_concurrent_scrub_tasks = {configured} exceeds the fan-out ceiling of {MAX_FANOUT}; clamping to {MAX_FANOUT}"
        );
    }
    let opts = ScrubFlags::from_options(scrub_opts, fanout);
    let tally = Arc::new(ActionTally::default());
    let sink = report::counting_sink(inner, tally.clone());
    Arc::new(Ctx {
        metadata_store,
        sink: Arc::new(Mutex::new(sink)),
        opts,
        tally,
        findings,
        prune_unknown_supported,
    })
}

/// The shared `Ctx`, namespace checkers, and optional replication drain a
/// single-metadata-node maintenance command (`policy`, `replication`) needs to
/// run. `replication_drain` is `Some` only when [`prepare`] was asked to wire it.
pub struct Prepared {
    pub ctx: Arc<Ctx>,
    pub namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    pub replication_drain: Option<ReplicationDrain>,
}

/// Consumer queue and handler for draining reconcile-enqueued replication jobs
/// in-process. `concurrency` bounds the parallel claim loops so a cold-mirror
/// reconcile does not push one tag at a time.
pub struct ReplicationDrain {
    consumer: Arc<JobStore>,
    handler: Box<dyn JobHandler>,
    concurrency: NonZeroUsize,
}

impl ReplicationDrain {
    /// Drains reconcile-enqueued replication jobs with up to
    /// `max_concurrent_replication_jobs` concurrent claim loops. A loop ends when
    /// no claimable job remains, so jobs already backed off to a future time are
    /// intentionally not awaited. Consuming `self` because it is the `replicate`
    /// node's `FnOnce` body.
    pub async fn drain(self) {
        info!(
            "Draining enqueued replication jobs to convergence ({} concurrent)",
            self.concurrency
        );
        let loops = (0..self.concurrency.get()).map(|_| async {
            let mut drained: u64 = 0;
            loop {
                match run_once(&self.consumer, self.handler.as_ref(), Queue::Replication).await {
                    Ok(true) => drained += 1,
                    Ok(false) => break,
                    Err(e) => {
                        warn!("Failed to claim a replication job during drain: {e}");
                        break;
                    }
                }
            }
            drained
        });
        let drained: u64 = join_all(loops).await.into_iter().sum();
        info!("Replication drain complete: processed {drained} job(s)");
    }
}

/// Builds the consumer queue and handler for the in-process drain.
/// Reconcile-enqueued pushes carry `source_ts = None`; the handler re-derives it
/// from the tag's `created_at`, so the receiver still runs last-writer-wins.
pub fn build_replication_drain(
    consumer: Arc<JobStore>,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
    concurrency: NonZeroUsize,
) -> ReplicationDrain {
    let handler =
        ReplicationJobHandler::new(resolver.clone(), blob_store.clone(), metadata_store.clone());
    ReplicationDrain {
        consumer,
        handler: Box::new(handler),
        concurrency,
    }
}

/// Build the inner action sink (and optional replication drain) every
/// maintenance command shares. Under dry-run this is a `DryRunSink` and no
/// drain; otherwise it is an `Executor` over a fresh per-run
/// `worker_tag`-tagged `JobStore`. `worker_tag` is the structured-log worker id
/// stamped on that `JobStore`, not a storage-queue name (queue routing is by the
/// `Queue` enum).
///
/// One `Arc<JobStore>` serves as both producer (Executor enqueue) and consumer
/// (end-of-run drain). Building the queue is cheap, so every non-dry-run
/// `Executor` owns one; the drain is wired only when `with_drain` is set (i.e.
/// with `--replicate`).
pub fn build_sink_and_drain(
    blob_backend: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    repositories: &Arc<RepositoryResolver>,
    dry_run: bool,
    worker_tag: &str,
    with_drain: bool,
    config: &Configuration,
) -> (Box<dyn ActionSink + Send>, Option<ReplicationDrain>) {
    if dry_run {
        info!("Dry-run mode: no changes will be made to the storage");
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
    let drain = with_drain.then(|| {
        build_replication_drain(
            job_store,
            blob_backend,
            metadata_store,
            repositories,
            config.global.max_concurrent_replication_jobs,
        )
    });
    (Box::new(executor), drain)
}

/// Assemble everything `policy` and `replication` build the same way: bootstrap
/// the stores, build the namespace checkers for `scrub_opts`, and wrap the sink
/// (a `DryRunSink` under dry-run, otherwise an `Executor` over a fresh per-run
/// `worker_tag`-tagged `JobStore`) in the shared `Ctx`. `worker_tag` is the
/// structured-log worker id stamped on that `JobStore`, not a storage-queue
/// name (queue routing is by the `Queue` enum). When `with_drain` is set the
/// same queue is drained in-process after the run, so `replication` passes
/// `true` and `policy` (which enqueues nothing to drain) passes `false`.
pub async fn prepare(
    config: &Configuration,
    scrub_opts: &Options,
    worker_tag: &str,
    with_drain: bool,
) -> Result<Prepared, Error> {
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
        &findings,
    )?;

    let (inner, replication_drain) = build_sink_and_drain(
        &blob_backend,
        &metadata_store,
        &repositories,
        scrub_opts.dry_run,
        worker_tag,
        with_drain,
        config,
    );

    let ctx = build_ctx(metadata_store, inner, scrub_opts, config, findings, false);
    Ok(Prepared {
        ctx,
        namespace_checkers,
        replication_drain,
    })
}
