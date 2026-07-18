mod checker;

use std::{num::NonZeroUsize, sync::Arc};

use argh::FromArgs;
use futures_util::future::join_all;
use tracing::{info, warn};

pub use checker::ReplicationChecker;

use crate::{
    command::{
        bootstrap,
        maintenance::{
            Error, check,
            executor::{ActionSink, DryRunSink, Executor, run_job_store},
        },
    },
    configuration::Configuration,
    jobs::Queue,
    jobs::runner::run_once,
    jobs::store::{JobHandler, JobStore},
    registry::{
        blob_store::BlobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationJobHandler,
};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "replicate",
    description = "Reconcile replicated namespaces with their configured downstreams"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
}

/// Consumer queue and handler for draining reconcile-enqueued replication jobs
/// in-process, since no running worker is assumed; a transiently failing push
/// is rescheduled with backoff onto the durable queue for a worker or a later
/// run. `concurrency` bounds the parallel claim loops so a cold-mirror
/// reconcile does not push one tag at a time.
pub struct ReplicationDrain {
    consumer: Arc<JobStore>,
    handler: Box<dyn JobHandler>,
    concurrency: NonZeroUsize,
}

impl ReplicationDrain {
    /// Builds the consumer queue and handler for the in-process drain.
    /// Reconcile-enqueued pushes carry `source_ts = None`; the handler re-derives
    /// it from the tag's `created_at`, so the receiver still runs last-writer-wins.
    pub fn new(
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

    /// Drains reconcile-enqueued replication jobs with up to `concurrency`
    /// concurrent claim loops. A loop ends when no claimable job remains, so
    /// jobs already backed off to a future time are intentionally not awaited.
    pub async fn drain(&self) {
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

/// Reconciles every replicated namespace against all its configured
/// downstreams, then drains the enqueued jobs in-process.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let bootstrap::MaintenanceContext {
        blob_store: blob_backend,
        metadata_store,
        repositories,
    } = bootstrap::maintenance_context(config).await?;

    let checker = ReplicationChecker::new(metadata_store.clone(), repositories.clone());

    let mut drain = None;
    let sink: Box<dyn ActionSink> = if options.dry_run {
        info!("Dry-run mode: no changes will be made to the storage");
        Box::new(DryRunSink)
    } else {
        // One `Arc<JobStore>` serves as both producer (Executor enqueue) and
        // consumer (end-of-run drain).
        let job_store = run_job_store(&metadata_store, "replicate");
        drain = Some(ReplicationDrain::new(
            job_store.clone(),
            &blob_backend,
            &metadata_store,
            &repositories,
            config.global.max_concurrent_replication_jobs,
        ));
        Box::new(Executor::new(
            blob_backend.clone(),
            metadata_store.clone(),
            job_store,
        ))
    };

    // Sequential on purpose: reconciliation enqueues downstream work in
    // listing order and has no per-namespace concurrency knob.
    check::check_namespaces(&metadata_store, &checker, sink.as_ref(), 1).await?;
    if let Some(drain) = &drain {
        drain.drain().await;
    }
    metadata_store.flush_access_times().await;
    Ok(())
}
