mod checker;

use std::num::NonZeroUsize;

use argh::FromArgs;
use futures_util::future::join_all;
use tracing::{info, warn};

use checker::ReplicationChecker;

use crate::command::bootstrap::{self, StoreContext};
use crate::command::maintenance::executor::{Executor, run_job_store, run_sink};
use crate::command::maintenance::{Error, check};
use crate::configuration::Configuration;
use crate::jobs::Queue;
use crate::jobs::runner::run_once;
use crate::jobs::store::JobStore;
use crate::replication::ReplicationJobHandler;

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "replication",
    description = "Reconcile replicated namespaces with their configured downstreams"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
}

/// Reconciles every replicated namespace against all its configured
/// downstreams, then drains the enqueued jobs in-process.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let StoreContext {
        blob_store,
        metadata_store,
        repositories,
    } = bootstrap::maintenance_context(config).await?;

    let checker = ReplicationChecker::new(metadata_store.clone(), repositories.clone());

    // One store serves as producer (Executor enqueue) and consumer (drain).
    let mut job_store = None;
    let sink = run_sink(options.dry_run, || {
        let store = run_job_store(&metadata_store, "reconcile");
        job_store = Some(store.clone());
        Ok(Executor::new(
            blob_store.clone(),
            metadata_store.clone(),
            store,
            None,
        ))
    })?;

    // Sequential on purpose: there is no per-namespace concurrency knob here.
    check::check_namespaces(&metadata_store, &checker, sink.as_ref(), 1).await?;
    if let Some(job_store) = job_store {
        let handler = ReplicationJobHandler::new(repositories, blob_store, metadata_store);
        drain(
            &job_store,
            &handler,
            config.global.max_concurrent_replication_jobs,
        )
        .await;
    }
    Ok(())
}

/// Drains the enqueued jobs, since no running worker is assumed. A claim loop
/// ends when no claimable job remains, so a push backed off to a future time
/// is left on the durable queue for a worker or a later run. Reconcile-enqueued
/// pushes carry `source_ts = None`; the handler re-derives it from the tag's
/// `created_at`, so the receiver still runs last-writer-wins.
async fn drain(job_store: &JobStore, handler: &ReplicationJobHandler, concurrency: NonZeroUsize) {
    info!("Draining enqueued replication jobs to convergence ({concurrency} concurrent)");
    let loops = (0..concurrency.get()).map(|_| async {
        let mut drained: u64 = 0;
        loop {
            match run_once(job_store, handler, Queue::Replication).await {
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
