use std::{num::NonZeroUsize, sync::Arc};

use argh::FromArgs;
use futures_util::{StreamExt, future::join_all};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap,
        scrub::{
            check::{
                BlobChecker, LayoutChecker, MultipartChecker, NamespaceChecker, StoreChecker,
                list_all,
            },
            error::Error,
            executor::{ActionSink, DryRunSink, Executor},
            setup,
        },
        worker::runner::execute_one,
    },
    configuration::Configuration,
    registry::{
        blob_store::BlobStore,
        job_store::{JobHandler, JobStore},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    replication::{REPLICATION_QUEUE, ReplicationJobHandler},
};

#[derive(FromArgs, PartialEq, Debug)]
#[allow(clippy::struct_excessive_bools)]
#[argh(
    subcommand,
    name = "scrub",
    description = "Check the storage backend for inconsistencies"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(option, short = 'u')]
    /// check for obsolete uploads with specified timeout
    pub uploads: Option<humantime::Duration>,
    #[argh(option, short = 'p')]
    /// abort orphan S3 multipart uploads older than the specified timeout
    pub multipart: Option<humantime::Duration>,
    #[argh(switch, short = 't')]
    /// check for invalid tag digests
    pub tags: bool,
    #[argh(switch, short = 'm')]
    /// check for manifests inconsistencies
    pub manifests: bool,
    #[argh(switch, short = 'b')]
    /// check for blob inconsistencies
    pub blobs: bool,
    #[argh(switch, short = 'r')]
    /// enforce retention policies
    pub retention: bool,
    #[argh(switch, short = 'l')]
    /// fix links format inconsistencies
    pub links: bool,
    #[argh(switch, short = 'M')]
    /// backfill missing `media_type` on manifest links
    pub media_types: bool,
    #[argh(switch, short = 'R')]
    /// check for and remove orphan referrer links whose referrer manifest is no longer a current revision
    pub referrers: bool,
    #[argh(switch)]
    /// reconcile every replicated namespace with all its downstreams; additive by
    /// default (enqueues a replication push for each diverging or downstream-missing
    /// tag), and for a downstream marked prune = true also enqueues a replication
    /// delete for each downstream-only tag (one-way mirror; unsafe for active-active
    /// peers)
    pub replicate: bool,
}

pub struct Command {
    metadata_store: Arc<MetadataStore>,
    namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    layout_checker: LayoutChecker,
    blob_checker: Option<BlobChecker>,
    multipart_checker: Option<MultipartChecker>,
    sink: Box<dyn ActionSink + Send>,
    /// When `--replicate` runs outside dry-run, the enqueued replication jobs are
    /// drained in-process (no running worker is assumed): every job that is ready
    /// is claimed, and a job whose push succeeds converges within this invocation.
    /// A job whose push transiently fails is rescheduled with backoff onto the
    /// durable queue and left for a worker or a later `scrub` run, so a transient
    /// failure does not necessarily converge here. `None` disables the drain
    /// (dry-run, or `--replicate` absent).
    replication_drain: Option<ReplicationDrain>,
}

/// Consumer queue + handler used to drain replication jobs enqueued by the
/// reconciliation checker, within the scrub CLI run. `concurrency`
/// (`[global] max_concurrent_replication_jobs`) bounds the parallel claim
/// loops, matching the worker and in-process drains (a cold-mirror reconcile
/// would otherwise push one tag at a time).
struct ReplicationDrain {
    consumer: Arc<JobStore>,
    handler: Box<dyn JobHandler>,
    concurrency: NonZeroUsize,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
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

        let namespace_checkers = setup::namespace_checkers(
            options,
            config,
            &blob_backend,
            &metadata_store,
            &repositories,
        )?;
        let layout_checker = setup::layout_checker(&blob_backend);
        let blob_checker = setup::blob_checker(options, &blob_backend, &metadata_store);
        let multipart_checker = setup::multipart_checker(options, &blob_backend)?;

        // The reconciliation checker emits `EnqueueReplicationPush` actions that the
        // `Executor` lands on a durable queue; share one `Arc<JobStore>` as both the
        // producer (Executor enqueue) and the consumer (end-of-run drain) over the
        // metadata store's `Store` façade. Building the queue is cheap, so a
        // non-dry-run `Executor` always owns one; the drain is only wired when
        // reconcile actually enqueues (`--replicate`).
        let mut replication_drain: Option<ReplicationDrain> = None;
        let sink: Box<dyn ActionSink + Send> = if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
            Box::new(DryRunSink)
        } else {
            let job_store = Arc::new(JobStore::new(
                metadata_store.store_arc(),
                format!("scrub-{}", Uuid::new_v4()),
            ));
            let executor = Executor::builder()
                .blob_store(blob_backend.clone())
                .metadata_store(metadata_store.clone())
                .job_store(job_store.clone())
                .build()?;
            if options.replicate {
                replication_drain = Some(Self::build_replication_drain(
                    job_store,
                    &blob_backend,
                    &metadata_store,
                    &repositories,
                    config.global.max_concurrent_replication_jobs,
                )?);
            }
            Box::new(executor)
        };

        Ok(Self {
            metadata_store,
            namespace_checkers,
            layout_checker,
            blob_checker,
            multipart_checker,
            sink,
            replication_drain,
        })
    }

    /// Builds the consumer queue + handler used to drain reconcile-enqueued
    /// replication jobs in-process during the CLI run (jobs whose push succeeds
    /// converge here; a transiently-failing push is rescheduled onto the durable
    /// queue for a worker or a later `scrub` run).
    ///
    /// Reconcile-enqueued pushes carry `source_ts = None`; the handler re-derives
    /// `source_ts` from the resolved tag's `created_at`, so the receiver still runs
    /// last-writer-wins.
    fn build_replication_drain(
        consumer: Arc<JobStore>,
        blob_store: &Arc<BlobStore>,
        metadata_store: &Arc<MetadataStore>,
        resolver: &Arc<RepositoryResolver>,
        concurrency: NonZeroUsize,
    ) -> Result<ReplicationDrain, Error> {
        let handler = ReplicationJobHandler::builder()
            .resolver(resolver.clone())
            .blob_store(blob_store.clone())
            .metadata_store(metadata_store.clone())
            .build()
            .map_err(|e| {
                Error::Initialization(format!("failed to build replication handler: {e}"))
            })?;
        Ok(ReplicationDrain {
            consumer,
            handler: Box::new(handler),
            concurrency,
        })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.migrate_storage_layout().await?;
        self.scrub_metadata().await?;
        self.scrub_blobs().await?;
        self.scrub_multipart_uploads().await?;
        self.drain_replication_jobs().await;
        self.metadata_store.flush_access_times().await;
        Ok(())
    }

    async fn migrate_storage_layout(&mut self) -> Result<(), Error> {
        if let Err(e) = self.layout_checker.check_all(self.sink.as_mut()).await {
            warn!("Storage layout migration checker failed: {e}");
        }
        Ok(())
    }

    async fn scrub_metadata(&mut self) -> Result<(), Error> {
        let mut namespaces = list_all::namespaces(&self.metadata_store);
        while let Some(namespace) = namespaces.next().await {
            let namespace = namespace?;
            for i in 0..self.namespace_checkers.len() {
                if let Err(e) = self.namespace_checkers[i]
                    .check(&namespace, self.sink.as_mut())
                    .await
                {
                    warn!("Scrub checker failed for namespace '{namespace}': {e}");
                }
            }
        }
        Ok(())
    }

    async fn scrub_blobs(&mut self) -> Result<(), Error> {
        if let Some(checker) = &self.blob_checker
            && let Err(e) = checker.check_all(self.sink.as_mut()).await
        {
            warn!("Blob scrub checker failed: {e}");
        }
        Ok(())
    }

    async fn scrub_multipart_uploads(&mut self) -> Result<(), Error> {
        if let Some(checker) = &self.multipart_checker
            && let Err(e) = checker.check_all(self.sink.as_mut()).await
        {
            warn!("Multipart scrub checker failed: {e}");
        }
        Ok(())
    }

    /// Drains the replication jobs the reconciliation checker enqueued. No-op
    /// unless `--replicate` ran outside dry-run. Up to
    /// `max_concurrent_replication_jobs` claim loops run concurrently (a
    /// cold-mirror reconcile would otherwise push one tag at a time); each
    /// cycle claims one ready job and drives it through `execute_one`: a job
    /// whose push succeeds converges here, while one whose push transiently
    /// fails is failed-over (rescheduled with backoff onto the durable queue
    /// for a worker or a later `scrub` run). A loop ends when the queue has no
    /// claimable (ready) job left, so jobs already backed off for a future
    /// time are intentionally not awaited.
    async fn drain_replication_jobs(&mut self) {
        let Some(drain) = &self.replication_drain else {
            return;
        };

        info!(
            "Draining enqueued replication jobs to convergence ({} concurrent)",
            drain.concurrency
        );
        let loops = (0..drain.concurrency.get()).map(|_| async {
            let mut drained: u64 = 0;
            loop {
                let outcome = match drain.consumer.claim_one(REPLICATION_QUEUE).await {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        warn!("Failed to claim a replication job during drain: {e}");
                        break;
                    }
                };
                let Some(claimed) = outcome.claimed else {
                    break;
                };
                execute_one(&drain.consumer, drain.handler.as_ref(), claimed).await;
                drained += 1;
            }
            drained
        });
        let drained: u64 = join_all(loops).await.into_iter().sum();
        info!("Replication drain complete: processed {drained} job(s)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_command_new_with_valid_config() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let config_content = format!(
            r#"
            [blob_store.fs]
            root_dir = "{path}"

            [metadata_store.fs]
            root_dir = "{path}"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false

            [global.retention_policy]
            rules = []
            "#
        );

        let config: Configuration = toml::from_str(&config_content).unwrap();

        let options = Options {
            dry_run: true,
            uploads: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            multipart: None,
            tags: true,
            manifests: true,
            blobs: true,
            retention: true,
            links: false,
            media_types: false,
            referrers: false,
            replicate: false,
        };

        let command = Command::new(&options, &config).await;

        assert!(command.is_ok());
        let cmd = command.unwrap();
        // retention, uploads, tags, manifests = 4 namespace checkers
        assert_eq!(cmd.namespace_checkers.len(), 4);
        assert!(cmd.blob_checker.is_some());
    }
}
