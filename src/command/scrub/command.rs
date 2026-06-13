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
                BlobChecker, LayoutChecker, MultipartChecker, NamespaceChecker, OrphanGrantChecker,
                OrphanJobChecker, StoreChecker, list_all,
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
    /// reconcile replicated namespaces with their downstreams: enqueue a push for
    /// each diverging or missing tag, and for a downstream marked prune = true a
    /// delete for each downstream-only tag (one-way mirror; unsafe for
    /// active-active peers)
    pub replicate: bool,
    #[argh(switch)]
    /// delete replication jobs (pending and dead-lettered) whose downstream or
    /// repository is no longer configured
    pub replication_orphans: bool,
    #[argh(switch)]
    /// delete cache jobs (pending and dead-lettered) whose repository is no
    /// longer configured for pull-through
    pub cache_orphans: bool,
    #[argh(option)]
    /// revoke orphaned blob-ownership grants older than the specified age: a
    /// blob a namespace owns but no manifest references (e.g. a replication push
    /// that lost last-writer-wins), reclaiming the bytes when it was the last
    /// reference
    pub orphan_grants: Option<humantime::Duration>,
}

pub struct Command {
    metadata_store: Arc<MetadataStore>,
    namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    layout_checker: LayoutChecker,
    blob_checker: Option<BlobChecker>,
    multipart_checker: Option<MultipartChecker>,
    orphan_grant_checker: Option<OrphanGrantChecker>,
    /// One checker per queue selected by `--replication-orphans` and
    /// `--cache-orphans`; empty when neither flag is set.
    orphan_job_checkers: Vec<OrphanJobChecker>,
    sink: Box<dyn ActionSink + Send>,
    /// Drains reconcile-enqueued replication jobs in-process, since no running
    /// worker is assumed; a transiently failing push is rescheduled with backoff
    /// onto the durable queue for a worker or a later run. `None` when dry-run or
    /// `--replicate` is absent.
    replication_drain: Option<ReplicationDrain>,
}

/// Consumer queue and handler for draining reconcile-enqueued replication jobs
/// in-process. `concurrency` bounds the parallel claim loops so a cold-mirror
/// reconcile does not push one tag at a time.
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
        let orphan_grant_checker =
            setup::orphan_grant_checker(options, &blob_backend, &metadata_store)?;
        let orphan_job_checkers =
            setup::orphan_job_checkers(options, &metadata_store, &repositories)?;

        // One `Arc<JobStore>` serves as both producer (Executor enqueue) and
        // consumer (end-of-run drain). Building the queue is cheap, so every
        // non-dry-run `Executor` owns one; the drain is wired only with
        // `--replicate`.
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
            orphan_grant_checker,
            orphan_job_checkers,
            sink,
            replication_drain,
        })
    }

    /// Builds the consumer queue and handler for the in-process drain.
    /// Reconcile-enqueued pushes carry `source_ts = None`; the handler re-derives
    /// it from the tag's `created_at`, so the receiver still runs last-writer-wins.
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
        self.scrub_orphan_grants().await?;
        self.scrub_multipart_uploads().await?;
        // Orphan jobs (replication and cache queues) must be scrubbed before
        // the drain: the drain claims any pending replication job and would
        // churn its orphans through retries first.
        self.scrub_orphan_jobs().await?;
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

    async fn scrub_orphan_grants(&mut self) -> Result<(), Error> {
        if let Some(checker) = &self.orphan_grant_checker
            && let Err(e) = checker.check_all(self.sink.as_mut()).await
        {
            warn!("Orphan blob-grant scrub checker failed: {e}");
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

    async fn scrub_orphan_jobs(&mut self) -> Result<(), Error> {
        for checker in &self.orphan_job_checkers {
            if let Err(e) = checker.check_all(self.sink.as_mut()).await {
                warn!("Orphan job scrub checker failed: {e}");
            }
        }
        Ok(())
    }

    /// Drains reconcile-enqueued replication jobs with up to
    /// `max_concurrent_replication_jobs` concurrent claim loops. A loop ends when
    /// no claimable job remains, so jobs already backed off to a future time are
    /// intentionally not awaited.
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
            replication_orphans: false,
            cache_orphans: false,
            orphan_grants: None,
        };

        let command = Command::new(&options, &config).await;

        assert!(command.is_ok());
        let cmd = command.unwrap();
        // retention, uploads, tags, manifests = 4 namespace checkers
        assert_eq!(cmd.namespace_checkers.len(), 4);
        assert!(cmd.blob_checker.is_some());
    }
}
