use std::sync::Arc;

use argh::FromArgs;
use futures_util::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap, prune,
        replicate::ReplicationDrain,
        scrub::{
            action::Action,
            check::{LayoutChecker, NamespaceChecker, StoreChecker, TagChecker, list_all},
            error::Error,
            executor::{ActionSink, DryRunSink, Executor},
            setup::{self, LabeledStoreCheckers},
        },
    },
    configuration::Configuration,
    oci::{Namespace, Tag},
    registry::{Registry, job_store::JobStore, metadata_store::MetadataStore},
};

#[derive(FromArgs, PartialEq, Debug, Default)]
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
    /// deprecated, use 'angos prune': enforce retention policies
    pub retention: bool,
    #[argh(switch, short = 'l')]
    /// fix links format inconsistencies
    pub links: bool,
    #[argh(switch)]
    /// rebuild blob-index entries missing relative to the manifests that
    /// reference each blob (repairs an index corrupted out-of-band, e.g. storage
    /// corruption or manual tampering); reads every manifest, so it is expensive
    pub reconcile_blob_index: bool,
    #[argh(switch, short = 'M')]
    /// backfill missing `media_type` on manifest links
    pub media_types: bool,
    #[argh(switch, short = 'R')]
    /// check for and remove orphan referrer links whose referrer manifest is no longer a current revision
    pub referrers: bool,
    #[argh(switch)]
    /// deprecated, use 'angos replicate': reconcile replicated namespaces with
    /// their downstreams
    pub replicate: bool,
    #[argh(switch)]
    /// delete replication jobs (pending and dead-lettered) whose downstream or
    /// repository is no longer configured
    pub replication_orphans: bool,
    #[argh(switch)]
    /// delete cache jobs (pending and dead-lettered) whose repository is no
    /// longer configured for pull-through
    pub cache_orphans: bool,
    #[argh(switch, short = 'n')]
    /// delete all content for namespaces not owned by any configured repository;
    /// destructive (run --dry-run first); ignored when no repositories configured
    pub orphan_namespaces: bool,
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
    /// Per-tag checkers driven by the tag walk in `scrub_metadata`, run before
    /// the namespace checkers so the invalid-tag gate and per-tag checks precede
    /// the aggregate tag checks. `None` when `--tags` is absent.
    tag_checkers: Option<Vec<Box<dyn TagChecker>>>,
    layout_checker: LayoutChecker,
    /// Store-wide checkers (blobs, orphan grants/namespaces/jobs, multipart),
    /// each paired with a stable label for failure attribution, pre-ordered by
    /// [`setup::store_checkers`] and applied in one pass.
    store_checkers: LabeledStoreCheckers,
    sink: Box<dyn ActionSink + Send>,
    /// Drains reconcile-enqueued replication jobs in-process at the end of the
    /// run. `None` when dry-run or `--replicate` is absent.
    replication_drain: Option<ReplicationDrain>,
    /// Registry the deprecated `--retention` deletions run through; held so the
    /// end of the run can drain in-flight async webhook deliveries.
    retention_registry: Option<Arc<Registry>>,
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
        let tag_checkers = setup::tag_checkers(options, &blob_backend, &metadata_store);
        let layout_checker = setup::layout_checker(&blob_backend);
        let store_checkers =
            setup::store_checkers(options, &blob_backend, &metadata_store, &repositories)?;

        // One `Arc<JobStore>` serves as both producer (Executor enqueue) and
        // consumer (end-of-run drain). Building the queue is cheap, so every
        // non-dry-run `Executor` owns one; the drain is wired only with
        // `--replicate`.
        let mut replication_drain: Option<ReplicationDrain> = None;
        let mut retention_registry: Option<Arc<Registry>> = None;
        let sink: Box<dyn ActionSink + Send> = if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
            Box::new(DryRunSink)
        } else {
            let job_store = Arc::new(JobStore::new(
                metadata_store.store_arc(),
                format!("scrub-{}", Uuid::new_v4()),
            ));
            let mut executor = Executor::new(
                blob_backend.clone(),
                metadata_store.clone(),
                job_store.clone(),
            );
            // The deprecated `--retention` deletes through the registry, like
            // `angos prune`.
            if options.retention {
                let registry = bootstrap::registry(
                    config,
                    blob_backend.clone(),
                    metadata_store.clone(),
                    repositories.clone(),
                    job_store.clone(),
                )?;
                retention_registry = Some(registry.clone());
                executor = executor.with_registry(registry);
            }
            if options.replicate {
                replication_drain = Some(ReplicationDrain::new(
                    job_store,
                    &blob_backend,
                    &metadata_store,
                    &repositories,
                    config.global.max_concurrent_replication_jobs,
                ));
            }
            Box::new(executor)
        };

        Ok(Self {
            metadata_store,
            namespace_checkers,
            tag_checkers,
            layout_checker,
            store_checkers,
            sink,
            replication_drain,
            retention_registry,
        })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.migrate_storage_layout().await?;
        self.scrub_metadata().await?;
        // Store-wide checkers run in the order `setup::store_checkers` built
        // them: orphan-namespace clearing frees manifest bytes for the blob
        // reclaim, and the orphan-job sweep precedes the replication drain so
        // the drain does not churn orphaned jobs through retries first.
        self.scrub_store().await;
        if let Some(drain) = &self.replication_drain {
            drain.drain().await;
        }
        self.metadata_store.flush_access_times().await;
        if let Some(registry) = &self.retention_registry {
            registry
                .shutdown_with_timeout(prune::EVENT_DRAIN_TIMEOUT)
                .await;
        }
        Ok(())
    }

    async fn migrate_storage_layout(&mut self) -> Result<(), Error> {
        if let Err(e) = self.layout_checker.check_all(self.sink.as_mut()).await {
            warn!("Storage layout migration checker failed: {e}");
        }
        Ok(())
    }

    async fn scrub_metadata(&mut self) -> Result<(), Error> {
        // Clone the Arc so the namespace stream borrows a local, leaving `self`
        // free for the `&mut self` per-namespace methods called in the loop.
        let metadata_store = self.metadata_store.clone();
        let mut namespaces = list_all::namespaces(&metadata_store);
        while let Some(namespace) = namespaces.next().await {
            let namespace = namespace?;
            let namespace = match Namespace::new(&namespace) {
                Ok(namespace) => namespace,
                Err(e) => {
                    warn!("Skipping invalid enumerated namespace '{namespace}': {e}");
                    continue;
                }
            };
            if let Err(e) = self.scrub_tags(&namespace).await {
                warn!("Tag scrub failed for namespace '{namespace}': {e}");
            }
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

    /// Walks a namespace's tags once: an invalid name is deleted and skipped, a
    /// valid tag is dispatched to each enabled per-tag checker. Runs before the
    /// aggregate namespace checkers.
    async fn scrub_tags(&mut self, namespace: &Namespace) -> Result<(), Error> {
        let Some(tag_checkers) = &self.tag_checkers else {
            return Ok(());
        };
        let mut names = list_all::unparsed_tags(&self.metadata_store, namespace);
        while let Some(name) = names.next().await {
            let name = name?;
            let tag = match Tag::try_from(name.as_str()) {
                Ok(tag) => tag,
                Err(reason) => {
                    warn!("Deleting invalid tag directory '{namespace}:{name}': {reason}");
                    if let Err(e) = self
                        .sink
                        .apply(Action::DeleteInvalidTag {
                            namespace: namespace.clone(),
                            tag: name,
                        })
                        .await
                    {
                        error!("Failed to delete invalid tag directory in '{namespace}': {e}");
                    }
                    continue;
                }
            };
            for checker in tag_checkers {
                if let Err(e) = checker.check_tag(namespace, &tag, self.sink.as_mut()).await {
                    error!("Tag check failed for '{namespace}:{tag}': {e}");
                }
            }
        }
        Ok(())
    }

    /// Apply every enabled store-wide checker in order, logging and continuing
    /// past a failed checker (scrub is best-effort).
    async fn scrub_store(&mut self) {
        for i in 0..self.store_checkers.len() {
            let (name, checker) = &self.store_checkers[i];
            if let Err(e) = checker.check_all(self.sink.as_mut()).await {
                warn!("Store scrub checker '{name}' failed: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    // Scrub tests point both stores at one temporary root whose contents the
    // assertions inspect, which the shared configuration fixture cannot
    // express with its fixed store paths.
    fn scrub_config(root: &str) -> Configuration {
        let toml = format!(
            r#"
            [blob_store.fs]
            root_dir = "{root}"

            [metadata_store.fs]
            root_dir = "{root}"

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
        Configuration::load_from_str(&toml).unwrap()
    }

    #[tokio::test]
    async fn test_command_new_with_valid_config() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = scrub_config(&path);

        let options = Options {
            dry_run: true,
            uploads: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            tags: true,
            manifests: true,
            blobs: true,
            retention: true,
            ..Options::default()
        };

        let command = Command::new(&options, &config).await;

        assert!(command.is_ok());
        let cmd = command.unwrap();
        // retention, uploads, manifests = 3 namespace checkers (the tag walk is
        // a free function driven by `tag_checkers`, not a namespace checker)
        assert_eq!(cmd.namespace_checkers.len(), 3);
        // `tags` is enabled, so the per-tag checkers are present.
        assert!(cmd.tag_checkers.is_some());
        // `blobs` is the only store-wide checker enabled by these options.
        assert_eq!(cmd.store_checkers.len(), 1);
    }

    #[tokio::test]
    async fn scrub_metadata_deletes_invalid_tag_directory() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        // Plant an invalid tag directory on disk before building the command.
        // A leading '-' is a legal key segment but fails the tag grammar, and
        // the `_manifests` ancestor makes `test-repo/app` an enumerable
        // namespace.
        let tag_dir = format!("{path}/v2/repositories/test-repo/app/_manifests/tags/-bad");
        std::fs::create_dir_all(format!("{tag_dir}/current")).unwrap();
        std::fs::write(
            format!("{tag_dir}/current/link"),
            b"sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let config = scrub_config(&path);

        let options = Options {
            tags: true,
            ..Options::default()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.scrub_metadata().await.unwrap();

        assert!(
            !std::path::Path::new(&tag_dir).exists(),
            "scrub must delete the invalid tag directory"
        );
    }
}
