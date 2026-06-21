use std::{num::NonZeroUsize, sync::Arc};

use argh::FromArgs;
use futures_util::future::join_all;
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    command::{
        bootstrap,
        scrub::{
            context::{Ctx, DEFAULT_FANOUT, ScrubFlags},
            error::Error,
            executor::{ActionSink, DryRunSink, Executor},
            node::{self, NodeParts},
            scheduler, setup,
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
    #[argh(switch)]
    /// rebuild blob-index entries missing relative to the manifests that
    /// reference each blob (repairs an index corrupted out-of-band, e.g. storage
    /// corruption or manual tampering); reads every manifest, so it is expensive
    pub reconcile_blob_index: bool,
    #[argh(switch)]
    /// migrate the on-disk storage layout: rewrite legacy single-file blob
    /// indexes into the sharded layout and prune the pre-1.3 namespace-registry
    /// index. Scans every blob, so run it only when migrating, not on routine
    /// scrubs
    pub migrate: bool,
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
    #[argh(switch)]
    /// structural job reconcile: drop dangling `_jobs/index` lock-key entries
    /// whose pending envelope has vanished. With --prune-unknown it also removes
    /// unknown-named queue directories. Independent of the config-drift orphan
    /// sweeps (--replication-orphans / --cache-orphans)
    pub jobs: bool,
    #[argh(switch)]
    /// opt-in: also delete structurally-invalid objects scrub would otherwise
    /// only report — invalid-named namespaces and unknown job-queue directories.
    /// Off by default so no existing flag ever deletes more than before. Run
    /// with -d first
    pub prune_unknown: bool,
}

pub struct Command {
    /// Shared context (stores, sink, flags, semaphore) threaded into every node.
    ctx: Arc<Ctx>,
    /// The pre-built checkers and drain. Constructed in `new`, consumed by
    /// `build_nodes` in `run` (hence `Option` + `take`).
    parts: Option<NodeParts>,
}

/// Consumer queue and handler for draining reconcile-enqueued replication jobs
/// in-process. `concurrency` bounds the parallel claim loops so a cold-mirror
/// reconcile does not push one tag at a time.
pub(crate) struct ReplicationDrain {
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
    pub(crate) async fn drain(self) {
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

        // Reuse the existing checker builders verbatim; the only orchestration
        // change is that the store-wide checkers are now distinct nodes (their
        // order is encoded as DAG deps) instead of one labeled vec.
        let namespace_checkers = setup::namespace_checkers(
            options,
            config,
            &blob_backend,
            &metadata_store,
            &repositories,
        )?;
        let tag_checkers = setup::tag_checkers(options, &blob_backend, &metadata_store);
        let layout_checker = options
            .migrate
            .then(|| setup::layout_checker(&blob_backend));
        let blob_checker = setup::blob_checker(options, &blob_backend, &metadata_store);
        let multipart_checker = setup::multipart_checker(options, &blob_backend)?;
        let orphan_grant_checker =
            setup::orphan_grant_checker(options, &blob_backend, &metadata_store)?;
        let orphan_namespace_checker =
            setup::orphan_namespace_checker(options, &blob_backend, &metadata_store, &repositories);
        let orphan_job_checkers =
            setup::orphan_job_checkers(options, &metadata_store, &repositories);
        let job_checker = setup::job_checker(options, &metadata_store);

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
            let executor = Executor::new(
                blob_backend.clone(),
                metadata_store.clone(),
                job_store.clone(),
            );
            if options.replicate {
                replication_drain = Some(Self::build_replication_drain(
                    job_store,
                    &blob_backend,
                    &metadata_store,
                    &repositories,
                    config.global.max_concurrent_replication_jobs,
                ));
            }
            Box::new(executor)
        };

        let opts = ScrubFlags::from_options(options)?;

        let ctx = Arc::new(Ctx {
            blob_store: blob_backend,
            metadata_store,
            resolver: repositories,
            sink: Arc::new(Mutex::new(sink)),
            opts,
            sem: Arc::new(Semaphore::new(DEFAULT_FANOUT)),
        });

        let parts = NodeParts {
            layout_checker,
            namespace_checkers,
            tag_checkers,
            blob_checker,
            multipart_checker,
            orphan_grant_checker,
            orphan_namespace_checker,
            orphan_job_checkers,
            job_checker,
            replication_drain,
        };

        Ok(Self {
            ctx,
            parts: Some(parts),
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
    ) -> ReplicationDrain {
        let handler = ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        );
        ReplicationDrain {
            consumer,
            handler: Box::new(handler),
            concurrency,
        }
    }

    /// Build the enabled-node set from the flags and run it as a DAG. The DAG
    /// edges encode main's observable order (see `node.rs`): metadata and
    /// orphan-namespaces before the blob GC, grants after blob, the replication
    /// drain after the orphan-job sweep. After every node completes, the access
    /// times accumulated by the Executor are flushed, exactly as the old
    /// sequential `run` did.
    pub async fn run(&mut self) -> Result<(), Error> {
        let parts = self
            .parts
            .take()
            .expect("Command::run called more than once");
        let nodes = node::build_nodes(&self.ctx, parts);
        scheduler::run_dag(nodes, self.ctx.sem.clone()).await;
        self.ctx.metadata_store.flush_access_times().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// A minimal valid fs-backed configuration with an empty retention policy.
    fn test_config(path: &str) -> Configuration {
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
        toml::from_str(&config_content).unwrap()
    }

    /// All-false `Options` so each test toggles only the flags it cares about.
    fn base_options() -> Options {
        Options {
            dry_run: true,
            uploads: None,
            multipart: None,
            tags: false,
            manifests: false,
            blobs: false,
            retention: false,
            links: false,
            reconcile_blob_index: false,
            migrate: false,
            media_types: false,
            referrers: false,
            replicate: false,
            replication_orphans: false,
            cache_orphans: false,
            orphan_namespaces: false,
            orphan_grants: None,
            jobs: false,
            prune_unknown: false,
        }
    }

    /// Build the command and project its node set to an id -> deps map so tests
    /// can assert the flag -> DAG mapping without depending on node internals.
    async fn node_map(
        options: &Options,
        config: &Configuration,
    ) -> HashMap<&'static str, Vec<&'static str>> {
        let mut cmd = Command::new(options, config).await.unwrap();
        let parts = cmd.parts.take().unwrap();
        node::build_nodes(&cmd.ctx, parts)
            .into_iter()
            .map(|n| (n.id, n.deps.to_vec()))
            .collect()
    }

    #[tokio::test]
    async fn command_new_builds_the_expected_node_set_and_edges() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        // The flags from the former structural test: tags + manifests + blobs +
        // retention + uploads (dry-run). The metadata-scoped steps collapse into
        // one `metadata` node; `blobs` is the only store node, and it depends on
        // the metadata barrier (orphan-namespaces is absent, so that edge drops).
        let options = Options {
            uploads: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            tags: true,
            manifests: true,
            blobs: true,
            retention: true,
            ..base_options()
        };

        let nodes = node_map(&options, &config).await;

        let mut ids: Vec<&str> = nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["blob", "metadata"]);
        // The blob node always declares the metadata + orphan-namespaces + migrate
        // barrier; the scheduler drops the edges to the absent orphan-namespaces
        // and migrate nodes.
        assert_eq!(
            nodes.get("blob").unwrap(),
            &vec!["metadata", "orphan-namespaces", "migrate"]
        );
        // `--migrate` is absent, so no migration node is built.
        assert!(!nodes.contains_key("migrate"));
    }

    #[tokio::test]
    async fn no_flags_builds_no_nodes() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let nodes = node_map(&base_options(), &config).await;
        assert!(nodes.is_empty(), "no flags => no nodes: {nodes:?}");
    }

    #[tokio::test]
    async fn store_node_edges_match_main_ordering() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        // Enable every store-side flag (no repositories configured means the
        // orphan-namespace gate refuses, matching its safety guard, so it is
        // intentionally absent here). Assert the dependency edges that encode
        // main's observable order survive even when a dep node is absent.
        let options = Options {
            blobs: true,
            multipart: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            orphan_grants: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            manifests: true,
            ..base_options()
        };

        let nodes = node_map(&options, &config).await;

        // metadata -> blob barrier (migrate edge present but its node absent here).
        assert_eq!(
            nodes.get("blob").unwrap(),
            &vec!["metadata", "orphan-namespaces", "migrate"]
        );
        // blob -> orphan-grants.
        assert_eq!(nodes.get("orphan-grants").unwrap(), &vec!["blob"]);
        // multipart is independent.
        assert_eq!(nodes.get("multipart").unwrap(), &Vec::<&str>::new());
    }

    #[tokio::test]
    async fn metadata_node_deletes_invalid_tag_directory() {
        use tempfile::TempDir;

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

        let config = test_config(&path);

        // Only `--tags`, mutate (not dry-run). The single enabled node is
        // `metadata`; running the DAG exercises the real metadata node body,
        // including the invalid-tag gate. Behavioral assertion unchanged.
        let options = Options {
            dry_run: false,
            tags: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            !std::path::Path::new(&tag_dir).exists(),
            "scrub must delete the invalid tag directory"
        );
    }

    /// `--jobs` builds an independent `jobs` node; it does not gate or depend on
    /// the metadata/blob barrier or the config-drift orphan-jobs sweep.
    #[tokio::test]
    async fn jobs_flag_builds_independent_jobs_node() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            jobs: true,
            ..base_options()
        };

        let nodes = node_map(&options, &config).await;

        let mut ids: Vec<&str> = nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["jobs"]);
        assert_eq!(nodes.get("jobs").unwrap(), &Vec::<&str>::new());
    }

    /// Plants an invalid-named namespace (uppercase fails the `Namespace`
    /// grammar) carrying a `_manifests` marker so `list_all_namespaces`
    /// enumerates it, and returns its metadata subtree path.
    fn plant_invalid_namespace(path: &str) -> String {
        let ns_dir = format!("{path}/v2/repositories/BADNS");
        std::fs::create_dir_all(format!("{ns_dir}/_manifests/revisions")).unwrap();
        std::fs::write(format!("{ns_dir}/_manifests/revisions/marker"), b"x").unwrap();
        ns_dir
    }

    /// Without `--prune-unknown` an invalid-named namespace is report-only: the
    /// metadata walk warns and skips it, leaving the directory in place
    /// (main's behavior).
    #[tokio::test]
    async fn metadata_node_keeps_invalid_namespace_without_prune_unknown() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        // `--manifests` builds the metadata node and mutates (not dry-run), but
        // `--prune-unknown` is OFF, so no namespace deletion may occur.
        let options = Options {
            dry_run: false,
            manifests: true,
            prune_unknown: false,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            std::path::Path::new(&ns_dir).exists(),
            "without --prune-unknown an invalid-named namespace must survive"
        );
    }

    /// With `--prune-unknown` (and not dry-run) the metadata walk reclaims the
    /// invalid-named namespace's subtree.
    #[tokio::test]
    async fn metadata_node_deletes_invalid_namespace_under_prune_unknown() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        let options = Options {
            dry_run: false,
            manifests: true,
            prune_unknown: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            !std::path::Path::new(&ns_dir).exists(),
            "--prune-unknown must delete the invalid-named namespace's subtree"
        );
    }

    /// `-l` now visits **link-only** namespaces (those carrying `_layers`/`_config`
    /// but no `_manifests`), the accepted behavioral counterpart to `-u` reaching
    /// upload-only namespaces. On main, the metadata walk keyed off `_manifests`
    /// (`list_namespaces`), so a manifest-less namespace was never enumerated and
    /// `-l` skipped it. With `list_all_namespaces`, the walk now reaches it, so
    /// `LinkReferencesChecker` prunes the orphan layer link's phantom referrer and
    /// cascade-deletes the now referrer-less link. This is strictly-more-correct
    /// cleanup of links whose owning manifests are already gone.
    #[tokio::test]
    async fn links_flag_reaps_orphan_link_in_link_only_namespace() {
        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
        use futures_util::StreamExt;
        use tempfile::TempDir;

        use crate::{
            oci::{Digest, Namespace},
            registry::{
                metadata_store::{LinkKind, LinkOperation},
                test_utils,
            },
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        // Plant a link-only namespace over the same FS root the command will read:
        // a `_layers` link whose sole referrer is a phantom (no `Digest` link) and
        // no `_manifests` content, so `list_namespaces` (the catalog) excludes it
        // and main's `-l` would have skipped it.
        let link_only = Namespace::new("link-only/orphan").unwrap();
        let phantom =
            Digest::sha256("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                .unwrap();
        {
            let object: Arc<dyn ObjectStore> =
                Arc::new(StorageFsBackend::builder(path.as_str()).build());
            let executor = test_utils::build_test_fs_executor(path.as_str(), false);
            let metadata_store = test_utils::metadata_store_over(object, executor);

            let layer = test_utils::put_blob_direct(metadata_store.store(), b"orphan layer").await;
            metadata_store
                .update_links(
                    &link_only,
                    &[LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        layer.clone(),
                        phantom.clone(),
                    )],
                )
                .await
                .unwrap();

            // The link-only namespace is absent from the catalog (keyed off
            // `_manifests`), confirming main's `-l` would never have visited it.
            let (catalog, _) = metadata_store.list_namespaces(1000, None).await.unwrap();
            assert!(
                !catalog.contains(&link_only.to_string()),
                "the planted namespace must be link-only (absent from the catalog)"
            );
        }

        let config = test_config(&path);

        // Only `--links`, mutate (not dry-run). The single enabled node is
        // `metadata`; running the DAG exercises the real metadata node body over
        // `list_all_namespaces`, which now reaches the link-only namespace.
        let options = Options {
            dry_run: false,
            links: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        // Re-open the store and assert the orphan layer link was reclaimed: its
        // only referrer was a phantom, so pruning it left no referrers and the
        // executor cascade-deleted the link.
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);
        let mut layers =
            crate::command::scrub::check::list_all::layer_links(&metadata_store, &link_only);
        assert!(
            layers.next().await.is_none(),
            "-l must reclaim the orphan layer link in a link-only namespace"
        );
    }

    /// Under `--prune-unknown` but dry-run, the invalid-named namespace must
    /// survive: `-d` previews, never mutates.
    #[tokio::test]
    async fn metadata_node_dry_run_keeps_invalid_namespace_under_prune_unknown() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        let options = Options {
            dry_run: true,
            manifests: true,
            prune_unknown: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            std::path::Path::new(&ns_dir).exists(),
            "dry-run must not delete the invalid-named namespace even under --prune-unknown"
        );
    }
}
