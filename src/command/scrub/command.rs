use std::sync::Arc;

use argh::FromArgs;
use tracing::warn;

use crate::{
    command::{
        maintenance::{self, Stores},
        scrub::{
            context::Ctx,
            error::Error,
            node::{self, NodeParts},
            report::{self, Findings},
            scheduler, setup,
        },
    },
    configuration::Configuration,
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
    /// deprecated: backfill missing `media_type` on legacy manifest links
    /// written before `media_type` was recorded at push time; `-M` will be
    /// removed in a future release
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
    /// only report (invalid-named namespaces and unknown job-queue directories).
    /// Off by default so no existing flag ever deletes more than before. Run
    /// with -d first
    pub prune_unknown: bool,
}

impl Options {
    /// Build a scrub `Options` with every gate off except `retention` /
    /// `replicate` and `dry_run`, so `policy` (retention) and `replication`
    /// (replicate) reuse the unchanged `setup::*` builders. `media_types` stays
    /// false on purpose: backfill is the deprecated standalone `scrub -M` only.
    pub fn with_gates(dry_run: bool, retention: bool, replicate: bool) -> Self {
        Self {
            dry_run,
            retention,
            replicate,
            ..Default::default()
        }
    }
}

/// The deprecated `scrub` operation flags passed to this run, each paired with
/// the dedicated subcommand that now replaces it, in retention then replicate
/// order. One list so the flag and its replacement have a single source of
/// truth. Returned rather than logged inline so the deprecation logic is
/// unit-testable. `--media-types` is absent: it is link-metadata repair, warned
/// separately by [`warn_deprecated_media_types_flag`].
pub fn deprecated_policy_flags(options: &Options) -> Vec<(&'static str, &'static str)> {
    let mut flags = Vec::new();
    if options.retention {
        flags.push(("--retention", "angos policy --retention"));
    }
    if options.replicate {
        flags.push(("--replicate", "angos replication"));
    }
    flags
}

/// Warn once for each deprecated operation flag still passed to `scrub`. The
/// flags keep working unchanged; the warning points at the dedicated subcommand.
fn warn_deprecated_policy_flags(options: &Options) {
    for (flag, replacement) in deprecated_policy_flags(options) {
        warn!(
            "'angos scrub {flag}' is deprecated and will be removed in a future release; \
             use '{replacement}' instead. It still runs unchanged for now."
        );
    }
}

/// Warn once for the standalone `--media-types`/`-M` flag. Manifests now record
/// their `media_type` at push time, so only legacy links written before that
/// need backfill; `-M` keeps backfilling unchanged but is slated for removal.
fn warn_deprecated_media_types_flag(options: &Options) {
    if options.media_types {
        warn!(
            "'angos scrub --media-types' (-M) is deprecated and will be removed in a future \
             release; manifests now record their media_type at push time, so only links written \
             before that need backfill. It still backfills unchanged for now."
        );
    }
}

pub struct Command {
    /// Shared context (stores, sink, flags) threaded into every node.
    ctx: Arc<Ctx>,
    /// The pre-built checkers and drain. Constructed in `new`, consumed by
    /// `build_nodes` in `run` (hence `Option` + `take`).
    parts: Option<NodeParts>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        warn_deprecated_policy_flags(options);
        warn_deprecated_media_types_flag(options);
        let Stores {
            blob_backend,
            metadata_store,
            repositories,
        } = maintenance::bootstrap_stores(config).await?;

        // The shared report-only findings accumulator. Built BEFORE the checker
        // builders so the same `Findings` (an `Arc` inside) is threaded into the
        // `ManifestChecker` and moved into `Ctx`; both point at one accumulator.
        let findings = Findings::default();

        // Reuse the existing checker builders verbatim; the only orchestration
        // change is that the store-wide checkers are now distinct nodes (their
        // order is encoded as DAG deps) instead of one labeled vec.
        let namespace_checkers = setup::namespace_checkers(
            options,
            config,
            &blob_backend,
            &metadata_store,
            &repositories,
            &findings,
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

        let (inner, replication_drain) = maintenance::build_sink_and_drain(
            &blob_backend,
            &metadata_store,
            &repositories,
            options.dry_run,
            "scrub",
            options.replicate,
            config,
        );

        let ctx = maintenance::build_ctx(metadata_store, inner, options, config, findings, true);

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
            .ok_or_else(|| Error::Initialization("run called more than once".into()))?;
        let nodes = node::build_nodes(&self.ctx, parts);
        scheduler::run_dag(nodes).await;
        self.ctx.metadata_store.flush_access_times().await;
        let findings = self.ctx.findings.snapshot().await;
        report::log_summary(
            "scrub",
            self.ctx.opts.dry_run,
            self.ctx.prune_unknown_supported,
            &self.ctx.tally,
            &findings,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use wiremock::MockServer;

    use super::*;
    use crate::command::scrub::test_support::{
        LogCapture, config_with_replication, minimal_fs_config, mount_out_of_sync_downstream,
    };

    /// A minimal valid fs-backed configuration with an empty retention policy.
    fn test_config(path: &str) -> Configuration {
        minimal_fs_config(path, "")
    }

    /// All-false `Options` so each test toggles only the flags it cares about.
    fn base_options() -> Options {
        Options::default()
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

    /// `scrub --links` must NOT backfill media types: media-type backfill is
    /// dissociated from `-l`, so `setup::namespace_checkers` builds the
    /// `MediaTypeChecker` only under `-M`, never under `-l`. `media_type` is now
    /// recorded at manifest-push time, so `-l` stays a cheap link-format/referrer
    /// repair. A manifest revision link missing `media_type` must STILL be missing
    /// after a `-l` (mutate) run, pinning that the `MediaTypeChecker` does not
    /// ride `--links`.
    #[tokio::test]
    async fn links_flag_does_not_backfill_media_type() {
        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
        use tempfile::TempDir;

        use crate::{
            oci::Namespace,
            registry::{
                metadata_store::{LinkKind, LinkOperation},
                test_utils,
            },
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let media_type = "application/vnd.oci.image.manifest.v1+json";

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // A self-referential manifest revision link whose blob carries a
        // mediaType but whose link metadata is missing it.
        let manifest_content = format!(
            r#"{{"schemaVersion":2,"mediaType":"{media_type}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[]}}"#
        );
        let manifest_digest =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Digest(manifest_digest.clone()),
                    manifest_digest.clone(),
                )],
            )
            .await
            .unwrap();

        let digest_link = metadata_store
            .read_link(&namespace, &LinkKind::Digest(manifest_digest.clone()))
            .await
            .unwrap();
        assert!(
            digest_link.media_type.is_none(),
            "precondition: revision link has no media_type"
        );

        let config = test_config(&path);

        // Only `--links`, mutate (not dry-run). No `--media-types`.
        let options = Options {
            dry_run: false,
            links: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        let digest_link = metadata_store
            .read_link(&namespace, &LinkKind::Digest(manifest_digest.clone()))
            .await
            .unwrap();
        assert!(
            digest_link.media_type.is_none(),
            "scrub -l must NOT backfill media_type (MediaTypeChecker does not ride --links)"
        );
    }

    /// `scrub --media-types` still backfills standalone: the `-M` flag is now
    /// DEPRECATED (it backfills only legacy links written before `media_type` was
    /// recorded at push time, and emits a one-time deprecation warning) but keeps
    /// working unchanged for now. A revision link missing `media_type` must still
    /// get it set by a `-M` (mutate) run.
    #[tokio::test]
    async fn media_types_flag_still_backfills() {
        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
        use tempfile::TempDir;

        use crate::{
            oci::Namespace,
            registry::{
                metadata_store::{LinkKind, LinkOperation},
                test_utils,
            },
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let media_type = "application/vnd.oci.image.manifest.v1+json";

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        let manifest_content = format!(
            r#"{{"schemaVersion":2,"mediaType":"{media_type}","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[]}}"#
        );
        let manifest_digest =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Digest(manifest_digest.clone()),
                    manifest_digest.clone(),
                )],
            )
            .await
            .unwrap();

        let config = test_config(&path);

        // Only `--media-types`, mutate (not dry-run). No `--links`.
        let options = Options {
            dry_run: false,
            media_types: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        let digest_link = metadata_store
            .read_link(&namespace, &LinkKind::Digest(manifest_digest.clone()))
            .await
            .unwrap();
        assert_eq!(
            digest_link.media_type.as_deref(),
            Some(media_type),
            "scrub -M must still backfill media_type standalone"
        );
    }

    /// `-M` carries a media-type deprecation warning; the retention/replicate
    /// policy flags do not (they have their own `angos policy` warning), and the
    /// structural/link-repair flags carry no media-type deprecation at all. The
    /// boolean gate `warn_deprecated_media_types_flag` reads is the unit-testable
    /// stand-in for "warns once" (no log-capture harness in-tree); the warning
    /// fires exactly when `options.media_types` is set.
    #[test]
    fn scrub_warns_on_deprecated_media_types_flag() {
        // `-M` set => media-type deprecation applies.
        let with_m = Options {
            media_types: true,
            ..base_options()
        };
        // It does not double-count as a policy deprecation.
        assert!(deprecated_policy_flags(&with_m).is_empty());

        // Policy and structural flags do not trigger the media-type deprecation.
        let without_m = Options {
            retention: true,
            replicate: true,
            links: true,
            tags: true,
            ..base_options()
        };
        assert!(
            !without_m.media_types,
            "the -M deprecation must not fire for policy/structural flags (including -l alone)"
        );
    }

    /// Guards the warn wiring: `warn_deprecated_media_types_flag` and
    /// `warn_deprecated_policy_flags` are otherwise un-invoked by any test, so a
    /// regression dropping the `warn!` would ship silently. Under a capturing
    /// subscriber we call them directly and assert on stable substrings of the
    /// emitted text (not the whole message, to stay non-brittle).
    #[test]
    fn deprecated_flags_emit_expected_warnings() {
        // `-M` (media-types) deprecation: the message names the flag and its
        // short form, says deprecated, and steers operators by mentioning that
        // media_type is now recorded at push time. It must NOT steer to `-l`.
        let media_capture = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(media_capture.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                let options = Options {
                    media_types: true,
                    ..base_options()
                };
                warn_deprecated_media_types_flag(&options);
            });
        }
        let media_text = media_capture.contents();
        assert!(
            media_text.contains("--media-types"),
            "media-type deprecation must name the flag: {media_text}"
        );
        assert!(
            media_text.contains("(-M)"),
            "media-type deprecation must name the short form: {media_text}"
        );
        assert!(
            media_text.contains("deprecated"),
            "media-type deprecation must say deprecated: {media_text}"
        );
        assert!(
            media_text.contains("push time"),
            "media-type deprecation must explain media_type is now recorded at push time: {media_text}"
        );
        assert!(
            !media_text.contains("--links") && !media_text.contains("-l"),
            "media-type deprecation must NOT steer to -l/--links: {media_text}"
        );

        // Policy deprecation: `--retention` and `--replicate` together must each
        // point at their dedicated subcommand (`angos policy --retention` and
        // `angos replication`).
        let policy_capture = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(policy_capture.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                let options = Options {
                    retention: true,
                    replicate: true,
                    ..base_options()
                };
                warn_deprecated_policy_flags(&options);
            });
        }
        let policy_text = policy_capture.contents();
        assert!(
            policy_text.contains("angos policy --retention"),
            "retention deprecation must point at `angos policy --retention`: {policy_text}"
        );
        assert!(
            policy_text.contains("angos replication"),
            "replicate deprecation must point at `angos replication`: {policy_text}"
        );

        // Structural / link-repair flags trigger NO deprecation from either warn
        // function: the capture must stay empty of any deprecation text.
        let quiet_capture = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(quiet_capture.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                let options = Options {
                    links: true,
                    tags: true,
                    ..base_options()
                };
                warn_deprecated_media_types_flag(&options);
                warn_deprecated_policy_flags(&options);
            });
        }
        let quiet_text = quiet_capture.contents();
        assert!(
            !quiet_text.contains("deprecated"),
            "structural/link flags must emit no deprecation warning: {quiet_text}"
        );
    }

    /// The deprecated `--retention` flag still runs through scrub `Command::run`:
    /// a tag dropped by the global retention policy is deleted, exactly as it was
    /// before the flag moved to `angos policy`. Guards the "deprecated alias still
    /// executes the same checker" half of the compat contract.
    #[tokio::test]
    async fn scrub_retention_still_runs_under_deprecated_flag() {
        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
        use tempfile::TempDir;

        use crate::{
            oci::{Namespace, Tag},
            registry::{
                metadata_store::{LinkKind, LinkOperation},
                test_utils,
            },
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        let manifest = test_utils::put_blob_direct(metadata_store.store(), b"manifest-bytes").await;
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // A rule retaining only "keep-me": the tag "v0.0.1" must be deleted.
        let config = minimal_fs_config(&path, "\"image.tag == 'keep-me'\"");

        let options = Options {
            dry_run: false,
            retention: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_err(),
            "deprecated scrub --retention must still delete the tag dropped by the policy"
        );
    }

    /// The `--replicate` twin of `scrub_retention_still_runs_under_deprecated_flag`:
    /// the deprecated `scrub --replicate` alias still reconciles. Driven through
    /// scrub `Command::run`, the `replicate` node enqueues a push for the tag the
    /// downstream is missing and drains it in-process, so the wiremock downstream
    /// receives the tagged-manifest PUT. Proves the deprecated flag still drives
    /// the enqueue + drain end-to-end (not just that the flag parses).
    #[tokio::test]
    async fn scrub_replicate_still_runs_under_deprecated_flag() {
        use std::sync::atomic::Ordering;

        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
        use tempfile::TempDir;

        use crate::{metrics_provider, oci::Namespace, registry::test_utils};

        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        let (manifest_digest, config_digest, layer_digest) =
            test_utils::seed_manifest(metadata_store.store(), &metadata_store, &namespace).await;

        mount_out_of_sync_downstream(
            &mock_server,
            &manifest_digest,
            &config_digest,
            &layer_digest,
        )
        .await;

        // A config whose `nginx` repository event+reconcile-replicates to the
        // wiremock downstream.
        let config = config_with_replication(&path, &mock_server.uri(), false);

        let mut cmd = Command::new(
            &Options {
                dry_run: false,
                replicate: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();
        cmd.run().await.unwrap();

        // The fixture plants exactly one missing-downstream tag, so exactly one
        // push is enqueued (and drained, verified by the PUT `.expect(1)` below).
        assert_eq!(
            cmd.ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "deprecated scrub --replicate must still enqueue the missing-downstream push"
        );

        // Drop explicitly so the wiremock `.expect(...)` counts (notably the
        // single tagged-manifest PUT) are verified here, proving the scrub
        // `replicate` node drove the enqueue + in-process drain.
        drop(mock_server);
    }

    /// The deprecation proxy: `deprecated_policy_flags` returns the set policy
    /// flags in retention → replicate order. This is the unit-testable stand-in
    /// for "warns once per deprecated flag" (no log-capture harness in-tree);
    /// `warn_deprecated_policy_flags` iterates exactly this vec. `--media-types`
    /// is link-metadata backfill, not a policy operation: it is deprecated on its
    /// own terms (the standalone `-M`, warned by `warn_deprecated_media_types_flag`)
    /// and so must NOT appear among the *policy* deprecation flags.
    #[test]
    fn scrub_warns_on_deprecated_policy_flags() {
        let all = Options {
            retention: true,
            replicate: true,
            ..base_options()
        };
        // Each deprecated flag is paired with the subcommand it steers operators
        // at: retention enforcement is `angos policy`, replication reconcile is a
        // sync operation living in `angos replication` (NOT `angos policy`).
        assert_eq!(
            deprecated_policy_flags(&all),
            vec![
                ("--retention", "angos policy --retention"),
                ("--replicate", "angos replication"),
            ]
        );

        // Structural / link-repair flags must not be flagged as *policy*
        // deprecations: `--media-types` is link-metadata backfill (deprecated
        // separately as the standalone `-M`), not a policy operation.
        let structural = Options {
            blobs: true,
            tags: true,
            manifests: true,
            links: true,
            media_types: true,
            ..base_options()
        };
        assert!(
            deprecated_policy_flags(&structural).is_empty(),
            "structural and media-type backfill flags must not trigger a policy deprecation warning"
        );
    }

    /// The config-drift cleaners and structural flags must never be treated as
    /// deprecated policy flags. Guards against accidentally deprecating
    /// `--orphan-namespaces` / `--replication-orphans` / `--cache-orphans` /
    /// `--orphan-grants` / `--reconcile-blob-index` along with the policy flags.
    #[test]
    fn scrub_orphan_flags_do_not_emit_policy_deprecation() {
        let options = Options {
            orphan_namespaces: true,
            replication_orphans: true,
            cache_orphans: true,
            orphan_grants: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            reconcile_blob_index: true,
            ..base_options()
        };
        assert!(
            deprecated_policy_flags(&options).is_empty(),
            "config-drift cleaners must stay on scrub with no deprecation warning"
        );
    }

    /// A mutate run that deletes an invalid tag directory increments the
    /// summary tally (`tags`, `total`). Reuses the
    /// `metadata_node_deletes_invalid_tag_directory` fixture. The tally is read
    /// post-`run()` via `cmd.ctx`, matching the established convention of these
    /// tests reaching into `cmd.ctx`.
    #[tokio::test]
    async fn summary_tally_counts_invalid_tag_deletion_mutate() {
        use std::sync::atomic::Ordering;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let tag_dir = format!("{path}/v2/repositories/test-repo/app/_manifests/tags/-bad");
        std::fs::create_dir_all(format!("{tag_dir}/current")).unwrap();
        std::fs::write(
            format!("{tag_dir}/current/link"),
            b"sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let config = test_config(&path);
        let options = Options {
            dry_run: false,
            tags: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert_eq!(
            cmd.ctx.tally.tags.load(Ordering::Relaxed),
            1,
            "the deleted invalid tag must be counted in the summary tally"
        );
        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action was applied"
        );
    }

    /// An invalid-named namespace skipped without `--prune-unknown` is
    /// counted as a report-only finding, and no namespace-prune action is
    /// emitted. Reuses `plant_invalid_namespace`.
    #[tokio::test]
    async fn summary_findings_count_skipped_invalid_namespace() {
        use std::sync::atomic::Ordering;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        let options = Options {
            dry_run: false,
            manifests: true,
            prune_unknown: false,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        // Behavior unchanged: the namespace survives.
        assert!(
            std::path::Path::new(&ns_dir).exists(),
            "without --prune-unknown the invalid namespace must survive"
        );
        // Counted as a report-only finding, never as an applied action.
        let findings = cmd.ctx.findings.snapshot().await;
        assert_eq!(
            findings.skipped_invalid_namespaces, 1,
            "the skipped invalid namespace must be a report-only finding"
        );
        assert_eq!(
            cmd.ctx.tally.namespaces_pruned.load(Ordering::Relaxed),
            0,
            "report-only findings must emit no namespace-prune action"
        );
    }

    /// A manifest whose layer bytes are gone is a
    /// report-only finding, surfaced in the summary without any delete. Reuses
    /// the `manifest_checker_reports_dangling_layer_without_deleting` shape,
    /// driven end-to-end through `scrub -m` so the `Findings` wiring is exercised.
    #[tokio::test]
    async fn summary_findings_count_dangling_layer() {
        use std::sync::atomic::Ordering;

        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
        use tempfile::TempDir;

        use crate::{
            oci::Namespace,
            registry::{
                blob_store::{self, BlobStore, BlobStoreConfig},
                metadata_store::{LinkKind, LinkOperation},
                test_utils,
            },
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/app").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // A BlobStore over the same fs root, used only to delete the layer bytes.
        let blob_store: Arc<BlobStore> = Arc::new(
            BlobStoreConfig::FS(blob_store::FsBackendConfig {
                root_dir: path.clone(),
                sync_to_disk: false,
            })
            .build_backend()
            .unwrap(),
        );

        // A self-referential image manifest whose single layer's bytes are then
        // deleted, leaving every forward link intact (so the only fault is the
        // dangling layer reference).
        let layer = test_utils::put_blob_direct(metadata_store.store(), b"layer bytes").await;
        let manifest_content = format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{layer}","size":11}}]}}"#
        );
        let manifest_digest =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
        metadata_store
            .update_links(
                &namespace,
                &[
                    LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    ),
                    LinkOperation::create(LinkKind::Layer(layer.clone()), layer.clone()),
                ],
            )
            .await
            .unwrap();
        blob_store.delete_blob(&layer).await.unwrap();

        let config = test_config(&path);
        let options = Options {
            dry_run: false,
            manifests: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        let findings = cmd.ctx.findings.snapshot().await;
        assert_eq!(
            findings.dangling_layer, 1,
            "the missing layer must be a report-only dangling-layer finding"
        );
        assert_eq!(
            cmd.ctx.tally.orphan_manifests.load(Ordering::Relaxed),
            0,
            "a dangling layer is report-only and must never delete the manifest"
        );
    }

    /// Dry-run "would" counts equal mutate "done" counts; only the side
    /// effect differs. One invalid tag dir is the single categorized action.
    #[tokio::test]
    async fn summary_dry_run_would_counts_match_mutate_done_counts() {
        use std::sync::atomic::Ordering;

        use tempfile::TempDir;

        // Plant the same invalid tag dir twice, run once dry, once mutate.
        let plant = |path: &str| {
            let tag_dir = format!("{path}/v2/repositories/test-repo/app/_manifests/tags/-bad");
            std::fs::create_dir_all(format!("{tag_dir}/current")).unwrap();
            std::fs::write(
                format!("{tag_dir}/current/link"),
                b"sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();
            tag_dir
        };

        // Dry-run: counts the would-delete, leaves the directory.
        let dry_dir = TempDir::new().unwrap();
        let dry_path = dry_dir.path().to_string_lossy().to_string();
        let dry_tag = plant(&dry_path);
        let mut dry_cmd = Command::new(
            &Options {
                dry_run: true,
                tags: true,
                ..base_options()
            },
            &test_config(&dry_path),
        )
        .await
        .unwrap();
        dry_cmd.run().await.unwrap();

        // Mutate: counts the done-delete, removes the directory.
        let wet_dir = TempDir::new().unwrap();
        let wet_path = wet_dir.path().to_string_lossy().to_string();
        let wet_tag = plant(&wet_path);
        let mut wet_cmd = Command::new(
            &Options {
                dry_run: false,
                tags: true,
                ..base_options()
            },
            &test_config(&wet_path),
        )
        .await
        .unwrap();
        wet_cmd.run().await.unwrap();

        // Identical counts in both modes.
        assert_eq!(dry_cmd.ctx.tally.tags.load(Ordering::Relaxed), 1);
        assert_eq!(wet_cmd.ctx.tally.tags.load(Ordering::Relaxed), 1);
        assert_eq!(dry_cmd.ctx.tally.total.load(Ordering::Relaxed), 1);
        assert_eq!(wet_cmd.ctx.tally.total.load(Ordering::Relaxed), 1);

        // Only the side effect differs.
        assert!(
            std::path::Path::new(&dry_tag).exists(),
            "dry-run must leave the invalid tag directory in place"
        );
        assert!(
            !std::path::Path::new(&wet_tag).exists(),
            "mutate must delete the invalid tag directory"
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

    // ------------------------------------------------------------------
    // Command-level DAG behavior (bucket A): the flag -> node-set -> edge
    // wiring. The scheduler's own ordering/overlap/disabled-edge-drop
    // mechanics are unit-tested in `scheduler.rs`; these tests pin the
    // *command* wiring (which flags build which nodes, with which deps) and,
    // where it is tractable, run the real DAG end-to-end to prove the wiring
    // does not strand a node.
    // ------------------------------------------------------------------

    /// Plants an orphan blob (no manifest references it) over the FS root the
    /// command will read, and returns its digest. Reuses the same
    /// `put_blob_direct` fixture the `BlobChecker` orphan tests use: a blob
    /// with no recorded index references classifies as an orphan and is
    /// reclaimed by `-b`.
    async fn plant_orphan_blob(path: &str, content: &[u8]) -> crate::oci::Digest {
        use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

        use crate::registry::test_utils;

        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(path).build());
        let executor = test_utils::build_test_fs_executor(path, false);
        let metadata_store = test_utils::metadata_store_over(object, executor);
        test_utils::put_blob_direct(metadata_store.store(), content).await
    }

    /// Re-opens a `BlobStore` over the FS root so a test can assert whether a
    /// planted blob survived a run.
    fn reopen_blob_store(path: &str) -> Arc<crate::registry::blob_store::BlobStore> {
        use crate::registry::blob_store::{self, BlobStoreConfig};

        Arc::new(
            BlobStoreConfig::FS(blob_store::FsBackendConfig {
                root_dir: path.to_string(),
                sync_to_disk: false,
            })
            .build_backend()
            .unwrap(),
        )
    }

    /// With `-m -b` the `blob` node declares the `metadata` barrier AND the
    /// `metadata` node is actually present, so the edge is a real dependency
    /// (not dropped). Pins "the metadata -> blob barrier is honored when both
    /// nodes exist".
    #[tokio::test]
    async fn manifests_and_blobs_keep_metadata_to_blob_barrier() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            manifests: true,
            blobs: true,
            ..base_options()
        };

        let nodes = node_map(&options, &config).await;

        // Exactly the two nodes; nothing else is enabled.
        let mut ids: Vec<&str> = nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["blob", "metadata"]);

        // `blob` declares the full barrier; `metadata` is present, so that edge
        // is a live dependency (the absent orphan-namespaces/migrate edges drop
        // in the scheduler).
        assert_eq!(
            nodes.get("blob").unwrap(),
            &vec!["metadata", "orphan-namespaces", "migrate"]
        );
        assert!(
            nodes.contains_key("metadata"),
            "the metadata dep must be present so the barrier is a real edge, not a dropped one"
        );
    }

    /// With `-b` only, the node set is exactly `["blob"]` and every one of
    /// `blob`'s declared deps is absent. Running the real DAG over a planted
    /// orphan blob proves the dropped edges do not strand the node: it still
    /// runs and reclaims the orphan.
    #[tokio::test]
    async fn blobs_only_drops_absent_deps_and_still_reclaims_orphan() {
        use std::sync::atomic::Ordering;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let orphan = plant_orphan_blob(&path, b"orphan reclaimed under -b alone").await;
        let config = test_config(&path);

        // Node-set shape: only `blob`, and all of its declared deps are absent.
        let options = Options {
            dry_run: false,
            blobs: true,
            ..base_options()
        };
        let nodes = node_map(&options, &config).await;
        let ids: Vec<&str> = nodes.keys().copied().collect();
        assert_eq!(ids, vec!["blob"], "only --blobs builds only the blob node");
        let blob_deps = nodes.get("blob").unwrap();
        assert_eq!(blob_deps, &vec!["metadata", "orphan-namespaces", "migrate"]);
        for dep in blob_deps {
            assert!(
                !nodes.contains_key(dep),
                "{dep} must be absent so its edge to `blob` is dropped"
            );
        }

        // End-to-end: the orphan is reclaimed despite the dropped edges.
        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            reopen_blob_store(&path).read(&orphan).await.is_err(),
            "--blobs must reclaim the orphan even with every declared dep absent"
        );
        assert_eq!(
            cmd.ctx.tally.orphan_blobs.load(Ordering::Relaxed),
            1,
            "the reclaimed orphan blob must be counted in the summary tally"
        );
    }

    /// A pathologically large `max_concurrent_scrub_tasks` clamps to `MAX_FANOUT`;
    /// a sane value passes through. Pins the fan-out ceiling so a future bump
    /// cannot silently uncap the per-namespace enumeration.
    #[tokio::test]
    async fn fanout_is_clamped_to_max_fanout() {
        use std::num::NonZeroUsize;

        use tempfile::TempDir;

        use crate::command::scrub::context::MAX_FANOUT;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let mut config = test_config(&path);

        config.global.max_concurrent_scrub_tasks = NonZeroUsize::new(MAX_FANOUT + 10_000).unwrap();
        let cmd = Command::new(&base_options(), &config).await.unwrap();
        assert_eq!(
            cmd.ctx.opts.fanout, MAX_FANOUT,
            "a value above the ceiling must clamp to MAX_FANOUT"
        );

        config.global.max_concurrent_scrub_tasks = NonZeroUsize::new(32).unwrap();
        let cmd = Command::new(&base_options(), &config).await.unwrap();
        assert_eq!(
            cmd.ctx.opts.fanout, 32,
            "a value below the ceiling must pass through unclamped"
        );
    }

    /// The `replicate` node declares `blob` and `orphan-grants` deps (besides
    /// `orphan-jobs` and `metadata`) so the in-process drain runs strictly after
    /// any blob reclaim, matching main's ordering. The scheduler drops the edges
    /// to absent nodes, so this pins only the declared deps.
    #[tokio::test]
    async fn replicate_node_depends_on_blob_reclaim() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            replicate: true,
            ..base_options()
        };
        let nodes = node_map(&options, &config).await;
        assert_eq!(
            nodes.get("replicate").unwrap(),
            &vec!["orphan-jobs", "metadata", "blob", "orphan-grants"],
            "replicate must declare the blob-reclaim ordering deps"
        );
    }

    /// `--jobs` + `-p` (multipart) + `-m` build three dep-free root nodes.
    /// `jobs_flag_builds_independent_jobs_node` covers `jobs` alone; this pins
    /// the multi-flag case where all three are independent roots that can run in
    /// parallel.
    #[tokio::test]
    async fn jobs_multipart_and_metadata_are_independent_roots() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            jobs: true,
            multipart: Some(humantime::Duration::from(std::time::Duration::from_hours(
                1,
            ))),
            manifests: true,
            ..base_options()
        };

        let nodes = node_map(&options, &config).await;

        let mut ids: Vec<&str> = nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["jobs", "metadata", "multipart"]);
        // All three are dep-free roots: none gates or waits on another.
        assert_eq!(nodes.get("jobs").unwrap(), &Vec::<&str>::new());
        assert_eq!(nodes.get("multipart").unwrap(), &Vec::<&str>::new());
        assert_eq!(nodes.get("metadata").unwrap(), &Vec::<&str>::new());
    }

    /// A mutate run with `-m -b` over a planted orphan blob reclaims it. With
    /// both nodes present the
    /// `metadata -> blob` edge is a live dependency, so the blob GC runs only
    /// after the metadata walk has been persisted; the orphan (which no
    /// surviving link references) is then classified orphan and deleted. This
    /// exercises the barrier under the real scheduler, not just `node_map`.
    #[tokio::test]
    async fn manifests_then_blobs_reclaims_orphan_under_real_scheduler() {
        use std::sync::atomic::Ordering;

        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let orphan = plant_orphan_blob(&path, b"orphan reclaimed after metadata barrier").await;
        let config = test_config(&path);

        // Both the metadata and blob nodes are enabled, so the barrier edge is
        // real; the scheduler must run `blob` strictly after `metadata`.
        let options = Options {
            dry_run: false,
            manifests: true,
            blobs: true,
            ..base_options()
        };

        // Confirm the barrier is a live edge (both nodes present) before running.
        let nodes = node_map(&options, &config).await;
        assert!(nodes.contains_key("metadata") && nodes.contains_key("blob"));
        assert!(
            nodes.get("blob").unwrap().contains(&"metadata"),
            "blob must depend on metadata so GC reads the persisted index"
        );

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            reopen_blob_store(&path).read(&orphan).await.is_err(),
            "the orphan blob must be reclaimed once the metadata barrier has run"
        );
        assert_eq!(
            cmd.ctx.tally.orphan_blobs.load(Ordering::Relaxed),
            1,
            "the reclaimed orphan must be counted once"
        );
    }
}
