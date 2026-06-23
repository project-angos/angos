//! `angos replication`: drive the replication reconcile through the same `Ctx` +
//! scheduler + `metadata_node` + `ReplicationDrain` that `scrub` uses.
//!
//! `Command::new` bootstraps the stores (exactly like `scrub::Command::new`),
//! reuses the unchanged `scrub::setup::namespace_checkers` to build the
//! `ReplicationChecker` (by synthesizing a scrub `Options` with only the
//! replicate gate on), and assembles a 1-2 node DAG by hand: a `metadata` node
//! riding the reused `scrub::node::metadata_node`, and (only in mutate mode)
//! a `replicate` node riding the reused `ReplicationDrain::drain`.
//!
//! Replication reconcile is a sync operation, not policy enforcement, so it has
//! its own subcommand rather than living under `angos policy`.

use std::sync::Arc;

use argh::FromArgs;

use crate::{
    command::{
        maintenance::{self, Prepared},
        scrub::{
            self,
            context::Ctx,
            node, report,
            scheduler::{self, Node},
        },
    },
    configuration::Configuration,
};

use super::Error;

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "replication",
    description = "Reconcile replicated namespaces with their downstreams (sync)"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
}

pub struct Command {
    /// Shared context (stores, sink, flags) threaded into every node.
    ctx: Arc<Ctx>,
    /// `ReplicationChecker` built once in `new`, boxed by the reused
    /// `scrub::setup::namespace_checkers`. `Option` so `build_nodes` can `take`
    /// it; `None` after the run.
    namespace_checkers: Option<Vec<Box<dyn scrub::check::NamespaceChecker>>>,
    /// The in-process replication drain, wired only in mutate mode (`None`
    /// under `-d`). Taken by `build_nodes`.
    replication_drain: Option<maintenance::ReplicationDrain>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        // Reuse the scrub builder via a synthesized `Options` with only the
        // replicate gate on (retention is `angos policy`). `with_drain` wires the
        // in-process drain over the same queue the Executor enqueues onto.
        let scrub_opts = scrub::Options::with_gates(options.dry_run, false, true);
        let Prepared {
            ctx,
            namespace_checkers,
            replication_drain,
        } = maintenance::prepare(config, &scrub_opts, "replication", true).await?;

        Ok(Self {
            ctx,
            namespace_checkers: Some(namespace_checkers),
            replication_drain,
        })
    }

    /// Assemble the reconcile node set and run it as a DAG. The metadata node
    /// drives the `ReplicationChecker` over `list_all_namespaces` (reused
    /// `scrub::node::metadata_node`); the optional replicate node drains the
    /// reconcile-enqueued pushes after the metadata walk (reused
    /// `ReplicationDrain::drain`). Access times are flushed after the run,
    /// exactly as `scrub::Command::run` does.
    pub async fn run(&mut self) -> Result<(), Error> {
        let nodes = self.build_nodes()?;
        scheduler::run_dag(nodes).await;
        self.ctx.metadata_store.flush_access_times().await;
        let findings = self.ctx.findings.snapshot().await;
        report::log_summary(
            "replication",
            self.ctx.opts.dry_run,
            self.ctx.prune_unknown_supported,
            &self.ctx.tally,
            &findings,
        );
        Ok(())
    }

    /// Build the 1-2 reconcile nodes from the pre-built checker/drain. Returns an
    /// error if the checkers were already taken (i.e. `run` was called more than
    /// once).
    ///
    /// - `metadata` (deps `[]`): present whenever the checker exists.
    /// - `replicate` (deps `["metadata"]`): present only in mutate mode (a drain
    ///   was wired), so the drain runs after the enqueue walk, the same edge
    ///   scrub's `replicate` node carries.
    fn build_nodes(&mut self) -> Result<Vec<Node>, Error> {
        let namespace_checkers = Arc::new(
            self.namespace_checkers
                .take()
                .ok_or_else(|| Error::Initialization("run called more than once".into()))?,
        );
        let replication_drain = self.replication_drain.take();

        let mut nodes: Vec<Node> = Vec::new();

        if !namespace_checkers.is_empty() {
            let ctx = self.ctx.clone();
            let checkers = namespace_checkers.clone();
            nodes.push(Node {
                id: "metadata",
                deps: &[],
                run: Box::new(move || Box::pin(node::metadata_node(ctx, checkers, Arc::new(None)))),
            });
        }

        if let Some(drain) = replication_drain {
            nodes.push(Node {
                id: "replicate",
                deps: &["metadata"],
                run: Box::new(move || Box::pin(drain.drain())),
            });
        }

        Ok(nodes)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::atomic::Ordering};

    use tempfile::TempDir;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use super::*;
    use crate::{
        command::scrub::test_support::{config_with_replication, mount_out_of_sync_downstream},
        metrics_provider,
        oci::Namespace,
        registry::test_utils,
    };

    /// A minimal valid fs-backed configuration with no replication configured.
    fn test_config(path: &str) -> Configuration {
        scrub::test_support::minimal_fs_config(path, "")
    }

    /// Build the command and project its node set to an id -> deps map so tests
    /// can assert the DAG shape without depending on node internals.
    async fn node_map(
        options: &Options,
        config: &Configuration,
    ) -> HashMap<&'static str, Vec<&'static str>> {
        let mut cmd = Command::new(options, config).await.unwrap();
        cmd.build_nodes()
            .unwrap()
            .into_iter()
            .map(|n| (n.id, n.deps.to_vec()))
            .collect()
    }

    fn opts(dry_run: bool) -> Options {
        Options { dry_run }
    }

    // ------------------------------------------------------------------
    // (0) CLI surface: `angos replication` accepts only -d
    // ------------------------------------------------------------------

    /// `angos replication` runs the reconcile by default (mutate); the only flag
    /// is `-d`/`--dry-run`. Unknown switches are rejected at parse time.
    #[test]
    fn replication_accepts_only_dry_run() {
        use argh::FromArgs;

        assert_eq!(
            Options::from_args(&["replication"], &[]).unwrap(),
            opts(false),
        );
        assert_eq!(
            Options::from_args(&["replication"], &["-d"]).unwrap(),
            opts(true),
        );
        assert!(
            Options::from_args(&["replication"], &["--replicate"]).is_err(),
            "angos replication takes no --replicate switch; reconcile is the default action"
        );
        assert!(
            Options::from_args(&["replication"], &["-r"]).is_err(),
            "angos replication is not a policy subcommand and takes no -r"
        );
    }

    // ------------------------------------------------------------------
    // (1) Node-set shape
    // ------------------------------------------------------------------

    /// Mutate (default): the drain is wired, so the `replicate` node exists and
    /// depends on the metadata enqueue walk.
    #[tokio::test]
    async fn mutate_builds_metadata_then_replicate() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let nodes = node_map(&opts(false), &config).await;

        let mut ids: Vec<&str> = nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["metadata", "replicate"]);
        assert_eq!(nodes.get("replicate").unwrap(), &vec!["metadata"]);
    }

    /// `-d`: the drain only exists in mutate mode (it consumes the durable queue
    /// the Executor enqueued onto), matching scrub. So dry-run previews via the
    /// metadata node alone.
    #[tokio::test]
    async fn dry_run_builds_no_drain() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let nodes = node_map(&opts(true), &config).await;
        let mut ids: Vec<&str> = nodes.keys().copied().collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["metadata"]);
    }

    // ------------------------------------------------------------------
    // (2) Reconcile runs end-to-end through the replication command
    // ------------------------------------------------------------------

    /// `angos replication` against a config with no downstream must run cleanly:
    /// the metadata walk finds nothing to reconcile, the drain drains nothing,
    /// and the summary tallies no actions.
    #[tokio::test]
    async fn no_downstream_runs_clean_and_tallies_nothing() {
        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let mut cmd = Command::new(&opts(false), &config).await.unwrap();
        cmd.run().await.unwrap();

        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            0,
            "a reconcile with no configured downstream must tally no actions"
        );
    }

    /// `angos replication` (mutate) enqueues a push for a tag missing on the
    /// downstream, then the `replicate` node drains it: the wiremock downstream
    /// must receive the tagged manifest PUT, proving both the metadata enqueue
    /// walk and the drain node ran.
    #[tokio::test]
    async fn replication_enqueues_and_drains() {
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

        // A config whose `nginx` repository replicates to the wiremock downstream.
        let config = config_with_replication(&path, &mock_server.uri(), false);

        let mut cmd = Command::new(&opts(false), &config).await.unwrap();
        cmd.run().await.unwrap();

        // The shared summary tally works for `replication` via the same
        // `Ctx`; the enqueued push is counted. The fixture plants exactly one
        // missing-downstream tag, so exactly one action is tallied.
        assert_eq!(
            cmd.ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "replication mutate run must count the enqueued push in the summary tally"
        );
        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action was applied"
        );

        // Drop explicitly so the wiremock `.expect(...)` counts (notably the
        // single tagged-manifest PUT) are verified here, not at teardown.
        drop(mock_server);
    }

    /// `angos replication -d` against an out-of-sync downstream must mutate
    /// nothing: with `DryRunSink` no push is enqueued and the downstream never
    /// receives a manifest PUT (the `.expect(0)` enforces it).
    #[tokio::test]
    async fn replication_dry_run_makes_no_changes() {
        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        // Named `root` (not `path`) to avoid shadowing the `wiremock` `path`
        // matcher used below.
        let root = temp_dir.path().to_string_lossy().to_string();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(root.as_str()).build());
        let executor = test_utils::build_test_fs_executor(root.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        let (manifest_digest, _config_digest, _layer_digest) =
            test_utils::seed_manifest(metadata_store.store(), &metadata_store, &namespace).await;

        // The reconcile HEAD-probes the tag (a divergence is fine to discover);
        // but a dry-run must never PUT a manifest to the downstream.
        Mock::given(method("HEAD"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(ResponseTemplate::new(201).insert_header(
                crate::registry::DOCKER_CONTENT_DIGEST,
                manifest_digest.to_string().as_str(),
            ))
            .expect(0)
            .mount(&mock_server)
            .await;

        let config = config_with_replication(&root, &mock_server.uri(), false);

        let mut cmd = Command::new(&opts(true), &config).await.unwrap();
        cmd.run().await.unwrap();

        // The `ReplicationChecker` is dry-run-agnostic; it still emits the
        // `EnqueueReplicationPush` through the wrapped `DryRunSink`, so the
        // summary counts the would-enqueue. The drain node is absent in dry-run,
        // so nothing is actually pushed (the `.expect(0)` above proves it). The
        // fixture plants exactly one missing-downstream tag.
        assert_eq!(
            cmd.ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "replication dry-run must count the would-enqueue push in the summary tally"
        );
        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action would have been applied"
        );

        // Drop explicitly so the `.expect(0)` on the manifest PUT is verified
        // here, proving dry-run pushed nothing.
        drop(mock_server);
    }

    /// `angos replication` (mutate) against a `prune = true` downstream that owns
    /// a stray tag the source lacks: the metadata walk enqueues a delete for the
    /// downstream-only tag and the `replicate` node drains it, so the wiremock
    /// downstream must receive the tag DELETE. Mirrors
    /// `scrub_replicate_deletes_downstream_only_tag` in `check/replication.rs`.
    #[tokio::test]
    async fn replication_prunes_downstream_only_tag() {
        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        // Named `root` (not `path`) to avoid shadowing the `wiremock` `path`
        // matcher used below.
        let root = temp_dir.path().to_string_lossy().to_string();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(root.as_str()).build());
        let executor = test_utils::build_test_fs_executor(root.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // Seed a local tag `v1`; the downstream lists `v1` plus a stray tag the
        // source lacks, so only the stray is pruned.
        let (manifest_digest, _config_digest, _layer_digest) =
            test_utils::seed_manifest(metadata_store.store(), &metadata_store, &namespace).await;

        // `v1` is converged (matching digest) so it is not pushed; the downstream
        // tag list carries the stray; the stray's DELETE is the only mutation.
        Mock::given(method("HEAD"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        crate::registry::DOCKER_CONTENT_DIGEST,
                        manifest_digest.to_string().as_str(),
                    )
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v2/nginx/tags/list"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "tags": ["v1", "stray"] })),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v2/nginx/manifests/stray"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = config_with_replication(&root, &mock_server.uri(), true);

        let mut cmd = Command::new(&opts(false), &config).await.unwrap();
        cmd.run().await.unwrap();

        // Exactly one downstream-only stray tag, so exactly one enqueued delete.
        assert_eq!(
            cmd.ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "replication prune run must count the enqueued delete in the summary tally"
        );

        // Drop explicitly so the `.expect(1)` on the stray-tag DELETE is verified
        // here, proving the prune drained.
        drop(mock_server);
    }
}
