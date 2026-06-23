//! `angos policy`: drive the retention checker through the same `Ctx` +
//! scheduler + `metadata_node` that `scrub` uses.
//!
//! `Command::new` bootstraps the stores (exactly like `scrub::Command::new`),
//! reuses the unchanged `scrub::setup::namespace_checkers` to build the
//! retention checker (by synthesizing a scrub `Options` with only the retention
//! gate on), and assembles a single `metadata` node riding the reused
//! `scrub::node::metadata_node`.
//!
//! Replication reconcile is a sync operation, not policy enforcement, and lives
//! in its own `angos replication` subcommand (`crate::command::replication`).

use std::sync::Arc;

use argh::FromArgs;
use tracing::info;

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
    name = "policy",
    description = "Enforce storage policy: retention"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(switch, short = 'r')]
    /// enforce retention policies
    pub retention: bool,
}

pub struct Command {
    /// Shared context (stores, sink, flags) threaded into every node.
    ctx: Arc<Ctx>,
    /// Retention checkers built once in `new`, boxed by the reused
    /// `scrub::setup::namespace_checkers`. `Option` so `build_nodes` can `take`
    /// them; `None` after the run.
    namespace_checkers: Option<Vec<Box<dyn scrub::check::NamespaceChecker>>>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        // Reuse the scrub builder via a synthesized `Options` with only the
        // retention gate on (replicate is `angos replication`, not policy).
        // Policy enqueues nothing to drain, so `with_drain` is false.
        let scrub_opts = scrub::Options::with_gates(options.dry_run, options.retention, false);
        let Prepared {
            ctx,
            namespace_checkers,
            ..
        } = maintenance::prepare(config, &scrub_opts, "policy", false).await?;

        Ok(Self {
            ctx,
            namespace_checkers: Some(namespace_checkers),
        })
    }

    /// Assemble the policy node set and run it as a DAG. The metadata node drives
    /// the retention checker over `list_all_namespaces` (reused
    /// `scrub::node::metadata_node`). Access times are flushed after the run,
    /// exactly as `scrub::Command::run` does.
    pub async fn run(&mut self) -> Result<(), Error> {
        let nodes = self.build_nodes()?;
        if nodes.is_empty() {
            info!("No policy operation selected; pass -r");
        }
        scheduler::run_dag(nodes).await;
        self.ctx.metadata_store.flush_access_times().await;
        let findings = self.ctx.findings.snapshot().await;
        report::log_summary(
            "policy",
            self.ctx.opts.dry_run,
            self.ctx.prune_unknown_supported,
            &self.ctx.tally,
            &findings,
        );
        Ok(())
    }

    /// Build the policy node set from the pre-built checkers. Returns an error if
    /// the checkers were already taken (i.e. `run` was called more than once).
    ///
    /// - `metadata` (deps `[]`): present whenever at least one checker exists.
    fn build_nodes(&mut self) -> Result<Vec<Node>, Error> {
        let namespace_checkers = Arc::new(
            self.namespace_checkers
                .take()
                .ok_or_else(|| Error::Initialization("run called more than once".into()))?,
        );

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

        Ok(nodes)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::atomic::Ordering};

    use tempfile::TempDir;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use super::*;
    use crate::{
        command::scrub::test_support::minimal_fs_config,
        oci::{Namespace, Tag},
        registry::{
            metadata_store::{LinkKind, LinkOperation},
            test_utils,
        },
    };

    fn test_config(path: &str) -> Configuration {
        minimal_fs_config(path, "")
    }

    /// Build the command and project its node set to an id -> deps map so tests
    /// can assert the flag -> DAG mapping without depending on node internals.
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

    fn opts(dry_run: bool, retention: bool) -> Options {
        Options { dry_run, retention }
    }

    // ------------------------------------------------------------------
    // (0) CLI surface: `angos policy` lacks --media-types/-M and --replicate
    // ------------------------------------------------------------------

    /// Media-type backfill is link-metadata repair, not a policy operation, so
    /// `angos policy` must NOT accept `--media-types`/`-M`. argh rejects an
    /// unknown switch at parse time. The retention flag still parses.
    #[test]
    fn policy_rejects_media_types_flag() {
        use argh::FromArgs;

        assert!(
            Options::from_args(&["policy"], &["--media-types"]).is_err(),
            "angos policy must reject --media-types (it is a scrub link-repair flag)"
        );
        assert!(
            Options::from_args(&["policy"], &["-M"]).is_err(),
            "angos policy must reject -M (it is a scrub link-repair flag)"
        );

        // The real policy flag still parses.
        assert_eq!(
            Options::from_args(&["policy"], &["-r"]).unwrap(),
            opts(false, true),
        );
    }

    /// Replication reconcile is a sync operation, not policy enforcement: it
    /// moved to `angos replication`. `angos policy` must NOT accept
    /// `--replicate`; argh rejects the unknown switch at parse time.
    #[test]
    fn policy_rejects_replicate_flag() {
        use argh::FromArgs;

        assert!(
            Options::from_args(&["policy"], &["--replicate"]).is_err(),
            "angos policy must reject --replicate (replication reconcile moved to 'angos replication')"
        );
    }

    // ------------------------------------------------------------------
    // (1) Node-set shape
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn retention_flag_builds_only_metadata_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        for options in [
            opts(true, true),  // -r (dry-run)
            opts(false, true), // -r (mutate)
        ] {
            let nodes = node_map(&options, &config).await;
            let mut ids: Vec<&str> = nodes.keys().copied().collect();
            ids.sort_unstable();
            assert_eq!(
                ids,
                vec!["metadata"],
                "retention must build only the metadata node: {options:?}"
            );
        }
    }

    #[tokio::test]
    async fn no_flags_builds_no_nodes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let nodes = node_map(&opts(false, false), &config).await;
        assert!(nodes.is_empty(), "no flags => no nodes: {nodes:?}");
    }

    /// `angos policy` with no flags must run cleanly: it builds no nodes (the
    /// "No policy operation selected" branch), takes no action, and emits a
    /// zeroed summary.
    #[tokio::test]
    async fn no_flags_runs_clean_and_tallies_nothing() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let mut cmd = Command::new(&opts(false, false), &config).await.unwrap();
        cmd.run().await.unwrap();

        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            0,
            "a no-op policy run must tally no actions"
        );
    }

    // ------------------------------------------------------------------
    // (2) Retention runs end-to-end through the policy command
    // ------------------------------------------------------------------

    /// `angos policy -r` (mutate) deletes a tag the global retention policy drops.
    /// Proves retention runs through the reused checker + metadata node.
    #[tokio::test]
    async fn policy_retention_deletes_expired_tag() {
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

        // A rule that retains only "keep-me": "v0.0.1" must be deleted.
        let config = minimal_fs_config(&path, "\"image.tag == 'keep-me'\"");

        let mut cmd = Command::new(&opts(false, true), &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_err(),
            "policy -r must delete the tag dropped by the retention policy"
        );

        // The shared summary tally works for `policy` via the same `Ctx`.
        // The fixture plants exactly one tag, so exactly one action is tallied.
        assert_eq!(
            cmd.ctx.tally.tags.load(Ordering::Relaxed),
            1,
            "policy mutate run must count the deleted tag in the summary tally"
        );
        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action was applied"
        );
    }

    /// `angos policy -r -d` against planted data must mutate nothing
    /// (`DryRunSink`). Mirrors scrub's dry-run guarantee.
    #[tokio::test]
    async fn policy_dry_run_makes_no_changes() {
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

        // A dropping retention rule that WOULD delete the tag if not dry-run.
        let config = minimal_fs_config(&path, "\"image.tag == 'keep-me'\"");

        let mut cmd = Command::new(&opts(true, true), &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_ok(),
            "dry-run must not delete the tag even with a dropping retention rule"
        );

        // The dry-run still counts the would-delete (the checker emits the
        // action through the wrapped `DryRunSink`); only the side effect is
        // suppressed. The fixture plants exactly one tag.
        assert_eq!(
            cmd.ctx.tally.tags.load(Ordering::Relaxed),
            1,
            "policy dry-run must count the would-delete tag in the summary tally"
        );
        assert_eq!(
            cmd.ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action would have been applied"
        );
    }
}
