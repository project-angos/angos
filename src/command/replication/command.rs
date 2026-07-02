//! `angos replication`: drive the replication reconcile through the same `Ctx`,
//! maintenance sequence, `metadata_node`, and `ReplicationDrain` that `scrub`
//! uses. In mutate mode (no `-d`) the run holds the shared maintenance lock,
//! refuses the memory lock strategy, and the drain runs as the replicate phase
//! after the walk.

use std::{
    mem,
    sync::{Arc, atomic::Ordering},
};

use argh::FromArgs;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use angos_tx_engine::lock::LockStrategy;

use crate::{
    command::{
        maintenance::{self, Prepared},
        scrub::{
            self,
            context::Ctx,
            node::{self, FailedNamespaces},
            sweep_sink::MarkerStatus,
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
    ctx: Arc<Ctx>,
    namespace_checkers: Vec<Box<dyn scrub::check::NamespaceChecker>>,
    /// In-process drain, wired only in mutate mode (`None` under `-d`).
    replication_drain: Option<maintenance::ReplicationDrain>,
    lock_strategy: LockStrategy,
    registry_id: String,
    max_hold_secs: u64,
    run_cancel: CancellationToken,
    /// Whether this run mutates (`!-d`): gates the memory-strategy refusal and
    /// the maintenance lock acquisition.
    mutate: bool,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        // Only the replicate gate is on; `with_drain` wires the in-process drain
        // over the same queue the Executor enqueues onto.
        let scrub_opts = scrub::Options::with_gates(options.dry_run, false, true);
        let Prepared {
            ctx,
            namespace_checkers,
            replication_drain,
            lock_strategy,
            registry_id,
            max_hold_secs,
            run_cancel,
        } = maintenance::prepare(config, &scrub_opts, "replication", true).await?;

        Ok(Self {
            ctx,
            namespace_checkers,
            replication_drain,
            lock_strategy,
            registry_id,
            max_hold_secs,
            run_cancel,
            mutate: !options.dry_run,
        })
    }

    /// Drive the `ReplicationChecker` over every namespace, then (mutate mode
    /// only) drain the enqueued pushes, then flush access times. A mutating run
    /// refuses the memory lock strategy and holds the shared maintenance lock
    /// for its duration; a lost lock cancels every remaining mutation. Returns
    /// the run's terminal [`MarkerStatus`].
    pub async fn run(mut self) -> Result<MarkerStatus, Error> {
        maintenance::refuse_memory_mutation(&self.lock_strategy, self.mutate)?;
        let lock = if self.mutate {
            Some(
                maintenance::acquire_lock(
                    &self.lock_strategy,
                    self.ctx.metadata_store.as_ref(),
                    &self.registry_id,
                    self.max_hold_secs,
                    self.run_cancel.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        let checkers = mem::take(&mut self.namespace_checkers);
        let ctx = self.ctx.clone();
        let failed = FailedNamespaces::default();
        let done = node::guard(
            "metadata",
            node::metadata_node(ctx, Arc::new(checkers), failed.clone()),
        )
        .await;
        let walk_complete = done == Some(true);
        if !walk_complete {
            warn!("replication: the namespace walk did not complete");
        }
        let failed = node::failed_snapshot(&failed);
        if !failed.is_empty() {
            warn!(
                failed = failed.len(),
                "replication: one or more namespaces failed their walk"
            );
        }
        // The drain runs strictly after the enqueue walk completes, skipped
        // whole once the run is cancelled.
        let mut drain_panicked = false;
        let mut drain_failed: u64 = 0;
        if let Some(drain) = self.replication_drain.take() {
            if self.run_cancel.is_cancelled() {
                warn!("replication: run cancelled; skipping the drain");
            } else {
                match node::guard("replicate", drain.drain(self.run_cancel.clone())).await {
                    None => {
                        warn!("replication: the drain task panicked");
                        drain_panicked = true;
                    }
                    Some(failed_pushes) => {
                        drain_failed = failed_pushes;
                        if failed_pushes > 0 {
                            warn!(
                                failed = failed_pushes,
                                "replication: one or more downstream pushes failed to converge"
                            );
                        }
                    }
                }
            }
        }

        if let Some(lock) = lock {
            lock.finish().await;
        }
        maintenance::report_run(&self.ctx, "replication").await;

        // A run whose drain left pushes unconverged is degraded even though the
        // enqueue walk itself succeeded, so the exit code reflects the failure.
        let degraded = !walk_complete
            || !failed.is_empty()
            || drain_panicked
            || drain_failed > 0
            || self.ctx.tally.failed.load(Ordering::Relaxed) > 0;
        Ok(if self.run_cancel.is_cancelled() {
            MarkerStatus::Aborted
        } else if degraded {
            MarkerStatus::Degraded
        } else {
            MarkerStatus::Clean
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use tempfile::TempDir;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use super::*;
    use crate::{
        command::scrub::test_support::{cleanup_s3_prefix, s3_config_with_replication},
        metrics_provider,
        oci::Namespace,
        registry::{DOCKER_CONTENT_DIGEST, test_utils},
    };

    fn test_config(path: &str) -> Configuration {
        scrub::test_support::minimal_fs_config(path, "")
    }

    fn opts(dry_run: bool) -> Options {
        Options { dry_run }
    }

    /// `angos replication` runs the reconcile by default; the only flag is `-d`.
    #[test]
    fn replication_accepts_only_dry_run() {
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

    /// A mutating `angos replication` (no `-d`) on the memory lock strategy is
    /// refused: no downstream needs to be configured for the refusal to apply.
    #[tokio::test]
    async fn replication_mutate_on_memory_strategy_is_refused() {
        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let cmd = Command::new(&opts(false), &config).await.unwrap();
        let result = cmd.run().await;
        match result {
            Err(Error::Initialization(message)) => {
                assert!(
                    message.contains("memory") && message.contains("s3 or redis"),
                    "the memory refusal must explain the exclusion gap: {message}"
                );
            }
            other => panic!("expected a memory-mutation refusal, got: {other:?}"),
        }
    }

    /// `angos replication -d` with no configured downstream proceeds on the
    /// memory strategy (no lock is needed) and tallies no actions.
    #[tokio::test]
    async fn no_downstream_dry_run_runs_clean_and_tallies_nothing() {
        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let cmd = Command::new(&opts(true), &config).await.unwrap();
        let ctx = cmd.ctx.clone();
        cmd.run().await.unwrap();

        assert_eq!(
            ctx.tally.total.load(Ordering::Relaxed),
            0,
            "a reconcile with no configured downstream must tally no actions"
        );
    }

    /// `angos replication` enqueues a push for a downstream-missing tag and the
    /// `replicate` node drains it: the downstream must receive the manifest PUT.
    /// Runs on the s3 lock strategy (memory refuses mutation) under the shared
    /// maintenance lock.
    #[tokio::test]
    async fn replication_enqueues_and_drains() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        let (config, _prefix) = s3_config_with_replication(&mock_server.uri(), false);

        let cmd = Command::new(&opts(false), &config).await.unwrap();
        let ctx = cmd.ctx.clone();

        let (manifest_digest, config_digest, layer_digest) =
            test_utils::seed_manifest(ctx.metadata_store.store(), &ctx.metadata_store, &namespace)
                .await;

        scrub::test_support::mount_out_of_sync_downstream(
            &mock_server,
            &manifest_digest,
            &config_digest,
            &layer_digest,
        )
        .await;

        cmd.run().await.unwrap();

        // The fixture plants exactly one missing-downstream tag.
        assert_eq!(
            ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "replication mutate run must count the enqueued push in the summary tally"
        );
        assert_eq!(
            ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action was applied"
        );

        cleanup_s3_prefix(ctx.metadata_store.as_ref()).await;
        // Drop so the wiremock `.expect(...)` counts are verified here.
        drop(mock_server);
    }

    /// `angos replication -d` against an out-of-sync downstream mutates nothing:
    /// no push is enqueued and the downstream never receives a manifest PUT.
    #[tokio::test]
    async fn replication_dry_run_makes_no_changes() {
        metrics_provider::init_for_tests();
        let temp_dir = TempDir::new().unwrap();
        // `root`, not `path`, to avoid shadowing the wiremock `path` matcher.
        let root = temp_dir.path().to_string_lossy().to_string();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(root.as_str()).build());
        let executor = test_utils::build_test_fs_executor(root.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        let (manifest_digest, _config_digest, _layer_digest) =
            test_utils::seed_manifest(metadata_store.store(), &metadata_store, &namespace).await;

        // The reconcile HEAD-probes the tag; a dry-run must never PUT a manifest.
        Mock::given(method("HEAD"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(0)
            .mount(&mock_server)
            .await;

        let config = scrub::test_support::config_with_replication(&root, &mock_server.uri(), false);

        let cmd = Command::new(&opts(true), &config).await.unwrap();
        let ctx = cmd.ctx.clone();
        cmd.run().await.unwrap();

        // The dry-run still counts the would-enqueue; the drain node is absent,
        // so nothing is pushed (the `.expect(0)` above proves it). The fixture
        // plants exactly one missing-downstream tag.
        assert_eq!(
            ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "replication dry-run must count the would-enqueue push in the summary tally"
        );
        assert_eq!(
            ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action would have been applied"
        );

        // Drop so the `.expect(0)` on the manifest PUT is verified here.
        drop(mock_server);
    }

    /// `angos replication` against a `prune = true` downstream that owns a stray
    /// tag the source lacks: the walk enqueues a delete and the `replicate` node
    /// drains it, so the downstream must receive the tag DELETE.
    #[tokio::test]
    async fn replication_prunes_downstream_only_tag() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        let (config, _prefix) = s3_config_with_replication(&mock_server.uri(), true);

        let cmd = Command::new(&opts(false), &config).await.unwrap();
        let ctx = cmd.ctx.clone();

        // Seed local tag `v1`; the downstream lists `v1` plus a stray the source
        // lacks. `v1` is converged so it is not pushed; only the stray is pruned.
        let (manifest_digest, _config_digest, _layer_digest) =
            test_utils::seed_manifest(ctx.metadata_store.store(), &ctx.metadata_store, &namespace)
                .await;

        Mock::given(method("HEAD"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
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

        cmd.run().await.unwrap();

        // Exactly one downstream-only stray tag, so exactly one enqueued delete.
        assert_eq!(
            ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "replication prune run must count the enqueued delete in the summary tally"
        );

        cleanup_s3_prefix(ctx.metadata_store.as_ref()).await;
        // Drop so the `.expect(1)` on the stray-tag DELETE is verified here.
        drop(mock_server);
    }
}
