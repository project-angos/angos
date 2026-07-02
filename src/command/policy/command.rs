//! `angos policy`: drive the retention checker through the same `Ctx`,
//! maintenance sequence, and `metadata_node` that `scrub` uses. A mutating run
//! (`-r` without `-d`) holds the shared maintenance lock and refuses the memory
//! lock strategy; a dry run takes no lock.

use std::{
    mem,
    sync::{Arc, atomic::Ordering},
};

use argh::FromArgs;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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
    ctx: Arc<Ctx>,
    namespace_checkers: Vec<Box<dyn scrub::check::NamespaceChecker>>,
    lock_strategy: LockStrategy,
    registry_id: String,
    max_hold_secs: u64,
    run_cancel: CancellationToken,
    /// Whether this run mutates (`-r` without `-d`): gates the memory-strategy
    /// refusal and the maintenance lock acquisition.
    mutate: bool,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        // Only the retention gate is on; policy enqueues nothing, so no drain.
        let scrub_opts = scrub::Options::with_gates(options.dry_run, options.retention, false);
        let Prepared {
            ctx,
            namespace_checkers,
            lock_strategy,
            registry_id,
            max_hold_secs,
            run_cancel,
            ..
        } = maintenance::prepare(config, &scrub_opts, "policy", false).await?;

        Ok(Self {
            ctx,
            namespace_checkers,
            lock_strategy,
            registry_id,
            max_hold_secs,
            run_cancel,
            mutate: options.retention && !options.dry_run,
        })
    }

    /// Drive the retention checker over every namespace, then flush access
    /// times. A mutating run refuses the memory lock strategy and holds the
    /// shared maintenance lock for its duration; a lost lock cancels every
    /// remaining mutation. Returns the run's terminal [`MarkerStatus`].
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
        let (walk_complete, failed_count) = if checkers.is_empty() {
            info!("No policy operation selected; pass -r");
            (true, 0)
        } else {
            let ctx = self.ctx.clone();
            let failed = FailedNamespaces::default();
            let done = node::guard(
                "metadata",
                node::metadata_node(ctx, Arc::new(checkers), failed.clone()),
            )
            .await;
            let walk_complete = done == Some(true);
            if !walk_complete {
                warn!("policy: the namespace walk did not complete");
            }
            let failed = node::failed_snapshot(&failed);
            if !failed.is_empty() {
                warn!(
                    failed = failed.len(),
                    "policy: one or more namespaces failed their walk"
                );
            }
            (walk_complete, failed.len())
        };

        if let Some(lock) = lock {
            lock.finish().await;
        }
        maintenance::report_run(&self.ctx, "policy").await;

        let degraded =
            !walk_complete || failed_count > 0 || self.ctx.tally.failed.load(Ordering::Relaxed) > 0;
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

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        command::scrub::{
            scrub_lock,
            test_support::{cleanup_s3_prefix, minimal_fs_config, s3_lock_config},
        },
        oci::{Namespace, Tag},
        registry::{
            metadata_store::{LinkKind, LinkOperation},
            test_utils,
        },
    };

    fn test_config(path: &str) -> Configuration {
        minimal_fs_config(path, "")
    }

    fn opts(dry_run: bool, retention: bool) -> Options {
        Options { dry_run, retention }
    }

    /// `angos policy` rejects `--media-types`/`-M` (a scrub link-repair flag);
    /// the retention flag still parses.
    #[test]
    fn policy_rejects_media_types_flag() {
        assert!(
            Options::from_args(&["policy"], &["--media-types"]).is_err(),
            "angos policy must reject --media-types (it is a scrub link-repair flag)"
        );
        assert!(
            Options::from_args(&["policy"], &["-M"]).is_err(),
            "angos policy must reject -M (it is a scrub link-repair flag)"
        );

        assert_eq!(
            Options::from_args(&["policy"], &["-r"]).unwrap(),
            opts(false, true),
        );
    }

    /// Replication reconcile moved to `angos replication`, so `angos policy`
    /// rejects `--replicate`.
    #[test]
    fn policy_rejects_replicate_flag() {
        assert!(
            Options::from_args(&["policy"], &["--replicate"]).is_err(),
            "angos policy must reject --replicate (replication reconcile moved to 'angos replication')"
        );
    }

    /// `angos policy` with no flags builds no checkers, takes no action, and
    /// tallies nothing.
    #[tokio::test]
    async fn no_flags_runs_clean_and_tallies_nothing() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let cmd = Command::new(&opts(false, false), &config).await.unwrap();
        let ctx = cmd.ctx.clone();
        cmd.run().await.unwrap();

        assert_eq!(
            ctx.tally.total.load(Ordering::Relaxed),
            0,
            "a no-op policy run must tally no actions"
        );
    }

    /// A mutating `angos policy -r` on the memory lock strategy is refused:
    /// there is zero cross-process exclusion for its deletes.
    #[tokio::test]
    async fn policy_mutate_on_memory_strategy_is_refused() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = minimal_fs_config(&path, "\"image.tag == 'keep-me'\"");

        let cmd = Command::new(&opts(false, true), &config).await.unwrap();
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

    /// `angos policy -r` deletes a tag the global retention policy drops, under
    /// the shared maintenance lock (s3 lock strategy; memory refuses mutation).
    #[tokio::test]
    async fn policy_retention_deletes_expired_tag() {
        // A rule that retains only "keep-me": "v0.0.1" must be deleted.
        let (config, _prefix) = s3_lock_config("\"image.tag == 'keep-me'\"");

        let cmd = Command::new(&opts(false, true), &config).await.unwrap();
        let ctx = cmd.ctx.clone();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let manifest =
            test_utils::put_blob_direct(ctx.metadata_store.store(), b"manifest-bytes").await;
        ctx.metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        cmd.run().await.unwrap();

        assert!(
            ctx.metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_err(),
            "policy -r must delete the tag dropped by the retention policy"
        );

        // The fixture plants exactly one tag, so exactly one action is tallied.
        assert_eq!(
            ctx.tally.tags.load(Ordering::Relaxed),
            1,
            "policy mutate run must count the deleted tag in the summary tally"
        );
        assert_eq!(
            ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action was applied"
        );

        cleanup_s3_prefix(ctx.metadata_store.as_ref()).await;
    }

    /// While the maintenance lock is held, a mutating `angos policy -r` refuses
    /// to start.
    #[tokio::test]
    async fn policy_mutate_refused_while_maintenance_lock_held() {
        let (config, _prefix) = s3_lock_config("\"image.tag == 'keep-me'\"");

        let cmd = Command::new(&opts(false, true), &config).await.unwrap();
        let ctx = cmd.ctx.clone();

        let held = scrub_lock::acquire(
            &cmd.lock_strategy,
            ctx.metadata_store.as_ref(),
            &cmd.registry_id,
            cmd.max_hold_secs,
        )
        .await
        .expect("acquire the held maintenance lock");

        let result = cmd.run().await;
        match result {
            Err(Error::Initialization(message)) => {
                assert!(
                    message.contains("another maintenance command"),
                    "the refusal must name the shared maintenance lock: {message}"
                );
            }
            other => panic!("expected a lock-contention refusal, got: {other:?}"),
        }

        held.release().await;
        cleanup_s3_prefix(ctx.metadata_store.as_ref()).await;
    }

    /// `angos policy -r -d` mutates nothing even with a dropping retention rule,
    /// proceeds on the memory strategy, and takes no lock.
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

        let cmd = Command::new(&opts(true, true), &config).await.unwrap();
        let ctx = cmd.ctx.clone();
        cmd.run().await.unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_ok(),
            "dry-run must not delete the tag even with a dropping retention rule"
        );

        // Dry-run still counts the would-delete; only the side effect is
        // suppressed. The fixture plants exactly one tag.
        assert_eq!(
            ctx.tally.tags.load(Ordering::Relaxed),
            1,
            "policy dry-run must count the would-delete tag in the summary tally"
        );
        assert_eq!(
            ctx.tally.total.load(Ordering::Relaxed),
            1,
            "exactly one action would have been applied"
        );
    }
}
