use std::{
    collections::HashSet,
    future::Future,
    mem,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use angos_storage::{ConditionalStore, ObjectStore};
use angos_tx_engine::janitor::{BodyJanitor, LockJanitor};
use angos_tx_engine::lock::LockStrategy;
use argh::FromArgs;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::future::join_all;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    command::{
        maintenance::{self, ReplicationDrain, SinkGates, Stores},
        scrub::{
            check::{
                JobChecker, MultipartChecker, NamespaceChecker, OrphanJobChecker, RebuildChecker,
            },
            context::Ctx,
            error::Error,
            node::{self, FailedNamespaces},
            raw_sweep,
            report::Findings,
            setup,
            sweep_sink::{self, MarkerStatus, MarkerTallies, RunMarker, RunMode},
        },
    },
    configuration::{Configuration, RegistryStorageConfig},
    registry::{blob_store::BlobStore, repository_resolver::RepositoryResolver},
};

/// Outcome of one scrub run, surfaced to `main` for the process exit code. The
/// status and exit code agree by construction.
#[derive(Debug)]
pub struct ScrubOutcome {
    /// 0 = clean, 1 = refused at entry, 2 = degraded, 3 = aborted.
    pub exit_code: i32,
    pub status: MarkerStatus,
}

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
    #[argh(switch, short = 'r')]
    /// enforce retention policies
    pub retention: bool,
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
    #[argh(switch)]
    /// structural job reconcile: drop dangling `_jobs/index` lock-key entries
    /// whose pending envelope has vanished. With --prune-unknown it also removes
    /// unknown-named queue directories. Independent of the config-drift orphan
    /// sweeps (--replication-orphans / --cache-orphans)
    pub jobs: bool,
    #[argh(switch)]
    /// opt-in: also delete structurally-invalid objects scrub would otherwise
    /// only report (invalid-named namespaces and unknown job-queue directories).
    /// Off by default. Run with -d first
    pub prune_unknown: bool,
    #[argh(switch)]
    /// global deletion gate: enable destructive actions across all sweep
    /// categories (off by default; run report-only first). Mutually exclusive
    /// with --dry-run
    pub commit: bool,
    #[argh(switch)]
    /// run one on-demand reclaim pass of the storage engine's pure-delete
    /// janitors: orphan transaction staging directories (.tx-bodies/) older
    /// than 1h and expired engine locks (.tx-locks/). Honours
    /// --dry-run/--commit: lists without deleting unless --commit. The
    /// engine's recovery replay is skipped
    pub reclaim_engine: bool,
    #[argh(switch)]
    /// also delete unrecognized keys, gated on --commit. Without --commit the
    /// unrecognized pass classifies + tallies but deletes nothing; with --commit
    /// each unrecognized key older than the maintenance grace is deleted
    pub delete_unrecognized: bool,
}

impl Options {
    /// Build a scrub `Options` with every gate off except `retention` /
    /// `replicate` and `dry_run`, so `policy` and `replication` reuse the
    /// `setup::*` builders.
    pub fn with_gates(dry_run: bool, retention: bool, replicate: bool) -> Self {
        Self {
            dry_run,
            retention,
            replicate,
            ..Default::default()
        }
    }

    /// Whether policy actions (retention deletes, replication enqueues) mutate:
    /// under the global `--commit`, or under the deprecated `-r`/`--replicate`
    /// aliases' own `!--dry-run` gate, matching the dedicated subcommands. GC
    /// actions mutate only under `--commit`.
    pub fn policy_mutate(&self) -> bool {
        self.commit || ((self.retention || self.replicate) && !self.dry_run)
    }

    /// Whether any category mutates: policy via its alias gate, GC via
    /// `--commit`. Drives the memory-strategy refusal and the summary verb.
    pub fn any_mutate(&self) -> bool {
        self.policy_mutate() || self.commit
    }
}

/// One consolidated deprecation warning for the working aliases kept for
/// compatibility: `-r`/`--retention`, `--replicate`, and `-d`/`--dry-run`.
fn warn_deprecated_aliases(options: &Options) {
    let mut aliases = Vec::new();
    if options.retention {
        aliases.push("--retention (use 'angos policy --retention')");
    }
    if options.replicate {
        aliases.push("--replicate (use 'angos replication')");
    }
    if options.dry_run {
        aliases.push("--dry-run (report-only is the default; --commit gates deletion)");
    }
    if aliases.is_empty() {
        return;
    }
    warn!(
        "deprecated scrub flags in use, to be removed in a future release: {}",
        aliases.join(", ")
    );
}

#[allow(clippy::struct_excessive_bools)]
pub struct Command {
    /// Shared context (stores, sink, flags) threaded into every node.
    ctx: Arc<Ctx>,
    /// Per-namespace fatal publication written by the `RebuildChecker` and read
    /// once after the rebuild barrier in `run_phases`. Held on the command (not
    /// `Ctx`) because `Ctx` is shared with policy/replication, which never
    /// rebuild.
    failed_namespaces: FailedNamespaces,
    /// Blob backend handle for the raw sweep's `v2/blobs` enumeration (separate
    /// from the registry store the [`Ctx`] holds).
    blob_store: Arc<BlobStore>,
    /// The pre-built checkers and drain, constructed in `new` and consumed once
    /// in `run` via `take`/`mem::take`. A body runs iff its checker is present;
    /// `metadata` is gated on `ctx.opts.metadata_enabled()`.
    namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    multipart_checker: Option<MultipartChecker>,
    orphan_job_checkers: Vec<OrphanJobChecker>,
    job_checker: Option<JobChecker>,
    replication_drain: Option<ReplicationDrain>,
    /// Run-twice guard: the checkers are consumed in `run`. Set on entry to
    /// `run_phases` and refused on a repeat.
    checkers_taken: bool,
    /// Global deletion gate, the sweep's only consumer.
    commit: bool,
    /// Unrecognized deletion opt-in, gated on `commit`.
    delete_unrecognized: bool,
    /// Whether the blob and metadata stores resolve to one physical backend,
    /// deciding which extra roots the sweep walks in a split topology.
    shared_backend: bool,
    /// Repository resolver for the config-ownership sweep's `resolve(ns)`.
    resolver: Arc<RepositoryResolver>,
    /// The metadata store's lock strategy. The maintenance lock is built from
    /// it, and a Memory strategy with any mutating configuration is refused (no
    /// cross-process exclusion).
    lock_strategy: LockStrategy,
    /// Registry identity scoping the in-process memory lock map.
    registry_id: String,
    /// Operator ceiling on one run's lock hold (`maintenance_lock_max_hold_secs`).
    max_hold_secs: u64,
    /// Whether any category mutates; drives the memory-strategy refusal.
    mutate_configured: bool,
    /// Run-level cancellation created at construction and baked into the `Ctx`
    /// sink chain; the lock session's cancellation forwards into it.
    run_cancel: CancellationToken,
    /// The operator maintenance grace, the reap precondition for every GC
    /// category and the unknown-queue quiescence window.
    grace: ChronoDuration,
    /// On-demand engine-key reclaim opt-in; deletes only under `--commit`.
    reclaim_engine: bool,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        if options.dry_run && options.commit {
            return Err(Error::Initialization(
                "--commit and --dry-run are mutually exclusive".into(),
            ));
        }
        warn_deprecated_aliases(options);
        let (lock_strategy, registry_id) = maintenance::resolve_lock_strategy(config)?;
        let Stores {
            blob_backend,
            metadata_store,
            repositories,
        } = maintenance::bootstrap_stores(config).await?;

        // Shared findings accumulator, threaded into both the `RebuildChecker`
        // and `Ctx`.
        let findings = Findings::default();
        let run_cancel = CancellationToken::new();
        // The config bound rejects values chrono cannot represent, and the
        // fallible conversion saturates instead of panicking regardless.
        let grace = ChronoDuration::try_seconds(
            i64::try_from(config.global.maintenance_grace_secs).unwrap_or(i64::MAX),
        )
        .unwrap_or(ChronoDuration::MAX);

        // The scrub-only unconditional rebuild runs FIRST per namespace, so the
        // shared checkers (retention in particular) read a repaired index. Built
        // here, not in `setup`, so policy/replication never run it. Its repair
        // writes are gated on --commit: a report-only run reports divergence
        // and mutates nothing but the run marker.
        let mut namespace_checkers: Vec<Box<dyn NamespaceChecker>> =
            vec![Box::new(RebuildChecker::new(
                blob_backend.clone(),
                metadata_store.clone(),
                findings.clone(),
                !options.commit,
                run_cancel.clone(),
            ))];
        namespace_checkers.extend(setup::namespace_checkers(
            options,
            config,
            &blob_backend,
            &metadata_store,
            &repositories,
        )?);
        let multipart_checker = setup::multipart_checker(options, &blob_backend)?;
        let orphan_job_checkers =
            setup::orphan_job_checkers(options, &metadata_store, &repositories);
        let job_checker = setup::job_checker(options, &metadata_store, grace, run_cancel.clone());

        // Per-category mutate gates for the Action sink; a report-only run
        // routes every action through the `DryRunSink`, and the deprecated
        // policy aliases open only the policy gate. The rebuild writes its
        // additive repairs directly, gated by its own `--dry-run` field.
        let gates = SinkGates {
            policy_mutate: options.policy_mutate(),
            gc_mutate: options.commit,
        };
        let (inner, replication_drain) = maintenance::build_sink_and_drain(
            &blob_backend,
            &metadata_store,
            &repositories,
            gates,
            "scrub",
            options.replicate,
            config,
        );

        let ctx = maintenance::build_ctx(
            metadata_store,
            inner,
            options,
            config,
            findings,
            true,
            gates,
            run_cancel.clone(),
        );

        Ok(Self {
            ctx,
            failed_namespaces: FailedNamespaces::default(),
            blob_store: blob_backend,
            namespace_checkers,
            multipart_checker,
            orphan_job_checkers,
            job_checker,
            replication_drain,
            checkers_taken: false,
            commit: options.commit,
            delete_unrecognized: options.delete_unrecognized,
            shared_backend: shared_storage_backend(config),
            resolver: repositories,
            lock_strategy,
            registry_id,
            max_hold_secs: config.global.maintenance_lock_max_hold_secs.get(),
            mutate_configured: options.any_mutate(),
            run_cancel,
            grace,
            reclaim_engine: options.reclaim_engine,
        })
    }

    /// Acquire the single-instance maintenance lock, run the phases and the raw
    /// sweep, then release the lock.
    ///
    /// A Memory lock strategy with any mutating configuration is refused (no
    /// cross-process exclusion). If another maintenance command holds the lock
    /// the run refuses to start. The lock session's cancellation forwards into
    /// the run token so a max-hold or ownership-loss event aborts all mutations.
    pub async fn run(&mut self) -> Result<ScrubOutcome, Error> {
        let started_at = Utc::now();

        if let Err(error) =
            maintenance::refuse_memory_mutation(&self.lock_strategy, self.mutate_configured)
        {
            self.write_refused_marker(started_at).await;
            return Err(error);
        }

        let lock = match maintenance::acquire_lock(
            &self.lock_strategy,
            self.ctx.metadata_store.as_ref(),
            &self.registry_id,
            self.max_hold_secs,
            self.run_cancel.clone(),
        )
        .await
        {
            Ok(lock) => lock,
            Err(error) => {
                self.write_refused_marker(started_at).await;
                return Err(error);
            }
        };

        let result = self.run_phases(started_at).await;

        lock.finish().await;
        result
    }

    /// The run mode recorded in the marker, so a watcher can tell performed
    /// deletions from would-counts. `--dry-run` and `--commit` are mutually
    /// exclusive; neither is the report-only default.
    fn run_mode(&self) -> RunMode {
        if self.ctx.opts.dry_run {
            RunMode::DryRun
        } else if self.commit {
            RunMode::Commit
        } else {
            RunMode::ReportOnly
        }
    }

    /// Write the refused-run marker before an entry refusal returns. Refusals
    /// precede any phase, so the tallies are empty. Best-effort.
    async fn write_refused_marker(&self, started_at: DateTime<Utc>) {
        let now = Utc::now();
        let marker = RunMarker {
            run_ts: RunMarker::run_ts_for(now),
            started_at: started_at.to_rfc3339(),
            finished_at: now.to_rfc3339(),
            status: MarkerStatus::Refused,
            exit_code: MarkerStatus::Refused.exit_code(),
            mode: self.run_mode(),
            tallies: MarkerTallies::default(),
            failed_namespaces: Vec::new(),
            walk_complete: false,
        };
        self.write_marker(&marker).await;
    }

    /// Write the marker to `_scrub-audit/latest.json`. Best-effort: a put failure
    /// is warned and swallowed so it never flips the exit code.
    async fn write_marker(&self, marker: &RunMarker) {
        if let Err(error) =
            sweep_sink::write_latest_marker(self.ctx.metadata_store.store(), marker).await
        {
            warn!(%error, "scrub: failed to write the liveness marker (_scrub-audit/latest.json)");
        }
    }

    /// Drive the enabled maintenance bodies as a fixed sequence, consuming the
    /// checkers held on the command. Phase A (`metadata`, `jobs`, `orphan-jobs`,
    /// `multipart`) runs concurrently, then `replicate`. Blob GC is not a phase;
    /// the raw sweep (run after) owns it. Each body runs iff its checker is
    /// present (`metadata` iff `ctx.opts.metadata_enabled()`) and is wrapped in
    /// [`node::guard`].
    ///
    /// Returns whether the metadata walk completed: `false` on an enumeration
    /// error, a panicked metadata body, or a cancellation, so the caller
    /// demotes the sweep to report-only.
    async fn run_scrub_sequence(&mut self) -> bool {
        let ctx = self.ctx.clone();
        let walk_complete = Arc::new(AtomicBool::new(true));

        // Phase A roots run concurrently. The metadata node runs when any
        // namespace-scoped step is enabled or a namespace checker is present
        // (scrub always builds the unconditional rebuild).
        let mut phase_a: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();
        if ctx.opts.metadata_enabled() || !self.namespace_checkers.is_empty() {
            let ctx = ctx.clone();
            let namespace_checkers = Arc::new(mem::take(&mut self.namespace_checkers));
            let failed = self.failed_namespaces.clone();
            let flag = walk_complete.clone();
            phase_a.push(Box::pin(async move {
                let done = node::guard(
                    "metadata",
                    node::metadata_node(ctx, namespace_checkers, failed),
                )
                .await;
                flag.store(done.unwrap_or(false), Ordering::SeqCst);
            }));
        }
        if let Some(checker) = self.job_checker.take() {
            let ctx = ctx.clone();
            phase_a.push(Box::pin(async move {
                let _ = node::guard("jobs", async move {
                    node::store_node(&ctx, "jobs", Box::new(checker)).await;
                })
                .await;
            }));
        }
        let orphan_job_checkers = mem::take(&mut self.orphan_job_checkers);
        if !orphan_job_checkers.is_empty() {
            let ctx = ctx.clone();
            phase_a.push(Box::pin(async move {
                let _ = node::guard("orphan-jobs", async move {
                    for checker in orphan_job_checkers {
                        node::store_node(&ctx, "orphan-jobs", Box::new(checker)).await;
                    }
                })
                .await;
            }));
        }
        if let Some(checker) = self.multipart_checker.take() {
            let ctx = ctx.clone();
            phase_a.push(Box::pin(async move {
                let _ = node::guard("multipart", async move {
                    node::store_node(&ctx, "multipart", Box::new(checker)).await;
                })
                .await;
            }));
        }
        join_all(phase_a).await;

        // Replicate runs after phase A, skipped whole once the run is cancelled.
        if let Some(drain) = self.replication_drain.take() {
            if self.run_cancel.is_cancelled() {
                warn!("scrub: run cancelled; skipping the replication drain");
            } else {
                let _ = node::guard("replicate", drain.drain(self.run_cancel.clone())).await;
            }
        }

        walk_complete.load(Ordering::SeqCst)
    }

    /// Run the maintenance bodies as the fixed sequence, then the raw-enumeration
    /// sweep, then write the run marker. Held under the maintenance lock by
    /// `run`; the run token (fed by the lock session) aborts further mutations,
    /// and `started_at` is stamped into the marker.
    async fn run_phases(&mut self, started_at: DateTime<Utc>) -> Result<ScrubOutcome, Error> {
        if self.checkers_taken {
            return Err(Error::Initialization("run called more than once".into()));
        }
        self.checkers_taken = true;
        let walk_complete = self.run_scrub_sequence().await;
        // Snapshot the failed-namespace set once, after the phase-A barrier;
        // reading mid-phase would race the concurrent namespace tasks. The raw
        // names feed the per-namespace sweep skip and the run marker.
        let failed_set = node::failed_snapshot(&self.failed_namespaces);
        let fatal_namespaces: Arc<HashSet<String>> = Arc::new(failed_set.iter().cloned().collect());
        let any_failed = !fatal_namespaces.is_empty();
        maintenance::report_run(&self.ctx, "scrub").await;
        // Engine-key reclaim touches only `.tx-bodies/`/`.tx-locks/`, disjoint
        // from the sweep's grant/link state.
        if self.reclaim_engine {
            self.reclaim_engine_pass().await;
        }
        // Prune the dead `_registry/` prefix before the sweep: an idempotent,
        // commit-gated one-shot, so `_registry` never reaches the unrecognized
        // classifier. Legacy blob-index convergence rides the sweep's own walk.
        self.prune_legacy_namespace_registry().await;
        // Run identity and age anchor: names the run in the marker and is the
        // instant the age gate measures against.
        let run_epoch = Utc::now();
        if self.commit && !walk_complete {
            warn!(
                "scrub: the metadata walk did not complete; demoting the sweep to report-only \
                 (keep over reap)"
            );
        }
        let tally = raw_sweep::run_sweep(
            &self.ctx.metadata_store,
            &self.blob_store,
            &self.resolver,
            &raw_sweep::SweepParams {
                run_epoch,
                commit: self.commit && walk_complete,
                delete_unrecognized: self.delete_unrecognized,
                fatal_namespaces: fatal_namespaces.clone(),
                any_rebuild_fatal: any_failed,
                grace: self.grace,
                shared_backend: self.shared_backend,
                fanout: self.ctx.opts.fanout,
                // Convergence rewrites storage, so like every other write it
                // requires --commit; report-only reports the count instead.
                converge: self.commit,
            },
            &self.run_cancel,
        )
        .await;

        let failed_count = fatal_namespaces.len() as u64;
        if failed_count > 0 {
            warn!(
                failed_count,
                "scrub: one or more namespaces failed their metadata walk; the run is degraded \
                 (exit 2) and their destructive sweep passes were skipped (keep over reap)"
            );
        }
        let action_failed = self.ctx.tally.failed.load(Ordering::Relaxed);
        let (mut exit_code, mut status) =
            tally.ran_exit_code(failed_count, action_failed, walk_complete);
        if self.run_cancel.is_cancelled() {
            // The lock was lost or its max hold elapsed: the run aborted even
            // when no mutation point observed the token.
            status = MarkerStatus::Aborted;
            exit_code = status.exit_code();
        }
        let mut tallies = tally.to_marker_tallies();
        // The sweep tally does not carry the action counter.
        tallies.action_failed = action_failed;
        let marker = RunMarker {
            run_ts: RunMarker::run_ts_for(run_epoch),
            started_at: started_at.to_rfc3339(),
            finished_at: Utc::now().to_rfc3339(),
            status,
            exit_code,
            mode: self.run_mode(),
            tallies,
            failed_namespaces: failed_set.into_iter().collect(),
            walk_complete,
        };
        self.write_marker(&marker).await;

        Ok(ScrubOutcome { exit_code, status })
    }

    /// Run one on-demand pass of the engine's pure-delete janitors over the
    /// bootstrapped metadata store handles, mirroring `spawn_engine_maintenance`.
    /// The janitors touch only `.tx-bodies/`/`.tx-locks/` and are safe alongside
    /// the live engine. They delete only under `--commit`, else list.
    ///
    /// They keep their default orphan ages (`BodyJanitor` 1h, `LockJanitor` 5min
    /// on top of each lock's TTL). The 1h body age is a safety guard: a live push
    /// stages bodies before writing its intent, so a young no-intent staging dir
    /// may belong to an in-flight transaction. A zero age would corrupt it.
    ///
    /// The run token is checked between the two sweeps, so a lost lock
    /// short-circuits the pass. The `RecoveryLoop` portion is not pure-delete
    /// and is skipped (see [`Self::skip_recovery_portion`]).
    async fn reclaim_engine_pass(&self) {
        let dry_run = !self.commit;
        info!(
            dry_run,
            "reclaim-engine: reclaiming orphan transaction staging and expired engine locks \
             (pure deletes)"
        );

        let store = self.ctx.metadata_store.store();
        let object = store.object_store().clone();
        let executor = store.executor();
        let conditional_store = executor.conditional_store();

        run_reclaim_janitors(object, conditional_store, dry_run, &self.run_cancel).await;

        self.skip_recovery_portion();
    }

    /// Prune the dead `_registry/` namespace-index prefix. Idempotent and
    /// commit-gated: a report-only run performs no storage mutation beyond the
    /// run marker. Best-effort.
    async fn prune_legacy_namespace_registry(&self) {
        if !self.commit {
            info!("scrub: the legacy _registry prune requires --commit; leaving it in place");
            return;
        }
        if self.run_cancel.is_cancelled() {
            warn!("scrub: run cancelled; skipping the _registry prune");
            return;
        }
        if let Err(error) = self
            .ctx
            .metadata_store
            .delete_legacy_namespace_registry()
            .await
        {
            warn!(%error, "scrub: failed to prune the legacy _registry prefix; continuing");
        }
    }

    /// Log why the engine recovery-replay portion of `--reclaim-engine` is
    /// skipped: it is not a pure delete and scrub cannot confirm it safe.
    fn skip_recovery_portion(&self) {
        if self.ctx.opts.dry_run {
            warn!("reclaim-engine: recovery replay is not a delete and is skipped under --dry-run");
            return;
        }

        // Neither strategy lets scrub confirm no live recovery replay is
        // running, so the non-pure-delete replay is skipped with a
        // strategy-specific reason.
        if matches!(self.lock_strategy, LockStrategy::Memory) {
            warn!(
                "reclaim-engine: skipping the recovery replay on the memory lock strategy; the \
                 honest precondition is the serving process is stopped, which scrub cannot verify"
            );
        } else {
            warn!(
                "reclaim-engine: skipping the recovery replay; scrub cannot confirm the serving \
                 process's own recovery is idle; stop serving first"
            );
        }
    }
}

/// Whether the blob and metadata stores resolve to one physical backend,
/// compared by storage identity (FS root, or S3 endpoint + bucket + prefix)
/// rather than full config equality, so an explicit metadata section differing
/// only in lock strategy or tuning still reads as shared.
fn shared_storage_backend(config: &Configuration) -> bool {
    match (
        config.resolve_registry_storage(),
        RegistryStorageConfig::from_blob_store(&config.blob_store),
    ) {
        (RegistryStorageConfig::FS(meta), RegistryStorageConfig::FS(blob)) => {
            meta.root_dir.trim_end_matches('/') == blob.root_dir.trim_end_matches('/')
        }
        (RegistryStorageConfig::S3(meta), RegistryStorageConfig::S3(blob)) => {
            meta.connection.endpoint == blob.connection.endpoint
                && meta.connection.bucket == blob.connection.bucket
                && meta.connection.key_prefix == blob.connection.key_prefix
        }
        _ => false,
    }
}

/// Run the two pure-delete engine janitors once over `object`, mirroring
/// `spawn_engine_maintenance`. `dry_run` lists without mutating; a `cancel` fired
/// between the two sweeps short-circuits the pass.
///
/// Free function so the wiring is testable over an arbitrary `ObjectStore` (e.g.
/// a backdating wrapper) without bootstrapping a full `Command`.
async fn run_reclaim_janitors(
    object: Arc<dyn ObjectStore>,
    conditional_store: Option<Arc<dyn ConditionalStore>>,
    dry_run: bool,
    cancel: &CancellationToken,
) {
    BodyJanitor::builder(object.clone())
        .build()
        .sweep(dry_run)
        .await;

    if cancel.is_cancelled() {
        warn!("reclaim-engine: scrub lock lost; skipping the expired-lock sweep");
        return;
    }

    let mut lock_builder = LockJanitor::builder(object);
    if let Some(cs) = conditional_store {
        lock_builder = lock_builder.conditional_store(cs);
    }
    lock_builder.build().sweep(dry_run).await;
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Mutex};

    use angos_storage::{MemoryObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::StorageError;
    use angos_tx_engine::lock::storage::LockBody;
    use async_trait::async_trait;
    use bytes::Bytes;
    use tempfile::TempDir;
    use wiremock::MockServer;

    use super::*;
    use crate::{
        command::scrub::{
            context::MAX_FANOUT,
            executor::ActionSink,
            scrub_lock,
            test_support::{
                self, AgedObjectStore, LogCapture, cleanup_s3_prefix, minimal_fs_config,
                mount_out_of_sync_downstream, s3_config_with_replication,
            },
        },
        metrics_provider,
        oci::{Digest, Namespace, Reference, Tag},
        registry::{
            blob_store::{BlobStoreConfig, FsBackendConfig},
            manifest::{link_plan, parse_and_validate_manifest},
            metadata_store::{
                BlobIndexOperation, LinkKind, LinkOperation, MetadataStore,
                tests::{legacy_blob_index_with, put_legacy_index},
            },
            path_builder, test_utils,
        },
    };

    /// A minimal valid fs-backed configuration with an empty retention policy.
    fn test_config(path: &str) -> Configuration {
        minimal_fs_config(path, "")
    }

    /// The shared S3-lock config with no retention rules.
    fn s3_lock_config() -> (Configuration, String) {
        test_support::s3_lock_config("")
    }

    /// All-false `Options` so each test toggles only the flags it cares about.
    fn base_options() -> Options {
        Options::default()
    }

    /// A minimal valid OCI manifest body for tag fixtures: an invalid body
    /// would fail its namespace in the rebuild, which skips the namespace's
    /// remaining checkers (retention included).
    const VALID_MANIFEST_BODY: &str = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;

    /// `--commit` and `--dry-run` are mutually exclusive: `Command::new` rejects
    /// the pair before any store bootstrap.
    #[tokio::test]
    async fn command_new_rejects_commit_and_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            dry_run: true,
            commit: true,
            ..base_options()
        };
        let result = Command::new(&options, &config).await;
        assert!(
            matches!(result, Err(Error::Initialization(_))),
            "--commit with --dry-run must be rejected"
        );
    }

    /// `--commit` without `--dry-run` constructs cleanly (smoke).
    #[tokio::test]
    async fn command_new_allows_commit_without_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            commit: true,
            ..base_options()
        };
        assert!(Command::new(&options, &config).await.is_ok());
    }

    /// The invalid tag directory link key (a leading '-' is a legal key segment
    /// but fails the tag grammar; the `_manifests` ancestor makes `test-repo/app`
    /// an enumerable namespace).
    const INVALID_TAG_LINK: &str =
        "v2/repositories/test-repo/app/_manifests/tags/-bad/current/link";

    /// Plant an invalid tag directory on the command's metadata store so the
    /// unconditional metadata-phase cleanup has something to reclaim.
    async fn plant_invalid_tag(cmd: &Command) {
        cmd.ctx
            .metadata_store
            .store()
            .put(
                INVALID_TAG_LINK,
                Bytes::from_static(
                    b"sha256:0000000000000000000000000000000000000000000000000000000000000000",
                ),
            )
            .await
            .expect("plant invalid tag link");
    }

    #[tokio::test]
    async fn metadata_node_deletes_invalid_tag_directory() {
        let (config, _prefix) = s3_lock_config();

        // A committed scrub with no operation flags: the unconditional rebuild
        // builds the metadata node, and the invalid-tag-directory cleanup runs
        // UNCONDITIONALLY inside it (independent of any `-t`/`-m` flag). Its delete
        // rides the Action sink, so it needs --commit like every other sink delete
        // (report-only default deletes nothing; the next test pins that). The s3
        // lock strategy is required because memory refuses --commit.
        let options = Options {
            commit: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        plant_invalid_tag(&cmd).await;
        cmd.run().await.unwrap();

        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(INVALID_TAG_LINK).await,
                Err(StorageError::NotFound)
            ),
            "a committed scrub must delete the invalid tag directory without needing any op flag"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A report-only scrub (no --commit, no --dry-run) must NOT delete the
    /// invalid tag directory: its cleanup rides the Action sink, which is the
    /// report-only `DryRunSink` until --commit. Report-only classifies but mutates
    /// nothing.
    #[tokio::test]
    async fn report_only_keeps_invalid_tag_directory() {
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

        let mut cmd = Command::new(&base_options(), &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            std::path::Path::new(&tag_dir).exists(),
            "report-only (no --commit) must not delete the invalid tag directory"
        );
    }

    /// Plants an invalid-named namespace (uppercase fails the `Namespace`
    /// grammar) carrying a `_manifests` marker so the namespace walk
    /// enumerates it, and returns its metadata subtree path.
    fn plant_invalid_namespace(path: &str) -> String {
        let ns_dir = format!("{path}/v2/repositories/BADNS");
        std::fs::create_dir_all(format!("{ns_dir}/_manifests/revisions")).unwrap();
        std::fs::write(format!("{ns_dir}/_manifests/revisions/marker"), b"x").unwrap();
        ns_dir
    }

    /// The invalid-named namespace marker key, planted on the command's metadata
    /// store so the namespace walk enumerates `BADNS` (which fails the
    /// `Namespace` grammar).
    const INVALID_NAMESPACE_MARKER: &str = "v2/repositories/BADNS/_manifests/revisions/marker";

    /// Plant an invalid-named namespace marker on the command's metadata store.
    async fn plant_invalid_namespace_on_store(cmd: &Command) {
        cmd.ctx
            .metadata_store
            .store()
            .put(INVALID_NAMESPACE_MARKER, Bytes::from_static(b"x"))
            .await
            .expect("plant invalid namespace marker");
    }

    /// Without `--prune-unknown` an invalid-named namespace is report-only: the
    /// metadata walk warns and skips it, leaving the directory in place.
    #[tokio::test]
    async fn metadata_node_keeps_invalid_namespace_without_prune_unknown() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        // `--manifests` builds the metadata node and mutates (not dry-run), but
        // `--prune-unknown` is OFF, so no namespace deletion may occur.
        let options = Options {
            dry_run: false,
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

    /// With `--prune-unknown --commit` the metadata walk reclaims the
    /// invalid-named namespace's subtree. The s3 lock strategy is required because
    /// memory refuses --commit.
    #[tokio::test]
    async fn metadata_node_deletes_invalid_namespace_under_prune_unknown() {
        let (config, _prefix) = s3_lock_config();

        let options = Options {
            prune_unknown: true,
            commit: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        plant_invalid_namespace_on_store(&cmd).await;
        cmd.run().await.unwrap();

        assert!(
            matches!(
                cmd.ctx
                    .metadata_store
                    .store()
                    .head(INVALID_NAMESPACE_MARKER)
                    .await,
                Err(StorageError::NotFound)
            ),
            "--prune-unknown --commit must delete the invalid-named namespace's subtree"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// `--prune-unknown` WITHOUT `--commit` is report-only: the invalid-name
    /// delete rides the Action sink, which is the report-only `DryRunSink` until
    /// --commit, so the namespace subtree survives (classified, not deleted).
    #[tokio::test]
    async fn report_only_prune_unknown_keeps_invalid_namespace() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        let options = Options {
            prune_unknown: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            std::path::Path::new(&ns_dir).exists(),
            "report-only --prune-unknown (no --commit) must not delete the invalid namespace"
        );
    }

    /// `--dry-run` is fully read-only, so a revision link missing `media_type`
    /// is NOT stamped by a `--dry-run` scrub. The report-only default is
    /// equally write-free (pinned separately); the rebuild's repair writes are
    /// gated on `--commit`.
    #[tokio::test]
    async fn dry_run_performs_no_rebuild_writes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let media_type = "application/vnd.oci.image.manifest.v1+json";

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // A revision link whose blob carries a mediaType but whose link metadata
        // is missing it, the same precondition the stamping test plants.
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

        // A `--dry-run` scrub: fully read-only, so the rebuild must not write.
        let options = Options {
            dry_run: true,
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
            "--dry-run must NOT stamp media_type: it is fully read-only, unlike the report-only \
             default"
        );
    }

    /// A committed scrub (no operation flags beyond `--commit`) runs the
    /// unconditional rebuild: a fully-stripped layer link and grant are
    /// restored, proving the metadata node runs and the rebuild writes without
    /// any operation flag. Report-only performs no rebuild write (pinned by
    /// `report_only_performs_no_rebuild_writes`).
    #[tokio::test]
    async fn committed_scrub_runs_the_unconditional_rebuild() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/app").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // Seed a config blob, a layer blob, and a manifest body; commit the full
        // push so links + grants are correct.
        let config = test_utils::put_blob_direct(metadata_store.store(), b"bare cfg").await;
        let layer = test_utils::put_blob_direct(metadata_store.store(), b"bare layer").await;
        let manifest_content = format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{config}","size":8}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{layer}","size":10}}]}}"#
        );
        let revision =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
        metadata_store
            .seed_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(revision.clone()), revision.clone()),
                    LinkOperation::create_with_referrer(
                        LinkKind::Config(config.clone()),
                        config.clone(),
                        revision.clone(),
                    ),
                    LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        layer.clone(),
                        revision.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        // Strip the layer link and grant: an out-of-band corruption the rebuild
        // must repair.
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::delete_with_referrer(
                    LinkKind::Layer(layer.clone()),
                    revision.clone(),
                )],
            )
            .await
            .unwrap();
        for link in [
            LinkKind::Layer(layer.clone()),
            LinkKind::Blob(layer.clone()),
        ] {
            metadata_store
                .update_blob_index(&namespace, &layer, BlobIndexOperation::Remove(link))
                .await
                .unwrap();
        }

        let config_toml = test_config(&path);
        // No operation flags beyond --commit. run_phases directly: the memory
        // strategy refuses --commit in `run`.
        let options = Options {
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config_toml).await.unwrap();
        cmd.run_phases(Utc::now()).await.unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Layer(layer.clone()))
                .await
                .is_ok(),
            "a committed scrub's unconditional rebuild must restore the stripped layer link"
        );
        assert!(
            metadata_store.has_blob_references(&layer).await.unwrap(),
            "the restored grant must pin the layer against orphan GC"
        );
    }

    /// The report-only default performs no rebuild write: the same stripped
    /// layer link stays stripped, so report-only mutates nothing but the run
    /// marker.
    #[tokio::test]
    async fn report_only_performs_no_rebuild_writes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/app").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        let config = test_utils::put_blob_direct(metadata_store.store(), b"ro cfg").await;
        let layer = test_utils::put_blob_direct(metadata_store.store(), b"ro layer").await;
        let manifest_content = format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{config}","size":6}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{layer}","size":8}}]}}"#
        );
        let revision =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
        metadata_store
            .seed_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(revision.clone()), revision.clone()),
                    LinkOperation::create_with_referrer(
                        LinkKind::Config(config.clone()),
                        config.clone(),
                        revision.clone(),
                    ),
                    LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        layer.clone(),
                        revision.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::delete_with_referrer(
                    LinkKind::Layer(layer.clone()),
                    revision.clone(),
                )],
            )
            .await
            .unwrap();

        let config_toml = test_config(&path);
        let mut cmd = Command::new(&base_options(), &config_toml).await.unwrap();
        cmd.run().await.unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Layer(layer.clone()))
                .await
                .is_err(),
            "report-only must NOT restore the stripped layer link; the repair needs --commit"
        );
    }

    /// The deprecated `--retention` flag still runs through scrub `Command::run`:
    /// a tag dropped by the global retention policy is deleted, exactly as it was
    /// before the flag moved to `angos policy`. Guards the "deprecated alias still
    /// executes the same checker" half of the compat contract. The s3 lock
    /// strategy is required because memory refuses any mutating configuration.
    #[tokio::test]
    async fn scrub_retention_still_runs_under_deprecated_flag() {
        // A rule retaining only "keep-me": the tag "v0.0.1" must be deleted.
        let (config, _prefix) = test_support::s3_lock_config("\"image.tag == 'keep-me'\"");

        let options = Options {
            dry_run: false,
            retention: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let manifest = test_utils::put_blob_direct(
            cmd.ctx.metadata_store.store(),
            VALID_MANIFEST_BODY.as_bytes(),
        )
        .await;
        cmd.ctx
            .metadata_store
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
            cmd.ctx
                .metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_err(),
            "deprecated scrub --retention must still delete the tag dropped by the policy"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// The `--replicate` twin of `scrub_retention_still_runs_under_deprecated_flag`:
    /// the deprecated `scrub --replicate` alias still reconciles. Driven through
    /// scrub `Command::run`, the `replicate` node enqueues a push for the tag the
    /// downstream is missing and drains it in-process, so the wiremock downstream
    /// receives the tagged-manifest PUT. Proves the deprecated flag still drives
    /// the enqueue + drain end-to-end (not just that the flag parses).
    #[tokio::test]
    async fn scrub_replicate_still_runs_under_deprecated_flag() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let namespace = Namespace::new("nginx").unwrap();
        // A config whose `nginx` repository event+reconcile-replicates to the
        // wiremock downstream, on the s3 lock strategy (memory refuses any
        // mutating configuration).
        let (config, _prefix) = s3_config_with_replication(&mock_server.uri(), false);

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

        let (manifest_digest, config_digest, layer_digest) = test_utils::seed_manifest(
            cmd.ctx.metadata_store.store(),
            &cmd.ctx.metadata_store,
            &namespace,
        )
        .await;

        mount_out_of_sync_downstream(
            &mock_server,
            &manifest_digest,
            &config_digest,
            &layer_digest,
        )
        .await;

        cmd.run().await.unwrap();

        // The fixture plants exactly one missing-downstream tag, so exactly one
        // push is enqueued (and drained, verified by the PUT `.expect(1)` below).
        assert_eq!(
            cmd.ctx.tally.replication_enqueued.load(Ordering::Relaxed),
            1,
            "deprecated scrub --replicate must still enqueue the missing-downstream push"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
        // Drop explicitly so the wiremock `.expect(...)` counts (notably the
        // single tagged-manifest PUT) are verified here, proving the scrub
        // `replicate` node drove the enqueue + in-process drain.
        drop(mock_server);
    }

    /// A committed run that deletes an invalid tag directory increments the
    /// summary tally (`tags`, `total`). Reuses the invalid-tag fixture and the s3
    /// lock strategy (memory refuses --commit). The tally is read post-`run()` via
    /// `cmd.ctx`, matching the established convention of these tests.
    #[tokio::test]
    async fn summary_tally_counts_invalid_tag_deletion_mutate() {
        let (config, _prefix) = s3_lock_config();
        let options = Options {
            commit: true,
            ..base_options()
        };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        plant_invalid_tag(&cmd).await;
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

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// An invalid-named namespace skipped without `--prune-unknown` is
    /// counted as a report-only finding, and no namespace-prune action is
    /// emitted. Reuses `plant_invalid_namespace`.
    #[tokio::test]
    async fn summary_findings_count_skipped_invalid_namespace() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        let options = Options {
            dry_run: false,
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

    /// A manifest whose layer bytes are gone is a report-only finding, surfaced
    /// in the summary without any delete. Driven end-to-end through a bare scrub
    /// so the unconditional rebuild's `Findings` wiring is exercised.
    #[tokio::test]
    async fn summary_findings_count_dangling_layer() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/app").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // A BlobStore over the same fs root, used only to delete the layer bytes.
        let blob_store: Arc<BlobStore> = Arc::new(
            BlobStoreConfig::FS(FsBackendConfig {
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
        // A BARE scrub (no -m): the rebuild runs unconditionally and records the
        // dangling layer.
        let options = base_options();

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
        // Dry-run: counts the would-delete through the report-only DryRunSink, and
        // leaves the invalid tag in place. Uses the s3 lock strategy so the
        // committed leg below can run against the same fixture shape.
        let (dry_config, _dry_prefix) = s3_lock_config();
        let mut dry_cmd = Command::new(
            &Options {
                dry_run: true,
                ..base_options()
            },
            &dry_config,
        )
        .await
        .unwrap();
        plant_invalid_tag(&dry_cmd).await;
        dry_cmd.run().await.unwrap();

        // Committed: counts the done-delete through the Executor and removes the
        // invalid tag. --commit is the mutate gate (memory would refuse it, so s3).
        let (wet_config, _wet_prefix) = s3_lock_config();
        let mut wet_cmd = Command::new(
            &Options {
                commit: true,
                ..base_options()
            },
            &wet_config,
        )
        .await
        .unwrap();
        plant_invalid_tag(&wet_cmd).await;
        wet_cmd.run().await.unwrap();

        // Identical counts in both modes.
        assert_eq!(dry_cmd.ctx.tally.tags.load(Ordering::Relaxed), 1);
        assert_eq!(wet_cmd.ctx.tally.tags.load(Ordering::Relaxed), 1);
        assert_eq!(dry_cmd.ctx.tally.total.load(Ordering::Relaxed), 1);
        assert_eq!(wet_cmd.ctx.tally.total.load(Ordering::Relaxed), 1);

        // Only the side effect differs.
        assert!(
            dry_cmd
                .ctx
                .metadata_store
                .store()
                .head(INVALID_TAG_LINK)
                .await
                .is_ok(),
            "dry-run must leave the invalid tag directory in place"
        );
        assert!(
            matches!(
                wet_cmd
                    .ctx
                    .metadata_store
                    .store()
                    .head(INVALID_TAG_LINK)
                    .await,
                Err(StorageError::NotFound)
            ),
            "a committed run must delete the invalid tag directory"
        );

        cleanup_s3_prefix(dry_cmd.ctx.metadata_store.as_ref()).await;
        cleanup_s3_prefix(wet_cmd.ctx.metadata_store.as_ref()).await;
    }

    /// Under `--prune-unknown` but dry-run, the invalid-named namespace must
    /// survive: `-d` previews, never mutates.
    #[tokio::test]
    async fn metadata_node_dry_run_keeps_invalid_namespace_under_prune_unknown() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let ns_dir = plant_invalid_namespace(&path);
        let config = test_config(&path);

        let options = Options {
            dry_run: true,
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
    // Command-level node-set wiring: which flags build which nodes.
    // ------------------------------------------------------------------

    /// A pathologically large `max_concurrent_scrub_tasks` clamps to `MAX_FANOUT`;
    /// a sane value passes through. Pins the fan-out ceiling so a future bump
    /// cannot silently uncap the per-namespace enumeration.
    #[tokio::test]
    async fn fanout_is_clamped_to_max_fanout() {
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

    // ------------------------------------------------------------------
    // Single-instance scrub lock and cancellation gate.
    // ------------------------------------------------------------------

    /// While the registry lock is held, a second scrub refuses to start: the
    /// dedicated lock's `try_acquire` returns `Ok(None)`, so `Command::run`
    /// returns an `Initialization` error naming the held registry lock. The DAG
    /// and the sweep never run.
    #[tokio::test]
    async fn second_scrub_refused_while_lock_held() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(&base_options(), &config).await.unwrap();

        // Hold the lock through the same builder the command uses, over the
        // command's own metadata store (same bucket+prefix), so the command's
        // try_acquire on `scrub:registry` sees it held.
        let held = scrub_lock::acquire(
            &cmd.lock_strategy,
            cmd.ctx.metadata_store.as_ref(),
            &cmd.registry_id,
            cmd.max_hold_secs,
        )
        .await
        .expect("acquire the held maintenance lock");

        let result = cmd.run().await;
        match result {
            Err(Error::Initialization(message)) => {
                assert!(
                    message.contains("another maintenance command")
                        && message.contains("maintenance:registry"),
                    "the refusal must name the shared maintenance lock: {message}"
                );
            }
            other => panic!("expected an Initialization refusal, got: {other:?}"),
        }

        held.release().await;
        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// After a clean run the lock is released, so a subsequent dedicated acquire
    /// over the same metadata store succeeds (`Ok(Some(_))`).
    #[tokio::test]
    async fn lock_released_after_run() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(&base_options(), &config).await.unwrap();

        // Report-only run over an empty registry: classifies, deletes nothing.
        cmd.run().await.expect("the report-only run succeeds");

        let lock = scrub_lock::build(
            &cmd.lock_strategy,
            cmd.ctx.metadata_store.as_ref(),
            &cmd.registry_id,
            cmd.max_hold_secs,
        )
        .expect("build the dedicated lock");
        let session = lock
            .try_acquire(&[scrub_lock::MAINTENANCE_LOCK_KEY.to_string()])
            .await
            .expect("no hard error")
            .expect("the prior run must have released the lock");
        session.release().await;

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// `lock_strategy = memory` with `--commit` is refused: an in-process lock
    /// gives zero cross-process exclusion, so committed deletion is unsafe. The
    /// refusal lives in `run` (not `new`), so `new` still succeeds.
    #[tokio::test]
    async fn memory_commit_is_refused() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let options = Options {
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        let result = cmd.run().await;
        match result {
            Err(Error::Initialization(message)) => {
                assert!(
                    message.contains("memory") && message.contains("s3 or redis"),
                    "the memory refusal must explain the cross-process exclusion gap: {message}"
                );
            }
            other => panic!("expected a memory-commit Initialization refusal, got: {other:?}"),
        }
    }

    /// `lock_strategy = memory` report-only (no `--commit`) proceeds normally: it
    /// acquires the in-process lock and runs the DAG without refusing. Report-only
    /// deletes nothing, so a planted invalid tag directory survives (the delete
    /// rides the Action sink, which is the report-only `DryRunSink` until
    /// `--commit`, which memory refuses anyway).
    #[tokio::test]
    async fn memory_report_only_proceeds() {
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
        let options = Options { ..base_options() };

        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run().await.expect("memory report-only must proceed");

        assert!(
            std::path::Path::new(&tag_dir).exists(),
            "memory report-only must proceed (acquire the in-process lock) but delete nothing"
        );
    }

    /// A report-only run (no `--commit`) proceeds regardless of the backend's
    /// mtime capability: it never reaps, so it is never refused even when
    /// `last_modified` is unavailable.
    #[tokio::test]
    async fn report_only_run_proceeds_on_any_backend() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let config = test_config(&path);
        let mut cmd = Command::new(&base_options(), &config).await.unwrap();
        let outcome = cmd
            .run()
            .await
            .expect("a report-only run must proceed regardless of mtime capability");
        assert_eq!(
            outcome.status,
            MarkerStatus::Clean,
            "a report-only run reaps nothing, so it is never refused for a no-mtime backend"
        );
    }

    // ------------------------------------------------------------------
    // --reclaim-engine: the on-demand engine-key reclaim pass.
    // ------------------------------------------------------------------

    /// Plant a `.tx-bodies/<uuid>/staged/0` orphan body (no matching
    /// `.tx-log/<uuid>.json` intent) directly on the command's engine object
    /// store, returning the full key so a test can assert whether it survived a
    /// reclaim pass.
    async fn plant_orphan_tx_body(cmd: &Command) -> String {
        let tx_id = uuid::Uuid::new_v4();
        let key = format!(".tx-bodies/{tx_id}/staged/0");
        cmd.ctx
            .metadata_store
            .store()
            .object_store()
            .put(&key, Bytes::from_static(b"orphan staged body"))
            .await
            .expect("plant orphan .tx-bodies object");
        key
    }

    /// The on-demand reclaim pass reaps an AGED orphan `.tx-bodies/` staging
    /// object under commit: planted with no `.tx-log/<uuid>.json` intent and aged
    /// past the `BodyJanitor`'s default 1h orphan age via `AgedObjectStore`, a
    /// committing (`dry_run = false`) `run_reclaim_janitors` pass deletes the
    /// orphan prefix. This exercises the same janitor-construction the scrub
    /// `--reclaim-engine --commit` wiring runs, over a backdating store an S3
    /// backend's server-assigned `last_modified` cannot emulate. The reap depends
    /// on age, NOT a forced-zero age, so it proves the safe default-age path.
    #[tokio::test]
    async fn reclaim_engine_commit_reaps_aged_orphan_tx_body() {
        let inner = Arc::new(MemoryObjectStore::new());
        let tx_id = uuid::Uuid::new_v4();
        let key = format!(".tx-bodies/{tx_id}/staged/0");
        inner
            .put(&key, Bytes::from_static(b"aged orphan staged body"))
            .await
            .expect("plant aged orphan .tx-bodies object");

        // Backdate every head() by 2h so the default 1h body age fires.
        let aged: Arc<dyn ObjectStore> = Arc::new(AgedObjectStore::new(
            inner.clone(),
            ChronoDuration::hours(2),
        ));

        run_reclaim_janitors(aged, None, false, &CancellationToken::new()).await;

        assert!(
            matches!(inner.head(&key).await, Err(StorageError::NotFound)),
            "the committing reclaim pass must reap the aged orphan .tx-bodies object"
        );
    }

    /// A cancellation fired before the pass short-circuits it after the
    /// `BodyJanitor` sweep, so the `LockJanitor` never runs: an expired lock the
    /// `LockJanitor` would otherwise reap survives. Pins that
    /// `run_reclaim_janitors` honours the scrub lock session token between its two
    /// sweeps.
    #[tokio::test]
    async fn reclaim_engine_cancellation_skips_lock_janitor() {
        let inner = Arc::new(MemoryObjectStore::new());
        let lock_key = ".tx-locks/00/cold-key";
        let expired = LockBody {
            refreshed_at: Utc::now() - ChronoDuration::minutes(30),
            ttl_secs: 30,
            writer_nonce: uuid::Uuid::new_v4(),
            recovery_margin_secs: 0,
        };
        inner
            .put(
                lock_key,
                Bytes::from(serde_json::to_vec(&expired).expect("serialise lock body")),
            )
            .await
            .expect("plant expired lock");

        let object: Arc<dyn ObjectStore> = inner.clone();
        let cancel = CancellationToken::new();
        cancel.cancel();

        run_reclaim_janitors(object, None, false, &cancel).await;

        inner
            .head(lock_key)
            .await
            .expect("a fired cancellation must skip the LockJanitor, leaving the expired lock");
    }

    /// The production safety guard: a FRESH orphan `.tx-bodies/` staging object
    /// (within the `BodyJanitor`'s default 1h age) is PRESERVED even under
    /// `--reclaim-engine --commit` run end-to-end through `Command::run` over the
    /// real S3 engine store. A live push stages bodies before writing its intent,
    /// so a young no-intent staging dir may belong to an in-flight transaction;
    /// the default age guard keeps the on-demand pass from reaping it (the unsafe
    /// behaviour a zero age would produce). Pairs with
    /// `reclaim_engine_commit_reaps_aged_orphan_tx_body`, which proves the aged
    /// orphan IS reaped.
    #[tokio::test]
    async fn reclaim_engine_commit_preserves_fresh_orphan_tx_body() {
        let (config, _prefix) = s3_lock_config();
        let options = Options {
            reclaim_engine: true,
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        let key = plant_orphan_tx_body(&cmd).await;

        cmd.run().await.expect("the commit reclaim run succeeds");

        cmd.ctx
            .metadata_store
            .store()
            .object_store()
            .head(&key)
            .await
            .expect(
                "--reclaim-engine --commit must PRESERVE a fresh orphan body within the 1h safety \
                 window (it may belong to an in-flight push)",
            );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// `--reclaim-engine --dry-run` leaves the planted orphan in place: the
    /// on-demand janitors list without deleting.
    #[tokio::test]
    async fn reclaim_engine_dry_run_leaves_orphan_tx_body() {
        let (config, _prefix) = s3_lock_config();
        let options = Options {
            reclaim_engine: true,
            dry_run: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        let key = plant_orphan_tx_body(&cmd).await;

        cmd.run().await.expect("the dry-run reclaim run succeeds");

        cmd.ctx
            .metadata_store
            .store()
            .object_store()
            .head(&key)
            .await
            .expect("--reclaim-engine --dry-run must leave the orphan .tx-bodies object in place");

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// Under `--dry-run`, the `RecoveryLoop` replay portion is skipped with the
    /// dry-run reason, asserted through a capturing subscriber.
    #[tokio::test]
    async fn reclaim_engine_dry_run_skips_recovery_with_reason() {
        let (config, _prefix) = s3_lock_config();
        let options = Options {
            reclaim_engine: true,
            dry_run: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        let capture = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            let _guard = tracing::subscriber::set_default(subscriber);
            cmd.run().await.expect("the dry-run reclaim run succeeds");
        }

        let text = capture.contents();
        assert!(
            text.contains("recovery replay") && text.contains("--dry-run"),
            "dry-run must skip the recovery portion with the dry-run reason: {text}"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// On the memory lock strategy with `--commit`, the `RecoveryLoop` replay
    /// portion is skipped with the memory-specific reason. The reclaim pass runs
    /// (memory report-only would block --commit, but the memory refusal lives in
    /// `run` only for committed deletion of the scrub categories; the engine
    /// janitors are pure deletes, so this test asserts the skip message via a
    /// report-only run to avoid the memory-commit refusal).
    #[tokio::test]
    async fn reclaim_engine_memory_skips_recovery_with_reason() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        // Report-only (no --commit, no --dry-run): the memory strategy proceeds,
        // dry_run for the engine janitors is true (delete only under --commit),
        // and the recovery skip takes the memory arm because opts.dry_run is
        // false.
        let options = Options {
            reclaim_engine: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        let capture = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            let _guard = tracing::subscriber::set_default(subscriber);
            cmd.run()
                .await
                .expect("memory report-only reclaim run succeeds");
        }

        let text = capture.contents();
        assert!(
            text.contains("memory lock strategy") && text.contains("serving process is stopped"),
            "memory must skip the recovery portion with the memory-specific reason: {text}"
        );
    }

    /// Plant a blob (its `/data` key so `list_blobs` enumerates it) carrying a
    /// legacy single-file `index.json` granting `namespace` a `Blob` reference.
    /// Returns the blob digest.
    async fn plant_legacy_indexed_blob(cmd: &Command, namespace: &str) -> Digest {
        let content = format!("legacy-blob-{}", uuid::Uuid::new_v4());
        let digest = Digest::sha256_of_bytes(content.as_bytes());
        cmd.ctx
            .metadata_store
            .store()
            .put(
                &path_builder::blob_path(&digest),
                Bytes::from(content.into_bytes()),
            )
            .await
            .expect("plant blob /data key");

        let legacy =
            legacy_blob_index_with(vec![(namespace, vec![LinkKind::Blob(digest.clone())])]);
        put_legacy_index(cmd.ctx.metadata_store.as_ref(), &digest, &legacy).await;
        digest
    }

    /// A blob carrying a legacy `index.json` is converged into a sharded `refs/`
    /// shard by a committed scrub run, and the legacy file is removed.
    /// Convergence rewrites storage, so like every other write it requires
    /// `--commit`; the report-only default leaves it in place (pinned below).
    #[tokio::test]
    async fn committed_run_converges_legacy_blob_index() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(
            &Options {
                commit: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();

        let namespace = "ns/app";
        let digest = plant_legacy_indexed_blob(&cmd, namespace).await;
        let ns = Namespace::new(namespace).unwrap();
        let legacy_path = path_builder::blob_index_path(&digest);
        let shard_path = path_builder::blob_index_shard_path(&digest, &ns);

        // Precondition: legacy present, sharded ref absent.
        cmd.ctx
            .metadata_store
            .store()
            .head(&legacy_path)
            .await
            .expect("legacy index.json is present before the run");
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&shard_path).await,
                Err(StorageError::NotFound)
            ),
            "the sharded ref must not exist before convergence"
        );

        cmd.run().await.expect("the committed run succeeds");

        // Postcondition: legacy gone, sharded ref written, grant round-trips.
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&legacy_path).await,
                Err(StorageError::NotFound)
            ),
            "convergence must remove the drained legacy index.json"
        );
        cmd.ctx
            .metadata_store
            .store()
            .head(&shard_path)
            .await
            .expect("convergence must write the sharded ref");
        let index = cmd
            .ctx
            .metadata_store
            .read_blob_index(&digest)
            .await
            .expect("read the converged blob index");
        let links = index.namespace.get(&ns).expect("ns/app grant survives");
        assert!(
            links.contains(&LinkKind::Blob(digest.clone())),
            "the drained Blob grant round-trips through the sharded layout"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// The report-only default leaves the legacy `index.json` in place and
    /// writes no sharded ref: report-only mutates nothing but the run marker.
    #[tokio::test]
    async fn report_only_leaves_legacy_blob_index_in_place() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(&base_options(), &config).await.unwrap();

        let namespace = "ns/app";
        let digest = plant_legacy_indexed_blob(&cmd, namespace).await;
        let ns = Namespace::new(namespace).unwrap();
        let legacy_path = path_builder::blob_index_path(&digest);
        let shard_path = path_builder::blob_index_shard_path(&digest, &ns);

        cmd.run().await.expect("the report-only run succeeds");

        cmd.ctx
            .metadata_store
            .store()
            .head(&legacy_path)
            .await
            .expect("report-only must leave the legacy index.json in place");
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&shard_path).await,
                Err(StorageError::NotFound)
            ),
            "report-only must not write a sharded ref (convergence requires --commit)"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// Under `--dry-run` the legacy `index.json` is LEFT in place and no sharded
    /// ref is written: convergence issues mutations, so it is suppressed when the
    /// run is fully read-only.
    #[tokio::test]
    async fn dry_run_suppresses_legacy_blob_index_convergence() {
        let (config, _prefix) = s3_lock_config();
        let options = Options {
            dry_run: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        let namespace = "ns/app";
        let digest = plant_legacy_indexed_blob(&cmd, namespace).await;
        let ns = Namespace::new(namespace).unwrap();
        let legacy_path = path_builder::blob_index_path(&digest);
        let shard_path = path_builder::blob_index_shard_path(&digest, &ns);

        cmd.run().await.expect("the dry-run run succeeds");

        cmd.ctx
            .metadata_store
            .store()
            .head(&legacy_path)
            .await
            .expect("--dry-run must leave the legacy index.json in place");
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&shard_path).await,
                Err(StorageError::NotFound)
            ),
            "--dry-run must not write a sharded ref (convergence suppressed)"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// Convergence is idempotent and leaves no-legacy blobs untouched: a second
    /// run over an already-converged blob is a no-op, and a blob whose sharded
    /// ref was written through the normal grant path (with no legacy file) is
    /// neither re-migrated nor given a spurious legacy file.
    #[tokio::test]
    async fn convergence_is_idempotent_and_skips_no_legacy_blobs() {
        let (config, _prefix) = s3_lock_config();
        let commit_options = Options {
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&commit_options, &config).await.unwrap();

        // Blob A: planted with a legacy index, converged by the first run.
        let namespace = "ns/app";
        let digest_a = plant_legacy_indexed_blob(&cmd, namespace).await;
        let ns = Namespace::new(namespace).unwrap();
        let legacy_a = path_builder::blob_index_path(&digest_a);
        let shard_a = path_builder::blob_index_shard_path(&digest_a, &ns);

        // Blob B: a real sharded ref written through the normal grant path, with
        // NO legacy file. Convergence must leave it untouched.
        let content_b = format!("granted-blob-{}", uuid::Uuid::new_v4());
        let digest_b = Digest::sha256_of_bytes(content_b.as_bytes());
        cmd.ctx
            .metadata_store
            .store()
            .put(
                &path_builder::blob_path(&digest_b),
                Bytes::from(content_b.into_bytes()),
            )
            .await
            .expect("plant blob B /data key");
        cmd.ctx
            .metadata_store
            .update_blob_index(
                &ns,
                &digest_b,
                BlobIndexOperation::Insert(LinkKind::Blob(digest_b.clone())),
            )
            .await
            .expect("seed blob B sharded ref via the normal grant path");
        let legacy_b = path_builder::blob_index_path(&digest_b);
        let shard_b = path_builder::blob_index_shard_path(&digest_b, &ns);
        let shard_b_before = cmd
            .ctx
            .metadata_store
            .store()
            .get(&shard_b)
            .await
            .expect("blob B sharded ref exists before the run");

        // First run: converges A, leaves B untouched.
        cmd.run().await.expect("the first run succeeds");
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&legacy_a).await,
                Err(StorageError::NotFound)
            ),
            "blob A legacy file is drained by the first run"
        );
        cmd.ctx
            .metadata_store
            .store()
            .head(&shard_a)
            .await
            .expect("blob A sharded ref exists after the first run");

        // Second run over the SAME prefix. `Command::run` consumes its checkers,
        // so a fresh Command is built against the same config (a real second
        // scrub invocation) to prove idempotency.
        let mut cmd = Command::new(&commit_options, &config).await.unwrap();
        cmd.run().await.expect("the second run succeeds");
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&legacy_a).await,
                Err(StorageError::NotFound)
            ),
            "blob A legacy file stays absent on the idempotent second run"
        );
        cmd.ctx
            .metadata_store
            .store()
            .head(&shard_a)
            .await
            .expect("blob A sharded ref is unchanged on the second run");

        // Blob B: no legacy file was ever created and its shard is byte-identical.
        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head(&legacy_b).await,
                Err(StorageError::NotFound)
            ),
            "convergence must not create a legacy file for a no-legacy blob"
        );
        let shard_b_after = cmd
            .ctx
            .metadata_store
            .store()
            .get(&shard_b)
            .await
            .expect("blob B sharded ref survives");
        assert_eq!(
            shard_b_before, shard_b_after,
            "blob B sharded ref is unchanged (convergence is a no-op for no-legacy blobs)"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    // ------------------------------------------------------------------
    // Legacy `_registry/` prune one-shot (commit-gated).
    // ------------------------------------------------------------------

    /// A planted object under the dead `_registry/` prefix is pruned by a
    /// committed run; the report-only default leaves it in place (report-only
    /// mutates nothing but the run marker).
    #[tokio::test]
    async fn commit_prunes_legacy_registry_prefix_report_only_keeps_it() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(&base_options(), &config).await.unwrap();

        cmd.ctx
            .metadata_store
            .store()
            .put("_registry/index", Bytes::from_static(b"x"))
            .await
            .expect("plant a _registry/index object");

        cmd.run().await.expect("the report-only run succeeds");

        cmd.ctx
            .metadata_store
            .store()
            .head("_registry/index")
            .await
            .expect("report-only must leave the dead _registry/ prefix in place");

        let mut cmd = Command::new(
            &Options {
                commit: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();
        cmd.run().await.expect("the committed run succeeds");

        assert!(
            matches!(
                cmd.ctx.metadata_store.store().head("_registry/index").await,
                Err(StorageError::NotFound)
            ),
            "a committed run must prune the dead _registry/ prefix"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// Under `--dry-run` the planted `_registry/` object is LEFT in place: the
    /// prune issues a real delete, so it is suppressed when the run is fully
    /// read-only, mirroring the convergence dry-run gate.
    #[tokio::test]
    async fn dry_run_leaves_legacy_registry_prefix() {
        let (config, _prefix) = s3_lock_config();
        let options = Options {
            dry_run: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        cmd.ctx
            .metadata_store
            .store()
            .put("_registry/index", Bytes::from_static(b"x"))
            .await
            .expect("plant a _registry/index object");

        cmd.run().await.expect("the dry-run run succeeds");

        cmd.ctx
            .metadata_store
            .store()
            .head("_registry/index")
            .await
            .expect("--dry-run must leave the _registry/ prefix in place");

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    // ------------------------------------------------------------------
    // Liveness / result marker (_scrub-audit/latest.json) + exit codes.
    // ------------------------------------------------------------------

    /// Fetch and parse the run marker at `_scrub-audit/latest.json` on the
    /// command's metadata store, or panic if it is absent or malformed.
    async fn read_latest_marker(cmd: &Command) -> RunMarker {
        let body = cmd
            .ctx
            .metadata_store
            .store()
            .get("_scrub-audit/latest.json")
            .await
            .expect("the liveness marker must be present after a run");
        serde_json::from_slice(&body).expect("the marker parses as a RunMarker")
    }

    /// An unrecognized key on the blob store (junk hash tail) the raw sweep deletes
    /// under `--commit --delete-unrecognized`.
    fn unrecognized_blob_key(tag: &str) -> String {
        format!("{}/sha256/aa/{tag}/foreign", path_builder::blobs_root_dir())
    }

    /// A committed run writes `_scrub-audit/latest.json` whose status and tallies
    /// match the run: with a zero grace a planted unrecognized key is deleted, so
    /// the marker reports `unrecognized >= 1`, `deleted >= 1`, status Clean, exit 0.
    #[tokio::test]
    async fn committed_run_writes_marker_matching_the_run() {
        let (mut config, _prefix) = s3_lock_config();
        // A zero grace so the just-planted key passes the age gate.
        config.global.maintenance_grace_secs = 0;
        let mut cmd = Command::new(
            &Options {
                commit: true,
                delete_unrecognized: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();

        let key = unrecognized_blob_key(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        cmd.blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");

        let outcome = cmd.run().await.expect("the committed run succeeds");

        let marker = read_latest_marker(&cmd).await;
        assert_eq!(marker.exit_code, outcome.exit_code);
        assert_eq!(marker.status, outcome.status);
        assert_eq!(marker.status, MarkerStatus::Clean);
        assert_eq!(marker.exit_code, 0);
        assert_eq!(
            marker.mode,
            RunMode::Commit,
            "a committed run's tallies are performed deletions"
        );
        assert!(
            marker.tallies.unrecognized >= 1,
            "the planted unrecognized key must be tallied: {:?}",
            marker.tallies
        );
        assert!(
            marker.tallies.deleted >= 1,
            "the unrecognized key must be deleted at grace 0: {:?}",
            marker.tallies
        );
        assert!(
            marker.failed_namespaces.is_empty(),
            "no namespace failed on this run (only an unrecognized key was planted)"
        );
        assert!(marker.walk_complete, "the metadata walk completed");

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A committed run with the default (non-zero) grace keeps a freshly-planted
    /// unrecognized key: every GC reap category applies the maintenance grace.
    #[tokio::test]
    async fn committed_run_keeps_fresh_unrecognized_under_default_grace() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(
            &Options {
                commit: true,
                delete_unrecognized: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();

        let key =
            unrecognized_blob_key("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
        cmd.blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");

        cmd.run().await.expect("the committed run succeeds");

        cmd.blob_store
            .store
            .head(&key)
            .await
            .expect("a fresh unrecognized key must survive the default grace");
        let marker = read_latest_marker(&cmd).await;
        assert_eq!(
            marker.tallies.deleted, 0,
            "nothing younger than the grace may be deleted"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A report-only run (no `--commit`, no `--dry-run`) still writes a marker:
    /// status Clean, mode `report-only`. The marker is unconditional on the
    /// ran-path.
    #[tokio::test]
    async fn report_only_run_writes_clean_marker() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(&base_options(), &config).await.unwrap();

        cmd.run().await.expect("the report-only run succeeds");

        let marker = read_latest_marker(&cmd).await;
        assert_eq!(marker.status, MarkerStatus::Clean);
        assert_eq!(marker.exit_code, 0);
        assert_eq!(
            marker.mode,
            RunMode::ReportOnly,
            "the report-only default must be distinguishable in the marker"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A report-only run's marker is distinguishable from a committed run and
    /// its tallies are identifiable as would-counts: the planted unrecognized
    /// key is tallied under `deleted` while it verifiably survives in place.
    #[tokio::test]
    async fn report_only_marker_identifies_would_counts() {
        let (mut config, _prefix) = s3_lock_config();
        // A zero grace so the just-planted key passes the age gate.
        config.global.maintenance_grace_secs = 0;
        let mut cmd = Command::new(
            &Options {
                delete_unrecognized: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();

        let key =
            unrecognized_blob_key("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        cmd.blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");

        cmd.run().await.expect("the report-only run succeeds");

        cmd.blob_store
            .store
            .head(&key)
            .await
            .expect("report-only must leave the unrecognized key in place");
        let marker = read_latest_marker(&cmd).await;
        assert_eq!(
            marker.mode,
            RunMode::ReportOnly,
            "a watcher must be able to tell nothing was deleted"
        );
        assert!(
            marker.tallies.deleted >= 1,
            "the would-delete count is still reported: {:?}",
            marker.tallies
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A `--dry-run` run writes a marker noting it was a dry run. The marker is
    /// the one observability write allowed under dry-run.
    #[tokio::test]
    async fn dry_run_writes_marker_noted_as_dry_run() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(
            &Options {
                dry_run: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();

        cmd.run().await.expect("the dry-run run succeeds");

        let marker = read_latest_marker(&cmd).await;
        assert_eq!(
            marker.mode,
            RunMode::DryRun,
            "the --dry-run marker must note it was a dry run"
        );
        assert_eq!(marker.status, MarkerStatus::Clean);
        assert_eq!(marker.exit_code, 0);

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A lock-held refusal writes a marker with status Refused and exit 1, and
    /// `Command::run` returns the refused outcome path (Err).
    #[tokio::test]
    async fn lock_held_refusal_writes_refused_marker_and_exit_one() {
        let (config, _prefix) = s3_lock_config();
        let mut cmd = Command::new(
            &Options {
                commit: true,
                ..base_options()
            },
            &config,
        )
        .await
        .unwrap();

        // Pre-hold the registry lock through the same builder the command uses.
        let held = scrub_lock::acquire(
            &cmd.lock_strategy,
            cmd.ctx.metadata_store.as_ref(),
            &cmd.registry_id,
            cmd.max_hold_secs,
        )
        .await
        .expect("acquire the held maintenance lock");

        let result = cmd.run().await;
        assert!(
            matches!(result, Err(Error::Initialization(_))),
            "a lock-held run must refuse"
        );

        // The refused marker was written before the refusal returned.
        let marker = read_latest_marker(&cmd).await;
        assert_eq!(marker.status, MarkerStatus::Refused);
        assert_eq!(marker.exit_code, 1);

        held.release().await;
        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A corrupt manifest body fails its namespace: the marker lists it under
    /// `failed_namespaces`, the run is degraded (status Degraded, exit 2), and
    /// under `--commit` the namespace's destructive missing-body pass is
    /// skipped, so a second, body-less revision in the same namespace survives.
    #[tokio::test]
    async fn corrupt_body_namespace_listed_failed_and_excluded_from_sweep() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let namespace = Namespace::new("test-repo/corrupt").unwrap();

        // Seed a current revision whose manifest body is non-JSON, on the same fs
        // path the command bootstraps its stores from.
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);
        let blob_store: Arc<BlobStore> = Arc::new(
            BlobStoreConfig::FS(FsBackendConfig {
                root_dir: path.clone(),
                sync_to_disk: false,
            })
            .build_backend()
            .unwrap(),
        );

        let revision =
            test_utils::put_blob_direct(metadata_store.store(), b"this is not json {{{").await;
        // A second revision whose body is deleted: reapable by the missing-body
        // pass, unless the failed namespace is excluded.
        let missing =
            test_utils::put_blob_direct(metadata_store.store(), b"body soon missing").await;
        metadata_store
            .update_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(revision.clone()), revision.clone()),
                    LinkOperation::create(LinkKind::Digest(missing.clone()), missing.clone()),
                ],
            )
            .await
            .unwrap();
        blob_store.delete_blob(&missing).await.unwrap();

        let config = test_config(&path);
        let options = Options {
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        // run_phases directly: the memory-strategy refusal guards `run`, and
        // this test needs the committed sweep against the failed namespace.
        let outcome = cmd.run_phases(Utc::now()).await.expect("the run completes");

        let marker = read_latest_marker(&cmd).await;
        assert!(
            marker
                .failed_namespaces
                .contains(&"test-repo/corrupt".to_string()),
            "the corrupt-body namespace must be listed in the marker's failed_namespaces: {:?}",
            marker.failed_namespaces
        );
        assert_eq!(marker.status, MarkerStatus::Degraded);
        assert_eq!(marker.exit_code, 2);
        assert_eq!(outcome.status, MarkerStatus::Degraded);
        assert_eq!(outcome.exit_code, 2);
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(missing.clone()))
                .await
                .is_ok(),
            "a failed namespace must be excluded from the destructive missing-body pass"
        );
    }

    /// After a rebuild failure the namespace's remaining checkers are skipped:
    /// a retention rule that would drop the tag does not run against state the
    /// rebuild could not verify (keep over reap).
    #[tokio::test]
    async fn rebuild_failure_skips_remaining_checkers_for_namespace() {
        // A rule retaining only "keep-me": retention would delete "v0.0.1".
        let (config, _prefix) = test_support::s3_lock_config("\"image.tag == 'keep-me'\"");
        let options = Options {
            retention: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        let namespace = Namespace::new("test-repo/corrupt").unwrap();
        // A corrupt manifest body fails the namespace in the rebuild, which
        // runs before the retention checker.
        let manifest = test_utils::put_blob_direct(
            cmd.ctx.metadata_store.store(),
            b"this is not json {{{",
        )
        .await;
        cmd.ctx
            .metadata_store
            .update_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(manifest.clone()), manifest.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
                        manifest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        cmd.run().await.unwrap();

        assert!(
            cmd.ctx
                .metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_ok(),
            "retention must be skipped for a namespace whose rebuild failed"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// A panicking namespace checker makes the metadata walk incomplete: the
    /// committed sweep is demoted to report-only (a reapable missing-body
    /// revision survives), the run exits 2 Degraded, and the marker records
    /// `walk_complete = false`.
    #[tokio::test]
    async fn incomplete_walk_demotes_committed_sweep() {
        struct PanickingChecker;

        #[async_trait]
        impl NamespaceChecker for PanickingChecker {
            async fn check(
                &self,
                _namespace: &Namespace,
                _sink: &mut (dyn ActionSink + Send),
            ) -> Result<(), Error> {
                panic!("checker panicked mid-walk");
            }
        }

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/panic").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);
        let blob_store: Arc<BlobStore> = Arc::new(
            BlobStoreConfig::FS(FsBackendConfig {
                root_dir: path.clone(),
                sync_to_disk: false,
            })
            .build_backend()
            .unwrap(),
        );

        // A missing-body revision the committed sweep would reap.
        let missing =
            test_utils::put_blob_direct(metadata_store.store(), b"body soon missing").await;
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Digest(missing.clone()),
                    missing.clone(),
                )],
            )
            .await
            .unwrap();
        blob_store.delete_blob(&missing).await.unwrap();

        let config = test_config(&path);
        let options = Options {
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.namespace_checkers.insert(0, Box::new(PanickingChecker));
        let outcome = cmd.run_phases(Utc::now()).await.expect("the run completes");

        assert_eq!(outcome.exit_code, 2, "an incomplete walk degrades the run");
        assert_eq!(outcome.status, MarkerStatus::Degraded);
        let marker = read_latest_marker(&cmd).await;
        assert!(
            !marker.walk_complete,
            "the marker must record the incomplete walk"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(missing.clone()))
                .await
                .is_ok(),
            "an incomplete walk must demote every destructive sweep pass to report-only"
        );
    }

    /// A pre-cancelled run token aborts every phase-A delete: the invalid tag
    /// directory survives a committed run and the outcome is exit 3 / Aborted.
    #[tokio::test]
    async fn cancelled_run_aborts_phase_a_deletes() {
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
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run_cancel.cancel();
        let outcome = cmd.run_phases(Utc::now()).await.expect("the run completes");

        assert_eq!(outcome.exit_code, 3, "a cancelled run must exit 3");
        assert_eq!(outcome.status, MarkerStatus::Aborted);
        assert!(
            std::path::Path::new(&tag_dir).exists(),
            "a cancelled run must not delete the invalid tag directory"
        );
        let marker = read_latest_marker(&cmd).await;
        assert_eq!(marker.status, MarkerStatus::Aborted);
    }

    /// Executor apply failures reach the exit code and the marker: a run whose
    /// deletes failed exits 2 Degraded with `tallies.action_failed` set.
    #[tokio::test]
    async fn failed_action_applies_degrade_the_exit_code() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = test_config(&path);

        let mut cmd = Command::new(&base_options(), &config).await.unwrap();
        // Simulate a failed apply recorded by the CountingSink.
        cmd.ctx.tally.failed.fetch_add(1, Ordering::Relaxed);
        let outcome = cmd.run().await.expect("the run completes");

        assert_eq!(
            outcome.exit_code, 2,
            "a run with failed action applies must exit 2"
        );
        assert_eq!(outcome.status, MarkerStatus::Degraded);
        let marker = read_latest_marker(&cmd).await;
        assert_eq!(marker.tallies.action_failed, 1);
    }

    /// The deprecated `--retention` alias opens only the policy gate: in the
    /// same run a retention-dropped tag is deleted while the GC categories
    /// (invalid tag directory, invalid namespace under --prune-unknown) survive
    /// without `--commit`, counted as suppressed.
    #[tokio::test]
    async fn retention_alias_cannot_mutate_gc_categories_without_commit() {
        // A rule retaining only "keep-me": the planted tag must be deleted.
        let (config, _prefix) = test_support::s3_lock_config("\"image.tag == 'keep-me'\"");
        let options = Options {
            retention: true,
            prune_unknown: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();

        let namespace = Namespace::new("test-repo/app").unwrap();
        let manifest = test_utils::put_blob_direct(
            cmd.ctx.metadata_store.store(),
            VALID_MANIFEST_BODY.as_bytes(),
        )
        .await;
        cmd.ctx
            .metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();
        plant_invalid_tag(&cmd).await;
        plant_invalid_namespace_on_store(&cmd).await;

        cmd.run().await.unwrap();

        assert!(
            cmd.ctx
                .metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_err(),
            "the deprecated --retention alias must still delete the policy-dropped tag"
        );
        cmd.ctx
            .metadata_store
            .store()
            .head(INVALID_TAG_LINK)
            .await
            .expect("--retention without --commit must NOT delete the invalid tag directory");
        cmd.ctx
            .metadata_store
            .store()
            .head(INVALID_NAMESPACE_MARKER)
            .await
            .expect("--retention --prune-unknown without --commit must NOT delete the namespace");
        assert!(
            cmd.ctx.tally.suppressed.load(Ordering::Relaxed) >= 2,
            "the suppressed GC actions must be counted"
        );

        cleanup_s3_prefix(cmd.ctx.metadata_store.as_ref()).await;
    }

    /// The memory lock strategy refuses ANY mutating configuration, not just
    /// `--commit`: the deprecated `--retention` alias is refused with a refused
    /// marker, while `--retention --dry-run` proceeds.
    #[tokio::test]
    async fn memory_mutating_alias_is_refused() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let config = minimal_fs_config(&path, "\"image.tag == 'keep-me'\"");

        let options = Options {
            retention: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        let result = cmd.run().await;
        assert!(
            matches!(result, Err(Error::Initialization(_))),
            "a mutating alias on the memory strategy must refuse"
        );
        let marker = read_latest_marker(&cmd).await;
        assert_eq!(marker.status, MarkerStatus::Refused);
        assert_eq!(marker.exit_code, 1);

        let options = Options {
            retention: true,
            dry_run: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        cmd.run()
            .await
            .expect("--retention --dry-run must proceed on the memory strategy");
    }

    /// Every removed no-op flag is rejected by the parser, while the kept
    /// aliases still parse.
    #[test]
    fn removed_flags_are_rejected_by_the_parser() {
        for flag in [
            "-m",
            "--manifests",
            "-l",
            "--links",
            "-M",
            "--media-types",
            "-t",
            "--tags",
            "-R",
            "--referrers",
            "-n",
            "--orphan-namespaces",
            "-b",
            "--blobs",
            "--reconcile-blob-index",
        ] {
            assert!(
                Options::from_args(&["scrub"], &[flag]).is_err(),
                "removed flag '{flag}' must be rejected by the parser"
            );
        }
        assert!(
            Options::from_args(&["scrub"], &["--orphan-grants", "1h"]).is_err(),
            "removed flag '--orphan-grants' must be rejected by the parser"
        );
        for flag in ["-r", "--retention", "--replicate", "-d", "--dry-run"] {
            assert!(
                Options::from_args(&["scrub"], &[flag]).is_ok(),
                "kept alias '{flag}' must still parse"
            );
        }
    }

    /// The consolidated deprecation warning names each in-use alias and its
    /// replacement, and stays silent when none is used.
    #[test]
    fn deprecated_aliases_emit_one_consolidated_warning() {
        let capture = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                warn_deprecated_aliases(&Options {
                    retention: true,
                    replicate: true,
                    dry_run: true,
                    ..base_options()
                });
            });
        }
        let text = capture.contents();
        assert!(
            text.contains("angos policy --retention"),
            "the warning must steer --retention at 'angos policy': {text}"
        );
        assert!(
            text.contains("angos replication"),
            "the warning must steer --replicate at 'angos replication': {text}"
        );
        assert!(
            text.contains("--dry-run"),
            "the warning must cover the -d alias: {text}"
        );
        assert_eq!(
            text.matches("deprecated scrub flags").count(),
            1,
            "the deprecation must be one consolidated warning: {text}"
        );

        let quiet = LogCapture::default();
        {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(quiet.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                warn_deprecated_aliases(&base_options());
            });
        }
        assert!(
            !quiet.contents().contains("deprecated"),
            "no warning may fire when no alias is used: {}",
            quiet.contents()
        );
    }

    /// End-to-end for the lost-revision-link repair: a still-tagged manifest
    /// whose `Digest` link file was lost keeps its full layer closure through a
    /// committed run; the link is restored before the sweep classifies, so
    /// nothing is dismantled.
    #[tokio::test]
    async fn commit_run_keeps_layer_closure_of_tag_with_lost_revision_link() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/lost").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // A full pushed revision (config, layer, links, grants) plus a tag.
        let config_blob = test_utils::put_blob_direct(metadata_store.store(), b"lost cfg").await;
        let layer = test_utils::put_blob_direct(metadata_store.store(), b"lost layer").await;
        let manifest_content = format!(
            r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{config_blob}","size":8}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{layer}","size":10}}]}}"#
        );
        let revision =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
        let mut manifest = parse_and_validate_manifest(manifest_content.as_bytes(), None).unwrap();
        let effective = manifest.media_type.clone();
        let ops = link_plan::push(
            &mut manifest,
            &revision,
            &Reference::Digest(revision.clone()),
            effective.as_ref(),
            manifest_content.len() as u64,
            &[],
        );
        metadata_store.seed_links(&namespace, &ops).await.unwrap();
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("live").unwrap()),
                    revision.clone(),
                )],
            )
            .await
            .unwrap();

        // Raw-delete the Digest revision link file (out-of-band loss).
        metadata_store
            .store()
            .delete(&path_builder::link_path(
                &LinkKind::Digest(revision.clone()),
                &namespace,
            ))
            .await
            .unwrap();

        let config = test_config(&path);
        let options = Options {
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        // run_phases directly: memory refuses --commit in `run`, and this test
        // needs the committed sweep after the repair.
        let outcome = cmd.run_phases(Utc::now()).await.expect("the run completes");

        assert_eq!(outcome.exit_code, 0, "the repaired run is clean");
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(revision.clone()))
                .await
                .is_ok(),
            "the lost revision link must be restored from its live tag"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Layer(layer.clone()))
                .await
                .is_ok(),
            "the layer link must survive the committed sweep"
        );
        assert!(
            metadata_store.has_blob_references(&layer).await.unwrap(),
            "the layer grant must survive the committed sweep"
        );
        assert!(
            std::path::Path::new(&format!("{path}/{}", path_builder::blob_path(&layer))).exists(),
            "the layer bytes must survive the committed sweep"
        );
    }

    /// The rebuild runs FIRST per namespace: a spy checker inserted right after
    /// it observes the repaired state (a stripped `media_type` already restamped)
    /// before the shared checkers run.
    #[tokio::test]
    async fn rebuild_runs_before_the_shared_checkers() {
        /// Records whether the digest link carried a `media_type` when it ran.
        struct SpyChecker {
            metadata_store: Arc<MetadataStore>,
            revision: Digest,
            saw_media_type: Arc<Mutex<Option<bool>>>,
        }

        #[async_trait]
        impl NamespaceChecker for SpyChecker {
            async fn check(
                &self,
                namespace: &Namespace,
                _sink: &mut (dyn ActionSink + Send),
            ) -> Result<(), Error> {
                let link = self
                    .metadata_store
                    .read_link(namespace, &LinkKind::Digest(self.revision.clone()))
                    .await?;
                *self.saw_media_type.lock().unwrap() = Some(link.media_type.is_some());
                Ok(())
            }
        }

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let namespace = Namespace::new("test-repo/order").unwrap();

        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(path.as_str()).build());
        let executor = test_utils::build_test_fs_executor(path.as_str(), false);
        let metadata_store = test_utils::metadata_store_over(object, executor);

        // A self-referential revision whose link is missing its media_type.
        let manifest_content = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;
        let revision =
            test_utils::put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Digest(revision.clone()),
                    revision.clone(),
                )],
            )
            .await
            .unwrap();

        let config = test_config(&path);
        // The deprecated -r alias builds the retention checker, so the vec has
        // a shared checker to order against; --commit lets the rebuild write
        // its repair. run_phases directly: `run` would refuse the mutating
        // configuration on the memory strategy.
        let options = Options {
            retention: true,
            commit: true,
            ..base_options()
        };
        let mut cmd = Command::new(&options, &config).await.unwrap();
        let saw_media_type = Arc::new(Mutex::new(None));
        cmd.namespace_checkers.insert(
            1,
            Box::new(SpyChecker {
                metadata_store: cmd.ctx.metadata_store.clone(),
                revision: revision.clone(),
                saw_media_type: saw_media_type.clone(),
            }),
        );
        cmd.run_phases(Utc::now()).await.expect("the run completes");

        assert_eq!(
            *saw_media_type.lock().unwrap(),
            Some(true),
            "the spy (inserted right after the rebuild) must observe the repaired media_type"
        );
    }

    /// The per-category gate math: the aliases open only the policy gate,
    /// `--commit` opens both, and `--dry-run` closes the alias gate.
    #[test]
    fn mutate_gates_follow_flags() {
        let alias = Options {
            retention: true,
            ..base_options()
        };
        assert!(alias.policy_mutate() && alias.any_mutate() && !alias.commit);

        let alias_dry = Options {
            retention: true,
            dry_run: true,
            ..base_options()
        };
        assert!(!alias_dry.policy_mutate() && !alias_dry.any_mutate());

        let commit = Options {
            commit: true,
            ..base_options()
        };
        assert!(commit.policy_mutate() && commit.any_mutate());

        assert!(!base_options().any_mutate());
    }
}
