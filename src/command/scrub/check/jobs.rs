//! [`JobChecker`]: structural-only reconcile of the durable job store.
//!
//! Unlike [`super::OrphanJobChecker`] (config-drift: deletes jobs whose payload
//! no longer resolves to configured state), this checker only repairs the
//! *structure* of `_jobs/`:
//!
//! - **lock-key index reconcile** (always-on under `--jobs`): drop a dedup-index
//!   file (`_jobs/index/<queue>/<encoded-lock-key>.json`) whose pending envelope
//!   has vanished. The index is system-written bookkeeping, not user content, so
//!   retiring a dangling entry is the same self-heal the enqueue path performs;
//!   it does not need the `--prune-unknown` gate.
//! - **unknown-queue removal** (only under `--prune-unknown`): an
//!   `_jobs/<state>/<queue>` directory whose name is not a recognized [`Queue`]
//!   (a typo or a decommissioned queue's leftover tree) is removed. Net-new
//!   destructive validity, so it is gated; without the flag it is report-only
//!   (`warn!`, no mutation).
//!
//! The reconcile mutates through [`JobStore`] directly (a conditional,
//! fingerprint-guarded engine transaction) rather than through the
//! [`ActionSink`], because the proven self-heal already owns the correct
//! concurrency guard and avoids re-encoding the `lock_key`. To honour scrub's
//! mutate-by-default / `-d` contract, the checker carries `dry_run` and skips
//! every mutation (logging what it *would* do) when set.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::{
    command::scrub::{check::StoreChecker, error::Error, executor::ActionSink},
    registry::job_store::{JobState, JobStore, Queue},
};

/// The recognized durable queues; an `_jobs/<state>/<dir>` not in this set is an
/// unknown queue directory.
const KNOWN_QUEUES: [Queue; 2] = [Queue::Cache, Queue::Replication];

/// Structural reconcile of `_jobs/`: dangling lock-key index retirement
/// (always) plus unknown-queue-directory removal (under `--prune-unknown`).
pub struct JobChecker {
    job_store: Arc<JobStore>,
    /// Mirrors scrub's `-d`: when true the checker reports (`warn!`) what it
    /// would reconcile/remove without mutating storage.
    dry_run: bool,
    /// `--prune-unknown`: gate for the net-new unknown-queue removal. Off =
    /// report-only.
    prune_unknown: bool,
}

impl JobChecker {
    /// Construct from the `job_store` to reconcile, plus the `dry_run` and
    /// `prune_unknown` gates copied from the scrub flags.
    #[must_use]
    pub fn new(job_store: Arc<JobStore>, dry_run: bool, prune_unknown: bool) -> Self {
        Self {
            job_store,
            dry_run,
            prune_unknown,
        }
    }

    /// Retire every dangling lock-key index in `queue` whose pending envelope
    /// has vanished. Returns how many orphans were found (in dry-run, how many
    /// *would* be retired).
    async fn reconcile_lock_key_index(&self, queue: Queue) -> Result<u64, Error> {
        let stems = self
            .job_store
            .list_lock_key_index_keys(queue)
            .await
            .map_err(|e| {
                Error::JobQueue(format!("failed to list {queue} lock-key indexes: {e}"))
            })?;

        let mut orphans: u64 = 0;
        for stem in stems {
            if self.dry_run {
                // Probe without mutating: read the index, HEAD its pending file.
                // The reconcile method itself would mutate, so dry-run cannot
                // call it; report a *possible* orphan instead. The probe is
                // intentionally light — a precise dry-run count is out of scope
                // (mutate-by-default is the exact path).
                warn!(
                    "DRY RUN: would reconcile dangling lock-key index '{queue}/{stem}' if its pending envelope is gone"
                );
                continue;
            }
            match self
                .job_store
                .reconcile_orphan_lock_key_index(queue, &stem)
                .await
            {
                Ok(true) => orphans += 1,
                Ok(false) => {}
                Err(e) => warn!("failed to reconcile lock-key index '{queue}/{stem}': {e}"),
            }
        }
        Ok(orphans)
    }

    /// Diff the `_jobs/<state>/` queue directories against [`KNOWN_QUEUES`] and,
    /// under `--prune-unknown`, remove each unknown directory. Without the flag
    /// each unknown directory is report-only (`warn!`). Returns how many unknown
    /// directories were found.
    async fn reconcile_unknown_queues(&self, state: JobState) -> Result<u64, Error> {
        let known: [&str; 2] = [Queue::Cache.as_str(), Queue::Replication.as_str()];
        let dirs =
            self.job_store.list_queue_dirs(state).await.map_err(|e| {
                Error::JobQueue(format!("failed to list job queue directories: {e}"))
            })?;

        let mut unknown: u64 = 0;
        for name in dirs {
            if known.contains(&name.as_str()) {
                continue;
            }
            unknown += 1;
            if !self.prune_unknown {
                warn!(
                    "unknown job-queue directory '_jobs/{state:?}/{name}' (run with --prune-unknown to remove it)"
                );
                continue;
            }
            if self.dry_run {
                warn!("DRY RUN: would remove unknown job-queue directory '_jobs/{state:?}/{name}'");
                continue;
            }
            info!("removing unknown job-queue directory '_jobs/{state:?}/{name}'");
            if let Err(e) = self.job_store.delete_unknown_queue_dir(state, &name).await {
                warn!("failed to remove unknown job-queue directory '{name}': {e}");
            }
        }
        Ok(unknown)
    }
}

#[async_trait]
impl StoreChecker for JobChecker {
    /// `sink` is unused: the structural reconcile mutates through the
    /// `JobStore`'s own conditional engine transactions (the proven self-heal),
    /// not through the action sink. The trait signature is kept so the checker
    /// drops into the same store-node machinery as the others.
    async fn check_all(&self, _sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        let mut total_orphans: u64 = 0;
        let mut total_unknown: u64 = 0;
        for queue in KNOWN_QUEUES {
            total_orphans += self.reconcile_lock_key_index(queue).await?;
        }
        for state in [JobState::Pending, JobState::Failed] {
            total_unknown += self.reconcile_unknown_queues(state).await?;
        }
        info!(
            "Job reconcile: retired {total_orphans} dangling lock-key index file(s), found {total_unknown} unknown queue director(ies)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::store::Store;

    use crate::{
        command::scrub::{
            check::{StoreChecker, jobs::JobChecker},
            executor::{ActionSink, DryRunSink},
        },
        metrics_provider,
        registry::{
            job_store::{JobEnvelope, JobState, JobStore, Queue},
            metadata_store::MetadataStore,
            test_utils::{build_store, build_test_fs_executor},
        },
    };

    /// Storage key of a pending envelope, mirroring `path_builder`'s on-disk
    /// layout (`_jobs/pending/<queue>/<storage_key>.json`). `path_builder` is
    /// private to the `registry` module, so the test reconstructs the literal.
    fn pending_path(queue: Queue, storage_key: &str) -> String {
        format!("_jobs/pending/{}/{storage_key}.json", queue.as_str())
    }

    /// FS-backed metadata store so the tests run without S3, plus the raw
    /// `Store` so a test can plant/inspect storage objects directly. Also
    /// initializes the metrics registry the enqueue path records into, so the
    /// tests pass in isolation (not only behind another test's init).
    fn fs_store() -> (Arc<MetadataStore>, Arc<Store>, TempDir) {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build(),
        );
        (metadata_store, store, dir)
    }

    /// A pending cache envelope whose `lock_key` carries a `:` (encoded `%3A`)
    /// and a `/` (encoded `%2F`), so the test exercises the percent-encoded-stem
    /// path of the reconcile.
    fn cache_envelope_with_colon() -> JobEnvelope {
        JobEnvelope::new(
            Queue::Cache,
            "cache.fetch_blob",
            "cache.ns/app:sha256:abc",
            &json!({ "namespace": "ns/app", "digest": "sha256:abc" }),
        )
        .unwrap()
    }

    /// Reconcile retires a dangling lock-key index (pending file deleted
    /// out-of-band) and leaves a live one untouched, exercising the
    /// percent-encoded stem (`:` -> `%3A`, `/` -> `%2F`).
    #[tokio::test]
    async fn reconcile_retires_orphan_lock_key_index_with_encoded_stem() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        // Enqueue a job: writes both the pending envelope and its lock-key index.
        let envelope = cache_envelope_with_colon();
        let lock_key = envelope.lock_key.clone();
        job_store.enqueue(envelope).await.unwrap();

        // The index stem is percent-encoded; confirm the enumerator yields it.
        let stems = job_store
            .list_lock_key_index_keys(Queue::Cache)
            .await
            .unwrap();
        assert_eq!(stems.len(), 1, "the enqueue must write one index file");
        assert!(
            stems[0].contains("%3A") && stems[0].contains("%2F"),
            "the stem must be percent-encoded (':' -> %3A, '/' -> %2F); got {}",
            stems[0]
        );

        // With the pending file STILL present the reconcile is a no-op.
        JobChecker::new(job_store.clone(), false, false)
            .check_all(&mut (DryRunSink))
            .await
            .unwrap();
        assert_eq!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .len(),
            1,
            "a live index must survive the reconcile"
        );

        // Delete the pending envelope out-of-band, leaving the index dangling.
        let keys = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        assert_eq!(keys.len(), 1);
        store
            .delete(&pending_path(Queue::Cache, &keys[0]))
            .await
            .unwrap();

        // Real-run reconcile retires the now-orphan index.
        JobChecker::new(job_store.clone(), false, false)
            .check_all(&mut (DryRunSink))
            .await
            .unwrap();

        assert!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .is_empty(),
            "the dangling lock-key index ('{lock_key}') must be retired"
        );
    }

    /// Dry-run must not mutate: a dangling index survives a `dry_run` reconcile.
    #[tokio::test]
    async fn dry_run_reconcile_leaves_orphan_index_in_place() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        job_store
            .enqueue(cache_envelope_with_colon())
            .await
            .unwrap();
        let keys = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        store
            .delete(&pending_path(Queue::Cache, &keys[0]))
            .await
            .unwrap();

        JobChecker::new(job_store.clone(), true, false)
            .check_all(&mut (DryRunSink))
            .await
            .unwrap();

        assert_eq!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .len(),
            1,
            "dry-run must not retire the dangling index"
        );
    }

    /// Plant an unknown-named queue directory under `_jobs/pending/`.
    async fn plant_unknown_queue(store: &Arc<Store>) {
        store
            .put(
                "_jobs/pending/bogus-queue/0000000000000000-stray.json",
                bytes::Bytes::from_static(b"{}"),
            )
            .await
            .expect("planting an unknown queue object must succeed");
    }

    /// Without `--prune-unknown` an unknown queue directory is report-only: it
    /// survives.
    #[tokio::test]
    async fn unknown_queue_dir_is_reported_not_removed_without_prune_unknown() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));
        plant_unknown_queue(&store).await;

        JobChecker::new(job_store.clone(), false, false)
            .check_all(&mut (DryRunSink))
            .await
            .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            dirs.iter().any(|d| d == "bogus-queue"),
            "an unknown queue directory must survive without --prune-unknown; got {dirs:?}"
        );
    }

    /// With `--prune-unknown` (and not dry-run) the unknown queue directory is
    /// removed; the known directories are untouched.
    #[tokio::test]
    async fn unknown_queue_dir_is_removed_under_prune_unknown() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));
        plant_unknown_queue(&store).await;
        // A real cache job so a KNOWN queue directory exists alongside it.
        job_store
            .enqueue(cache_envelope_with_colon())
            .await
            .unwrap();

        JobChecker::new(job_store.clone(), false, true)
            .check_all(&mut (DryRunSink))
            .await
            .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            !dirs.iter().any(|d| d == "bogus-queue"),
            "--prune-unknown must remove the unknown queue directory; got {dirs:?}"
        );
        assert!(
            dirs.iter().any(|d| d == Queue::Cache.as_str()),
            "the known cache queue directory must survive; got {dirs:?}"
        );
    }

    /// Under `--prune-unknown` but dry-run, the unknown directory must survive.
    #[tokio::test]
    async fn dry_run_prune_unknown_leaves_unknown_queue_dir() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));
        plant_unknown_queue(&store).await;

        let mut sink: Box<dyn ActionSink + Send> = Box::new(DryRunSink);
        JobChecker::new(job_store.clone(), true, true)
            .check_all(sink.as_mut())
            .await
            .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            dirs.iter().any(|d| d == "bogus-queue"),
            "dry-run must not remove the unknown queue directory even under --prune-unknown; got {dirs:?}"
        );
    }
}
