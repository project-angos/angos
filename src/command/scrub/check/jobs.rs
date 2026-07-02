//! [`JobChecker`] repairs the structure of `_jobs/`, distinct from
//! [`super::OrphanJobChecker`] which deletes config-drifted job payloads.
//!
//! - lock-key index reconcile (always on): drop a dedup-index file whose pending
//!   envelope has vanished or does not parse, and re-index a present body whose
//!   entry is missing, corrupt, or stale. This is system bookkeeping, so it
//!   needs no gate.
//! - unknown-queue removal (under `--prune-unknown`): remove an
//!   `_jobs/<state>/<queue>` directory whose name is not a recognized [`Queue`].
//!   Without the flag it is report-only, and even with it the directory is
//!   removed only once quiescent for the grace period (a newer replica may
//!   operate a queue this binary does not recognize).
//!
//! Mutations go through [`JobStore`] directly (conditional engine transactions)
//! rather than the [`ActionSink`]. The checker carries `dry_run` and skips every
//! mutation when set.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    command::scrub::{check::StoreChecker, error::Error, executor::ActionSink},
    registry::job_store::{JobState, JobStore, MAX_SCAN, Queue},
};

/// Recognized durable queues; any other `_jobs/<state>/<dir>` is unknown.
const KNOWN_QUEUES: [Queue; 2] = [Queue::Cache, Queue::Replication];

/// Maximum pending bodies enumerated per queue for the re-index direction.
/// Deeper queues re-index the leading page now and the rest on a later run.
const MAX_REINDEX_SCAN: u16 = MAX_SCAN;

/// Structural reconcile of `_jobs/`: lock-key index reconcile (always) plus
/// unknown-queue-directory removal (under `--prune-unknown`, age-gated by
/// `unknown_queue_grace`).
pub struct JobChecker {
    job_store: Arc<JobStore>,
    dry_run: bool,
    prune_unknown: bool,
    unknown_queue_grace: Duration,
    /// Run-level cancellation, checked per item so a lost maintenance lock
    /// stops further reconcile mutations.
    cancel: CancellationToken,
}

impl JobChecker {
    #[must_use]
    pub fn new(
        job_store: Arc<JobStore>,
        dry_run: bool,
        prune_unknown: bool,
        unknown_queue_grace: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            job_store,
            dry_run,
            prune_unknown,
            unknown_queue_grace,
            cancel,
        }
    }

    /// Retire every dangling lock-key index in `queue` whose pending envelope
    /// has vanished. Returns how many orphans were found (in dry-run, how many
    /// would be retired).
    async fn reconcile_lock_key_index(&self, queue: Queue) -> Result<u64, Error> {
        let mut orphans: u64 = 0;
        let mut after: Option<String> = None;
        // Drain one keyset page at a time so resident state stays at one page
        // regardless of how many dedup indexes the queue holds.
        loop {
            let (stems, next) = self
                .job_store
                .list_lock_key_index_keys_page(queue, after.as_deref())
                .await
                .map_err(|e| {
                    Error::JobQueue(format!("failed to list {queue} lock-key indexes: {e}"))
                })?;
            for stem in stems {
                if self.cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if self.dry_run {
                    // Probe read-only so the dry-run count matches a real run.
                    let orphaned = self
                        .job_store
                        .is_orphan_lock_key_index(queue, &stem)
                        .await
                        .map_err(|e| {
                            Error::JobQueue(format!(
                                "failed to probe lock-key index '{queue}/{stem}': {e}"
                            ))
                        })?;
                    if orphaned {
                        orphans += 1;
                        warn!("DRY RUN: would reconcile dangling lock-key index '{queue}/{stem}'");
                    }
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
            match next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(orphans)
    }

    /// Re-index every present pending body in `queue` whose lock-key dedup index
    /// is missing or stale, the reverse of [`Self::reconcile_lock_key_index`].
    /// Returns how many bodies were re-indexed (in dry-run, how many would be).
    ///
    /// A body whose `job:{lock_key}` execution lock is held is skipped by the
    /// store: a claimed job's index was retired on purpose, and re-indexing it
    /// would silently coalesce (drop) a same-lock-key event enqueued while the
    /// worker executes. Idempotent.
    async fn reindex_lock_key_index(&self, queue: Queue) -> Result<u64, Error> {
        let bodies = self
            .job_store
            .list_pending(queue, MAX_REINDEX_SCAN)
            .await
            .map_err(|e| Error::JobQueue(format!("failed to list {queue} pending bodies: {e}")))?;

        let mut reindexed: u64 = 0;
        for storage_key in bodies {
            if self.cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if self.dry_run {
                // Probe read-only so the dry-run count matches a real run.
                let needs = self
                    .job_store
                    .needs_reindex_pending(queue, &storage_key)
                    .await
                    .map_err(|e| {
                        Error::JobQueue(format!(
                            "failed to probe pending body '{queue}/{storage_key}': {e}"
                        ))
                    })?;
                if needs {
                    reindexed += 1;
                    warn!(
                        "DRY RUN: would re-index present pending body '{queue}/{storage_key}' whose lock-key index is missing or stale"
                    );
                }
                continue;
            }
            match self
                .job_store
                .reindex_orphan_pending_body(queue, &storage_key)
                .await
            {
                Ok(true) => reindexed += 1,
                Ok(false) => {}
                Err(e) => warn!("failed to re-index pending body '{queue}/{storage_key}': {e}"),
            }
        }
        Ok(reindexed)
    }

    /// Diff the `_jobs/<state>/` queue directories against [`KNOWN_QUEUES`] and,
    /// under `--prune-unknown`, remove each unknown directory (report-only
    /// otherwise). Returns how many unknown directories were found.
    ///
    /// Removal is age-gated: the directory may belong to a newer replica, so it
    /// is deleted only once its newest object has been quiescent for
    /// `unknown_queue_grace`; an object whose age cannot be established keeps
    /// the directory.
    async fn reconcile_unknown_queues(&self, state: JobState) -> Result<u64, Error> {
        let known = KNOWN_QUEUES.map(Queue::as_str);
        let dirs =
            self.job_store.list_queue_dirs(state).await.map_err(|e| {
                Error::JobQueue(format!("failed to list job queue directories: {e}"))
            })?;

        let mut unknown: u64 = 0;
        for name in dirs {
            if self.cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
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
            if !self.unknown_queue_quiescent(state, &name).await {
                continue;
            }
            if self.dry_run {
                warn!("DRY RUN: would remove unknown job-queue directory '_jobs/{state:?}/{name}'");
                continue;
            }
            // Re-probe immediately before the prefix delete: an object written
            // between the first probe and now (a newer replica going live)
            // keeps the directory this run.
            if !self.unknown_queue_quiescent(state, &name).await {
                continue;
            }
            info!("removing unknown job-queue directory '_jobs/{state:?}/{name}'");
            if let Err(e) = self.job_store.delete_unknown_queue_dir(state, &name).await {
                warn!("failed to remove unknown job-queue directory '{name}': {e}");
            }
        }
        Ok(unknown)
    }

    /// Whether the unknown queue directory has been quiescent for the grace: its
    /// newest object is older than `unknown_queue_grace`. An unknown age or a
    /// probe failure keeps the directory.
    async fn unknown_queue_quiescent(&self, state: JobState, name: &str) -> bool {
        let newest = match self.job_store.unknown_queue_newest_mtime(state, name).await {
            Ok(newest) => newest,
            Err(e) => {
                warn!("keeping unknown job-queue directory '_jobs/{state:?}/{name}': {e}");
                return false;
            }
        };
        if let Some(newest) = newest
            && Utc::now() - newest < self.unknown_queue_grace
        {
            info!(
                "keeping unknown job-queue directory '_jobs/{state:?}/{name}': newest object modified at {newest}, within the maintenance grace"
            );
            return false;
        }
        true
    }
}

#[async_trait]
impl StoreChecker for JobChecker {
    /// `sink` is unused: the reconcile mutates through the `JobStore`'s own
    /// conditional transactions. The trait signature drops the checker into the
    /// same store-node machinery as the others.
    async fn check_all(&self, _sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        let mut total_orphans: u64 = 0;
        let mut total_reindexed: u64 = 0;
        let mut total_unknown: u64 = 0;
        // One step's failure must not skip the remaining queues or scans, so
        // every step is warn-and-continue and the summary reports what ran.
        for queue in KNOWN_QUEUES {
            // Reconcile the lock-key index against pending bodies in both
            // directions: retire dangling entries and re-index stale/missing ones.
            match self.reconcile_lock_key_index(queue).await {
                Ok(n) => total_orphans += n,
                Err(e) => warn!("lock-key index reconcile for queue '{queue}' failed: {e}"),
            }
            match self.reindex_lock_key_index(queue).await {
                Ok(n) => total_reindexed += n,
                Err(e) => warn!("pending re-index for queue '{queue}' failed: {e}"),
            }
        }
        for state in [JobState::Pending, JobState::Failed] {
            match self.reconcile_unknown_queues(state).await {
                Ok(n) => total_unknown += n,
                Err(e) => warn!("unknown-queue scan for state '{state:?}' failed: {e}"),
            }
        }
        let (retired, reindexed) = if self.dry_run {
            ("would retire", "would re-index")
        } else {
            ("retired", "re-indexed")
        };
        info!(
            "Job reconcile: {retired} {total_orphans} dangling lock-key index file(s), {reindexed} {total_reindexed} present body/bodies with a missing or stale index, found {total_unknown} unknown queue director(ies)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Duration;
    use serde_json::json;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use angos_storage::{
        BoxedReader, ByteStream, ChildrenPage, Error as StorageError, MemoryObjectStore,
        MultipartUploadPage, ObjectMeta, ObjectStore, Page, fs::Backend as StorageFsBackend,
    };
    use angos_tx_engine::store::Store;

    use crate::{
        command::scrub::{
            check::{StoreChecker, jobs::JobChecker},
            executor::{ActionSink, DryRunSink},
            test_support::AgedObjectStore,
        },
        metrics_provider,
        registry::{
            job_store::{JobEnvelope, JobState, JobStore, Queue, serialize_lock_key_index},
            metadata_store::MetadataStore,
            test_utils::{build_store, build_test_fs_executor, locked_executor_over},
        },
    };

    /// On-disk path of a pending envelope, reconstructed because `path_builder`
    /// is private to `registry`.
    fn pending_path(queue: Queue, storage_key: &str) -> String {
        format!("_jobs/pending/{}/{storage_key}.json", queue.as_str())
    }

    /// FS-backed metadata store (no S3), the raw `Store` for planting objects,
    /// and the initialized metrics registry the enqueue path records into.
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

    /// A pending cache envelope whose `lock_key` carries `:` and `/`, exercising
    /// the percent-encoded-stem path of the reconcile.
    fn cache_envelope_with_colon() -> JobEnvelope {
        JobEnvelope::new(
            Queue::Cache,
            "cache.fetch_blob",
            "cache.ns/app:sha256:abc",
            &json!({ "namespace": "ns/app", "digest": "sha256:abc" }),
        )
        .unwrap()
    }

    /// Reconcile retires a dangling lock-key index and leaves a live one
    /// untouched, exercising the percent-encoded stem.
    #[tokio::test]
    async fn reconcile_retires_orphan_lock_key_index_with_encoded_stem() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        let envelope = cache_envelope_with_colon();
        let lock_key = envelope.lock_key.clone();
        job_store.enqueue(envelope).await.unwrap();

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

        // With the pending file still present the reconcile is a no-op.
        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
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
        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
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

    /// Dry-run reconcile counts the orphan it *would* retire (not a hard zero),
    /// matching the real-run count, and still mutates nothing.
    #[tokio::test]
    async fn dry_run_reconcile_counts_would_retire_orphan() {
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

        let would_retire = JobChecker::new(
            job_store.clone(),
            true,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .reconcile_lock_key_index(Queue::Cache)
        .await
        .unwrap();
        assert_eq!(
            would_retire, 1,
            "dry-run must count the orphan it would retire, not report a hard zero"
        );
        assert_eq!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .len(),
            1,
            "the dry-run count must not retire the dangling index"
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

        JobChecker::new(
            job_store.clone(),
            true,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
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

        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            dirs.iter().any(|d| d == "bogus-queue"),
            "an unknown queue directory must survive without --prune-unknown; got {dirs:?}"
        );
    }

    /// With `--prune-unknown` (and not dry-run) an unknown queue directory past
    /// the grace (zero here, so any age qualifies) is removed; the known
    /// directories are untouched.
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

        JobChecker::new(
            job_store.clone(),
            false,
            true,
            Duration::zero(),
            CancellationToken::new(),
        )
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
        JobChecker::new(
            job_store.clone(),
            true,
            true,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(sink.as_mut())
        .await
        .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            dirs.iter().any(|d| d == "bogus-queue"),
            "dry-run must not remove the unknown queue directory even under --prune-unknown; got {dirs:?}"
        );
    }

    /// On-disk path of a lock-key dedup index file, reconstructed with the same
    /// percent-encoding because `path_builder` is private to `registry`.
    fn index_path(queue: Queue, lock_key: &str) -> String {
        let encoded: String = lock_key
            .chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => {
                    format!("%{:02X}", c as u32)
                }
                c => c.to_string(),
            })
            .collect();
        format!("_jobs/index/{}/{encoded}.json", queue.as_str())
    }

    /// A present body whose lock-key index is missing is re-indexed, so a
    /// subsequent enqueue of the same `lock_key` is deduped.
    #[tokio::test]
    async fn reindex_recreates_missing_index_and_dedupes_enqueue() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        let envelope = cache_envelope_with_colon();
        let lock_key = envelope.lock_key.clone();
        job_store.enqueue(envelope).await.unwrap();

        // Delete the index out-of-band, leaving a present body with no entry.
        store
            .delete(&index_path(Queue::Cache, &lock_key))
            .await
            .unwrap();
        assert!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .is_empty(),
            "the index must be gone before the reconcile re-indexes it"
        );

        // Real-run reconcile re-indexes the present body.
        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();

        let (indexed_storage_key, _) = job_store
            .get_lock_key_index_raw(Queue::Cache, &lock_key)
            .await
            .unwrap()
            .expect("the reconcile must recreate the missing index");
        let pending = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "exactly one body before the dedup enqueue"
        );
        assert_eq!(
            indexed_storage_key, pending[0],
            "the re-indexed entry must point at the present body"
        );

        // A subsequent enqueue of the same lock_key is now deduped: still one body.
        job_store
            .enqueue(cache_envelope_with_colon())
            .await
            .unwrap();
        assert_eq!(
            job_store
                .list_pending(Queue::Cache, 10)
                .await
                .unwrap()
                .len(),
            1,
            "the re-indexed entry must dedupe a subsequent same-lock_key enqueue"
        );
    }

    /// A present body whose lock-key index is stale (points at a nonexistent
    /// body) is corrected to point at the present body rather than retired.
    #[tokio::test]
    async fn reindex_corrects_present_body_with_stale_entry() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        let envelope = cache_envelope_with_colon();
        let lock_key = envelope.lock_key.clone();
        job_store.enqueue(envelope).await.unwrap();
        let pending = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        let present_storage_key = pending[0].clone();

        // Overwrite the index to point at a bogus stem whose body does not exist.
        let stale_body = serialize_lock_key_index("0000000000000000-stale-phantom").unwrap();
        store
            .put(
                &index_path(Queue::Cache, &lock_key),
                bytes::Bytes::from(stale_body),
            )
            .await
            .unwrap();

        // Real-run reconcile corrects the stale entry to the present body.
        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();

        let (indexed_storage_key, _) = job_store
            .get_lock_key_index_raw(Queue::Cache, &lock_key)
            .await
            .unwrap()
            .expect("the corrected index must be present");
        assert_eq!(
            indexed_storage_key, present_storage_key,
            "the stale entry must be corrected to point at the present body, not retired"
        );
        assert_eq!(
            job_store
                .list_pending(Queue::Cache, 10)
                .await
                .unwrap()
                .len(),
            1,
            "correcting the entry must not touch the pending body"
        );
    }

    /// Dry-run re-indexes nothing: a present body whose index is missing stays
    /// missing, while the count reports what a real run would re-index.
    #[tokio::test]
    async fn dry_run_reindex_leaves_missing_index_but_counts_it() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        let envelope = cache_envelope_with_colon();
        let lock_key = envelope.lock_key.clone();
        job_store.enqueue(envelope).await.unwrap();
        store
            .delete(&index_path(Queue::Cache, &lock_key))
            .await
            .unwrap();

        let would_reindex = JobChecker::new(
            job_store.clone(),
            true,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .reindex_lock_key_index(Queue::Cache)
        .await
        .unwrap();
        assert_eq!(
            would_reindex, 1,
            "dry-run must count the present body it would re-index, not report a hard zero"
        );
        assert!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .is_empty(),
            "dry-run must not recreate the missing index"
        );
    }

    /// A claimed job's pending body is NOT re-indexed while its execution lock
    /// is held: the claim retired the index on purpose so a same-lock_key
    /// enqueue starts a fresh pending file instead of coalescing into the
    /// in-flight job. Once the claim releases, the body is re-indexed again.
    #[tokio::test]
    async fn reindex_skips_claimed_body_while_execution_lock_held() {
        let (metadata_store, _store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        job_store
            .enqueue(cache_envelope_with_colon())
            .await
            .unwrap();
        let claimed = job_store
            .claim_one(Queue::Cache)
            .await
            .unwrap()
            .claimed
            .expect("the job must be claimable");
        assert!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .is_empty(),
            "the claim must retire the dedup index"
        );

        // A real-run reconcile while the worker executes: the present body's
        // missing index must NOT be recreated.
        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();
        assert!(
            job_store
                .list_lock_key_index_keys(Queue::Cache)
                .await
                .unwrap()
                .is_empty(),
            "a claimed job's body must not be re-indexed while the execution lock is held"
        );

        // The worker crashes without completing: the lock releases, the body
        // remains, and the next reconcile re-indexes it.
        claimed.session.release().await;
        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
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
            "once the execution lock releases, the present body is re-indexed"
        );
    }

    /// A correct present index is a re-index no-op: a live enqueue's index is
    /// left untouched.
    #[tokio::test]
    async fn reindex_is_noop_for_correct_present_index() {
        let (metadata_store, _store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));

        let envelope = cache_envelope_with_colon();
        let lock_key = envelope.lock_key.clone();
        job_store.enqueue(envelope).await.unwrap();
        let (before, _) = job_store
            .get_lock_key_index_raw(Queue::Cache, &lock_key)
            .await
            .unwrap()
            .expect("a live enqueue writes an index");

        let reindexed = JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .reindex_lock_key_index(Queue::Cache)
        .await
        .unwrap();
        assert_eq!(reindexed, 0, "a correct present index must be a no-op");

        let (after, _) = job_store
            .get_lock_key_index_raw(Queue::Cache, &lock_key)
            .await
            .unwrap()
            .expect("the index survives the idempotent reconcile");
        assert_eq!(before, after, "the live index must be left untouched");
    }

    /// Job store + `Store` façade over an arbitrary object backend, for the
    /// tests that need control over `last_modified` or failure injection.
    fn job_store_over(object: Arc<dyn ObjectStore>) -> (Arc<JobStore>, Arc<Store>) {
        metrics_provider::init_for_tests();
        let store = build_store(object.clone(), locked_executor_over(object));
        (Arc::new(JobStore::new(store.clone(), "jobs-test")), store)
    }

    /// An unknown queue directory whose newest object is fresher than the grace
    /// survives `--prune-unknown`: it may be a newer replica's live queue.
    #[tokio::test]
    async fn fresh_unknown_queue_dir_is_kept_within_grace() {
        let (metadata_store, store, _dir) = fs_store();
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));
        plant_unknown_queue(&store).await;

        JobChecker::new(
            job_store.clone(),
            false,
            true,
            Duration::hours(1),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            dirs.iter().any(|d| d == "bogus-queue"),
            "a fresh unknown queue directory must survive within the grace; got {dirs:?}"
        );
    }

    /// An unknown queue directory whose newest object has been quiescent past
    /// the grace is removed under `--prune-unknown`.
    #[tokio::test]
    async fn aged_unknown_queue_dir_is_removed_past_grace() {
        let memory = Arc::new(MemoryObjectStore::new());
        let aged: Arc<dyn ObjectStore> = Arc::new(AgedObjectStore::new(memory, Duration::hours(2)));
        let (job_store, store) = job_store_over(aged);
        plant_unknown_queue(&store).await;

        JobChecker::new(
            job_store.clone(),
            false,
            true,
            Duration::hours(1),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            !dirs.iter().any(|d| d == "bogus-queue"),
            "an unknown queue directory quiescent past the grace must be removed; got {dirs:?}"
        );
    }

    /// An object without a `last_modified` keeps its directory: the age cannot
    /// be established, so the removal is refused even at zero grace.
    #[tokio::test]
    async fn unknown_queue_dir_with_unknowable_age_is_kept() {
        // `MemoryObjectStore` reports no `last_modified` on `head`.
        let memory: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::new());
        let (job_store, store) = job_store_over(memory);
        plant_unknown_queue(&store).await;

        JobChecker::new(
            job_store.clone(),
            false,
            true,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .unwrap();

        let dirs = job_store.list_queue_dirs(JobState::Pending).await.unwrap();
        assert!(
            dirs.iter().any(|d| d == "bogus-queue"),
            "a directory whose age cannot be established must never be removed; got {dirs:?}"
        );
    }

    /// Delegating store that fails `list_children` under one prefix, so one
    /// queue's index listing can be made to error while everything else works.
    struct FailingChildrenStore {
        inner: Arc<MemoryObjectStore>,
        fail_prefix: &'static str,
    }

    #[async_trait]
    impl ObjectStore for FailingChildrenStore {
        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.inner.get(key).await
        }
        async fn get_stream(
            &self,
            key: &str,
            offset: Option<u64>,
        ) -> Result<(BoxedReader, u64), StorageError> {
            self.inner.get_stream(key, offset).await
        }
        async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }
        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }
        async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
            self.inner.delete_prefix(prefix).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
        ) -> Result<Page<String>, StorageError> {
            self.inner.list(prefix, n, token).await
        }
        async fn list_children(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
            start_after: Option<String>,
        ) -> Result<ChildrenPage, StorageError> {
            if prefix.starts_with(self.fail_prefix) {
                return Err(StorageError::Backend("injected listing failure".into()));
            }
            self.inner
                .list_children(prefix, n, token, start_after)
                .await
        }
        async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
            self.inner.copy(source, destination).await
        }
        async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.create_upload(key).await
        }
        async fn write_upload(
            &self,
            key: &str,
            body: ByteStream,
            len: Option<u64>,
        ) -> Result<u64, StorageError> {
            self.inner.write_upload(key, body, len).await
        }
        async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.complete_upload(key).await
        }
        async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(key).await
        }
        async fn list_multipart_uploads(
            &self,
            key_marker: Option<&str>,
            upload_id_marker: Option<&str>,
        ) -> Result<MultipartUploadPage, StorageError> {
            self.inner
                .list_multipart_uploads(key_marker, upload_id_marker)
                .await
        }
    }

    /// A failing cache index listing must not skip the replication queue's
    /// reconcile or the unknown-dir scans: `check_all` warns and continues.
    #[tokio::test]
    async fn queue_listing_failure_does_not_skip_other_queue() {
        let failing: Arc<dyn ObjectStore> = Arc::new(FailingChildrenStore {
            inner: Arc::new(MemoryObjectStore::new()),
            fail_prefix: "_jobs/index/cache",
        });
        let (job_store, store) = job_store_over(failing);

        // A dangling replication index the reconcile must still retire.
        let stale = serialize_lock_key_index("0000000000000000-gone").unwrap();
        store
            .put(
                &index_path(Queue::Replication, "repl.ns:sha256:x"),
                Bytes::from(stale),
            )
            .await
            .unwrap();

        JobChecker::new(
            job_store.clone(),
            false,
            false,
            Duration::zero(),
            CancellationToken::new(),
        )
        .check_all(&mut (DryRunSink))
        .await
        .expect("check_all must warn and continue, not abort");

        assert!(
            job_store
                .list_lock_key_index_keys(Queue::Replication)
                .await
                .unwrap()
                .is_empty(),
            "the replication queue must still be reconciled after the cache listing failure"
        );
    }
}
