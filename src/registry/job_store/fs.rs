use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::registry::{
    fs_ops,
    job_store::{
        Error, JobEnvelope, JobStore, MAX_REPORTED_PENDING, STORAGE_KEY_PREFIX_LEN,
        make_storage_key, parse_lock_key_index, pending_ready_cutoff_prefix,
        serialize_dead_letter, serialize_lock_key_index,
    },
    path_builder,
};

#[derive(Clone, Debug, Deserialize)]
pub struct BackendConfig {
    pub root_dir: String,
}

/// On-disk payload for a lease file at `_jobs/leases/<lock_key>.json`.
#[derive(Serialize, Deserialize)]
struct LeaseFile {
    instance_id: String,
    worker_id: String,
    ttl_secs: u64,
}

/// Filesystem-backed `JobStore`.
///
/// Layout under `root_dir`:
/// ```text
/// _jobs/
///   pending/<queue>/<storage_key>.json
///   leases/<lock_key_encoded>.json
///   failed/<queue>/<storage_key>.json
/// ```
///
/// Storage keys have the form `<16-hex unix-millis>-<uuid>`; lexicographic
/// sort on filenames is therefore the same as sorting by `not_before`.
///
/// Lease creation uses `OpenOptions::create_new` for atomic exclusivity.
/// Stale leases are detected via `mtime + ttl < now` and stolen by removing
/// then re-creating the lease file.
pub struct Backend {
    root_dir: PathBuf,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Self {
        Self {
            root_dir: PathBuf::from(&config.root_dir),
        }
    }

    fn pending_dir(&self, queue: &str) -> PathBuf {
        self.root_dir.join(path_builder::job_pending_dir(queue))
    }

    fn pending_path(&self, queue: &str, storage_key: &str) -> PathBuf {
        self.root_dir
            .join(path_builder::job_pending_path(queue, storage_key))
    }

    fn lease_path(&self, lock_key: &str) -> PathBuf {
        self.root_dir.join(path_builder::job_lease_path(lock_key))
    }

    fn failed_path(&self, queue: &str, storage_key: &str) -> PathBuf {
        self.root_dir
            .join(path_builder::job_failed_path(queue, storage_key))
    }

    fn lock_key_index_path(&self, queue: &str, lock_key: &str) -> PathBuf {
        self.root_dir
            .join(path_builder::job_lock_key_index_path(queue, lock_key))
    }


    /// Write a new lease file using `create_new` atomicity. Returns the new
    /// instance id on success, or `Ok(None)` when the file already exists.
    async fn write_new_lease(
        &self,
        lock_path: &Path,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::Storage(format!("failed to create lease dir: {e}")))?;
        }

        let instance_id = Uuid::new_v4().to_string();
        let data = serde_json::to_vec(&LeaseFile {
            instance_id: instance_id.clone(),
            worker_id: worker_id.to_string(),
            ttl_secs,
        })
        .map_err(|e| Error::Storage(format!("lease serialization failed: {e}")))?;

        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&data)
                    .await
                    .map_err(|e| Error::Storage(format!("failed to write lease file: {e}")))?;
                Ok(Some(instance_id))
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(Error::Storage(format!("failed to create lease: {e}"))),
        }
    }

    /// Steal a stale lease via a rename-based ownership transfer. POSIX
    /// rename's atomicity is the CAS primitive here: two concurrent
    /// stealers cannot both succeed — the second `rename(lock_path, ...)`
    /// returns `ENOENT` because the first stealer already moved the
    /// source inode to its own side path.
    ///
    /// The previous implementation called `fs::remove_file(lock_path)`
    /// unconditionally and then `create_new` — if a fresh lease had been
    /// created in the meantime, the unconditional remove would silently
    /// nuke it.
    async fn try_steal_stale_lease(
        &self,
        lock_path: &Path,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error> {
        // 1. Snapshot the existing lease to decide staleness and capture
        //    the instance_id we expect to be stealing.
        let data = match fs::read(lock_path).await {
            Ok(d) => d,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Storage(format!("failed to read lease file: {e}"))),
        };
        let existing: LeaseFile = serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("corrupt lease file: {e}")))?;

        let mtime = fs::metadata(lock_path)
            .await
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let age_secs = mtime
            .elapsed()
            .unwrap_or(Duration::from_secs(u64::MAX))
            .as_secs();
        if age_secs < existing.ttl_secs {
            return Ok(None);
        }
        let expected_instance = existing.instance_id;

        debug!(
            lock_key = lock_path.display().to_string(),
            instance_id = expected_instance,
            age_secs,
            ttl_secs = existing.ttl_secs,
            "Stealing stale FS lease"
        );

        // 2. Atomically move the stale lease out of the way. POSIX rename's
        //    atomicity ensures that two concurrent stealers don't both
        //    succeed — the second gets ENOENT.
        let new_uuid = Uuid::new_v4().to_string();
        let side_path = lock_path.with_extension(format!("steal-{new_uuid}"));
        match fs::rename(lock_path, &side_path).await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(Error::Storage(format!(
                    "failed to rename stale lease: {e}"
                )));
            }
        }

        // 3. Verify the moved file is the stale lease we observed. If a
        //    fresh acquirer slipped a new lease in between our read at
        //    step 1 and our rename at step 2, we'd have just moved their
        //    fresh lease — restore it via `hard_link` (which won't
        //    clobber a new lease at `lock_path` if one has appeared).
        let moved_data = fs::read(&side_path)
            .await
            .map_err(|e| Error::Storage(format!("failed to read moved lease: {e}")))?;
        let moved: LeaseFile = serde_json::from_slice(&moved_data)
            .map_err(|e| Error::Storage(format!("corrupt moved lease: {e}")))?;
        if moved.instance_id != expected_instance {
            // Best-effort restore. If lock_path is now occupied by a new
            // acquirer, hard_link fails EEXIST and we just leak the side
            // file (cheap to scrub; only the inode survives, no
            // observable lease).
            if let Err(e) = fs::hard_link(&side_path, lock_path).await {
                warn!(
                    lock_key = lock_path.display().to_string(),
                    error = %e,
                    "Could not restore unexpectedly-moved lease; side path will need scrubbing",
                );
            }
            let _ = fs::remove_file(&side_path).await;
            return Ok(None);
        }

        // 4. Create our new lease at lock_path. A fresh acquirer may have
        //    raced us in the brief window when lock_path was absent
        //    (between steps 2 and here) — their create_new succeeds and
        //    ours gets EEXIST. Concede in that case.
        let our_data = serde_json::to_vec(&LeaseFile {
            instance_id: new_uuid.clone(),
            worker_id: worker_id.to_string(),
            ttl_secs,
        })
        .map_err(|e| Error::Storage(format!("lease serialization failed: {e}")))?;

        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&our_data).await.map_err(|e| {
                    Error::Storage(format!("failed to write lease file: {e}"))
                })?;
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&side_path).await;
                return Ok(None);
            }
            Err(e) => {
                let _ = fs::remove_file(&side_path).await;
                return Err(Error::Storage(format!("failed to create lease: {e}")));
            }
        }

        // 5. Successful steal — discard the side path.
        if let Err(e) = fs::remove_file(&side_path).await
            && e.kind() != ErrorKind::NotFound
        {
            warn!(
                lock_key = lock_path.display().to_string(),
                error = %e,
                "Failed to remove side path after steal",
            );
        }

        Ok(Some(new_uuid))
    }
}

#[async_trait]
impl JobStore for Backend {
    async fn put_pending(
        &self,
        queue: &str,
        envelope: &JobEnvelope,
        not_before: DateTime<Utc>,
    ) -> Result<String, Error> {
        let storage_key = make_storage_key(not_before, &envelope.id);
        let path = self.pending_path(queue, &storage_key);
        let data = serde_json::to_vec(envelope)
            .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?;
        fs_ops::atomic_write(&path, &data, false)
            .await
            .map_err(|e| Error::Storage(format!("failed to write pending job: {e}")))?;
        Ok(storage_key)
    }

    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
        let dir = self.pending_dir(queue);
        let mut entries = fs_ops::list_dir_or_empty(&dir)
            .await
            .map_err(|e| Error::Storage(format!("failed to list pending: {e}")))?;

        // Storage keys start with a fixed-width hex unix-millis prefix, so
        // lexicographic sort matches `not_before` order.
        entries.sort();

        Ok(entries
            .into_iter()
            .filter(|name| Path::new(name).extension().is_some_and(|ext| ext == "json"))
            .take(n as usize)
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect())
    }

    async fn read_pending(&self, queue: &str, storage_key: &str) -> Result<JobEnvelope, Error> {
        let path = self.pending_path(queue, storage_key);
        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == ErrorKind::NotFound {
                Error::NotFound
            } else {
                Error::Storage(format!("failed to read pending job: {e}"))
            }
        })?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse envelope: {e}")))
    }

    async fn try_create_lease(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error> {
        let path = self.lease_path(lock_key);
        match self.write_new_lease(&path, worker_id, ttl_secs).await? {
            Some(instance_id) => Ok(Some(instance_id)),
            None => self.try_steal_stale_lease(&path, worker_id, ttl_secs).await,
        }
    }

    async fn heartbeat_lease(
        &self,
        lock_key: &str,
        token: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<String, Error> {
        let path = self.lease_path(lock_key);

        // Open the lease file and operate on the **fd**, not the path. If a
        // concurrent stealer renames our inode out of the way and creates a
        // new lease at `path` between this open and the write below, our fd
        // still references the now-orphan inode — the write lands silently
        // on the orphan. Our next heartbeat opens by path and sees the new
        // owner's content, returning Err. The previous implementation read
        // and then wrote via `atomic_write` (temp + rename), which could
        // silently overwrite a stealer's fresh lease.
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| {
                if e.kind() == ErrorKind::NotFound {
                    Error::NotFound
                } else {
                    Error::Storage(format!("failed to open lease for heartbeat: {e}"))
                }
            })?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .await
            .map_err(|e| Error::Storage(format!("failed to read lease for heartbeat: {e}")))?;
        let existing: LeaseFile = serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("corrupt lease file: {e}")))?;

        if existing.instance_id != token {
            return Err(Error::Storage(format!(
                "heartbeat ownership check failed: expected {token}, found {}",
                existing.instance_id
            )));
        }

        let new_data = serde_json::to_vec(&LeaseFile {
            instance_id: token.to_string(),
            worker_id: worker_id.to_string(),
            ttl_secs,
        })
        .map_err(|e| Error::Storage(format!("heartbeat serialization failed: {e}")))?;

        // Truncate + rewind + write through the fd. The write itself
        // refreshes mtime (POSIX semantics), so no explicit `utimensat`
        // needed.
        file.set_len(0)
            .await
            .map_err(|e| Error::Storage(format!("heartbeat truncate failed: {e}")))?;
        file.rewind()
            .await
            .map_err(|e| Error::Storage(format!("heartbeat seek failed: {e}")))?;
        file.write_all(&new_data)
            .await
            .map_err(|e| Error::Storage(format!("heartbeat write failed: {e}")))?;

        Ok(token.to_string())
    }

    async fn remove_pending(&self, queue: &str, storage_key: &str) -> Result<(), Error> {
        let path = self.pending_path(queue, storage_key);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Storage(format!("failed to remove pending: {e}"))),
        }
    }

    async fn remove_lease(&self, lock_key: &str, token: &str) -> Result<(), Error> {
        let path = self.lease_path(lock_key);

        match fs::read(&path).await {
            Ok(data) => {
                if let Ok(existing) = serde_json::from_slice::<LeaseFile>(&data)
                    && existing.instance_id != token
                {
                    debug!(
                        lock_key,
                        expected = token,
                        found = existing.instance_id,
                        "Lease ownership changed, skipping remove"
                    );
                    return Ok(());
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                warn!(lock_key, error = %e, "Failed to read lease before remove");
            }
        }

        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Storage(format!("failed to remove lease: {e}"))),
        }
    }

    async fn move_to_failed(
        &self,
        queue: &str,
        storage_key: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error> {
        let data = serialize_dead_letter(envelope, last_error)?;
        let failed_path = self.failed_path(queue, storage_key);
        fs_ops::atomic_write(&failed_path, &data, false)
            .await
            .map_err(|e| Error::Storage(format!("failed to write dead-letter: {e}")))?;
        self.remove_pending(queue, storage_key).await?;
        // Drop the dedup index only if it still references this storage key —
        // a concurrent retry may have re-pointed it at a fresher envelope.
        self.remove_lock_key_index_if_matches(queue, &envelope.lock_key, storage_key)
            .await
    }

    /// O(1) dedup check backed by the per-`lock_key` index file. The index
    /// references a `storage_key`; if that pending file no longer exists the
    /// index is an orphan (left over by a crash mid-`complete`/`move_to_failed`)
    /// — delete it and report no-pending, so the next enqueue can proceed.
    async fn find_pending_with_lock_key(&self, queue: &str, lock_key: &str) -> Result<bool, Error> {
        let index_path = self.lock_key_index_path(queue, lock_key);
        let data = match fs::read(&index_path).await {
            Ok(d) => d,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) => {
                return Err(Error::Storage(format!(
                    "failed to read lock-key index: {e}"
                )));
            }
        };
        let index = parse_lock_key_index(&data)?;

        let pending_path = self.pending_path(queue, &index.storage_key);
        match fs::metadata(&pending_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Orphan: pending vanished but index lingers. Clear it so the
                // caller proceeds as a dedup miss.
                if let Err(remove_err) = fs::remove_file(&index_path).await
                    && remove_err.kind() != ErrorKind::NotFound
                {
                    warn!(
                        lock_key,
                        error = %remove_err,
                        "Failed to remove orphan lock-key index"
                    );
                }
                Ok(false)
            }
            Err(e) => Err(Error::Storage(format!(
                "failed to stat pending file via lock-key index: {e}"
            ))),
        }
    }

    async fn remove_lock_key_index_if_matches(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<(), Error> {
        let path = self.lock_key_index_path(queue, lock_key);
        match fs::read(&path).await {
            Ok(data) => match parse_lock_key_index(&data) {
                Ok(index) if index.storage_key == storage_key => {}
                Ok(_) => return Ok(()), // index now points elsewhere; leave it
                Err(_) => {} // corrupt — fall through and delete
            },
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                warn!(lock_key, error = %e, "Failed to read lock-key index before remove");
                return Ok(());
            }
        }
        // Best-effort: a race that re-wrote the index between our check and
        // this unlink, or an unrelated I/O blip, is fine — the next enqueue
        // either dedups against the new index or self-heals the orphan.
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => {
                warn!(lock_key, error = %e, "Failed to remove lock-key index");
                Ok(())
            }
        }
    }

    /// Atomic CAS-create of the dedup index. Uses `OpenOptions::create_new`
    /// (the same `O_EXCL` primitive the lease backend uses) so two racing
    /// producers cannot both claim the same `lock_key`.
    async fn try_claim_lock_key(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<bool, Error> {
        let path = self.lock_key_index_path(queue, lock_key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::Storage(format!("create index dir: {e}")))?;
        }
        let data = serialize_lock_key_index(storage_key)?;
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&data)
                    .await
                    .map_err(|e| Error::Storage(format!("write lock-key index: {e}")))?;
                Ok(true)
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(Error::Storage(format!("create lock-key index: {e}"))),
        }
    }

    async fn refresh_lock_key_index(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<(), Error> {
        let path = self.lock_key_index_path(queue, lock_key);
        let data = serialize_lock_key_index(storage_key)?;
        fs_ops::atomic_write(&path, &data, false)
            .await
            .map_err(|e| Error::Storage(format!("refresh lock-key index: {e}")))
    }

    async fn count_pending(&self, queue: &str, ready_horizon_secs: u64) -> Result<u64, Error> {
        let dir = self.pending_dir(queue);
        let mut entries = fs_ops::list_dir_or_empty(&dir)
            .await
            .map_err(|e| Error::Storage(format!("failed to count pending: {e}")))?;
        // Sort so the cutoff early-exit is correct: storage keys start with a
        // fixed-width hex unix-millis prefix, so lexicographic sort == time order.
        entries.sort();

        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut count: u64 = 0;
        for name in &entries {
            if Path::new(name).extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            if let Some(p) = name.get(..STORAGE_KEY_PREFIX_LEN)
                && p > cutoff_prefix.as_str()
            {
                break;
            }
            count += 1;
            if count >= MAX_REPORTED_PENDING {
                break;
            }
        }
        Ok(count.min(MAX_REPORTED_PENDING))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use chrono::Utc;
    use tempfile::TempDir;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        metrics_provider,
        registry::job_store::{
            JobEnvelope, JobQueue,
            durable::{ClaimedJob, DurableJobConsumer, DurableJobQueue, FailOutcome},
            parse_not_before,
        },
    };

    fn make_store(dir: &TempDir) -> Arc<Backend> {
        Arc::new(Backend::new(&BackendConfig {
            root_dir: dir.path().to_string_lossy().to_string(),
        }))
    }

    fn dummy_envelope(lock_key: &str) -> JobEnvelope {
        JobEnvelope::new("cache", "test.noop", lock_key, &()).expect("envelope")
    }

    fn make_consumer(store: Arc<Backend>) -> DurableJobConsumer {
        DurableJobConsumer::new(store, 30, "test-worker".to_string())
    }

    #[tokio::test]
    async fn enqueue_then_claim_succeeds() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let queue = DurableJobQueue::new(store.clone());
        let consumer = make_consumer(store);

        queue
            .enqueue(dummy_envelope("cache.ns:sha256:aaa"))
            .await
            .expect("enqueue");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim_one")
            .claimed
            .expect("Some");
        assert_eq!(claimed.envelope.lock_key, "cache.ns:sha256:aaa");
        consumer.complete(claimed).await.expect("complete");
        assert!(
            consumer
                .claim_one("cache")
                .await
                .expect("claim_one")
                .claimed
                .is_none(),
            "queue must be empty after complete"
        );
    }

    #[tokio::test]
    async fn shared_lock_key_serializes_claims() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let env1 = dummy_envelope("cache.ns:sha256:shared");
        let env2 = dummy_envelope("cache.ns:sha256:shared");
        store
            .put_pending("cache", &env1, Utc::now())
            .await
            .expect("put 1");
        store
            .put_pending("cache", &env2, Utc::now())
            .await
            .expect("put 2");

        let j1 = consumer
            .claim_one("cache")
            .await
            .expect("claim 1")
            .claimed
            .expect("Some");
        assert!(
            consumer
                .claim_one("cache")
                .await
                .expect("claim 2")
                .claimed
                .is_none(),
            "second claim on same lock_key must be None while first lease is held"
        );
        consumer.complete(j1).await.expect("complete j1");
    }

    #[tokio::test]
    async fn stale_lease_is_reclaimed() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let env = dummy_envelope("cache.ns:sha256:stale");
        store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");

        // ttl=0 makes the lease immediately expired regardless of timing.
        let data = serde_json::to_vec(&LeaseFile {
            instance_id: "stale-instance".to_string(),
            worker_id: "dead-worker".to_string(),
            ttl_secs: 0,
        })
        .expect("serialize");
        fs_ops::atomic_write(&store.lease_path(&env.lock_key), &data, false)
            .await
            .expect("write stale lease");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim after stale")
            .claimed
            .expect("Some");
        assert_eq!(claimed.envelope.lock_key, "cache.ns:sha256:stale");
        consumer.complete(claimed).await.expect("complete");
    }

    /// Heartbeat must refresh the mtime via the fd and preserve the
    /// body's `instance_id`. The previous implementation went through
    /// `atomic_write` (temp + rename), where a stealer's `remove + create`
    /// between the read and the write would be silently overwritten by
    /// the rename. The fd-based path no longer has that window.
    #[tokio::test]
    async fn heartbeat_refreshes_mtime_via_fd() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let lock_key = "cache.ns:sha256:heartbeat";

        let token = store
            .try_create_lease(lock_key, "worker-A", 30)
            .await
            .expect("create lease")
            .expect("first acquire wins");
        let path = store.lease_path(lock_key);

        // Force the file's mtime back into the past via std::fs (test-only
        // sync I/O) so we can verify the heartbeat brings it forward.
        let past = SystemTime::now() - Duration::from_secs(20);
        {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open for backdate");
            file.set_modified(past).expect("backdate mtime");
        }
        let mtime_before = tokio::fs::metadata(&path)
            .await
            .expect("stat")
            .modified()
            .expect("mtime");

        let returned = store
            .heartbeat_lease(lock_key, &token, "worker-A", 30)
            .await
            .expect("heartbeat");
        assert_eq!(returned, token, "FS heartbeat reuses the same token");

        let mtime_after = tokio::fs::metadata(&path)
            .await
            .expect("stat")
            .modified()
            .expect("mtime");
        assert!(
            mtime_after > mtime_before,
            "heartbeat must refresh mtime (before={mtime_before:?} after={mtime_after:?})"
        );

        // Body should still parse and still have our instance_id.
        let body = tokio::fs::read(&path).await.expect("read body");
        let parsed: LeaseFile = serde_json::from_slice(&body).expect("parse body");
        assert_eq!(parsed.instance_id, token);
    }

    /// Heartbeat against a lease that's been silently replaced (stealer
    /// did rename + `create_new`) must fail with an ownership-check error —
    /// the previous implementation would have written via `atomic_write`
    /// and clobbered the stealer's fresh lease.
    #[tokio::test]
    async fn heartbeat_rejects_replaced_lease() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let lock_key = "cache.ns:sha256:replaced";

        let our_token = store
            .try_create_lease(lock_key, "worker-A", 30)
            .await
            .expect("create lease")
            .expect("Some");

        // Simulate a successful steal-and-replace by another worker.
        let path = store.lease_path(lock_key);
        let stealer_body = serde_json::to_vec(&LeaseFile {
            instance_id: "stealer-instance".to_string(),
            worker_id: "worker-B".to_string(),
            ttl_secs: 30,
        })
        .expect("serialize");
        // remove + create_new mimics the stealer's atomic ownership transfer.
        tokio::fs::remove_file(&path).await.expect("remove");
        tokio::fs::write(&path, &stealer_body).await.expect("write");

        let err = store
            .heartbeat_lease(lock_key, &our_token, "worker-A", 30)
            .await
            .expect_err("heartbeat on replaced lease must fail");
        match err {
            Error::Storage(msg) => assert!(
                msg.contains("ownership check failed"),
                "expected ownership-check error, got: {msg}"
            ),
            other => panic!("expected Storage err, got: {other:?}"),
        }

        // The stealer's lease body must be untouched.
        let body = tokio::fs::read(&path).await.expect("read body");
        let parsed: LeaseFile = serde_json::from_slice(&body).expect("parse body");
        assert_eq!(
            parsed.instance_id, "stealer-instance",
            "stealer's lease must survive a rejected heartbeat"
        );
    }

    /// Two stealers race against the same stale lease. POSIX rename's
    /// atomicity guarantees that only one wins; the loser sees `ENOENT`
    /// on its rename and concedes. The previous implementation used
    /// `remove_file` (which doesn't differentiate "was already removed"
    /// from a normal success), so both stealers could end up writing
    /// fresh leases and trampling each other.
    #[tokio::test]
    async fn two_concurrent_stealers_serialize() {
        let dir = TempDir::new().expect("temp dir");
        let store = Arc::new(Backend::new(&BackendConfig {
            root_dir: dir.path().to_string_lossy().to_string(),
        }));
        let lock_key = "cache.ns:sha256:race-steal";
        let lease_path = store.lease_path(lock_key);

        // Seed a stale lease (ttl=0).
        let stale_body = serde_json::to_vec(&LeaseFile {
            instance_id: "stale-instance".to_string(),
            worker_id: "dead".to_string(),
            ttl_secs: 0,
        })
        .expect("serialize");
        fs_ops::atomic_write(&lease_path, &stale_body, false)
            .await
            .expect("seed stale lease");

        // Race two `try_create_lease` calls. Each one will first try
        // `write_new_lease` (which fails EEXIST) and then fall through
        // to `try_steal_stale_lease`. The rename inside steal is the
        // CAS — exactly one stealer's rename succeeds.
        let s1 = store.clone();
        let s2 = store.clone();
        let lk1 = lock_key.to_string();
        let lk2 = lock_key.to_string();
        let (r1, r2) = tokio::join!(
            async move { s1.try_create_lease(&lk1, "worker-1", 30).await },
            async move { s2.try_create_lease(&lk2, "worker-2", 30).await },
        );
        let r1 = r1.expect("worker-1 must not error");
        let r2 = r2.expect("worker-2 must not error");

        let winners = [r1.is_some(), r2.is_some()].iter().filter(|w| **w).count();
        assert_eq!(
            winners, 1,
            "exactly one stealer wins the race (got {winners}): r1={r1:?} r2={r2:?}"
        );

        // The surviving lease body must reflect the winner.
        let body = tokio::fs::read(&lease_path).await.expect("read body");
        let parsed: LeaseFile = serde_json::from_slice(&body).expect("parse body");
        let winning_token = r1.or(r2).expect("at least one winner");
        assert_eq!(
            parsed.instance_id, winning_token,
            "lease body must reflect the winning stealer"
        );
    }

    /// If a fresh acquirer slips a `create_new(lock_path)` between our
    /// `rename(lock_path → side)` and our final `create_new(lock_path)`,
    /// our `create_new` fails EEXIST and we concede — without clobbering
    /// the fresh lease.
    #[tokio::test]
    async fn steal_concedes_when_lock_path_recreated() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let lock_key = "cache.ns:sha256:eexist-during-steal";
        let lease_path = store.lease_path(lock_key);

        // Seed a stale lease (ttl=0).
        let stale_body = serde_json::to_vec(&LeaseFile {
            instance_id: "stale-instance".to_string(),
            worker_id: "dead".to_string(),
            ttl_secs: 0,
        })
        .expect("serialize");
        fs_ops::atomic_write(&lease_path, &stale_body, false)
            .await
            .expect("seed stale lease");

        // Manually do the rename half of the steal so we control timing.
        let new_uuid = "fake-stealer-uuid".to_string();
        let side_path = lease_path.with_extension(format!("steal-{new_uuid}"));
        tokio::fs::rename(&lease_path, &side_path)
            .await
            .expect("rename to side");

        // Simulate a fresh acquirer winning the brief window.
        let fresh_body = serde_json::to_vec(&LeaseFile {
            instance_id: "fresh-acquirer".to_string(),
            worker_id: "worker-Fresh".to_string(),
            ttl_secs: 30,
        })
        .expect("serialize");
        tokio::fs::write(&lease_path, &fresh_body)
            .await
            .expect("fresh create_new");

        // Now run the real `try_create_lease` — it should see lock_path
        // exists, try steal, but EEXIST out and concede. Crucially, the
        // fresh acquirer's lease must survive.
        let result = store
            .try_create_lease(lock_key, "worker-Loser", 30)
            .await
            .expect("try_create");
        assert!(
            result.is_none(),
            "must concede when lock_path was recreated during steal"
        );

        // Fresh acquirer's body is intact.
        let body = tokio::fs::read(&lease_path).await.expect("read body");
        let parsed: LeaseFile = serde_json::from_slice(&body).expect("parse");
        assert_eq!(parsed.instance_id, "fresh-acquirer");

        // (Cleanup our manually-created side path so the test temp dir is tidy.)
        let _ = tokio::fs::remove_file(&side_path).await;
    }

    #[tokio::test]
    async fn retry_writes_pending_with_backoff() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let mut env = dummy_envelope("cache.ns:sha256:retry");
        env.max_attempts = 3;
        store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .claimed
            .expect("Some");

        assert!(matches!(
            consumer.fail(claimed, "boom").await.expect("fail"),
            FailOutcome::Retried { .. }
        ));

        // Retry lands at a brand-new storage key with the bumped not_before
        // encoded in its prefix. Find it through list_pending so the test
        // doesn't have to know the new key shape.
        let pending = store.list_pending("cache", 10).await.expect("list");
        assert_eq!(pending.len(), 1, "exactly one retry envelope expected");
        let storage_key = &pending[0];
        let not_before = parse_not_before(storage_key).expect("parse prefix");
        assert!(not_before > Utc::now(), "retry must be backed off");

        let updated = store
            .read_pending("cache", storage_key)
            .await
            .expect("read updated");
        assert_eq!(updated.attempts, 1);
    }

    #[tokio::test]
    async fn dead_letter_after_max_attempts() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let mut env = dummy_envelope("cache.ns:sha256:dl");
        env.max_attempts = 1;
        store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .claimed
            .expect("Some");
        let storage_key = claimed.storage_key.clone();

        assert!(matches!(
            consumer.fail(claimed, "final error").await.expect("fail"),
            FailOutcome::MovedToDeadLetter
        ));
        assert!(matches!(
            store.read_pending("cache", &storage_key).await,
            Err(Error::NotFound)
        ));
        assert!(store.failed_path("cache", &storage_key).exists());
    }

    /// `count_pending` must saturate at `MAX_REPORTED_PENDING` so the
    /// autoscaler gauge has a bounded cost ceiling. All stub files are
    /// time-prefixed for *now* so they fall inside the readiness window.
    #[tokio::test]
    async fn count_pending_saturates_at_cap() {
        use crate::registry::job_store::{MAX_REPORTED_PENDING, make_storage_key};
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let queue_dir = store.pending_dir("cache");
        tokio::fs::create_dir_all(&queue_dir)
            .await
            .expect("mkdir pending dir");
        let now = Utc::now();
        for i in 0..(MAX_REPORTED_PENDING + 5) {
            let key = make_storage_key(now, &format!("stub-{i}"));
            tokio::fs::write(queue_dir.join(format!("{key}.json")), b"{}")
                .await
                .expect("write stub");
        }
        // Horizon doesn't matter here — every stub is scheduled at "now".
        let count = store.count_pending("cache", 600).await.expect("count");
        assert_eq!(count, MAX_REPORTED_PENDING);
    }

    /// `count_pending` reports only envelopes ready within the supplied
    /// readiness horizon. Envelopes scheduled past that horizon don't
    /// contribute to the autoscaler gauge.
    #[tokio::test]
    async fn count_pending_excludes_envelopes_past_readiness_horizon() {
        use crate::registry::job_store::make_storage_key;
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let queue_dir = store.pending_dir("cache");
        tokio::fs::create_dir_all(&queue_dir)
            .await
            .expect("mkdir pending dir");

        let now = Utc::now();
        // Two ready envelopes (not_before <= now).
        for i in 0..2 {
            let key = make_storage_key(now, &format!("ready-{i}"));
            tokio::fs::write(queue_dir.join(format!("{key}.json")), b"{}")
                .await
                .expect("write ready");
        }
        // Two envelopes scheduled an hour out, well past our 60-second
        // horizon below.
        let far_future = now + chrono::Duration::hours(1);
        for i in 0..2 {
            let key = make_storage_key(far_future, &format!("future-{i}"));
            tokio::fs::write(queue_dir.join(format!("{key}.json")), b"{}")
                .await
                .expect("write future");
        }

        let count = store.count_pending("cache", 60).await.expect("count");
        assert_eq!(count, 2, "only the ready envelopes must be counted");
    }

    /// A pending envelope whose storage-key prefix is in the future must
    /// surface as `next_ready` without being claimed. Verifies the
    /// filename-prefix-only path in `claim_one`.
    #[tokio::test]
    async fn future_storage_key_yields_next_ready_without_claiming() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let env = dummy_envelope("cache.ns:sha256:future");
        let scheduled = Utc::now() + chrono::Duration::seconds(3600);
        store
            .put_pending("cache", &env, scheduled)
            .await
            .expect("put");

        let outcome = consumer.claim_one("cache").await.expect("claim");
        assert!(
            outcome.claimed.is_none(),
            "future-scheduled job must not be claimed"
        );
        let next = outcome.next_ready.expect("next_ready must be set");
        // Tolerate millisecond rounding through the hex prefix encoding.
        let diff = (scheduled - next).num_milliseconds().abs();
        assert!(
            diff < 2,
            "next_ready ({next}) must match scheduled time ({scheduled}); diff={diff}ms"
        );
    }

    /// `find_pending_with_lock_key` must succeed without reading any pending
    /// envelope body: an enqueue under a very deep queue is a single index
    /// HEAD, not a scan. We verify this indirectly by tampering with all the
    /// pending bodies after the index is claimed — if the dedup path read
    /// any body it would fail (or at least return the wrong answer).
    #[tokio::test]
    async fn dedup_lookup_does_not_read_pending_bodies() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let lock_key = "cache.ns:sha256:nobody-reads-this";

        let env = dummy_envelope(lock_key);
        let storage_key = store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");
        assert!(
            store
                .try_claim_lock_key("cache", lock_key, &storage_key)
                .await
                .expect("claim"),
            "fresh lock_key must be claimable"
        );

        // Corrupt every pending body. If dedup ever reads a body, parsing
        // would fail and the test would surface that — both as a hard error
        // and by the dedup returning false.
        let queue_dir = store.pending_dir("cache");
        let mut entries = tokio::fs::read_dir(&queue_dir)
            .await
            .expect("read pending dir");
        while let Some(entry) = entries.next_entry().await.expect("next entry") {
            tokio::fs::write(entry.path(), b"NOT VALID JSON, ANYWHERE")
                .await
                .expect("corrupt pending");
        }

        // Dedup should still report a hit — purely from the index file.
        assert!(
            store
                .find_pending_with_lock_key("cache", lock_key)
                .await
                .expect("dedup"),
            "find_pending_with_lock_key must succeed via the index, not the body"
        );
    }

    /// Two replicas concurrently enqueue the same `lock_key` and both miss
    /// in the dedup-index lookup. They both write their pending file, then
    /// race on `try_claim_lock_key`. Exactly one wins; the loser is
    /// responsible for cleaning up its pending file so the queue carries
    /// no spurious duplicate.
    #[tokio::test]
    async fn try_claim_lock_key_resolves_enqueue_race() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let lock_key = "cache.ns:sha256:race";

        let env1 = dummy_envelope(lock_key);
        let env2 = dummy_envelope(lock_key);
        let key1 = store
            .put_pending("cache", &env1, Utc::now())
            .await
            .expect("put1");
        let key2 = store
            .put_pending("cache", &env2, Utc::now())
            .await
            .expect("put2");
        assert_ne!(key1, key2, "distinct envelope ids → distinct storage keys");

        // Replica 1 wins the claim.
        assert!(
            store
                .try_claim_lock_key("cache", lock_key, &key1)
                .await
                .expect("claim1"),
            "first claim wins"
        );
        // Replica 2's CAS fails.
        assert!(
            !store
                .try_claim_lock_key("cache", lock_key, &key2)
                .await
                .expect("claim2"),
            "second claim must lose"
        );

        // Replica 2 cleans up the pending file it wrote (this is what
        // `DurableJobQueue::enqueue` does on race-loss).
        store
            .remove_pending("cache", &key2)
            .await
            .expect("remove loser");

        // Only the winner's pending remains; the index points at it.
        let pending = store.list_pending("cache", 10).await.expect("list");
        assert_eq!(pending, vec![key1.clone()]);

        let data = tokio::fs::read(store.lock_key_index_path("cache", lock_key))
            .await
            .expect("read index");
        let index =
            crate::registry::job_store::parse_lock_key_index(&data).expect("parse index");
        assert_eq!(index.storage_key, key1);
    }

    /// A crash mid-`complete` could leave an orphan index pointing at a
    /// pending file that no longer exists. The next `find_pending_with_lock_key`
    /// must detect the orphan, delete the stale index, and report no-pending
    /// so the next enqueue can proceed.
    #[tokio::test]
    async fn orphan_index_is_self_healed_on_next_lookup() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let lock_key = "cache.ns:sha256:orphan";

        // Hand-craft an orphan: write the index pointing at a non-existent
        // pending file. (This is the on-disk shape after a crash between
        // remove_pending and remove_lock_key_index_if_matches.)
        let storage_key = make_storage_key(Utc::now(), "phantom-id");
        let index_path = store.lock_key_index_path("cache", lock_key);
        let data = crate::registry::job_store::serialize_lock_key_index(&storage_key)
            .expect("serialize");
        fs_ops::atomic_write(&index_path, &data, false)
            .await
            .expect("write index");
        assert!(index_path.exists(), "fixture: index file must exist");

        // First lookup detects the orphan and reports no-pending.
        let hit = store
            .find_pending_with_lock_key("cache", lock_key)
            .await
            .expect("lookup");
        assert!(!hit, "orphan index must not register as a dedup hit");

        // And the index file must be gone now (self-healed).
        assert!(
            !index_path.exists(),
            "orphan index must be deleted after detection"
        );
    }

    /// After a retry, the dedup index must point at the *new* storage key
    /// (the one with the bumped `not_before`), not the now-deleted old one.
    /// Otherwise the next enqueue would either dedup-miss (creating a
    /// duplicate) or treat the live entry as an orphan.
    #[tokio::test]
    async fn retry_updates_lock_key_index_to_new_storage_key() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());
        let lock_key = "cache.ns:sha256:retry-index";

        let mut env = dummy_envelope(lock_key);
        env.max_attempts = 3;
        store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .claimed
            .expect("Some");
        let old_storage_key = claimed.storage_key.clone();

        assert!(matches!(
            consumer.fail(claimed, "boom").await.expect("fail"),
            FailOutcome::Retried { .. }
        ));

        // Only one pending file remains — the new one.
        let pending = store.list_pending("cache", 10).await.expect("list");
        assert_eq!(pending.len(), 1);
        let new_storage_key = &pending[0];
        assert_ne!(new_storage_key, &old_storage_key);

        // Read the index and confirm it now points at the new storage key.
        let index_path = store.lock_key_index_path("cache", lock_key);
        let data = tokio::fs::read(&index_path).await.expect("read index");
        let index = crate::registry::job_store::parse_lock_key_index(&data).expect("parse");
        assert_eq!(&index.storage_key, new_storage_key);

        // And dedup still works through the index.
        assert!(
            store
                .find_pending_with_lock_key("cache", lock_key)
                .await
                .expect("dedup"),
            "dedup must still hit after retry"
        );
    }

    /// On `complete`, the dedup index must be removed so the next enqueue for
    /// the same `lock_key` proceeds (no false dedup against a finished job).
    #[tokio::test]
    async fn complete_removes_lock_key_index() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());
        let lock_key = "cache.ns:sha256:complete-clears-index";

        let env = dummy_envelope(lock_key);
        let storage_key = store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");
        assert!(
            store
                .try_claim_lock_key("cache", lock_key, &storage_key)
                .await
                .expect("claim index"),
            "fresh lock_key must be claimable"
        );
        let index_path = store.lock_key_index_path("cache", lock_key);
        assert!(index_path.exists(), "fixture: index must exist before complete");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .claimed
            .expect("Some");
        consumer.complete(claimed).await.expect("complete");

        assert!(
            !index_path.exists(),
            "index must be removed on complete"
        );
        assert!(
            !store
                .find_pending_with_lock_key("cache", lock_key)
                .await
                .expect("dedup"),
            "no dedup hit after complete"
        );
    }

    /// On dead-letter, the dedup index must also be removed: any future
    /// re-enqueue for the same `lock_key` should not be falsely deduplicated
    /// against a job that exhausted its retry budget.
    #[tokio::test]
    async fn dead_letter_removes_lock_key_index() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());
        let lock_key = "cache.ns:sha256:dlq-clears-index";

        let mut env = dummy_envelope(lock_key);
        env.max_attempts = 1;
        let storage_key = store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");
        assert!(
            store
                .try_claim_lock_key("cache", lock_key, &storage_key)
                .await
                .expect("claim index"),
            "fresh lock_key must be claimable"
        );
        let index_path = store.lock_key_index_path("cache", lock_key);
        assert!(index_path.exists(), "fixture: index must exist before DLQ");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .claimed
            .expect("Some");
        assert!(matches!(
            consumer.fail(claimed, "final").await.expect("fail"),
            FailOutcome::MovedToDeadLetter
        ));

        assert!(
            !index_path.exists(),
            "index must be removed on DLQ"
        );
    }

    #[tokio::test]
    async fn enqueue_dedup_skips_existing_lock_key() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let queue = DurableJobQueue::new(store.clone());

        queue
            .enqueue(dummy_envelope("cache.ns:sha256:dup"))
            .await
            .expect("enqueue 1");
        let before = store.count_pending("cache", 600).await.expect("count");
        queue
            .enqueue(dummy_envelope("cache.ns:sha256:dup"))
            .await
            .expect("enqueue 2");
        assert_eq!(
            before,
            store.count_pending("cache", 600).await.expect("count")
        );
    }

    /// Heartbeat against a non-existent lease file fails every tick; after
    /// `MAX_HEARTBEAT_FAILURES` consecutive failures the cancellation token
    /// must fire so `execute_one` can abort the job.
    #[tokio::test]
    async fn heartbeat_failures_cancel_lease_lost_token() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        // ttl_secs=3 → heartbeat ticks at 1 s; three consecutive failures
        // (lease file never existed) cancel the token within ~3 s.
        let consumer = DurableJobConsumer::new(store, 3, "test-worker".to_string());
        let claimed = ClaimedJob {
            envelope: dummy_envelope("cache.no-lease"),
            storage_key: "no-storage-key".to_string(),
            lease_token: "stale-token".to_string(),
        };
        let lease_lost = CancellationToken::new();
        let hb = consumer.spawn_heartbeat(&claimed, lease_lost.clone());

        timeout(Duration::from_secs(6), lease_lost.cancelled())
            .await
            .expect("lease_lost must be cancelled within 6s");
        let _ = hb.await;
    }
}
