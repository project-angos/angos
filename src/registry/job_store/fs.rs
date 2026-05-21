use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::registry::{
    fs_ops,
    job_store::{Error, JobEnvelope, JobStore, serialize_dead_letter},
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
///   pending/<queue>/<ulid>.json
///   leases/<lock_key_encoded>.json
///   failed/<queue>/<ulid>.json
/// ```
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

    fn pending_path(&self, queue: &str, id: &str) -> PathBuf {
        self.root_dir
            .join(path_builder::job_pending_path(queue, id))
    }

    fn lease_path(&self, lock_key: &str) -> PathBuf {
        self.root_dir.join(path_builder::job_lease_path(lock_key))
    }

    fn failed_path(&self, queue: &str, id: &str) -> PathBuf {
        self.root_dir.join(path_builder::job_failed_path(queue, id))
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

    /// Read the existing lease file and steal it if it is expired. Returns
    /// `Some(token)` when a stale lease was reclaimed, `None` otherwise.
    async fn try_steal_stale_lease(
        &self,
        lock_path: &Path,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<String>, Error> {
        let data = match fs::read(lock_path).await {
            Ok(d) => d,
            // Vanished between create_new and now; let the next claim_one loop
            // iteration retry instead of recursing.
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

        debug!(
            lock_key = lock_path.display().to_string(),
            instance_id = existing.instance_id,
            age_secs,
            ttl_secs = existing.ttl_secs,
            "Stealing stale FS lease"
        );

        if let Err(e) = fs::remove_file(lock_path).await
            && e.kind() != ErrorKind::NotFound
        {
            return Err(Error::Storage(format!("failed to remove stale lease: {e}")));
        }

        self.write_new_lease(lock_path, worker_id, ttl_secs).await
    }
}

#[async_trait]
impl JobStore for Backend {
    async fn put_pending(
        &self,
        queue: &str,
        id: &str,
        envelope: &JobEnvelope,
    ) -> Result<(), Error> {
        let path = self.pending_path(queue, id);
        let data = serde_json::to_vec(envelope)
            .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?;
        fs_ops::atomic_write(&path, &data, false)
            .await
            .map_err(|e| Error::Storage(format!("failed to write pending job: {e}")))
    }

    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
        let dir = self.pending_dir(queue);
        let mut entries = fs_ops::list_dir_or_empty(&dir)
            .await
            .map_err(|e| Error::Storage(format!("failed to list pending: {e}")))?;

        // ULID filenames sort lexicographically by creation time.
        entries.sort();

        Ok(entries
            .into_iter()
            .filter(|name| Path::new(name).extension().is_some_and(|ext| ext == "json"))
            .take(n as usize)
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect())
    }

    async fn read_pending(&self, queue: &str, id: &str) -> Result<JobEnvelope, Error> {
        let path = self.pending_path(queue, id);
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

        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == ErrorKind::NotFound {
                Error::NotFound
            } else {
                Error::Storage(format!("failed to read lease for heartbeat: {e}"))
            }
        })?;
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

        fs_ops::atomic_write(&path, &new_data, false)
            .await
            .map_err(|e| Error::Storage(format!("heartbeat write failed: {e}")))?;

        Ok(token.to_string())
    }

    async fn remove_pending(&self, queue: &str, id: &str) -> Result<(), Error> {
        let path = self.pending_path(queue, id);
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
        id: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error> {
        let data = serialize_dead_letter(envelope, last_error)?;
        let failed_path = self.failed_path(queue, id);
        fs_ops::atomic_write(&failed_path, &data, false)
            .await
            .map_err(|e| Error::Storage(format!("failed to write dead-letter: {e}")))?;
        self.remove_pending(queue, id).await
    }

    async fn count_pending(&self, queue: &str) -> Result<u64, Error> {
        let dir = self.pending_dir(queue);
        let entries = fs_ops::list_dir_or_empty(&dir)
            .await
            .map_err(|e| Error::Storage(format!("failed to count pending: {e}")))?;
        let count = entries
            .iter()
            .filter(|name| Path::new(name).extension().is_some_and(|ext| ext == "json"))
            .count();
        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use tempfile::TempDir;
    use ulid::Ulid;

    use super::*;
    use crate::{
        metrics_provider,
        registry::job_store::{
            JobEnvelope, JobQueue,
            durable::{DurableJobConsumer, DurableJobQueue, FailOutcome},
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
            .expect("Some");
        assert_eq!(claimed.envelope.lock_key, "cache.ns:sha256:aaa");
        consumer.complete(claimed).await.expect("complete");
        assert!(
            consumer
                .claim_one("cache")
                .await
                .expect("claim_one")
                .is_none(),
            "queue must be empty after complete"
        );
    }

    #[tokio::test]
    async fn shared_lock_key_serializes_claims() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let mut env = dummy_envelope("cache.ns:sha256:shared");
        store
            .put_pending("cache", &env.id, &env)
            .await
            .expect("put 1");
        env.id = Ulid::new().to_string();
        store
            .put_pending("cache", &env.id, &env)
            .await
            .expect("put 2");

        let j1 = consumer
            .claim_one("cache")
            .await
            .expect("claim 1")
            .expect("Some");
        assert!(
            consumer
                .claim_one("cache")
                .await
                .expect("claim 2")
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
            .put_pending("cache", &env.id, &env)
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
            .expect("Some");
        assert_eq!(claimed.envelope.lock_key, "cache.ns:sha256:stale");
        consumer.complete(claimed).await.expect("complete");
    }

    #[tokio::test]
    async fn retry_writes_pending_with_backoff() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let mut env = dummy_envelope("cache.ns:sha256:retry");
        env.max_attempts = 3;
        store
            .put_pending("cache", &env.id, &env)
            .await
            .expect("put");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .expect("Some");
        let id = claimed.envelope.id.clone();

        assert!(matches!(
            consumer.fail(claimed, "boom").await.expect("fail"),
            FailOutcome::Retried { .. }
        ));

        let updated = store
            .read_pending("cache", &id)
            .await
            .expect("read updated");
        assert_eq!(updated.attempts, 1);
        assert!(updated.not_before > Utc::now());
    }

    #[tokio::test]
    async fn dead_letter_after_max_attempts() {
        let dir = TempDir::new().expect("temp dir");
        let store = make_store(&dir);
        let consumer = make_consumer(store.clone());

        let mut env = dummy_envelope("cache.ns:sha256:dl");
        env.max_attempts = 1;
        store
            .put_pending("cache", &env.id, &env)
            .await
            .expect("put");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .expect("Some");
        let id = claimed.envelope.id.clone();

        assert!(matches!(
            consumer.fail(claimed, "final error").await.expect("fail"),
            FailOutcome::MovedToDeadLetter
        ));
        assert!(matches!(
            store.read_pending("cache", &id).await,
            Err(Error::NotFound)
        ));
        assert!(store.failed_path("cache", &id).exists());
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
        let before = store.count_pending("cache").await.expect("count");
        queue
            .enqueue(dummy_envelope("cache.ns:sha256:dup"))
            .await
            .expect("enqueue 2");
        assert_eq!(before, store.count_pending("cache").await.expect("count"));
    }
}
