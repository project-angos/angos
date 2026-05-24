//! [`JobStore`] backed by any [`ObjectStore`], with atomicity for the
//! per-`lock_key` dedup index provided by an external [`LockBackend`].
//!
//! Used by non-CAS storage backends (today: FS) where the storage layer
//! cannot offer real `If-Match` / `If-None-Match` semantics. The dedup
//! index (`try_claim_lock_key`, `remove_lock_key_index_if_matches`, and
//! the orphan-cleanup path inside `find_pending_with_lock_key`) runs under
//! a [`LockBackend::acquire`] guard; everything else is single-writer per
//! storage key and goes straight to the [`ObjectStore`].
//!
//! Lease lifecycle is **not** in this backend — see
//! [`crate::registry::job_store::lease`] for that.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::warn;

use crate::registry::{
    job_store::{
        Error, JobEnvelope, JobStore, MAX_REPORTED_PENDING, STORAGE_KEY_PREFIX_LEN,
        make_storage_key, parse_lock_key_index, pending_ready_cutoff_prefix, serialize_dead_letter,
        serialize_lock_key_index, with_lock,
    },
    metadata_store::lock::LockBackend,
    path_builder,
};
use angos_storage::{Error as StorageError, ObjectStore};

/// Builder for [`Backend`].
pub struct Builder {
    store: Option<Arc<dyn ObjectStore>>,
    lock: Option<Arc<dyn LockBackend + Send + Sync>>,
}

impl Builder {
    fn new() -> Self {
        Self {
            store: None,
            lock: None,
        }
    }

    /// The underlying object store (required).
    pub fn store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// The lock backend used to serialise read-modify-write on the dedup
    /// index (required). Memory for single-process deployments, Redis for
    /// multi-process worker pools.
    pub fn lock(mut self, lock: Arc<dyn LockBackend + Send + Sync>) -> Self {
        self.lock = Some(lock);
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let store = self
            .store
            .ok_or_else(|| Error::Initialization("locked::Backend requires a store".to_string()))?;
        let lock = self
            .lock
            .ok_or_else(|| Error::Initialization("locked::Backend requires a lock".to_string()))?;
        Ok(Backend { store, lock })
    }
}

/// [`JobStore`] implementation that combines a plain [`ObjectStore`] with a
/// [`LockBackend`] for atomicity on the dedup index.
pub struct Backend {
    store: Arc<dyn ObjectStore>,
    lock: Arc<dyn LockBackend + Send + Sync>,
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::new()
    }
}

/// Lock-key for serialising dedup-index writes against a given queue's
/// `lock_key` entry.
fn lock_key_index_lock_key(queue: &str, lock_key: &str) -> String {
    format!("lock_key_index:{queue}:{lock_key}")
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
        let key = path_builder::job_pending_path(queue, &storage_key);
        let data = serde_json::to_vec(envelope)
            .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?;
        self.store.put(&key, Bytes::from(data)).await?;
        Ok(storage_key)
    }

    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
        // `ObjectStore::list` returns flat keys in lexicographic order, which
        // matches `not_before` order because storage keys are fixed-width hex
        // unix-millis prefixed.
        let prefix = path_builder::job_pending_dir(queue);
        let page = self.store.list(&prefix, 1000, None).await?;

        Ok(page
            .items
            .into_iter()
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .take(n as usize)
            .collect())
    }

    async fn read_pending(&self, queue: &str, storage_key: &str) -> Result<JobEnvelope, Error> {
        let key = path_builder::job_pending_path(queue, storage_key);
        let data = self.store.get(&key).await?;
        serde_json::from_slice(&data)
            .map_err(|e| Error::Storage(format!("failed to parse envelope: {e}")))
    }

    async fn remove_pending(&self, queue: &str, storage_key: &str) -> Result<(), Error> {
        let key = path_builder::job_pending_path(queue, storage_key);
        self.store.delete(&key).await.map_err(Error::from)
    }

    async fn move_to_failed(
        &self,
        queue: &str,
        storage_key: &str,
        envelope: &JobEnvelope,
        last_error: &str,
    ) -> Result<(), Error> {
        let data = serialize_dead_letter(envelope, last_error)?;
        let failed_key = path_builder::job_failed_path(queue, storage_key);
        self.store.put(&failed_key, Bytes::from(data)).await?;
        self.remove_pending(queue, storage_key).await?;
        self.remove_lock_key_index_if_matches(queue, &envelope.lock_key, storage_key)
            .await
    }

    async fn find_pending_with_lock_key(&self, queue: &str, lock_key: &str) -> Result<bool, Error> {
        let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = match self.store.get(&index_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };
        let index = parse_lock_key_index(&data)?;

        let pending_key = path_builder::job_pending_path(queue, &index.storage_key);
        match self.store.head(&pending_key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound) => {
                // Orphan: pending vanished but the index lingers. Self-heal
                // under the index lock so concurrent claim/refresh writers
                // don't observe a torn state.
                let lock_keys = [lock_key_index_lock_key(queue, lock_key)];
                with_lock(
                    &*self.lock,
                    &lock_keys,
                    "lock invalidated during orphan index cleanup",
                    || async {
                        if let Err(remove_err) = self.store.delete(&index_path).await {
                            warn!(
                                lock_key,
                                error = %remove_err,
                                "Failed to remove orphan lock-key index"
                            );
                        }
                        Ok(())
                    },
                )
                .await?;
                Ok(false)
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    async fn remove_lock_key_index_if_matches(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<(), Error> {
        let lock_keys = [lock_key_index_lock_key(queue, lock_key)];
        with_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during lock-key index remove",
            || async {
                let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
                let data = match self.store.get(&index_path).await {
                    Ok(d) => d,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(e) => {
                        warn!(lock_key, error = %e, "Failed to read lock-key index before remove");
                        return Ok(());
                    }
                };
                match parse_lock_key_index(&data) {
                    Ok(index) if index.storage_key == storage_key => {}
                    Ok(_) => return Ok(()), // index now points elsewhere; leave it
                    Err(_) => {}            // corrupt — fall through and delete
                }
                if let Err(e) = self.store.delete(&index_path).await {
                    warn!(lock_key, error = %e, "Failed to delete lock-key index");
                }
                Ok(())
            },
        )
        .await
    }

    async fn try_claim_lock_key(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<bool, Error> {
        let lock_keys = [lock_key_index_lock_key(queue, lock_key)];
        with_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during lock-key index claim",
            || async {
                let path = path_builder::job_lock_key_index_path(queue, lock_key);
                match self.store.head(&path).await {
                    Ok(_) => Ok(false),
                    Err(StorageError::NotFound) => {
                        let data = serialize_lock_key_index(storage_key)?;
                        self.store.put(&path, Bytes::from(data)).await?;
                        Ok(true)
                    }
                    Err(e) => Err(Error::from(e)),
                }
            },
        )
        .await
    }

    async fn refresh_lock_key_index(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<(), Error> {
        let path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = serialize_lock_key_index(storage_key)?;
        self.store
            .put(&path, Bytes::from(data))
            .await
            .map_err(Error::from)
    }

    async fn count_pending(&self, queue: &str, ready_horizon_secs: u64) -> Result<u64, Error> {
        let prefix = path_builder::job_pending_dir(queue);
        let cutoff_prefix = pending_ready_cutoff_prefix(ready_horizon_secs);
        let mut count: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let page = self.store.list(&prefix, 1000, token).await?;
            for name in &page.items {
                let Some(stem) = name.strip_suffix(".json") else {
                    continue;
                };
                // Storage list returns lex-sorted keys, which equals
                // `not_before` order because the prefix is fixed-width hex
                // unix-millis. Stop counting at the first key past the
                // readiness cutoff.
                if let Some(p) = stem.get(..STORAGE_KEY_PREFIX_LEN)
                    && p > cutoff_prefix.as_str()
                {
                    return Ok(count.min(MAX_REPORTED_PENDING));
                }
                count += 1;
                if count >= MAX_REPORTED_PENDING {
                    return Ok(MAX_REPORTED_PENDING);
                }
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => return Ok(count),
            }
        }
    }
}

#[cfg(test)]
mod tests;
