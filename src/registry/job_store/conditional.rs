//! [`JobStore`] backed by any [`ConditionalStore`].
//!
//! Storage primitives only (pending/failed/dedup). Lease lifecycle lives
//! behind [`crate::registry::job_store::lease::conditional::Backend`].

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::warn;

use crate::registry::{
    job_store::{
        Error, JobEnvelope, JobStore, MAX_REPORTED_PENDING, STORAGE_KEY_PREFIX_LEN,
        make_storage_key, parse_lock_key_index, pending_ready_cutoff_prefix, serialize_dead_letter,
        serialize_lock_key_index,
    },
    path_builder,
};
use angos_storage::{ConditionalStore, Error as StorageError};

/// Builder for [`Backend`].
pub struct Builder {
    store: Option<Arc<dyn ConditionalStore>>,
}

impl Builder {
    fn new() -> Self {
        Self { store: None }
    }

    /// The underlying conditional store (required).
    pub fn store(mut self, store: Arc<dyn ConditionalStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let store = self.store.ok_or_else(|| {
            Error::Initialization("conditional::Backend requires a store".to_string())
        })?;
        Ok(Backend { store })
    }
}

/// [`JobStore`] implementation that delegates every primitive to a
/// [`ConditionalStore`].
pub struct Backend {
    store: Arc<dyn ConditionalStore>,
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::new()
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
        let key = path_builder::job_pending_path(queue, &storage_key);
        let data = serde_json::to_vec(envelope)
            .map_err(|e| Error::Storage(format!("envelope serialization failed: {e}")))?;
        self.store.put(&key, Bytes::from(data)).await?;
        Ok(storage_key)
    }

    async fn list_pending(&self, queue: &str, n: u16) -> Result<Vec<String>, Error> {
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
                // Orphan: pending vanished but the index lingers. Clear it.
                if let Err(remove_err) = self.store.delete(&index_path).await {
                    warn!(
                        lock_key,
                        error = %remove_err,
                        "Failed to remove orphan lock-key index"
                    );
                }
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
        let index_path = path_builder::job_lock_key_index_path(queue, lock_key);
        let (data, etag) = match self.store.get_with_etag(&index_path).await {
            Ok(result) => result,
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

        if let Some(etag) = etag {
            match self.store.delete_if_match(&index_path, &etag).await {
                Ok(()) | Err(StorageError::PreconditionFailed) => Ok(()),
                Err(e) => {
                    warn!(lock_key, error = %e, "Failed conditional delete of lock-key index");
                    Ok(())
                }
            }
        } else {
            // Endpoint didn't return ETag — fall back to unconditional delete.
            match self.store.delete(&index_path).await {
                Ok(()) | Err(StorageError::NotFound) => Ok(()),
                Err(e) => {
                    warn!(lock_key, error = %e, "Failed to delete lock-key index");
                    Ok(())
                }
            }
        }
    }

    async fn try_claim_lock_key(
        &self,
        queue: &str,
        lock_key: &str,
        storage_key: &str,
    ) -> Result<bool, Error> {
        let path = path_builder::job_lock_key_index_path(queue, lock_key);
        let data = serialize_lock_key_index(storage_key)?;
        match self.store.put_if_absent(&path, Bytes::from(data)).await {
            Ok(_) => Ok(true),
            Err(StorageError::PreconditionFailed) => Ok(false),
            Err(e) => Err(Error::from(e)),
        }
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
