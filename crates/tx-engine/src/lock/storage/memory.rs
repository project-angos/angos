//! In-process memory lock storage.
//!
//! Provides lock-object storage backed by an in-process `HashMap<String,
//! Bytes>` guarded by a `Mutex`. Suitable for single-process deployments and
//! tests. There is no TTL: a lock "expires" only when the holder releases it
//! (via [`Lock::release`](crate::lock::Lock)) or drops the session. Because
//! there is no heartbeat in the storage layer (the lock-expiry logic lives in
//! the heartbeat spawned by [`Lock`](crate::lock::Lock)), `get_with_etag`
//! returns a `last_modified` of `None`: the TTL-staleness check in
//! [`LockBody::is_expired`] will fall back to the embedded `refreshed_at`
//! field.

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex, PoisonError},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::lock::{
    Error,
    storage::{DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
};

/// A single entry in the in-memory store.
#[derive(Clone)]
struct Entry {
    body: Vec<u8>,
    /// Monotonically incrementing counter used as an `ETag` substitute. Format:
    /// `"<u64>"`.
    version: u64,
}

/// In-process lock storage. Safe to clone; all clones share the same internal
/// `HashMap`.
#[derive(Debug, Clone, Default)]
pub struct MemoryLockStorage {
    inner: Arc<Mutex<MemoryInner>>,
}

#[derive(Default)]
struct MemoryInner {
    entries: HashMap<String, Entry>,
    next_version: u64,
}

impl Debug for MemoryInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryInner")
            .field("entry_count", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl MemoryLockStorage {
    /// Create a new, empty in-memory lock storage.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next version counter and return it.
    fn alloc_version(inner: &mut MemoryInner) -> u64 {
        let v = inner.next_version;
        inner.next_version = inner.next_version.wrapping_add(1);
        v
    }
}

#[async_trait]
impl LockStorage for MemoryLockStorage {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutIfAbsentOutcome, Error> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.entries.contains_key(key) {
            return Ok(PutIfAbsentOutcome::AlreadyExists);
        }
        let v = Self::alloc_version(&mut inner);
        let etag = format!("\"{v}\"");
        inner
            .entries
            .insert(key.to_string(), Entry { body, version: v });
        Ok(PutIfAbsentOutcome::Created(etag))
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected_etag: &str,
        body: Vec<u8>,
    ) -> Result<PutIfMatchOutcome, Error> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match inner.entries.get(key) {
            None => return Ok(PutIfMatchOutcome::Mismatch),
            Some(entry) => {
                let current = format!("\"{}\"", entry.version);
                if current != expected_etag {
                    return Ok(PutIfMatchOutcome::Mismatch);
                }
            }
        }
        let v = Self::alloc_version(&mut inner);
        let new_etag = format!("\"{v}\"");
        inner
            .entries
            .insert(key.to_string(), Entry { body, version: v });
        Ok(PutIfMatchOutcome::Updated(new_etag))
    }

    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, String, Option<DateTime<Utc>>), Error> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match inner.entries.get(key) {
            None => Err(Error::NotFound),
            Some(entry) => {
                let etag = format!("\"{}\"", entry.version);
                Ok((entry.body.clone(), etag, None))
            }
        }
    }

    async fn delete_if_match(
        &self,
        key: &str,
        expected_etag: &str,
    ) -> Result<DeleteIfMatchOutcome, Error> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match inner.entries.get(key) {
            None => Ok(DeleteIfMatchOutcome::Deleted),
            Some(entry) => {
                let current = format!("\"{}\"", entry.version);
                if current != expected_etag {
                    return Ok(DeleteIfMatchOutcome::Mismatch);
                }
                inner.entries.remove(key);
                Ok(DeleteIfMatchOutcome::Deleted)
            }
        }
    }

    fn label(&self) -> &'static str {
        "memory"
    }
}
