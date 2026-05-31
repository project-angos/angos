//! S3 lock storage.
//!
//! Writes lock objects at `.tx-locks/<shard>/<key>` on a
//! [`ConditionalStore`](angos_storage::ConditionalStore) backend, using
//! `put_if_absent` for atomic acquire and `put_if_match` for heartbeat refresh.
//! Conditional delete is supported when the underlying provider advertises
//! `If-Match` on DELETE.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::{Digest as ShaDigest, Sha256};
use tracing::warn;

use angos_storage::{ConditionalStore, Error as StorageError, Etag, ObjectStore};

use crate::lock::{
    Error,
    storage::{DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
};

fn shard_prefix(key: &str) -> String {
    let hash = Sha256::digest(key.as_bytes());
    format!("{:02x}", hash[0])
}

/// Produce the S3 object path for a lock on `key`.
#[must_use]
pub fn lock_path(key: &str) -> String {
    let shard = shard_prefix(key);
    format!(".tx-locks/{shard}/{key}")
}

/// S3-backed lock storage.
///
/// Uses `put_if_absent` for atomic acquire and `put_if_match` for heartbeat
/// refresh. Conditional delete is used for release when
/// `supports_conditional_delete` is `true`; otherwise a plain delete is used
/// with an ownership check first.
///
/// Constructed via [`S3LockStorage::new`].
#[derive(Clone)]
pub struct S3LockStorage {
    store: Arc<dyn ConditionalStore>,
    supports_conditional_delete: bool,
}

impl Debug for S3LockStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3LockStorage")
            .field(
                "supports_conditional_delete",
                &self.supports_conditional_delete,
            )
            .finish_non_exhaustive()
    }
}

impl S3LockStorage {
    /// Construct an `S3LockStorage` from an existing conditional store.
    #[must_use]
    pub fn new(store: Arc<dyn ConditionalStore>, supports_conditional_delete: bool) -> Self {
        Self {
            store,
            supports_conditional_delete,
        }
    }

    /// Return the storage path for a given logical lock key.
    #[must_use]
    pub fn path_for(key: &str) -> String {
        lock_path(key)
    }
}

#[async_trait]
impl LockStorage for S3LockStorage {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutIfAbsentOutcome, Error> {
        let path = lock_path(key);
        match self.store.put_if_absent(&path, Bytes::from(body)).await {
            Ok(etag) => Ok(PutIfAbsentOutcome::Created(etag.map(Etag::into_inner))),
            Err(StorageError::PreconditionFailed) => Ok(PutIfAbsentOutcome::AlreadyExists),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected_etag: &str,
        body: Vec<u8>,
    ) -> Result<PutIfMatchOutcome, Error> {
        let path = lock_path(key);
        let etag = Etag::new(expected_etag);
        match self
            .store
            .put_if_match(&path, &etag, Bytes::from(body))
            .await
        {
            Ok(new_etag) => Ok(PutIfMatchOutcome::Updated(new_etag.map(Etag::into_inner))),
            Err(StorageError::PreconditionFailed) => Ok(PutIfMatchOutcome::Mismatch),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), Error> {
        let path = lock_path(key);
        match self.store.get_with_metadata(&path).await {
            Ok((body, etag, last_modified)) => {
                Ok((body, etag.map(Etag::into_inner), last_modified))
            }
            Err(StorageError::NotFound) => Err(Error::NotFound),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let path = lock_path(key);
        match ObjectStore::delete(self.store.as_ref(), &path).await {
            Ok(()) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => {
                warn!(key, error = %e, "S3LockStorage: delete failed");
                Err(Error::StorageBackend(e.to_string()))
            }
        }
    }

    async fn delete_if_match(
        &self,
        key: &str,
        expected_etag: &str,
    ) -> Result<DeleteIfMatchOutcome, Error> {
        if !self.supports_conditional_delete {
            self.delete(key).await?;
            return Ok(DeleteIfMatchOutcome::Deleted);
        }
        let path = lock_path(key);
        let etag = Etag::new(expected_etag);
        match self.store.delete_if_match(&path, &etag).await {
            Err(StorageError::PreconditionFailed) => Ok(DeleteIfMatchOutcome::Mismatch),
            Ok(()) | Err(StorageError::NotFound) => Ok(DeleteIfMatchOutcome::Deleted),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    fn label(&self) -> &'static str {
        "s3"
    }
}
