//! S3 lock storage.
//!
//! Writes lock objects at `_locks/<shard>/<key>` on the configured S3 client,
//! using `put_if_not_exists` for `put_if_absent` and `put_if_match` for
//! heartbeat refresh. Conditional delete is supported when the underlying S3
//! provider advertises `If-Match` on DELETE.

use std::{fmt::Debug, io::ErrorKind, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sha2::{Digest as ShaDigest, Sha256};
use tracing::warn;

use crate::lock::{
    Error,
    storage::{DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
};
use angos_s3_client as s3_client;

fn shard_prefix(key: &str) -> String {
    let hash = Sha256::digest(key.as_bytes());
    format!("{:02x}", hash[0])
}

/// Produce the S3 object path for a lock on `key`.
#[must_use]
pub fn lock_path(key: &str) -> String {
    let shard = shard_prefix(key);
    format!("_locks/{shard}/{key}")
}

/// S3-backed lock storage.
///
/// Uses `put_if_not_exists` for atomic acquire and `put_if_match` for
/// heartbeat refresh. Conditional delete is used for release when
/// `supports_conditional_delete` is `true`; otherwise a plain delete is
/// used with an ownership check first.
///
/// Constructed via [`S3LockStorage::new`].
#[derive(Clone)]
pub struct S3LockStorage {
    store: Arc<s3_client::Backend>,
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
    /// Construct an `S3LockStorage` from an existing S3 client backend.
    #[must_use]
    pub fn new(store: Arc<s3_client::Backend>, supports_conditional_delete: bool) -> Self {
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
        match self.store.put_object_if_not_exists(&path, body).await {
            Ok(etag) => Ok(PutIfAbsentOutcome::Created(etag)),
            Err(s3_client::Error::PreconditionFailed) => Ok(PutIfAbsentOutcome::AlreadyExists),
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
        match self
            .store
            .put_object_if_match(&path, expected_etag, body)
            .await
        {
            Ok(new_etag) => Ok(PutIfMatchOutcome::Updated(new_etag)),
            Err(s3_client::Error::PreconditionFailed) => Ok(PutIfMatchOutcome::Mismatch),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), Error> {
        let path = lock_path(key);
        match self.store.read_with_metadata(&path).await {
            Ok((body, etag, last_modified)) => Ok((body, etag, last_modified)),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(Error::NotFound),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let path = lock_path(key);
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
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
        match self.store.delete_if_match(&path, expected_etag).await {
            Err(s3_client::Error::PreconditionFailed) => Ok(DeleteIfMatchOutcome::Mismatch),
            Ok(()) | Err(s3_client::Error::NotFound(_)) => Ok(DeleteIfMatchOutcome::Deleted),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }

    fn label(&self) -> &'static str {
        "s3"
    }
}
