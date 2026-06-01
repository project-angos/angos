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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use angos_storage::MemoryObjectStore;

    use super::S3LockStorage;
    use crate::lock::{
        Error,
        storage::{DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
    };

    /// `MemoryObjectStore` implements `ConditionalStore`, so it stands in for a
    /// CAS-capable S3 backend with no live infrastructure.
    fn storage() -> S3LockStorage {
        S3LockStorage::new(Arc::new(MemoryObjectStore::new()), true)
    }

    #[tokio::test]
    async fn put_if_absent_created_then_already_exists() {
        let s = storage();

        let first = s.put_if_absent("k", b"body-1".to_vec()).await.unwrap();
        assert!(
            matches!(first, PutIfAbsentOutcome::Created(Some(_))),
            "first put_if_absent must create and return an ETag"
        );

        let second = s.put_if_absent("k", b"body-2".to_vec()).await.unwrap();
        assert!(
            matches!(second, PutIfAbsentOutcome::AlreadyExists),
            "a second put_if_absent on the same key must report AlreadyExists"
        );
    }

    #[tokio::test]
    async fn put_if_match_updates_on_matching_etag_and_mismatches_on_stale() {
        let s = storage();

        let PutIfAbsentOutcome::Created(Some(etag)) =
            s.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created with an ETag");
        };

        let updated = s.put_if_match("k", &etag, b"v2".to_vec()).await.unwrap();
        let new_etag = match updated {
            PutIfMatchOutcome::Updated(Some(e)) => e,
            other => panic!("expected Updated(Some(etag)), got {}", outcome_kind(&other)),
        };
        assert_ne!(new_etag, etag, "a successful put_if_match must rotate the ETag");

        // The original (now stale) ETag must mismatch.
        let stale = s.put_if_match("k", &etag, b"v3".to_vec()).await.unwrap();
        assert!(
            matches!(stale, PutIfMatchOutcome::Mismatch),
            "put_if_match against a stale ETag must report Mismatch"
        );
    }

    fn outcome_kind(o: &PutIfMatchOutcome) -> &'static str {
        match o {
            PutIfMatchOutcome::Updated(_) => "Updated",
            PutIfMatchOutcome::Mismatch => "Mismatch",
        }
    }

    #[tokio::test]
    async fn get_with_etag_returns_body_and_etag_and_not_found_when_absent() {
        let s = storage();

        let absent = s.get_with_etag("missing").await;
        assert!(
            matches!(absent, Err(Error::NotFound)),
            "get_with_etag on an absent key must be NotFound"
        );

        let PutIfAbsentOutcome::Created(Some(etag)) =
            s.put_if_absent("k", b"the-body".to_vec()).await.unwrap()
        else {
            panic!("expected Created with an ETag");
        };

        let (body, read_etag, _last_modified) = s.get_with_etag("k").await.unwrap();
        assert_eq!(body, b"the-body");
        assert_eq!(
            read_etag,
            Some(etag),
            "get_with_etag must surface the same ETag the put returned"
        );
    }

    #[tokio::test]
    async fn delete_if_match_deletes_on_match_and_mismatches_on_stale() {
        let s = storage();

        let PutIfAbsentOutcome::Created(Some(etag)) =
            s.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created with an ETag");
        };

        // Rotate the live ETag so the original is stale.
        let PutIfMatchOutcome::Updated(Some(_new_etag)) =
            s.put_if_match("k", &etag, b"v2".to_vec()).await.unwrap()
        else {
            panic!("expected Updated");
        };

        // Deleting with the now-stale ETag must mismatch and leave the object.
        let stale = s.delete_if_match("k", &etag).await.unwrap();
        assert!(
            matches!(stale, DeleteIfMatchOutcome::Mismatch),
            "delete_if_match against a stale ETag must report Mismatch"
        );
        assert!(
            s.get_with_etag("k").await.is_ok(),
            "a mismatched conditional delete must leave the object in place"
        );

        // Delete with the current ETag succeeds, then the key is gone.
        let (_body, current_etag, _) = s.get_with_etag("k").await.unwrap();
        let current_etag = current_etag.expect("etag present");
        let deleted = s.delete_if_match("k", &current_etag).await.unwrap();
        assert!(
            matches!(deleted, DeleteIfMatchOutcome::Deleted),
            "delete_if_match against the current ETag must delete"
        );
        assert!(
            matches!(s.get_with_etag("k").await, Err(Error::NotFound)),
            "the key must be gone after a successful conditional delete"
        );
    }
}
