//! Storage backends for the lock primitive.
//!
//! The [`LockStorage`] trait defines the minimal set of operations the concrete
//! [`Lock`](crate::lock::Lock) needs from its storage layer. Three implementations
//! are provided:
//!
//! - [`MemoryLockStorage`](memory::MemoryLockStorage) — in-process mutex map
//!   (no TTL; suitable for single-process deployments and tests).
//! - [`S3LockStorage`](s3::S3LockStorage) — uses `put_if_absent` /
//!   `put_if_match` / conditional delete on S3-compatible stores that advertise
//!   CAS support.
//! - [`RedisLockStorage`](redis::RedisLockStorage) (feature `redis`) — Redis
//!   `SET NX EX` + Lua scripts for atomic refresh and release; suitable for
//!   FS deployments under heavy concurrent load.

pub mod memory;
pub mod s3;

#[cfg(feature = "redis")]
pub mod redis;

use std::fmt::Debug;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::lock::Error;

/// Serialized body of a lock object stored at `_locks/<shard>/<key>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockBody {
    /// Wall-clock time this payload was last written. Used as a fallback when
    /// the storage layer does not populate `last_modified` on HEAD.
    pub refreshed_at: DateTime<Utc>,
    /// TTL in seconds declared by the holder.
    pub ttl_secs: u64,
}

impl LockBody {
    /// Returns `true` when the lock has expired according to `last_modified`
    /// (the server-assigned timestamp, preferred) or `refreshed_at` (the
    /// client-declared timestamp, fallback).
    #[must_use]
    pub fn is_expired(&self, last_modified: Option<DateTime<Utc>>) -> bool {
        let reference = last_modified.unwrap_or(self.refreshed_at);
        let expiry = reference + chrono::Duration::seconds(self.ttl_secs.min(3600).cast_signed());
        Utc::now() > expiry
    }
}

/// Result of a single conditional-put-if-absent attempt.
pub enum PutIfAbsentOutcome {
    /// The key was absent; PUT succeeded. Carries the server `ETag` when the
    /// backend provides one.
    Created(Option<String>),
    /// The key already existed (another holder owns the lock).
    AlreadyExists,
}

/// Result of a conditional-put-if-match attempt.
pub enum PutIfMatchOutcome {
    /// The `ETag` matched; PUT succeeded. Carries the new `ETag` when the backend
    /// provides one.
    Updated(Option<String>),
    /// The `ETag` did not match (another writer changed the object).
    Mismatch,
}

/// Result of a conditional-delete-if-match attempt.
pub enum DeleteIfMatchOutcome {
    /// The `ETag` matched; DELETE succeeded.
    Deleted,
    /// The `ETag` did not match.
    Mismatch,
}

/// The narrow storage interface the lock primitive requires.
///
/// Implementations must be `Send + Sync + Debug` and must provide at minimum
/// `put_if_absent`, `put_if_match`, `get_with_etag`, and `delete`. Conditional
/// delete is optional (`delete_if_match`); backends that do not support it
/// should return `DeleteIfMatchOutcome::Deleted` after a plain `delete` (or
/// fall through to the slow-path verify-then-delete in the lock release code).
#[async_trait]
pub trait LockStorage: Send + Sync + Debug {
    /// Atomically write `body` at `key` if and only if `key` does not currently
    /// exist. Implementations must guarantee that concurrent callers each see
    /// at most one `Created` outcome.
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutIfAbsentOutcome, Error>;

    /// Atomically write `body` at `key` if and only if the current `ETag` equals
    /// `expected_etag`. Returns `Mismatch` when the `ETag` does not match.
    async fn put_if_match(
        &self,
        key: &str,
        expected_etag: &str,
        body: Vec<u8>,
    ) -> Result<PutIfMatchOutcome, Error>;

    /// Read `key` and return its body bytes together with the `ETag` (if the
    /// backend provides one) and the server-assigned `last_modified` timestamp
    /// (if available). Returns `Err(Error::NotFound)` when the key is absent.
    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), Error>;

    /// Unconditionally delete `key`. A missing key is treated as success.
    async fn delete(&self, key: &str) -> Result<(), Error>;

    /// Conditionally delete `key` iff the current `ETag` equals `expected_etag`.
    ///
    /// The default implementation performs a plain `delete` (suitable for
    /// backends that do not expose ETag-conditional deletes). Override when the
    /// backend supports `If-Match` on DELETE for stronger correctness.
    async fn delete_if_match(
        &self,
        key: &str,
        _expected_etag: &str,
    ) -> Result<DeleteIfMatchOutcome, Error> {
        self.delete(key).await?;
        Ok(DeleteIfMatchOutcome::Deleted)
    }

    /// Returns a human-readable label for metrics / startup logs.
    fn label(&self) -> &'static str;
}
