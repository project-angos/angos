//! Storage backends for the lock primitive.
//!
//! The [`LockStorage`] trait defines the minimal set of operations the concrete
//! [`Lock`](crate::lock::Lock) needs from its storage layer. Three implementations
//! are provided:
//!
//! - [`MemoryLockStorage`](memory::MemoryLockStorage): in-process mutex map
//!   (no TTL; suitable for single-process deployments and tests).
//! - [`S3LockStorage`](s3::S3LockStorage): uses `put_if_absent`,
//!   `put_if_match`, and conditional delete on S3-compatible stores that
//!   advertise CAS support.
//! - [`RedisLockStorage`](redis::RedisLockStorage) (feature `redis`): Redis
//!   `SET NX EX` plus Lua scripts for atomic refresh and release; suitable for
//!   FS deployments under heavy concurrent load.

pub mod memory;
pub mod s3;

#[cfg(feature = "redis")]
pub mod redis;

use std::fmt::Debug;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lock::Error;

/// Prefix of the shared lock objects (`<shard>/<key>` below it).
/// Engine-owned; the `LockJanitor` reclaims expired ones.
pub const LOCK_OBJECTS_PREFIX: &str = ".tx-locks";

/// Serialized body of a lock object stored at `.tx-locks/<shard>/<key>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockBody {
    /// Wall-clock time this payload was last written. Used as a fallback when
    /// the storage layer does not populate `last_modified` on HEAD.
    pub refreshed_at: DateTime<Utc>,
    /// TTL in seconds declared by the holder.
    pub ttl_secs: u64,
    /// Per-write random nonce that makes every serialized lock body globally
    /// unique. After a conditional write returns an ambiguous (status-less)
    /// transport error, [`S3LockStorage`](s3::S3LockStorage) reads the object
    /// back and treats a byte-for-byte match as proof that *its own* write
    /// landed despite the lost acknowledgement, rather than a competitor's
    /// identical-looking body. The nonce is what makes that comparison
    /// collision-free. `#[serde(default)]` keeps lock objects written before
    /// this field existed readable: they deserialize to the nil UUID, which
    /// never matches a freshly minted nonce.
    #[serde(default)]
    pub writer_nonce: Uuid,
}

impl LockBody {
    /// Returns `true` when the lock has expired according to `last_modified`
    /// (the server-assigned timestamp, preferred) or `refreshed_at` (the
    /// client-declared timestamp, fallback).
    #[must_use]
    pub fn is_expired(&self, last_modified: Option<DateTime<Utc>>) -> bool {
        let reference = last_modified.unwrap_or(self.refreshed_at);
        let expiry = reference + Duration::seconds(self.ttl_secs.min(3600).cast_signed());
        Utc::now() > expiry
    }
}

/// Result of a single conditional-put-if-absent attempt.
pub enum PutIfAbsentOutcome {
    /// The key was absent; PUT succeeded. Carries the server `ETag`.
    Created(String),
    /// The key already existed (another holder owns the lock).
    AlreadyExists,
}

/// Result of a conditional-put-if-match attempt.
pub enum PutIfMatchOutcome {
    /// The `ETag` matched; PUT succeeded. Carries the new `ETag`.
    Updated(String),
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
/// Every write surfaces an `ETag` (an opaque fencing token: an HTTP `ETag` on
/// S3, a version counter in memory, the stored value on Redis) and every
/// delete is conditional on it, so a holder can only ever remove its own lock
/// object. Backends that cannot honour this contract do not qualify as lock
/// storage; the CAS capability gate enforces it for S3 providers.
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

    /// Read `key` and return its body bytes together with the `ETag` and the
    /// server-assigned `last_modified` timestamp (if available). Returns
    /// `Err(Error::NotFound)` when the key is absent.
    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, String, Option<DateTime<Utc>>), Error>;

    /// Atomically delete `key` if and only if the current `ETag` equals
    /// `expected_etag`. Returns `Mismatch` when the `ETag` does not match; a
    /// missing key counts as `Deleted`.
    async fn delete_if_match(
        &self,
        key: &str,
        expected_etag: &str,
    ) -> Result<DeleteIfMatchOutcome, Error>;

    /// Returns a human-readable label for metrics / startup logs.
    fn label(&self) -> &'static str;

    /// `true` when lock objects live in storage visible to every process (S3,
    /// Redis), so separate processes contend on the same locks; `false` for
    /// process-local storage.
    fn is_process_shared(&self) -> bool;
}
