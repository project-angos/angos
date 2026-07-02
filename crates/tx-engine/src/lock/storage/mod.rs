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
use crate::lock::primitive::MAX_LOCK_TTL_SECS;

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
    /// Recovery margin declared by the holder: peers and janitors treat the
    /// lock as reclaimable only after this many seconds of silence, even once
    /// the TTL has lapsed. 0 means steal on TTL expiry; `#[serde(default)]`
    /// keeps bodies written before this field existed readable.
    #[serde(default)]
    pub recovery_margin_secs: u64,
}

impl LockBody {
    /// Returns `true` when the lock has expired according to `last_modified`
    /// (the server-assigned timestamp, preferred) or `refreshed_at` (the
    /// client-declared timestamp, fallback).
    #[must_use]
    pub fn is_expired(&self, last_modified: Option<DateTime<Utc>>) -> bool {
        let reference = last_modified.unwrap_or(self.refreshed_at);
        let expiry = reference + Duration::seconds(self.ttl_secs.min(MAX_LOCK_TTL_SECS).cast_signed());
        Utc::now() > expiry
    }

    /// Returns `true` when the lock has not been refreshed for at least
    /// `margin_secs`, measured from `last_modified` (server timestamp, preferred)
    /// or `refreshed_at`. The long-hold scrub lock uses this so a genuinely dead
    /// holder is reclaimed only after a wide margin, never a live holder whose
    /// fast heartbeat briefly lagged. Unlike [`is_expired`](Self::is_expired) the
    /// margin is not capped, so a long crash-recovery window does not require
    /// raising the per-refresh TTL.
    #[must_use]
    pub fn is_stale_beyond(&self, last_modified: Option<DateTime<Utc>>, margin_secs: u64) -> bool {
        let reference = last_modified.unwrap_or(self.refreshed_at);
        Utc::now() > reference + Duration::seconds(margin_secs.cast_signed())
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

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use super::LockBody;

    fn body_refreshed_secs_ago(secs: i64, ttl_secs: u64) -> LockBody {
        LockBody {
            refreshed_at: Utc::now() - Duration::seconds(secs),
            ttl_secs,
            writer_nonce: Uuid::new_v4(),
            recovery_margin_secs: 0,
        }
    }

    #[test]
    fn body_without_margin_field_defaults_to_zero() {
        // The exact shape a 1.3 writer produces: no recovery_margin_secs field.
        let json = format!(
            r#"{{"refreshed_at":"{}","ttl_secs":30,"writer_nonce":"{}"}}"#,
            Utc::now().to_rfc3339(),
            Uuid::new_v4()
        );
        let body: LockBody = serde_json::from_str(&json).expect("1.3-shaped body parses");
        assert_eq!(
            body.recovery_margin_secs, 0,
            "bodies written before the field existed must default to steal-on-expiry"
        );
    }

    #[test]
    fn is_stale_beyond_respects_a_wide_margin() {
        // A holder refreshed 10s ago with a short ttl is already expired, but not
        // stale beyond a 600s recovery margin: a live holder must not be stolen.
        let body = body_refreshed_secs_ago(10, 9);
        assert!(body.is_expired(None), "expired against the 9s ttl");
        assert!(
            !body.is_stale_beyond(None, 600),
            "10s of silence is within the 600s recovery margin"
        );
    }

    #[test]
    fn is_stale_beyond_reclaims_a_long_dead_holder() {
        // A holder silent for 700s is stale beyond the 600s margin, so a crashed
        // run is still reclaimed.
        let body = body_refreshed_secs_ago(700, 9);
        assert!(body.is_stale_beyond(None, 600));
    }

    #[test]
    fn is_stale_beyond_uses_server_last_modified_when_present() {
        // refreshed_at says long ago, but a fresh server last_modified proves the
        // holder is alive; the margin must key on the server timestamp.
        let body = body_refreshed_secs_ago(10_000, 9);
        let fresh = Some(Utc::now() - Duration::seconds(5));
        assert!(!body.is_stale_beyond(fresh, 600));
    }
}
