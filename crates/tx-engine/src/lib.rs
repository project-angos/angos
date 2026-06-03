//! Transactional engine for `angos`.
//!
//! Provides a single `Transaction` value type and two executors (`LockedExecutor`
//! and `CasExecutor`) that drive transactions through a five-stage lifecycle:
//! Build → Prepare → Commit-intent → Apply → Reap. A `RecoveryLoop` replays
//! or rolls back stale intents; a `BodyJanitor` removes orphaned staging
//! bodies.
//!
//! The engine knows only `String` keys, `Bytes` bodies, and `Etag`
//! fingerprints. It has no knowledge of registry domain types (no `Digest`,
//! no `LinkKind`, no `JobEnvelope`).
//!
//! # Feature flags
//!
//! - `redis` — enables the Redis-backed lock storage.

pub mod error;
pub mod executor;
pub mod intent;
pub mod janitor;
pub mod lock;
pub mod periodic;
pub mod probe;
pub mod recovery;
pub mod store;
pub mod transaction;

// Storage value types re-exported for convenience, so call sites that already
// hold a `tx-engine` handle can name these types without a second `use`. The
// store traits and backends (`ObjectStore`, `ConditionalStore`, the S3/FS/memory
// backends) are obtained from `angos_storage` directly; the `Store` façade in
// [`store`] is the seam through which a subsystem's storage access flows.
pub use angos_storage::{
    BoxedReader, ByteStream, ChildrenPage, Error as StorageError, Etag, MultipartUploadPage,
    ObjectMeta, Page, PendingMultipartUpload,
};

/// Granular S3 conditional operation capabilities.
///
/// Each field corresponds to a distinct HTTP conditional header that the
/// S3-compatible provider may or may not support. Configure these explicitly
/// in `[metadata_store.s3.capabilities]` to skip the startup probe, or omit
/// them to auto-detect support when using S3 metadata storage.
///
/// Values are surfaced from [`probe_conditional_capabilities`] and passed
/// back through the configuration layer to [`build_executor`].
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConditionalCapabilities {
    /// `PutObject` with `If-None-Match: *` — create-only, reject if object exists.
    pub put_if_none_match: bool,
    /// `PutObject` with `If-Match: <etag>` — update-only, reject if `ETag` mismatch.
    pub put_if_match: bool,
    /// `DeleteObject` with `If-Match: <etag>` — conditional delete.
    pub delete_if_match: bool,
}

impl ConditionalCapabilities {
    /// Both conditional put operations are needed for CAS-based coordination
    /// and for the S3-based lock backend's acquire/heartbeat paths. The
    /// `delete_if_match` capability further enables race-free lock release;
    /// when absent, release falls back to plain delete (race-prone but
    /// functional).
    #[must_use]
    pub fn supports_cas(&self) -> bool {
        self.put_if_none_match && self.put_if_match
    }
}
