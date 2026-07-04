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
//! - `redis`: enables the Redis-backed lock storage.

pub mod error;
pub mod executor;
pub mod intent;
pub mod janitor;
pub mod lock;
pub mod periodic;
pub mod probe;
pub mod recovery;
pub mod store;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
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
