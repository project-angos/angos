//! Durable upload-session record.
//!
//! A JSON record at `upload-sessions/<namespace>/<uuid>.json` is the single
//! source of truth for all upload metadata that must survive a process crash.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use angos_storage::Part;

/// A durable upload-session record persisted at
/// `upload-sessions/<namespace>/<uuid>.json`.
///
/// The `session_id` equals the upload `uuid` so the existing
/// `(namespace, uuid)` addressing maps 1:1 without introducing a new ID space.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionRecord {
    /// Equals the upload UUID passed to `UploadStore::create`.
    pub session_id: String,
    /// OCI namespace owning this upload.
    pub namespace: String,
    /// Instance identifier of the process that created the session.
    pub owner: String,
    /// Wall-clock time at which the session was created (and refreshed on
    /// each `write` call so scrub's `UploadChecker` uses the latest activity
    /// time rather than creation time alone).
    pub started_at: DateTime<Utc>,
    /// Total bytes written so far (sum of all `write` calls).
    pub size: u64,
    /// Serialised SHA-256 state, base64-encoded (raw bytes from
    /// `Sha256Ext::serialized_state`). Persisted so the hash computation can
    /// resume after a crash without re-reading the uploaded bytes.
    pub hash_context: String,
    /// S3 multipart upload-id for this session, or `None` on the FS backend
    /// (which does not use multipart uploads) or before the first S3 part has
    /// been initiated.
    pub multipart_upload_id: Option<String>,
    /// Completed S3 multipart parts (empty on the FS backend and before any
    /// full part has been flushed).
    pub parts: Vec<Part>,
}
