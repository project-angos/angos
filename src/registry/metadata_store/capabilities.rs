//! S3 conditional operation capability declaration.

use serde::{Deserialize, Serialize};

/// Granular S3 conditional operation capabilities.
///
/// Each field corresponds to a distinct HTTP conditional header that the S3-compatible
/// provider may or may not support. Configure these explicitly in `[metadata_store.s3.capabilities]`
/// to skip the startup probe, or omit to auto-detect when using `lock_strategy = "s3"`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    /// when absent, release falls back to plain delete (race-prone but functional).
    pub fn supports_cas(&self) -> bool {
        self.put_if_none_match && self.put_if_match
    }
}
