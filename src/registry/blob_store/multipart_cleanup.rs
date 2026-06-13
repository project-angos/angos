//! Orphan multipart-upload detection and cleanup orchestration.
//!
//! This is registry-domain logic layered on the engine's raw upload
//! *primitives* ([`Store::list_multipart_uploads`](angos_tx_engine::store::Store::list_multipart_uploads)
//! and the keyed [`Store::abort_upload`](angos_tx_engine::store::Store::abort_upload)):
//! it walks in-flight multipart uploads, applies an age threshold, and skips
//! any upload that still has a live session (its `startedat` marker exists).
//! The engine stays oblivious to upload-session semantics; the policy lives
//! here. An orphan is cleaned with `abort_upload(key)`, which aborts every
//! in-flight multipart at the key and removes any staged remainder.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::registry::{
    blob_store::{BlobStore, Error},
    path_builder,
};

/// A multipart upload with no live session, eligible to be aborted.
pub struct OrphanMultipartUpload {
    pub key: String,
    pub upload_id: String,
}

/// Inverse of [`path_builder::upload_path`]: parses
/// `v2/repositories/{namespace}/_uploads/{uuid}/data` into `(namespace, uuid)`.
///
/// Returns slices borrowed from `key` so cleanup passes don't allocate per upload.
pub fn parse_upload_key(key: &str) -> Option<(&str, &str)> {
    key.strip_prefix(path_builder::repository_dir())?
        .strip_prefix('/')?
        .strip_suffix("/data")?
        .rsplit_once("/_uploads/")
}

/// Returns `true` when a multipart upload initiated at `initiated` should be
/// considered orphaned, i.e. its age as of `now` meets or exceeds `timeout`.
///
/// A negative age (clock skew where `initiated` is in the future) is never
/// considered orphaned.
pub fn is_orphan(initiated: DateTime<Utc>, now: DateTime<Utc>, timeout: Duration) -> bool {
    now.signed_duration_since(initiated) >= timeout
}

/// Orphan multipart-upload cleanup. Discovery and abort are split so a dry-run
/// caller (e.g. scrub without `--commit`) can list without mutating state.
#[async_trait]
pub trait MultipartCleanup: Send + Sync {
    /// Lists multipart uploads that have exceeded `timeout` and are not
    /// associated with a live upload session (i.e., the `startedat` marker is
    /// gone). Pure discovery: does not modify any state.
    async fn list_orphan_multipart_uploads(
        &self,
        timeout: Duration,
    ) -> Result<Vec<OrphanMultipartUpload>, Error>;

    /// Aborts a single orphan upload previously returned by
    /// [`Self::list_orphan_multipart_uploads`].
    async fn abort_orphan_multipart_upload(
        &self,
        upload: &OrphanMultipartUpload,
    ) -> Result<(), Error>;
}

#[async_trait]
impl MultipartCleanup for BlobStore {
    async fn list_orphan_multipart_uploads(
        &self,
        timeout: Duration,
    ) -> Result<Vec<OrphanMultipartUpload>, Error> {
        let mut orphans = Vec::new();
        let now = Utc::now();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        loop {
            let page = self
                .store
                .list_multipart_uploads(key_marker.as_deref(), upload_id_marker.as_deref())
                .await?;

            for upload in page.uploads {
                if !is_orphan(upload.initiated_at, now, timeout) {
                    continue;
                }
                let Some((namespace, uuid)) = parse_upload_key(&upload.key) else {
                    continue;
                };
                let startedat_path = path_builder::upload_start_date_path(namespace, uuid);
                if self.store.head(&startedat_path).await.is_ok() {
                    continue;
                }
                orphans.push(OrphanMultipartUpload {
                    key: upload.key,
                    upload_id: upload.upload_id,
                });
            }

            if page.next_key_marker.is_none() {
                break;
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
        }

        Ok(orphans)
    }

    async fn abort_orphan_multipart_upload(
        &self,
        upload: &OrphanMultipartUpload,
    ) -> Result<(), Error> {
        // `abort_upload` is keyed: it aborts every in-flight multipart at the
        // key and removes any staged remainder, subsuming the per-upload-id
        // abort.
        self.store
            .abort_upload(&upload.key)
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_orphan_recent_upload_is_not_orphan() {
        let now = Utc::now();
        let initiated = now - Duration::minutes(5);
        let timeout = Duration::hours(1);
        assert!(!is_orphan(initiated, now, timeout));
    }

    #[test]
    fn test_is_orphan_old_upload_is_orphan() {
        let now = Utc::now();
        let initiated = now - Duration::hours(2);
        let timeout = Duration::hours(1);
        assert!(is_orphan(initiated, now, timeout));
    }

    /// At the exact boundary (age == timeout), the upload is considered orphaned
    /// because the check uses `>=`.
    #[test]
    fn test_is_orphan_at_exact_timeout_boundary() {
        let now = Utc::now();
        let timeout = Duration::hours(1);
        let initiated = now - timeout;
        assert!(is_orphan(initiated, now, timeout));
    }

    /// An upload whose `initiated` timestamp is in the future (clock skew) must
    /// never be treated as orphaned.
    #[test]
    fn test_is_orphan_future_initiated_is_not_orphan() {
        let now = Utc::now();
        let initiated = now + Duration::minutes(10);
        let timeout = Duration::hours(1);
        assert!(!is_orphan(initiated, now, timeout));
    }

    #[test]
    fn test_parse_upload_key_valid() {
        let result = parse_upload_key("v2/repositories/my-repo/_uploads/abc-123-def/data");
        assert_eq!(result, Some(("my-repo", "abc-123-def")));
    }

    #[test]
    fn test_parse_upload_key_nested_namespace() {
        let result = parse_upload_key("v2/repositories/org/project/image/_uploads/uuid-here/data");
        assert_eq!(result, Some(("org/project/image", "uuid-here")));
    }

    #[test]
    fn test_parse_upload_key_invalid_prefix() {
        let result = parse_upload_key("invalid/prefix/_uploads/uuid/data");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_upload_key_invalid_suffix() {
        let result = parse_upload_key("v2/repositories/repo/_uploads/uuid/staged");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_upload_key_missing_uploads() {
        let result = parse_upload_key("v2/repositories/repo/blobs/sha256/abc/data");
        assert_eq!(result, None);
    }
}
