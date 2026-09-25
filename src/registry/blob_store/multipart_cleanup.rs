//! Orphan multipart-upload detection and cleanup: the registry-domain policy
//! layered on the storage backend's raw multipart primitives. It walks
//! in-flight uploads, applies an age threshold, and spares any upload that
//! still has a live session marker.

use chrono::{DateTime, Duration, Utc};
use futures_util::stream::{self, StreamExt};
use tracing::warn;

use angos_oci::{Namespace, UploadSessionId};

use crate::registry::{
    Error,
    blob_store::BlobStore,
    keys::{NamespaceKeys, REPOS_ROOT},
};

/// Fan-out for the per-upload session-marker probes.
const ORPHAN_PROBE_CONCURRENCY: usize = 16;

/// A multipart upload with no live session, eligible to be aborted.
pub struct OrphanMultipartUpload {
    pub key: String,
    pub upload_id: String,
}

/// Inverse of [`NamespaceKeys::upload_path`], parsing an upload's `data` key
/// into `(namespace, uuid)`. The coalesce scratch key parses to the same
/// session too, so a scratch multipart stranded by a crash is reclaimable.
pub fn parse_upload_key(key: &str) -> Option<(&str, &str)> {
    let rest = key.strip_prefix(REPOS_ROOT)?.strip_prefix('/')?;
    rest.strip_suffix("/data")
        .or_else(|| rest.strip_suffix("/staged/coalesce"))?
        .rsplit_once("/_uploads/")
}

/// Whether an upload initiated at `initiated` counts as orphaned, i.e. its age
/// at `now` meets or exceeds `timeout`. A negative age (clock skew) never does.
pub fn is_orphan(initiated: DateTime<Utc>, now: DateTime<Utc>, timeout: Duration) -> bool {
    now.signed_duration_since(initiated) >= timeout
}

/// Orphan multipart-upload cleanup. Discovery and abort are split so a dry-run
/// caller (`prune -d`) can list without mutating state.
impl BlobStore {
    /// Lists multipart uploads past `timeout` with no live session marker,
    /// mutating nothing.
    pub async fn list_orphan_multipart_uploads(
        &self,
        timeout: Duration,
    ) -> Result<Vec<OrphanMultipartUpload>, Error> {
        let mut orphans = Vec::new();
        let now = Utc::now();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        // The dual key/upload-id markers keep this loop bespoke.
        loop {
            let page = self
                .object
                .list_multipart_uploads(key_marker.as_deref(), upload_id_marker.as_deref())
                .await?;

            let candidates = page.uploads.into_iter().filter_map(|upload| {
                if !is_orphan(upload.initiated_at, now, timeout) {
                    return None;
                }
                let (namespace, session_id) = parse_upload_key(&upload.key)?;
                let namespace = Namespace::new(namespace).ok()?;
                // A key naming no session was never opened by angos, so it is
                // not ours to abort.
                let session_id: UploadSessionId = session_id.parse().ok()?;
                let session_path = namespace.upload_session_path(&session_id);
                Some((upload, session_path))
            });
            let page_orphans = stream::iter(candidates)
                .map(|(upload, session_path)| async move {
                    // Only the proven absence of the record condemns an upload:
                    // aborting on a transient probe failure would destroy a
                    // progressing upload's parts.
                    match self.object.exists(&session_path).await {
                        Ok(true) => None,
                        Ok(false) => Some(OrphanMultipartUpload {
                            key: upload.key,
                            upload_id: upload.upload_id,
                        }),
                        Err(e) => {
                            warn!(
                                "Leaving multipart upload at {} for a later pass: its liveness \
                                 probe failed: {e}",
                                upload.key
                            );
                            None
                        }
                    }
                })
                .buffer_unordered(ORPHAN_PROBE_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
            orphans.extend(page_orphans.into_iter().flatten());

            if page.next_key_marker.is_none() {
                break;
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
        }

        Ok(orphans)
    }

    /// Aborts one orphan returned by
    /// [`Self::list_orphan_multipart_uploads`].
    pub async fn abort_orphan_multipart_upload(
        &self,
        upload: &OrphanMultipartUpload,
    ) -> Result<(), Error> {
        // `abort_upload` is keyed: it aborts every in-flight multipart at the
        // key and removes any staged remainder.
        self.object
            .abort_upload(&upload.key)
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;

    use angos_storage::{
        BoxedReader, ByteStream, ChildrenPage, Error as StorageError, MultipartUploadPage,
        ObjectMeta, ObjectStore, Page, PendingMultipartUpload,
    };

    use crate::registry::blob_store::multipart_cleanup::*;

    #[test]
    fn an_upload_orphans_once_its_age_reaches_the_timeout() {
        let now = Utc::now();
        let timeout = Duration::hours(1);
        for (age, orphan, case) in [
            (Duration::minutes(5), false, "younger than the timeout"),
            (Duration::hours(2), true, "older than the timeout"),
            (timeout, true, "at the boundary, since the check uses `>=`"),
            (
                -Duration::minutes(10),
                false,
                "initiated in the future by clock skew",
            ),
        ] {
            assert_eq!(is_orphan(now - age, now, timeout), orphan, "{case}");
        }
    }

    #[test]
    fn an_upload_key_parses_to_its_namespace_and_session() {
        for (key, parsed) in [
            (
                "v2/repositories/my-repo/_uploads/abc-123-def/data",
                Some(("my-repo", "abc-123-def")),
            ),
            // The coalesce scratch names the same session, so one stranded by
            // a crash is reclaimable.
            (
                "v2/repositories/my-repo/_uploads/abc-123-def/staged/coalesce",
                Some(("my-repo", "abc-123-def")),
            ),
            (
                "v2/repositories/org/project/image/_uploads/uuid-here/data",
                Some(("org/project/image", "uuid-here")),
            ),
            ("invalid/prefix/_uploads/uuid/data", None),
            ("v2/repositories/repo/_uploads/uuid/staged", None),
            ("v2/repositories/repo/blobs/sha256/abc/data", None),
        ] {
            assert_eq!(parse_upload_key(key), parsed, "key {key:?}");
        }
    }

    /// Reports one long-abandoned multipart upload and answers every `head`
    /// with `head_error`.
    #[derive(Debug)]
    struct OneStaleUpload {
        key: String,
        head_error: StorageError,
    }

    #[async_trait]
    impl ObjectStore for OneStaleUpload {
        async fn list_multipart_uploads(
            &self,
            _key_marker: Option<&str>,
            _upload_id_marker: Option<&str>,
        ) -> Result<MultipartUploadPage, StorageError> {
            Ok(MultipartUploadPage {
                uploads: vec![PendingMultipartUpload {
                    key: self.key.clone(),
                    upload_id: "upload-1".to_string(),
                    initiated_at: Utc::now() - Duration::days(1),
                }],
                next_key_marker: None,
                next_upload_id_marker: None,
            })
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StorageError> {
            Err(self.head_error.clone())
        }

        async fn get(&self, _key: &str) -> Result<Vec<u8>, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn get_stream(
            &self,
            _key: &str,
            _offset: Option<u64>,
        ) -> Result<(BoxedReader, u64), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn put(&self, _key: &str, _data: Bytes) -> Result<(), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn delete(&self, _key: &str) -> Result<(), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn delete_prefix(&self, _prefix: &str) -> Result<(), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn list(
            &self,
            _prefix: &str,
            _n: u16,
            _token: Option<String>,
        ) -> Result<Page<String>, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn create_if_absent(&self, _key: &str, _data: Bytes) -> Result<bool, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn list_after(
            &self,
            _prefix: &str,
            _n: u16,
            _token: Option<String>,
            _start_after: Option<String>,
        ) -> Result<Page<String>, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn list_children(
            &self,
            _prefix: &str,
            _n: u16,
            _token: Option<String>,
            _start_after: Option<String>,
        ) -> Result<ChildrenPage, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn copy(&self, _source: &str, _destination: &str) -> Result<u64, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn create_upload(&self, _key: &str) -> Result<(), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn write_upload(
            &self,
            _key: &str,
            _body: ByteStream,
            _len: Option<u64>,
        ) -> Result<u64, StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn complete_upload(&self, _key: &str) -> Result<(), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
        async fn abort_upload(&self, _key: &str) -> Result<(), StorageError> {
            unimplemented!("not reached by orphan listing")
        }
    }

    /// The abort destroys every committed part, so a probe that merely failed
    /// must not condemn the upload.
    #[tokio::test]
    async fn a_failed_liveness_probe_does_not_orphan_a_live_upload() {
        let namespace = Namespace::new("test-repo").unwrap();
        let key = namespace.upload_path(&UploadSessionId::generate());
        let store = BlobStore::new(
            Arc::new(OneStaleUpload {
                key,
                head_error: StorageError::Backend("upstream is unavailable".to_string()),
            }),
            None,
        );

        let orphans = store
            .list_orphan_multipart_uploads(Duration::hours(1))
            .await
            .expect("a failed probe must not fail the sweep");

        assert!(
            orphans.is_empty(),
            "a transient probe failure must not condemn an upload, got {} orphan(s)",
            orphans.len()
        );
    }

    /// The counterpart: an absent marker still proves the session is gone.
    #[tokio::test]
    async fn an_absent_marker_still_orphans_a_stale_upload() {
        let namespace = Namespace::new("test-repo").unwrap();
        let key = namespace.upload_path(&UploadSessionId::generate());
        let store = BlobStore::new(
            Arc::new(OneStaleUpload {
                key: key.clone(),
                head_error: StorageError::NotFound,
            }),
            None,
        );

        let orphans = store
            .list_orphan_multipart_uploads(Duration::hours(1))
            .await
            .expect("listing must succeed");

        assert_eq!(orphans.len(), 1, "an absent marker must still be cleaned");
        assert_eq!(orphans[0].key, key);
    }
}
