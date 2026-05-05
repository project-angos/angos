use std::sync::Arc;

use chrono::Duration;
use tracing::info;

use crate::registry::blob_store::{Error, MultipartCleanup};

pub struct MultipartChecker {
    cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
    timeout: Duration,
    dry_run: bool,
}

impl MultipartChecker {
    pub fn new(
        cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
        timeout: Duration,
        dry_run: bool,
    ) -> Self {
        Self {
            cleanup,
            timeout,
            dry_run,
        }
    }

    pub async fn check_all(&self) -> Result<(), Error> {
        let orphans = self
            .cleanup
            .list_orphan_multipart_uploads(self.timeout)
            .await?;
        let count = orphans.len();
        for orphan in &orphans {
            if self.dry_run {
                info!(
                    "DRY RUN: would abort orphan multipart upload {}",
                    orphan.key
                );
            } else {
                info!("Aborting orphan multipart upload {}", orphan.key);
                self.cleanup.abort_orphan_multipart_upload(orphan).await?;
            }
        }
        info!("Cleaned up {count} orphan multipart upload(s)");
        Ok(())
    }
}

// Full integration coverage for `MultipartCleanup` (orphan abort, listing)
// lives in `src/registry/blob_store/s3/tests.rs` where a real MinIO
// bucket is available.  The tests below cover the pure `MultipartChecker`
// layer: construction and delegation.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::registry::blob_store::OrphanMultipartUpload;

    struct SpyCleanup {
        list_called_timeout_secs: AtomicI64,
        abort_call_count: AtomicUsize,
        orphans: Vec<String>,
    }

    impl SpyCleanup {
        fn new(orphan_keys: Vec<&str>) -> Arc<Self> {
            Arc::new(Self {
                list_called_timeout_secs: AtomicI64::new(-1),
                abort_call_count: AtomicUsize::new(0),
                orphans: orphan_keys.into_iter().map(str::to_owned).collect(),
            })
        }
    }

    #[async_trait]
    impl MultipartCleanup for SpyCleanup {
        async fn list_orphan_multipart_uploads(
            &self,
            timeout: Duration,
        ) -> Result<Vec<OrphanMultipartUpload>, Error> {
            self.list_called_timeout_secs
                .store(timeout.num_seconds(), Ordering::SeqCst);
            Ok(self
                .orphans
                .iter()
                .map(|k| OrphanMultipartUpload {
                    key: k.clone(),
                    upload_id: "spy-upload-id".to_string(),
                })
                .collect())
        }

        async fn abort_orphan_multipart_upload(
            &self,
            _upload: &OrphanMultipartUpload,
        ) -> Result<(), Error> {
            self.abort_call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn check_all_lists_orphans_and_skips_abort_in_dry_run() {
        let spy = SpyCleanup::new(vec!["ns/_uploads/uuid1/data", "ns/_uploads/uuid2/data"]);
        let checker = MultipartChecker::new(spy.clone(), Duration::hours(2), true);

        checker.check_all().await.unwrap();

        assert_eq!(
            spy.list_called_timeout_secs.load(Ordering::SeqCst),
            7200,
            "timeout forwarded as 2 h = 7200 s"
        );
        assert_eq!(
            spy.abort_call_count.load(Ordering::SeqCst),
            0,
            "dry_run must not invoke abort"
        );
    }

    #[tokio::test]
    async fn check_all_aborts_orphans_when_not_dry_run() {
        let spy = SpyCleanup::new(vec!["ns/_uploads/uuid1/data", "ns/_uploads/uuid2/data"]);
        let checker = MultipartChecker::new(spy.clone(), Duration::hours(2), false);

        checker.check_all().await.unwrap();

        assert_eq!(
            spy.list_called_timeout_secs.load(Ordering::SeqCst),
            7200,
            "timeout forwarded as 2 h = 7200 s"
        );
        assert_eq!(
            spy.abort_call_count.load(Ordering::SeqCst),
            2,
            "abort must be called once per orphan"
        );
    }

    #[tokio::test]
    async fn check_all_forwards_cleanup_error() {
        struct FailingCleanup;

        #[async_trait]
        impl MultipartCleanup for FailingCleanup {
            async fn list_orphan_multipart_uploads(
                &self,
                _timeout: Duration,
            ) -> Result<Vec<OrphanMultipartUpload>, Error> {
                Err(Error::StorageBackend("backend failure".to_string()))
            }

            async fn abort_orphan_multipart_upload(
                &self,
                _upload: &OrphanMultipartUpload,
            ) -> Result<(), Error> {
                Ok(())
            }
        }

        let checker = MultipartChecker::new(Arc::new(FailingCleanup), Duration::minutes(30), false);

        assert!(
            checker.check_all().await.is_err(),
            "errors from the cleanup backend must propagate"
        );
    }
}
