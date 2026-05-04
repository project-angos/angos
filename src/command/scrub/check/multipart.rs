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
        let count = self
            .cleanup
            .cleanup_orphan_multipart_uploads(self.timeout, self.dry_run)
            .await?;
        info!("Cleaned up {count} orphan multipart upload(s)");
        Ok(())
    }
}

// Full integration coverage for `MultipartCleanup` (orphan abort, dry-run
// count) lives in `src/registry/blob_store/s3/tests.rs` where a real MinIO
// bucket is available.  The tests below cover the pure `MultipartChecker`
// layer: construction and delegation.
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

    use async_trait::async_trait;

    use super::*;

    struct SpyCleanup {
        called_timeout_secs: AtomicI64,
        called_dry_run: AtomicBool,
        result: usize,
    }

    impl SpyCleanup {
        fn new(result: usize) -> Arc<Self> {
            Arc::new(Self {
                called_timeout_secs: AtomicI64::new(-1),
                called_dry_run: AtomicBool::new(false),
                result,
            })
        }
    }

    #[async_trait]
    impl MultipartCleanup for SpyCleanup {
        async fn cleanup_orphan_multipart_uploads(
            &self,
            timeout: Duration,
            dry_run: bool,
        ) -> Result<usize, Error> {
            self.called_timeout_secs
                .store(timeout.num_seconds(), Ordering::SeqCst);
            self.called_dry_run.store(dry_run, Ordering::SeqCst);
            Ok(self.result)
        }
    }

    #[tokio::test]
    async fn check_all_delegates_timeout_and_dry_run_to_cleanup() {
        let spy = SpyCleanup::new(3);
        let checker = MultipartChecker::new(spy.clone(), Duration::hours(2), true);

        checker.check_all().await.unwrap();

        assert_eq!(
            spy.called_timeout_secs.load(Ordering::SeqCst),
            7200,
            "timeout forwarded as 2 h = 7200 s"
        );
        assert!(
            spy.called_dry_run.load(Ordering::SeqCst),
            "dry_run flag must be forwarded"
        );
    }

    #[tokio::test]
    async fn check_all_forwards_cleanup_error() {
        struct FailingCleanup;

        #[async_trait]
        impl MultipartCleanup for FailingCleanup {
            async fn cleanup_orphan_multipart_uploads(
                &self,
                _timeout: Duration,
                _dry_run: bool,
            ) -> Result<usize, Error> {
                Err(Error::StorageBackend("backend failure".to_string()))
            }
        }

        let checker = MultipartChecker::new(Arc::new(FailingCleanup), Duration::minutes(30), false);

        assert!(
            checker.check_all().await.is_err(),
            "errors from the cleanup backend must propagate"
        );
    }
}
