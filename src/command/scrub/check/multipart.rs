use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;
use tracing::info;

use crate::{
    command::scrub::{action::Action, check::StoreChecker, error::Error, executor::ActionSink},
    registry::blob_store::{self, MultipartCleanup},
};

pub struct MultipartChecker {
    cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
    timeout: Duration,
}

impl MultipartChecker {
    pub fn new(cleanup: Arc<dyn MultipartCleanup + Send + Sync>, timeout: Duration) -> Self {
        Self { cleanup, timeout }
    }
}

#[async_trait]
impl StoreChecker for MultipartChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        let orphans = self
            .cleanup
            .list_orphan_multipart_uploads(self.timeout)
            .await
            .map_err(|e: blob_store::Error| Error::from(e))?;
        let count = orphans.len();
        for orphan in &orphans {
            sink.apply(Action::AbortMultipartUpload {
                key: orphan.key.clone(),
                upload_id: orphan.upload_id.clone(),
            })
            .await?;
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
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        registry::{blob_store::OrphanMultipartUpload, test_utils::backends},
    };

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
        ) -> Result<Vec<OrphanMultipartUpload>, blob_store::Error> {
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
        ) -> Result<(), blob_store::Error> {
            self.abort_call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn check_all_lists_orphans_and_captures_in_vec_sink() {
        let spy = SpyCleanup::new(vec!["ns/_uploads/uuid1/data", "ns/_uploads/uuid2/data"]);
        let checker = MultipartChecker::new(spy.clone(), Duration::hours(2));

        let mut sink: Vec<Action> = Vec::new();
        checker.check_all(&mut sink).await.unwrap();

        assert_eq!(
            spy.list_called_timeout_secs.load(Ordering::SeqCst),
            7200,
            "timeout forwarded as 2 h = 7200 s"
        );
        assert_eq!(
            spy.abort_call_count.load(Ordering::SeqCst),
            0,
            "Vec sink must not invoke abort"
        );
        assert_eq!(sink.len(), 2, "two orphans produce two actions");
        assert!(
            sink.iter()
                .all(|a| matches!(a, Action::AbortMultipartUpload { .. }))
        );
    }

    #[tokio::test]
    async fn check_all_aborts_orphans_when_executor_used() {
        let spy = SpyCleanup::new(vec!["ns/_uploads/uuid1/data", "ns/_uploads/uuid2/data"]);
        let checker = MultipartChecker::new(spy.clone(), Duration::hours(2));

        // Use the first available test backend just for stores; only
        // multipart_cleanup is exercised in this test.
        let test_case = backends().into_iter().next().unwrap();
        let mut executor = Executor::new(
            test_case.blob_store(),
            test_case.metadata_store(),
            test_case.upload_store(),
            spy.clone(),
        );

        checker.check_all(&mut executor).await.unwrap();

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
            ) -> Result<Vec<OrphanMultipartUpload>, blob_store::Error> {
                Err(blob_store::Error::StorageBackend(
                    "backend failure".to_string(),
                ))
            }

            async fn abort_orphan_multipart_upload(
                &self,
                _upload: &OrphanMultipartUpload,
            ) -> Result<(), blob_store::Error> {
                Ok(())
            }
        }

        let checker = MultipartChecker::new(Arc::new(FailingCleanup), Duration::minutes(30));
        let mut sink: Vec<Action> = Vec::new();

        assert!(
            checker.check_all(&mut sink).await.is_err(),
            "errors from the cleanup backend must propagate"
        );
    }
}
