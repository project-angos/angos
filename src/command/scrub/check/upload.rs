use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    registry::blob_store::{self, UploadStore, UploadSummary},
};

enum UploadVerdict {
    /// Upload state is broken (missing summary or corrupted data) — delete.
    DeleteInconsistent,
    /// Upload age exceeds the timeout — delete.
    DeleteObsolete,
    /// Upload is healthy or the failure was transient — leave alone.
    Keep,
}

#[allow(clippy::match_same_arms)]
fn classify_upload(
    summary: Result<&UploadSummary, &blob_store::Error>,
    timeout: Duration,
    now: DateTime<Utc>,
) -> UploadVerdict {
    match summary {
        Ok(s) if now.signed_duration_since(s.started_at) > timeout => UploadVerdict::DeleteObsolete,
        Ok(_) => UploadVerdict::Keep,
        Err(
            blob_store::Error::UploadNotFound
            | blob_store::Error::ReferenceNotFound
            | blob_store::Error::BlobNotFound
            | blob_store::Error::InvalidFormat(_)
            | blob_store::Error::HashSerialization(_)
            | blob_store::Error::JSONSerialization(_),
        ) => UploadVerdict::DeleteInconsistent,
        Err(_) => UploadVerdict::Keep,
    }
}

pub struct UploadChecker {
    upload_store: Arc<dyn UploadStore>,
    upload_timeout: Duration,
}

impl UploadChecker {
    pub fn new(upload_store: Arc<dyn UploadStore>, upload_timeout: Duration) -> Self {
        Self {
            upload_store,
            upload_timeout,
        }
    }

    async fn check_upload(
        &self,
        namespace: &str,
        uuid: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking upload '{namespace}/{uuid}'");
        let summary = self.upload_store.summary(namespace, uuid).await;
        let verdict = classify_upload(summary.as_ref(), self.upload_timeout, Utc::now());

        match verdict {
            UploadVerdict::DeleteInconsistent => {
                debug!("Inconsistent upload state '{namespace}/{uuid}', deleting it");
                sink.apply(Action::DeleteExpiredUpload {
                    namespace: namespace.to_string(),
                    uuid: uuid.to_string(),
                })
                .await?;
            }
            UploadVerdict::DeleteObsolete => {
                sink.apply(Action::DeleteExpiredUpload {
                    namespace: namespace.to_string(),
                    uuid: uuid.to_string(),
                })
                .await?;
            }
            UploadVerdict::Keep => {}
        }
        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for UploadChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking uploads from namespace '{namespace}'");

        let mut uploads = list_all::uploads(&self.upload_store, namespace);
        while let Some(uuid) = uploads.next().await {
            let uuid = uuid?;
            if let Err(e) = self.check_upload(namespace, &uuid, sink).await {
                error!("Failed to check upload from '{namespace}' ('{uuid}'): {e}");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use angos_s3_client as s3_client;
    use chrono::TimeZone;

    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        registry::{blob_store, test_utils::backends},
    };

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap()
    }

    fn summary_started_at(offset_secs: i64) -> UploadSummary {
        UploadSummary {
            size: 0,
            started_at: fixed_now() - Duration::seconds(offset_secs),
        }
    }

    #[test]
    fn classify_upload_keeps_recent_upload() {
        let timeout = Duration::hours(1);
        let now = fixed_now();
        let summary = summary_started_at(1800);
        let verdict = classify_upload(Ok(&summary), timeout, now);
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[test]
    fn classify_upload_deletes_obsolete_upload() {
        let timeout = Duration::hours(1);
        let now = fixed_now();
        let summary = summary_started_at(7200);
        let verdict = classify_upload(Ok(&summary), timeout, now);
        assert!(matches!(verdict, UploadVerdict::DeleteObsolete));
    }

    #[test]
    fn classify_upload_keeps_upload_at_exact_boundary() {
        let timeout = Duration::hours(1);
        let now = fixed_now();
        let summary = summary_started_at(3600);
        let verdict = classify_upload(Ok(&summary), timeout, now);
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[test]
    fn classify_upload_deletes_inconsistent_on_upload_not_found() {
        let verdict = classify_upload(
            Err(&blob_store::Error::UploadNotFound),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::DeleteInconsistent));
    }

    #[test]
    fn classify_upload_deletes_inconsistent_on_reference_not_found() {
        let verdict = classify_upload(
            Err(&blob_store::Error::ReferenceNotFound),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::DeleteInconsistent));
    }

    #[test]
    fn classify_upload_deletes_inconsistent_on_invalid_format() {
        let verdict = classify_upload(
            Err(&blob_store::Error::InvalidFormat("bad data".to_string())),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::DeleteInconsistent));
    }

    #[test]
    fn classify_upload_keeps_on_storage_backend_error() {
        let verdict = classify_upload(
            Err(&blob_store::Error::StorageBackend(
                "S3 throttle".to_string(),
            )),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[test]
    fn classify_upload_keeps_on_data_store_error() {
        let verdict = classify_upload(
            Err(&blob_store::Error::DataStore(s3_client::Error::Io(
                "timeout".to_string(),
            ))),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[tokio::test]
    async fn test_scrub_uploads_removes_obsolete() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let upload_store = test_case.upload_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            upload_store.create(namespace, &upload_uuid).await.unwrap();

            let mut executor = Executor::new(
                test_case.blob_store(),
                test_case.metadata_store(),
                upload_store.clone(),
            );

            let checker = UploadChecker::new(upload_store.clone(), Duration::zero());
            checker.check(namespace, &mut executor).await.unwrap();

            let result = upload_store.summary(namespace, &upload_uuid).await;
            assert!(result.is_err(), "Obsolete upload should be deleted");
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_scrub_uploads_keeps_recent() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let upload_store = test_case.upload_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            upload_store.create(namespace, &upload_uuid).await.unwrap();

            let checker = UploadChecker::new(upload_store.clone(), Duration::days(1));
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "Recent upload must not produce any actions"
            );

            let result = upload_store.summary(namespace, &upload_uuid).await;
            assert!(result.is_ok(), "Recent upload should be kept");
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_scrub_uploads_dry_run() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let upload_store = test_case.upload_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            upload_store.create(namespace, &upload_uuid).await.unwrap();

            let checker = UploadChecker::new(upload_store.clone(), Duration::zero());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::DeleteExpiredUpload { .. })),
                "Vec sink must capture DeleteExpiredUpload action"
            );

            let result = upload_store.summary(namespace, &upload_uuid).await;
            assert!(result.is_ok(), "Vec sink must not delete the upload");
            test_case.cleanup().await;
        }
    }
}
