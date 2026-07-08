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
    oci::Namespace,
    registry::{
        Error as RegistryError,
        blob_store::{BlobStore, UploadSummary},
    },
};

enum UploadVerdict {
    /// Upload state is broken (missing summary or corrupted data): delete.
    DeleteInconsistent,
    /// Upload age exceeds the timeout: delete.
    DeleteObsolete,
    /// Upload is healthy or the failure was transient: leave alone.
    Keep,
}

#[allow(clippy::match_same_arms)]
fn classify_upload(
    summary: Result<&UploadSummary, &RegistryError>,
    timeout: Duration,
    now: DateTime<Utc>,
) -> UploadVerdict {
    match summary {
        Ok(s) if now.signed_duration_since(s.started_at) > timeout => UploadVerdict::DeleteObsolete,
        Ok(_) => UploadVerdict::Keep,
        Err(
            RegistryError::BlobUploadUnknown | RegistryError::NotFound | RegistryError::BlobUnknown,
        ) => UploadVerdict::DeleteInconsistent,
        // A corrupt session record and a transient backend read both collapse to
        // `Internal` now, so this arm covers both; it errs on the safe side and
        // keeps the upload rather than risk deleting one on a transient error.
        Err(_) => UploadVerdict::Keep,
    }
}

pub struct UploadChecker {
    blob_store: Arc<BlobStore>,
    upload_timeout: Duration,
}

impl UploadChecker {
    pub fn new(blob_store: Arc<BlobStore>, upload_timeout: Duration) -> Self {
        Self {
            blob_store,
            upload_timeout,
        }
    }

    async fn check_upload(
        &self,
        namespace: &Namespace,
        uuid: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking upload '{namespace}/{uuid}'");
        let summary = self.blob_store.upload_summary(namespace, uuid).await;
        let verdict = classify_upload(summary.as_ref(), self.upload_timeout, Utc::now());

        match verdict {
            UploadVerdict::DeleteInconsistent => {
                debug!("Inconsistent upload state '{namespace}/{uuid}', deleting it");
                sink.apply(Action::DeleteExpiredUpload {
                    namespace: namespace.clone(),
                    uuid: uuid.to_string(),
                })
                .await?;
            }
            UploadVerdict::DeleteObsolete => {
                sink.apply(Action::DeleteExpiredUpload {
                    namespace: namespace.clone(),
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
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking uploads from namespace '{namespace}'");

        let mut uploads = list_all::uploads(&self.blob_store, namespace);
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
    use chrono::TimeZone;

    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        registry::test_utils::for_each_backend,
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
            Err(&RegistryError::BlobUploadUnknown),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::DeleteInconsistent));
    }

    #[test]
    fn classify_upload_deletes_inconsistent_on_reference_not_found() {
        let verdict = classify_upload(
            Err(&RegistryError::NotFound),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::DeleteInconsistent));
    }

    #[test]
    fn classify_upload_keeps_on_opaque_internal_error() {
        // A corrupt session record and a transient backend read both surface as
        // `Internal` after the error-chain collapse and can no longer be told
        // apart, so the checker conservatively keeps the upload for both.
        let verdict = classify_upload(
            Err(&RegistryError::Internal("S3 throttle".to_string())),
            Duration::hours(1),
            fixed_now(),
        );
        assert!(matches!(verdict, UploadVerdict::Keep));
    }

    #[tokio::test]
    async fn test_scrub_uploads_removes_obsolete() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&namespace, &upload_uuid)
                .await
                .unwrap();

            let mut executor =
                Executor::new_for_test(blob_store.clone(), test_case.metadata_store());

            let checker = UploadChecker::new(blob_store.clone(), Duration::zero());
            checker.check(&namespace, &mut executor).await.unwrap();

            let result = blob_store.upload_summary(&namespace, &upload_uuid).await;
            assert!(result.is_err(), "Obsolete upload should be deleted");
        })
        .await;
    }

    #[tokio::test]
    async fn test_scrub_uploads_keeps_recent() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&namespace, &upload_uuid)
                .await
                .unwrap();

            let checker = UploadChecker::new(blob_store.clone(), Duration::days(1));
            let mut sink: Vec<Action> = Vec::new();
            checker.check(&namespace, &mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "Recent upload must not produce any actions"
            );

            let result = blob_store.upload_summary(&namespace, &upload_uuid).await;
            assert!(result.is_ok(), "Recent upload should be kept");
        })
        .await;
    }

    #[tokio::test]
    async fn test_scrub_uploads_dry_run() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&namespace, &upload_uuid)
                .await
                .unwrap();

            let checker = UploadChecker::new(blob_store.clone(), Duration::zero());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(&namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::DeleteExpiredUpload { .. })),
                "Vec sink must capture DeleteExpiredUpload action"
            );

            let result = blob_store.upload_summary(&namespace, &upload_uuid).await;
            assert!(result.is_ok(), "Vec sink must not delete the upload");
        })
        .await;
    }
}
