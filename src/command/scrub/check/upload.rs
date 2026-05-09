use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    registry::blob_store::UploadStore,
};

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
        let Ok(summary) = self.upload_store.summary(namespace, uuid).await else {
            debug!("Inconsistent upload state '{namespace}/{uuid}', deleting it");
            sink.apply(Action::DeleteExpiredUpload {
                namespace: namespace.to_string(),
                uuid: uuid.to_string(),
            })
            .await?;
            return Ok(());
        };

        if self.is_upload_obsolete(summary.started_at) {
            sink.apply(Action::DeleteExpiredUpload {
                namespace: namespace.to_string(),
                uuid: uuid.to_string(),
            })
            .await?;
        }

        Ok(())
    }

    fn is_upload_obsolete(&self, start_date: chrono::DateTime<Utc>) -> bool {
        let now = Utc::now();
        let duration = now.signed_duration_since(start_date);
        duration > self.upload_timeout
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
    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        registry::test_utils::{NoopMultipart, backends},
    };

    #[tokio::test]
    async fn test_scrub_uploads_removes_obsolete() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let upload_store = test_case.upload_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            upload_store.create(namespace, &upload_uuid).await.unwrap();

            let mut executor = Executor::new(
                false,
                test_case.blob_store(),
                test_case.metadata_store(),
                upload_store.clone(),
                std::sync::Arc::new(NoopMultipart),
            );

            let checker = UploadChecker::new(upload_store.clone(), Duration::zero());
            checker.check(namespace, &mut executor).await.unwrap();

            let result = upload_store.summary(namespace, &upload_uuid).await;
            assert!(result.is_err(), "Obsolete upload should be deleted");
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
        }
    }
}
