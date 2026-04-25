use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use tracing::{debug, error, info};

use crate::{
    command::scrub::check::NamespaceChecker,
    registry::{Error, blob_store::UploadStore, pagination::for_each_page},
};

pub struct UploadChecker {
    upload_store: Arc<dyn UploadStore>,
    upload_timeout: Duration,
    dry_run: bool,
}

impl UploadChecker {
    pub fn new(
        upload_store: Arc<dyn UploadStore>,
        upload_timeout: Duration,
        dry_run: bool,
    ) -> Self {
        Self {
            upload_store,
            upload_timeout,
            dry_run,
        }
    }

    async fn check_upload(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        debug!("Checking upload '{namespace}/{uuid}'");
        let Ok(summary) = self.upload_store.summary(namespace, uuid).await else {
            debug!("Inconsistent upload state '{namespace}/{uuid}', deleting it");
            self.delete_upload(namespace, uuid).await?;
            return Ok(());
        };

        if self.is_upload_obsolete(summary.started_at) {
            self.delete_upload(namespace, uuid).await?;
        }

        Ok(())
    }

    async fn delete_upload(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        if self.dry_run {
            info!("DRY RUN: would delete expired upload '{namespace}/{uuid}'");
            return Ok(());
        }

        info!("Deleting expired upload from namespace '{namespace}/{uuid}'");
        self.upload_store.delete(namespace, uuid).await?;
        Ok(())
    }

    fn is_upload_obsolete(&self, start_date: chrono::DateTime<Utc>) -> bool {
        let now = Utc::now();
        let duration = now.signed_duration_since(start_date);
        duration > self.upload_timeout
    }

    async fn process_page(&self, namespace: &str, uploads: Vec<String>) -> Result<(), Error> {
        for uuid in &uploads {
            if let Err(e) = self.check_upload(namespace, uuid).await {
                error!("Failed to check upload from '{namespace}' ('{uuid}'): {e}");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for UploadChecker {
    async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking uploads from namespace '{namespace}'");

        for_each_page(
            |marker| async move {
                self.upload_store
                    .list(namespace, 100, marker)
                    .await
                    .map_err(Error::from)
            },
            |uploads| self.process_page(namespace, uploads),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::tests::backends;

    #[tokio::test]
    async fn test_scrub_uploads_removes_obsolete() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let upload_store = test_case.upload_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            upload_store.create(namespace, &upload_uuid).await.unwrap();

            let scrubber = UploadChecker::new(upload_store.clone(), Duration::zero(), false);

            scrubber.check_namespace(namespace).await.unwrap();

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

            let scrubber = UploadChecker::new(upload_store.clone(), Duration::days(1), false);

            scrubber.check_namespace(namespace).await.unwrap();

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

            let scrubber = UploadChecker::new(upload_store.clone(), Duration::zero(), true);

            scrubber.check_namespace(namespace).await.unwrap();

            let result = upload_store.summary(namespace, &upload_uuid).await;
            assert!(result.is_ok(), "Dry run should not delete obsolete upload");
        }
    }
}
