use std::sync::Arc;

use chrono::{Duration, Utc};
use tracing::{debug, error, info};

use crate::registry::{Error, blob_store::BlobStore};

pub struct UploadChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    upload_timeout: Duration,
    dry_run: bool,
}

impl UploadChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        upload_timeout: Duration,
        dry_run: bool,
    ) -> Self {
        Self {
            blob_store,
            upload_timeout,
            dry_run,
        }
    }

    pub async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking uploads from namespace '{namespace}'");

        let mut marker = None;
        loop {
            let (uploads, next_marker) =
                self.blob_store.list_uploads(namespace, 100, marker).await?;

            for uuid in &uploads {
                if let Err(e) = self.check_upload(namespace, uuid).await {
                    error!("Failed to check upload from '{namespace}' ('{uuid}'): {e}");
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        Ok(())
    }

    async fn check_upload(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        debug!("Checking upload '{namespace}/{uuid}'");
        let Ok((_, _, start_date)) = self.blob_store.read_upload_summary(namespace, uuid).await
        else {
            debug!("Inconsistent upload state '{namespace}/{uuid}', deleting it");
            self.delete_upload(namespace, uuid).await?;
            return Ok(());
        };

        if self.is_upload_obsolete(start_date) {
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
        self.blob_store.delete_upload(namespace, uuid).await?;
        Ok(())
    }

    fn is_upload_obsolete(&self, start_date: chrono::DateTime<Utc>) -> bool {
        let now = Utc::now();
        let duration = now.signed_duration_since(start_date);
        duration > self.upload_timeout
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
            let blob_store = test_case.blob_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(namespace, &upload_uuid)
                .await
                .unwrap();

            let scrubber = UploadChecker::new(blob_store.clone(), Duration::zero(), false);

            scrubber.check_namespace(namespace).await.unwrap();

            let result = blob_store
                .read_upload_summary(namespace, &upload_uuid)
                .await;
            assert!(result.is_err(), "Obsolete upload should be deleted");
        }
    }

    #[tokio::test]
    async fn test_scrub_uploads_keeps_recent() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(namespace, &upload_uuid)
                .await
                .unwrap();

            let scrubber = UploadChecker::new(blob_store.clone(), Duration::days(1), false);

            scrubber.check_namespace(namespace).await.unwrap();

            let result = blob_store
                .read_upload_summary(namespace, &upload_uuid)
                .await;
            assert!(result.is_ok(), "Recent upload should be kept");
        }
    }

    #[tokio::test]
    async fn test_scrub_uploads_dry_run() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();

            let upload_uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(namespace, &upload_uuid)
                .await
                .unwrap();

            let scrubber = UploadChecker::new(blob_store.clone(), Duration::zero(), true);

            scrubber.check_namespace(namespace).await.unwrap();

            let result = blob_store
                .read_upload_summary(namespace, &upload_uuid)
                .await;
            assert!(result.is_ok(), "Dry run should not delete obsolete upload");
        }
    }
}
