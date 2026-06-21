use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{StoreChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    registry::blob_store::BlobStore,
};

/// Migrates metadata layout documents that scrub can discover safely.
///
/// Backend-specific migration details live behind `MetadataStore`; this checker
/// only enumerates registry-wide subjects and emits ordinary scrub actions so
/// dry-run and real runs keep the same control flow as consistency repairs.
pub struct LayoutChecker {
    blob_store: Arc<BlobStore>,
}

impl LayoutChecker {
    pub fn new(blob_store: Arc<BlobStore>) -> Self {
        Self { blob_store }
    }
}

#[async_trait]
impl StoreChecker for LayoutChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        debug!("Migrating metadata storage layout");

        sink.apply(Action::PruneLegacyNamespaceRegistry).await?;

        let mut blobs = list_all::blobs(&self.blob_store);
        while let Some(blob) = blobs.next().await {
            let blob = blob?;
            if let Err(error) = sink.apply(Action::MigrateBlobIndex(blob.clone())).await {
                error!("Failed to migrate blob index layout for {blob}: {error}");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{blob_store, test_utils::put_blob_direct};
    use tempfile::TempDir;

    #[tokio::test]
    async fn layout_checker_emits_blob_migration_actions() {
        let temp_dir = TempDir::new().unwrap();
        let root_dir = temp_dir.path().to_string_lossy().into_owned();
        let blob_store = Arc::new(
            blob_store::BlobStoreConfig::FS(blob_store::FsBackendConfig {
                root_dir,
                sync_to_disk: false,
            })
            .build_backend()
            .unwrap(),
        );
        let digest = put_blob_direct(blob_store.store.as_ref(), b"layout migration").await;

        let checker = LayoutChecker::new(blob_store);
        let mut actions = Vec::new();
        checker.check_all(&mut actions).await.unwrap();

        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::PruneLegacyNamespaceRegistry))
        );
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::MigrateBlobIndex(blob) if blob == &digest))
        );
    }
}
