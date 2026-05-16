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
    blob_store: Arc<dyn BlobStore + Send + Sync>,
}

impl LayoutChecker {
    pub fn new(blob_store: Arc<dyn BlobStore + Send + Sync>) -> Self {
        Self { blob_store }
    }
}

#[async_trait]
impl StoreChecker for LayoutChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        debug!("Migrating metadata storage layout");

        sink.apply(Action::MigrateNamespaceRegistry).await?;

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
    use crate::registry::blob_store::{self, BlobStore};
    use tempfile::TempDir;

    #[tokio::test]
    async fn layout_checker_emits_namespace_and_blob_migration_actions() {
        let temp_dir = TempDir::new().unwrap();
        let blob_store = Arc::new(blob_store::fs::Backend::new(
            &crate::registry::data_store::fs::BackendConfig {
                root_dir: temp_dir.path().to_string_lossy().into_owned(),
                sync_to_disk: false,
            },
        ));
        let digest = blob_store.create(b"layout migration").await.unwrap();

        let checker = LayoutChecker::new(blob_store);
        let mut actions = Vec::new();
        checker.check_all(&mut actions).await.unwrap();

        assert!(matches!(
            actions.first(),
            Some(Action::MigrateNamespaceRegistry)
        ));
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::MigrateBlobIndex(blob) if blob == &digest))
        );
    }
}
