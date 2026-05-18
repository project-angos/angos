use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use crate::{
    command::scrub::{action::Action, error::Error},
    oci::{Manifest, Reference},
    registry::{
        blob_store::{BlobStore, MultipartCleanup, OrphanMultipartUpload, UploadStore},
        manifest::{find_tags_pointing_at, link_plan},
        metadata_store::{BlobIndexOperation, LinkOperation, MetadataStore, link_kind::LinkKind},
    },
};

/// A sink that receives `Action` values produced by scrub checkers.
#[async_trait]
pub trait ActionSink: Send {
    async fn apply(&mut self, action: Action) -> Result<(), Error>;
}

/// Logs actions as dry-run without applying any mutations to storage.
pub struct DryRunSink;

#[async_trait]
impl ActionSink for DryRunSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        info!("DRY RUN: would {action}");
        Ok(())
    }
}

/// Applies scrub actions against live storage backends.
pub struct Executor {
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    upload_store: Arc<dyn UploadStore>,
    multipart_cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
}

impl Executor {
    pub fn new(
        blob_store: Arc<dyn BlobStore>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        upload_store: Arc<dyn UploadStore>,
        multipart_cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            upload_store,
            multipart_cleanup,
        }
    }
}

#[async_trait]
impl ActionSink for Executor {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        info!("{action}");

        match action {
            Action::MigrateNamespaceRegistry => {
                self.metadata_store.migrate_namespace_registry().await?;
            }
            Action::MigrateBlobIndex(digest) => {
                self.metadata_store.migrate_blob_index(&digest).await?;
            }
            Action::DeleteOrphanBlob(digest) => {
                self.blob_store.delete(&digest).await?;
            }
            Action::RemoveBlobIndexLink {
                namespace,
                blob,
                link,
            } => {
                self.metadata_store
                    .update_blob_index(&namespace, &blob, BlobIndexOperation::Remove(link))
                    .await?;
            }
            Action::RecreateLink {
                namespace,
                link,
                target,
            } => {
                self.metadata_store
                    .update_links(&namespace, &[LinkOperation::create(link, target)])
                    .await?;
            }
            Action::AddReferrer {
                namespace,
                link,
                target,
                referrer,
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::create_with_referrer(link, target, referrer)],
                    )
                    .await?;
            }
            Action::SetMediaType {
                namespace,
                link,
                target,
                media_type,
                ..
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::create_with_media_type(
                            link,
                            target,
                            Some(media_type),
                        )],
                    )
                    .await?;
            }
            Action::AbortMultipartUpload { key, upload_id } => {
                self.multipart_cleanup
                    .abort_orphan_multipart_upload(&OrphanMultipartUpload { key, upload_id })
                    .await?;
            }
            Action::DeleteTag { namespace, tag } => {
                self.metadata_store
                    .update_links(&namespace, &[LinkOperation::delete(LinkKind::Tag(tag))])
                    .await?;
            }
            Action::DeleteOrphanManifest { namespace, digest } => {
                let content = self.blob_store.read(&digest).await?;
                let manifest = Manifest::from_slice(&content).ok();
                let tags = find_tags_pointing_at(self.metadata_store.as_ref(), &namespace, &digest)
                    .await?;
                let ops = link_plan::delete(&Reference::Digest(digest), manifest.as_ref(), &tags);
                self.metadata_store.update_links(&namespace, &ops).await?;
            }
            Action::DeleteExpiredUpload { namespace, uuid } => {
                self.upload_store.delete(&namespace, &uuid).await?;
            }
        }

        Ok(())
    }
}

/// Captures actions into a `Vec` without performing any I/O.
///
/// Used in tests to assert which actions a checker would produce without
/// touching any real storage backend.
#[async_trait]
impl ActionSink for Vec<Action> {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        self.push(action);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::{
        oci::Digest,
        registry::test_utils::{NoopMultipart, backends},
    };

    #[tokio::test]
    async fn executor_dry_run_does_not_delete_blob() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();

            let orphan_content = b"executor dry-run test";
            let orphan_digest = blob_store.create(orphan_content).await.unwrap();

            let mut sink = DryRunSink;
            sink.apply(Action::DeleteOrphanBlob(orphan_digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_ok(),
                "dry-run must not delete the blob"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_real_run_deletes_blob() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let orphan_content = b"executor real-run test";
            let orphan_digest = blob_store.create(orphan_content).await.unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store,
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::DeleteOrphanBlob(orphan_digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_err(),
                "real-run must delete the blob"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn vec_sink_captures_actions_without_io() {
        let digest = Digest::from_str(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        sink.apply(Action::DeleteOrphanBlob(digest.clone()))
            .await
            .unwrap();
        sink.apply(Action::DeleteExpiredUpload {
            namespace: "ns".to_string(),
            uuid: "uuid".to_string(),
        })
        .await
        .unwrap();

        assert_eq!(sink.len(), 2);
        assert!(matches!(sink[0], Action::DeleteOrphanBlob(_)));
        assert!(matches!(sink[1], Action::DeleteExpiredUpload { .. }));
    }
}
