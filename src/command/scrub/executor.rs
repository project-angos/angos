use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use crate::{
    command::scrub::{action::Action, error::Error},
    registry::{
        blob_store::{BlobStore, MultipartCleanup, OrphanMultipartUpload, UploadStore},
        metadata_store::{
            BlobIndexOperation, MetadataStore, MetadataStoreExt, link_kind::LinkKind,
        },
        parse_manifest_digests,
    },
};

/// A sink that receives `Action` values produced by scrub checkers.
///
/// The production implementation (`Executor`) either applies or skips the
/// action based on `dry_run`; the `Vec<Action>` implementation captures
/// actions for test assertions without touching any storage.
#[async_trait]
pub trait ActionSink: Send {
    async fn apply(&mut self, action: Action) -> Result<(), Error>;
}

/// Applies scrub actions against live storage backends.
///
/// This is the single place that honours `dry_run`: every checker emits
/// `Action` values unconditionally and this type decides whether to perform
/// or skip the underlying mutation.
pub struct Executor {
    dry_run: bool,
    blob_store: Arc<dyn BlobStore>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    upload_store: Arc<dyn UploadStore>,
    multipart_cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
}

impl Executor {
    pub fn new(
        dry_run: bool,
        blob_store: Arc<dyn BlobStore>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        upload_store: Arc<dyn UploadStore>,
        multipart_cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
    ) -> Self {
        Self {
            dry_run,
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
        if self.dry_run {
            info!("DRY RUN: would {action}");
            return Ok(());
        }

        info!("{action}");

        match action {
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
                let mut tx = self.metadata_store.begin_transaction(&namespace);
                tx.create_link(&link, &target).add();
                tx.commit().await?;
            }
            Action::AddReferrer {
                namespace,
                link,
                target,
                referrer,
            } => {
                let mut tx = self.metadata_store.begin_transaction(&namespace);
                tx.create_link(&link, &target)
                    .with_referrer(&referrer)
                    .add();
                tx.commit().await?;
            }
            Action::SetMediaType {
                namespace,
                link,
                target,
                media_type,
                ..
            } => {
                let mut tx = self.metadata_store.begin_transaction(&namespace);
                tx.create_link(&link, &target)
                    .with_media_type(&media_type)
                    .add();
                tx.commit().await?;
            }
            Action::AbortMultipartUpload { key, upload_id } => {
                self.multipart_cleanup
                    .abort_orphan_multipart_upload(&OrphanMultipartUpload { key, upload_id })
                    .await?;
            }
            Action::DeleteTag { namespace, tag } => {
                let mut tx = self.metadata_store.begin_transaction(&namespace);
                tx.delete_link(&LinkKind::Tag(tag));
                tx.commit().await?;
            }
            Action::DeleteOrphanManifest { namespace, digest } => {
                let content = self.blob_store.read(&digest).await?;
                let manifest = parse_manifest_digests(&content, None)?;

                let mut tx = self.metadata_store.begin_transaction(&namespace);

                if let Some(config) = &manifest.config {
                    tx.delete_link(&LinkKind::Config(config.clone()));
                }

                for layer in &manifest.layers {
                    tx.delete_link(&LinkKind::Layer(layer.clone()));
                }

                for child in &manifest.manifests {
                    tx.delete_link(&LinkKind::Manifest(digest.clone(), child.clone()));
                }

                if let Some(subject) = &manifest.subject {
                    tx.delete_link(&LinkKind::Referrer(subject.clone(), digest.clone()));
                }

                tx.delete_link(&LinkKind::Digest(digest));

                tx.commit().await?;
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
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let orphan_content = b"executor dry-run test";
            let orphan_digest = blob_store.create(orphan_content).await.unwrap();

            let mut executor = Executor::new(
                true,
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
                blob_store.read(&orphan_digest).await.is_ok(),
                "dry-run must not delete the blob"
            );
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
                false,
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
