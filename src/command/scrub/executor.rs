use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::{
    command::scrub::{action::Action, error::Error},
    oci::{Manifest, Reference},
    registry::{
        blob_store::{self, BlobStore, MultipartCleanup, OrphanMultipartUpload, UploadStore},
        manifest::{find_tags_pointing_at, link_plan},
        metadata_store::{
            BlobIndexOperation, Error as MetadataStoreError, LinkOperation, MetadataStore,
            link_kind::LinkKind,
        },
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
    #[allow(clippy::too_many_lines)]
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
                let guard = self.metadata_store.acquire_blob_data_lock(&digest).await?;

                let has_references = self.metadata_store.has_blob_references(&digest).await;
                let delete_result = match has_references {
                    Err(e) => Err(e.into()),
                    Ok(true) => {
                        info!("skipping orphan blob deletion: reference appeared for {digest}");
                        Ok(())
                    }
                    Ok(false) => match self.blob_store.delete(&digest).await {
                        Ok(())
                        | Err(
                            blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound,
                        ) => Ok(()),
                        Err(e) => Err(Error::from(e)),
                    },
                };

                let lock_valid = guard.is_valid();
                guard.release().await;
                delete_result?;

                if !lock_valid {
                    return Err(
                        MetadataStoreError::Lock(
                            "lock invalidated during orphan blob deletion".into(),
                        )
                        .into(),
                    );
                }
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
                let manifest = match self.blob_store.read(&digest).await {
                    Ok(content) => Manifest::from_slice(&content).ok(),
                    Err(
                        blob_store::Error::BlobNotFound
                        | blob_store::Error::ReferenceNotFound,
                    ) => {
                        warn!("Manifest blob missing for {digest}, proceeding with metadata-only deletion");
                        None
                    }
                    Err(e) => return Err(Error::from(e)),
                };
                let tags = find_tags_pointing_at(self.metadata_store.as_ref(), &namespace, &digest)
                    .await?;
                let ops = link_plan::delete(&Reference::Digest(digest), manifest.as_ref(), &tags);
                self.metadata_store.update_links(&namespace, &ops).await?;
            }
            Action::DeleteExpiredUpload { namespace, uuid } => {
                self.upload_store.delete(&namespace, &uuid).await?;
            }
            Action::DeleteOrphanReferrer {
                namespace,
                subject,
                referrer,
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::delete(LinkKind::Referrer(subject, referrer))],
                    )
                    .await?;
            }
            Action::RemoveReferrer {
                namespace,
                link,
                referrer,
            } => {
                self.metadata_store
                    .update_links(
                        &namespace,
                        &[LinkOperation::delete_with_referrer(link, referrer)],
                    )
                    .await?;
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
        registry::{
            metadata_store::{LinkOperation, link_kind::LinkKind},
            test_utils::{NoopMultipart, backends},
        },
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
    async fn executor_delete_orphan_manifest_missing_blob_still_removes_digest_link() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let namespace = "test-repo/app";

            // Write manifest blob and create a digest link, then delete the blob.
            let content = b"orphan manifest content for missing-blob test";
            let digest = blob_store.create(content).await.unwrap();
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();
            blob_store.delete(&digest).await.unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::DeleteOrphanManifest {
                    namespace: namespace.to_string(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                    .await
                    .is_err(),
                "digest link must be removed even when the blob is missing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_manifest_missing_blob_removes_tag_link() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let namespace = "test-repo/app";

            let content = b"orphan manifest with tag - missing blob";
            let digest = blob_store.create(content).await.unwrap();
            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(digest.clone()),
                            digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Tag("dangling".to_string()),
                            digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();
            blob_store.delete(&digest).await.unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::DeleteOrphanManifest {
                    namespace: namespace.to_string(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag("dangling".to_string()), false)
                    .await
                    .is_err(),
                "tag link pointing at missing-blob digest must be removed"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_blob_takes_blob_data_lock() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let content = b"blob with no ownership entry";
            let digest = blob_store.create(content).await.unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store,
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::DeleteOrphanBlob(digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&digest).await.is_err(),
                "unreferenced blob must be deleted"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_blob_skips_when_reference_appears_between_classification_and_apply(
    ) {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let content = b"blob that got ownership just in time";
            let digest = blob_store.create(content).await.unwrap();

            metadata_store
                .update_blob_index(
                    "test-repo/app",
                    &digest,
                    BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                )
                .await
                .unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store,
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::DeleteOrphanBlob(digest.clone()))
                .await
                .unwrap();

            assert!(
                blob_store.read(&digest).await.is_ok(),
                "blob with a reference must not be deleted"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_delete_orphan_referrer_removes_referrer_link() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let namespace = "test-repo/referrer-exec";

            let subject_digest = blob_store.create(b"subject for referrer exec").await.unwrap();
            let referrer_digest = blob_store.create(b"referrer for referrer exec").await.unwrap();

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(subject_digest.clone()),
                            subject_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone()),
                            referrer_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone()),
                        false,
                    )
                    .await
                    .is_ok(),
                "Referrer link must exist before applying the action"
            );

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::DeleteOrphanReferrer {
                    namespace: namespace.to_string(),
                    subject: subject_digest.clone(),
                    referrer: referrer_digest.clone(),
                })
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone()),
                        false,
                    )
                    .await
                    .is_err(),
                "Referrer link must be removed after applying the action"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn executor_remove_referrer_cascades_to_link_delete_when_referenced_by_becomes_empty() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let upload_store = test_case.upload_store();

            let namespace = "test-repo/remove-referrer-cascade";

            // Create a layer blob and the corresponding layer link with exactly
            // one phantom referrer so referenced_by = {phantom}.
            let layer_content = b"layer content for cascade test";
            let layer_digest = blob_store.create(layer_content).await.unwrap();
            let phantom_digest = Digest::from_str(
                "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            )
            .unwrap();

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                        phantom_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            // Confirm the layer link exists with the phantom referrer.
            let before = metadata_store
                .read_link(
                    namespace,
                    &LinkKind::Layer(layer_digest.clone()),
                    false,
                )
                .await
                .unwrap();
            assert!(
                before.referenced_by.contains(&phantom_digest),
                "phantom referrer must be present before the action"
            );

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                upload_store,
                Arc::new(NoopMultipart),
            );

            executor
                .apply(Action::RemoveReferrer {
                    namespace: namespace.to_string(),
                    link: LinkKind::Layer(layer_digest.clone()),
                    referrer: phantom_digest.clone(),
                })
                .await
                .unwrap();

            // After removing the only referrer the link itself must be gone.
            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Layer(layer_digest.clone()),
                        false,
                    )
                    .await
                    .is_err(),
                "layer link must be removed when referenced_by becomes empty"
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
