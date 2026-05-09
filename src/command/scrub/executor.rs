use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use crate::{
    command::scrub::{action::Action, error::Error},
    oci::Digest,
    registry::{
        ParsedManifestDigests,
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

/// Returns the `LinkKind`s that must be deleted to remove a manifest from a
/// namespace. Pure: no I/O, no state. Each entry corresponds to a
/// `LinkOperation::Delete` with no referrer when applied to a transaction.
///
/// Order is fixed: config (if present), layers, child manifests, subject's
/// referrer back-link (if present), then the manifest's own digest link.
fn build_delete_transaction(manifest: &ParsedManifestDigests, digest: &Digest) -> Vec<LinkKind> {
    let mut links = Vec::new();
    if let Some(config) = &manifest.config {
        links.push(LinkKind::Config(config.clone()));
    }
    for layer in &manifest.layers {
        links.push(LinkKind::Layer(layer.clone()));
    }
    for child in &manifest.manifests {
        links.push(LinkKind::Manifest(digest.clone(), child.clone()));
    }
    if let Some(subject) = &manifest.subject {
        links.push(LinkKind::Referrer(subject.clone(), digest.clone()));
    }
    links.push(LinkKind::Digest(digest.clone()));
    links
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
                for link in build_delete_transaction(&manifest, &digest) {
                    tx.delete_link(&link);
                }
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

    fn dummy_digest(byte: u8) -> Digest {
        let hex = format!("{byte:02x}").repeat(32);
        Digest::from_str(&format!("sha256:{hex}")).unwrap()
    }

    #[test]
    fn build_delete_transaction_emits_only_self_link_for_minimal_manifest() {
        let manifest = ParsedManifestDigests {
            subject: None,
            config: None,
            layers: vec![],
            manifests: vec![],
        };
        let digest = dummy_digest(0xaa);

        let links = build_delete_transaction(&manifest, &digest);

        assert_eq!(links.len(), 1);
        assert!(matches!(&links[0], LinkKind::Digest(d) if d == &digest));
    }

    #[test]
    fn build_delete_transaction_includes_config_layers_children_and_subject() {
        let digest = dummy_digest(0xff);
        let config_digest = dummy_digest(0x01);
        let layer_a = dummy_digest(0x02);
        let layer_b = dummy_digest(0x03);
        let child = dummy_digest(0x04);
        let subject = dummy_digest(0x05);

        let manifest = ParsedManifestDigests {
            subject: Some(subject.clone()),
            config: Some(config_digest.clone()),
            layers: vec![layer_a.clone(), layer_b.clone()],
            manifests: vec![child.clone()],
        };

        let links = build_delete_transaction(&manifest, &digest);

        // Order: config, layers (in input order), child manifests, subject referrer, self.
        assert_eq!(links.len(), 6);
        assert!(matches!(&links[0], LinkKind::Config(d) if d == &config_digest));
        assert!(matches!(&links[1], LinkKind::Layer(d) if d == &layer_a));
        assert!(matches!(&links[2], LinkKind::Layer(d) if d == &layer_b));
        assert!(
            matches!(&links[3], LinkKind::Manifest(parent, c) if parent == &digest && c == &child)
        );
        assert!(matches!(&links[4], LinkKind::Referrer(s, r) if s == &subject && r == &digest));
        assert!(matches!(&links[5], LinkKind::Digest(d) if d == &digest));
    }
}
