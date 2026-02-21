use std::sync::Arc;

use tracing::{debug, error, info};

use crate::oci::Manifest;
use crate::registry::Error;
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::link_kind::LinkKind;
use crate::registry::metadata_store::{MetadataStore, MetadataStoreExt};

pub struct MediaTypeChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    dry_run: bool,
}

impl MediaTypeChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        dry_run: bool,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            dry_run,
        }
    }

    pub async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking media_type field for namespace '{namespace}'");

        let mut marker = None;
        loop {
            let (revisions, next_marker) = self
                .metadata_store
                .list_revisions(namespace, 100, marker)
                .await?;

            for revision in &revisions {
                if let Err(e) = self.backfill_revision(namespace, revision).await {
                    error!(
                        "Failed to backfill media_type for '{namespace}' (revision '{revision}'): {e}"
                    );
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        let mut marker = None;
        loop {
            let (tags, next_marker) = self
                .metadata_store
                .list_tags(namespace, 100, marker)
                .await?;

            for tag in &tags {
                if let Err(e) = self.backfill_tag(namespace, tag).await {
                    error!("Failed to backfill media_type for '{namespace}' (tag '{tag}'): {e}");
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        Ok(())
    }

    async fn backfill_revision(
        &self,
        namespace: &str,
        revision: &crate::oci::Digest,
    ) -> Result<(), Error> {
        let link = LinkKind::Digest(revision.clone());
        let metadata = self
            .metadata_store
            .read_link(namespace, &link, false)
            .await?;

        if metadata.media_type.is_some() {
            debug!("Revision {revision} already has media_type, skipping");
            return Ok(());
        }

        let media_type = self.read_media_type(revision).await?;
        let Some(media_type) = media_type else {
            debug!("Revision {revision} has no media_type in manifest, skipping");
            return Ok(());
        };

        if self.dry_run {
            info!(
                "DRY RUN: would set media_type '{media_type}' on revision {revision} in namespace '{namespace}'"
            );
            return Ok(());
        }

        info!(
            "Setting media_type '{media_type}' on revision {revision} in namespace '{namespace}'"
        );
        let mut tx = self.metadata_store.begin_transaction(namespace);
        tx.create_link_with_media_type(&link, &metadata.target, &media_type);
        tx.commit().await?;

        Ok(())
    }

    async fn backfill_tag(&self, namespace: &str, tag: &str) -> Result<(), Error> {
        let link = LinkKind::Tag(tag.to_string());
        let metadata = self
            .metadata_store
            .read_link(namespace, &link, false)
            .await?;

        if metadata.media_type.is_some() {
            debug!("Tag '{tag}' already has media_type, skipping");
            return Ok(());
        }

        let media_type = self.read_media_type(&metadata.target).await?;
        let Some(media_type) = media_type else {
            debug!("Tag '{tag}' target has no media_type in manifest, skipping");
            return Ok(());
        };

        if self.dry_run {
            info!(
                "DRY RUN: would set media_type '{media_type}' on tag '{tag}' in namespace '{namespace}'"
            );
            return Ok(());
        }

        info!("Setting media_type '{media_type}' on tag '{tag}' in namespace '{namespace}'");
        let mut tx = self.metadata_store.begin_transaction(namespace);
        tx.create_link_with_media_type(&link, &metadata.target, &media_type);
        tx.commit().await?;

        Ok(())
    }

    async fn read_media_type(&self, digest: &crate::oci::Digest) -> Result<Option<String>, Error> {
        let content = self.blob_store.read_blob(digest).await?;
        let manifest: Manifest = serde_json::from_slice(&content).unwrap_or_default();
        Ok(manifest.media_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{metadata_store::MetadataStoreExt, test_utils, tests::backends};

    #[tokio::test]
    async fn test_media_type_checker_backfills_missing_media_type() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let (layer_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"layer content").await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": [
                    {{
                        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                        "digest": "{layer_digest}",
                        "size": 456
                    }}
                ]
            }}"#
            );

            let manifest_digest = blob_store
                .create_blob(manifest_content.as_bytes())
                .await
                .unwrap();

            // Create digest and tag links WITHOUT media_type
            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest);
            tx.create_link(&LinkKind::Tag("latest".to_string()), &manifest_digest);
            tx.commit().await.unwrap();

            // Verify media_type is None before check
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                digest_link.media_type.is_none(),
                "Digest link should start with no media_type"
            );

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
                .await
                .unwrap();
            assert!(
                tag_link.media_type.is_none(),
                "Tag link should start with no media_type"
            );

            // Run checker
            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone(), false);
            checker.check_namespace(namespace).await.unwrap();

            // Verify media_type is set after check
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert_eq!(
                digest_link.media_type.as_deref(),
                Some(media_type),
                "Digest link should have media_type after check"
            );

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
                .await
                .unwrap();
            assert_eq!(
                tag_link.media_type.as_deref(),
                Some(media_type),
                "Tag link should have media_type after check"
            );
        }
    }

    #[tokio::test]
    async fn test_media_type_checker_skips_links_with_media_type() {
        for test_case in backends() {
            let namespace = "test-repo/skip";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest = blob_store
                .create_blob(manifest_content.as_bytes())
                .await
                .unwrap();

            // Create links WITH media_type already set
            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link_with_media_type(
                &LinkKind::Digest(manifest_digest.clone()),
                &manifest_digest,
                media_type,
            );
            tx.create_link_with_media_type(
                &LinkKind::Tag("latest".to_string()),
                &manifest_digest,
                media_type,
            );
            tx.commit().await.unwrap();

            // Run checker - should be a no-op
            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone(), false);
            checker.check_namespace(namespace).await.unwrap();

            // Verify media_type is still the same
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert_eq!(
                digest_link.media_type.as_deref(),
                Some(media_type),
                "Digest link media_type should remain unchanged"
            );
        }
    }

    #[tokio::test]
    async fn test_media_type_checker_dry_run() {
        for test_case in backends() {
            let namespace = "test-repo/dry-run";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let media_type = "application/vnd.oci.image.manifest.v1+json";

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let manifest_content = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "{media_type}",
                "config": {{
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "{config_digest}",
                    "size": 123
                }},
                "layers": []
            }}"#
            );

            let manifest_digest = blob_store
                .create_blob(manifest_content.as_bytes())
                .await
                .unwrap();

            // Create links WITHOUT media_type
            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest);
            tx.create_link(&LinkKind::Tag("latest".to_string()), &manifest_digest);
            tx.commit().await.unwrap();

            // Run checker with dry_run = true
            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone(), true);
            checker.check_namespace(namespace).await.unwrap();

            // Verify media_type is still None (dry run should not modify)
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                digest_link.media_type.is_none(),
                "Digest link should still have no media_type after dry run"
            );

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
                .await
                .unwrap();
            assert!(
                tag_link.media_type.is_none(),
                "Tag link should still have no media_type after dry run"
            );
        }
    }
}
