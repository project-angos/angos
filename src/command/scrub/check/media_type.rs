use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, error, warn};

use crate::{
    command::scrub::{action::Action, check::NamespaceChecker, error::Error, executor::ActionSink},
    oci::{Digest, Manifest},
    registry::{
        blob_store::BlobStore,
        metadata_store::{MetadataStore, link_kind::LinkKind},
        pagination::collect_all_pages,
    },
};

pub struct MediaTypeChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
}

impl MediaTypeChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    async fn backfill_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        display_name: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let metadata = self
            .metadata_store
            .read_link(namespace, link, false)
            .await?;

        if metadata.media_type.is_some() {
            debug!("{display_name} already has media_type, skipping");
            return Ok(());
        }

        let media_type = self.read_media_type(&metadata.target).await?;
        let Some(media_type) = media_type else {
            debug!("{display_name} has no media_type in manifest, skipping");
            return Ok(());
        };

        sink.apply(Action::SetMediaType {
            namespace: namespace.to_string(),
            link: link.clone(),
            target: metadata.target,
            media_type,
            display_name: display_name.to_string(),
        })
        .await
    }

    async fn read_media_type(&self, digest: &Digest) -> Result<Option<String>, Error> {
        let content = self.blob_store.read(digest).await?;
        match serde_json::from_slice::<Manifest>(&content) {
            Ok(manifest) => Ok(manifest.media_type),
            Err(e) => {
                warn!("Failed to deserialize manifest for {digest}: {e}");
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl NamespaceChecker for MediaTypeChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking media_type field for namespace '{namespace}'");

        let revisions = collect_all_pages(|marker| async move {
            self.metadata_store
                .list_revisions(namespace, 100, marker)
                .await
        })
        .await
        .map_err(Error::from)?;

        for revision in &revisions {
            let link = LinkKind::Digest(revision.clone());
            if let Err(e) = self
                .backfill_link(namespace, &link, &format!("revision {revision}"), sink)
                .await
            {
                error!(
                    "Failed to backfill media_type for '{namespace}' (revision '{revision}'): {e}"
                );
            }
        }

        let tags = collect_all_pages(|marker| async move {
            self.metadata_store.list_tags(namespace, 100, marker).await
        })
        .await
        .map_err(Error::from)?;

        for tag in &tags {
            let link = LinkKind::Tag(tag.clone());
            if let Err(e) = self
                .backfill_link(namespace, &link, &format!("tag '{tag}'"), sink)
                .await
            {
                error!("Failed to backfill media_type for '{namespace}' (tag '{tag}'): {e}");
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
        registry::{
            metadata_store::MetadataStoreExt,
            test_utils::{self, NoopMultipart, backends},
        },
    };

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
                .create(manifest_content.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest)
                .add();
            tx.create_link(&LinkKind::Tag("latest".to_string()), &manifest_digest)
                .add();
            tx.commit().await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert!(digest_link.media_type.is_none());

            let mut executor = Executor::new(
                false,
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
                std::sync::Arc::new(NoopMultipart),
            );

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert_eq!(digest_link.media_type.as_deref(), Some(media_type));

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("latest".to_string()), false)
                .await
                .unwrap();
            assert_eq!(tag_link.media_type.as_deref(), Some(media_type));
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
                .create(manifest_content.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest)
                .with_media_type(media_type)
                .add();
            tx.create_link(&LinkKind::Tag("latest".to_string()), &manifest_digest)
                .with_media_type(media_type)
                .add();
            tx.commit().await.unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "No actions expected when media_type already set"
            );

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert_eq!(digest_link.media_type.as_deref(), Some(media_type));
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
                .create(manifest_content.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest)
                .add();
            tx.create_link(&LinkKind::Tag("latest".to_string()), &manifest_digest)
                .add();
            tx.commit().await.unwrap();

            let checker = MediaTypeChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            // Vec sink does not apply actions, so media_type remains None
            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                digest_link.media_type.is_none(),
                "Vec sink must not write: media_type should remain None"
            );

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::SetMediaType { .. })),
                "Vec sink must capture SetMediaType actions"
            );
        }
    }
}
