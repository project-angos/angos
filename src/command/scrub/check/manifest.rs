use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        check::{NamespaceChecker, ensure_link},
        error::Error,
        executor::ActionSink,
    },
    oci::Digest,
    registry::{
        blob_store::BlobStore,
        metadata_store::{MetadataStore, link_kind::LinkKind},
        pagination::collect_all_pages,
        parse_manifest_digests,
    },
};

pub struct ManifestChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
}

impl ManifestChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    async fn repair_manifest_links(
        &self,
        namespace: &str,
        revision: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let content = self.blob_store.read(revision).await?;
        let manifest = parse_manifest_digests(&content, None)?;

        for layer in &manifest.layers {
            ensure_link(
                &self.metadata_store,
                namespace,
                &LinkKind::Layer(layer.clone()),
                layer,
                sink,
            )
            .await?;
        }

        if let Some(config) = &manifest.config {
            ensure_link(
                &self.metadata_store,
                namespace,
                &LinkKind::Config(config.clone()),
                config,
                sink,
            )
            .await?;
        }

        if let Some(subject) = &manifest.subject {
            ensure_link(
                &self.metadata_store,
                namespace,
                &LinkKind::Referrer(subject.clone(), revision.clone()),
                revision,
                sink,
            )
            .await?;
        }

        for child in &manifest.manifests {
            ensure_link(
                &self.metadata_store,
                namespace,
                &LinkKind::Manifest(revision.clone(), child.clone()),
                child,
                sink,
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for ManifestChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking manifest inconsistencies from namespace '{namespace}'");

        let revisions: Vec<Digest> = collect_all_pages(|marker| async move {
            self.metadata_store
                .list_revisions(namespace, 100, marker)
                .await
        })
        .await
        .map_err(Error::from)?;

        for revision in &revisions {
            if let Err(e) = self.repair_manifest_links(namespace, revision, sink).await {
                error!("Failed to check tag from '{namespace}' (revision '{revision}'): {e}");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::scrub::executor::Executor,
        registry::{
            metadata_store::MetadataStoreExt,
            test_utils::{self, NoopMultipart, backends},
        },
    };

    #[tokio::test]
    async fn test_scrub_revisions_validates_manifest_links() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config content").await;

            let (layer_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"layer content").await;

            let manifest_content = format!(
                r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
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
            tx.commit().await.unwrap();

            let mut executor = Executor::new(
                false,
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
                std::sync::Arc::new(NoopMultipart),
            );

            let scrubber = ManifestChecker::new(blob_store.clone(), metadata_store.clone());
            scrubber.check(namespace, &mut executor).await.unwrap();

            let config_link = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest.clone()), false)
                .await;

            let layer_link = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()), false)
                .await;

            assert!(config_link.is_ok());
            assert!(layer_link.is_ok());
        }
    }
}
