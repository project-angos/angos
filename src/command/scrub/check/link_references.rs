use std::sync::Arc;

use tracing::{debug, error, info};

use crate::{
    oci::Digest,
    registry::{
        Error,
        blob_store::BlobStore,
        metadata_store::{self, MetadataStore, MetadataStoreExt, link_kind::LinkKind},
        parse_manifest_digests,
    },
};

pub struct LinkReferencesChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    dry_run: bool,
}

impl LinkReferencesChecker {
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
        debug!("Checking referenced_by field for namespace '{namespace}'");

        let mut marker = None;
        loop {
            let (revisions, next_marker) = self
                .metadata_store
                .list_revisions(namespace, 100, marker)
                .await?;

            for revision in &revisions {
                if let Err(e) = self.repair_referenced_by(namespace, revision).await {
                    error!(
                        "Failed to fix referenced_by for '{namespace}' (revision '{revision}'): {e}"
                    );
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        Ok(())
    }

    async fn repair_referenced_by(&self, namespace: &str, revision: &Digest) -> Result<(), Error> {
        let content = self.blob_store.read_blob(revision).await?;
        let manifest = parse_manifest_digests(&content, None)?;

        if let Some(config) = &manifest.config {
            self.ensure_referenced_by(
                namespace,
                &LinkKind::Config(config.clone()),
                config,
                revision,
            )
            .await?;
        }

        for layer in &manifest.layers {
            self.ensure_referenced_by(namespace, &LinkKind::Layer(layer.clone()), layer, revision)
                .await?;
        }

        for child in &manifest.manifests {
            self.ensure_referenced_by(
                namespace,
                &LinkKind::Manifest(revision.clone(), child.clone()),
                child,
                revision,
            )
            .await?;
        }

        Ok(())
    }

    async fn ensure_referenced_by(
        &self,
        namespace: &str,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        match self.metadata_store.read_link(namespace, link, false).await {
            Ok(metadata) => {
                if metadata.referenced_by.contains(referrer) {
                    debug!("Link {link} already has referrer {referrer}");
                    return Ok(());
                }

                if self.dry_run {
                    info!(
                        "DRY RUN: would add referrer {referrer} to link {link} in namespace '{namespace}'"
                    );
                    return Ok(());
                }

                info!("Adding referrer {referrer} to link {link} in namespace '{namespace}'");
                let mut tx = self.metadata_store.begin_transaction(namespace);
                tx.create_link_with_referrer(link, target, referrer);
                tx.commit().await?;
            }
            Err(metadata_store::Error::ReferenceNotFound) => {
                debug!("Link {link} not found, skipping");
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{metadata_store::MetadataStoreExt, test_utils, tests::backends};

    #[tokio::test]
    async fn test_link_references_checker_fixes_missing_references() {
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
                .create_blob(manifest_content.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest);
            tx.create_link(&LinkKind::Config(config_digest.clone()), &config_digest);
            tx.create_link(&LinkKind::Layer(layer_digest.clone()), &layer_digest);
            tx.commit().await.unwrap();

            let config_link_before = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                config_link_before.referenced_by.is_empty(),
                "Config link should start with empty referenced_by"
            );

            let checker =
                LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone(), false);
            checker.check_namespace(namespace).await.unwrap();

            let config_link_after = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                config_link_after.referenced_by.contains(&manifest_digest),
                "Config link should have manifest digest in referenced_by after check"
            );

            let layer_link_after = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()), false)
                .await
                .unwrap();
            assert!(
                layer_link_after.referenced_by.contains(&manifest_digest),
                "Layer link should have manifest digest in referenced_by after check"
            );
        }
    }
}
