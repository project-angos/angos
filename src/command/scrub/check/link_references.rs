use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, error, info};

use crate::{
    command::scrub::check::NamespaceChecker,
    oci::Digest,
    registry::{
        Error,
        blob_store::BlobStore,
        metadata_store::{self, MetadataStore, MetadataStoreExt, link_kind::LinkKind},
        pagination::for_each_page,
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

    async fn needs_referrer_update(
        &self,
        namespace: &str,
        link: &LinkKind,
        referrer: &Digest,
    ) -> Result<bool, Error> {
        match self.metadata_store.read_link(namespace, link, false).await {
            Ok(metadata) if metadata.referenced_by.contains(referrer) => Ok(false),
            Ok(_) => Ok(true),
            Err(metadata_store::Error::ReferenceNotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn add_referrer(
        &self,
        namespace: &str,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        if self.dry_run {
            info!(
                "DRY RUN: would add referrer {referrer} to link {link} in namespace '{namespace}'"
            );
            return Ok(());
        }

        info!("Adding referrer {referrer} to link {link} in namespace '{namespace}'");
        let mut tx = self.metadata_store.begin_transaction(namespace);
        tx.create_link(link, target).with_referrer(referrer).add();
        tx.commit().await?;
        Ok(())
    }

    async fn ensure_referenced_by(
        &self,
        namespace: &str,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        if self
            .needs_referrer_update(namespace, link, referrer)
            .await?
        {
            self.add_referrer(namespace, link, target, referrer).await?;
        }
        Ok(())
    }

    async fn process_page(&self, namespace: &str, revisions: Vec<Digest>) -> Result<(), Error> {
        for revision in &revisions {
            if let Err(e) = self.repair_referenced_by(namespace, revision).await {
                error!(
                    "Failed to fix referenced_by for '{namespace}' (revision '{revision}'): {e}"
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for LinkReferencesChecker {
    async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking referenced_by field for namespace '{namespace}'");

        for_each_page(
            |marker| async move {
                self.metadata_store
                    .list_revisions(namespace, 100, marker)
                    .await
                    .map_err(Error::from)
            },
            |revisions| self.process_page(namespace, revisions),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::{
        oci::Descriptor,
        registry::{
            Registry,
            metadata_store::{
                BlobIndex, BlobIndexOperation, ConditionalCapabilities, LinkMetadata,
                LinkOperation, MetadataStoreExt,
            },
            test_utils,
            tests::{FSRegistryTestCase, RegistryTestCase, backends},
        },
    };

    async fn create_manifest_scenario(
        registry: &Registry,
        metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
        blob_store: &Arc<dyn BlobStore>,
        namespace: &str,
        config_content: &[u8],
        layer_content: &[u8],
    ) -> (Digest, Digest, Digest) {
        let (config_digest, _) =
            test_utils::create_test_blob(registry, namespace, config_content).await;
        let (layer_digest, _) =
            test_utils::create_test_blob(registry, namespace, layer_content).await;

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
        tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest)
            .add();
        tx.create_link(&LinkKind::Config(config_digest.clone()), &config_digest)
            .add();
        tx.create_link(&LinkKind::Layer(layer_digest.clone()), &layer_digest)
            .add();
        tx.commit().await.unwrap();

        (manifest_digest, config_digest, layer_digest)
    }

    #[tokio::test]
    async fn test_link_references_checker_fixes_missing_references() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (manifest_digest, config_digest, layer_digest) = create_manifest_scenario(
                registry,
                &metadata_store,
                &blob_store,
                namespace,
                b"config content",
                b"layer content",
            )
            .await;

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

    #[tokio::test]
    async fn test_dry_run_makes_no_writes() {
        for test_case in backends() {
            let namespace = "test-repo/dry-run";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (_, config_digest, layer_digest) = create_manifest_scenario(
                registry,
                &metadata_store,
                &blob_store,
                namespace,
                b"dry-run config",
                b"dry-run layer",
            )
            .await;

            let checker =
                LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone(), true);
            checker.check_namespace(namespace).await.unwrap();

            let config_link = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest), false)
                .await
                .unwrap();
            assert!(
                config_link.referenced_by.is_empty(),
                "Dry-run must not write: config referenced_by should remain empty"
            );

            let layer_link = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest), false)
                .await
                .unwrap();
            assert!(
                layer_link.referenced_by.is_empty(),
                "Dry-run must not write: layer referenced_by should remain empty"
            );
        }
    }

    #[tokio::test]
    async fn test_reference_not_found_is_skipped() {
        for test_case in backends() {
            let namespace = "test-repo/not-found";
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let manifest_content = r#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size": 123
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "size": 456
                }
            ]
        }"#;

            let manifest_digest = blob_store
                .create_blob(manifest_content.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest)
                .add();
            tx.commit().await.unwrap();

            let checker =
                LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone(), false);
            let result = checker.check_namespace(namespace).await;
            assert!(
                result.is_ok(),
                "ReferenceNotFound must not propagate as an error"
            );

            let config_digest = Digest::Sha256(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            );
            let config_result = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest), false)
                .await;
            assert!(
                matches!(config_result, Err(metadata_store::Error::ReferenceNotFound)),
                "Config link should still not exist after check_namespace"
            );

            let layer_digest = Digest::Sha256(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            );
            let layer_result = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest), false)
                .await;
            assert!(
                matches!(layer_result, Err(metadata_store::Error::ReferenceNotFound)),
                "Layer link should still not exist after check_namespace"
            );
        }
    }

    // Stub used only to make read_link return a StorageBackend error; all other methods are unreachable.
    struct ErroringMetadataStore;

    #[async_trait]
    impl MetadataStore for ErroringMetadataStore {
        async fn list_namespaces(
            &self,
            _n: u16,
            _last: Option<String>,
        ) -> Result<(Vec<String>, Option<String>), metadata_store::Error> {
            unimplemented!()
        }

        async fn list_tags(
            &self,
            _namespace: &str,
            _n: u16,
            _last: Option<String>,
        ) -> Result<(Vec<String>, Option<String>), metadata_store::Error> {
            unimplemented!()
        }

        async fn list_referrers(
            &self,
            _namespace: &str,
            _digest: &Digest,
            _artifact_type: Option<String>,
        ) -> Result<Vec<Descriptor>, metadata_store::Error> {
            unimplemented!()
        }

        async fn has_referrers(
            &self,
            _namespace: &str,
            _subject: &Digest,
        ) -> Result<bool, metadata_store::Error> {
            unimplemented!()
        }

        async fn list_revisions(
            &self,
            _namespace: &str,
            _n: u16,
            _continuation_token: Option<String>,
        ) -> Result<(Vec<Digest>, Option<String>), metadata_store::Error> {
            unimplemented!()
        }

        async fn count_manifests(&self, _namespace: &str) -> Result<usize, metadata_store::Error> {
            unimplemented!()
        }

        async fn read_blob_index(
            &self,
            _digest: &Digest,
        ) -> Result<BlobIndex, metadata_store::Error> {
            unimplemented!()
        }

        async fn update_blob_index(
            &self,
            _namespace: &str,
            _digest: &Digest,
            _operation: BlobIndexOperation,
        ) -> Result<(), metadata_store::Error> {
            unimplemented!()
        }

        async fn read_link(
            &self,
            _namespace: &str,
            _link: &LinkKind,
            _update_access_time: bool,
        ) -> Result<LinkMetadata, metadata_store::Error> {
            Err(metadata_store::Error::StorageBackend(
                "simulated failure".to_string(),
            ))
        }

        async fn update_links(
            &self,
            _namespace: &str,
            _operations: &[LinkOperation],
        ) -> Result<(), metadata_store::Error> {
            unimplemented!()
        }

        fn conditional_capabilities(&self) -> Option<ConditionalCapabilities> {
            None
        }
    }

    #[tokio::test]
    async fn test_unexpected_error_is_propagated() {
        let fs_case = FSRegistryTestCase::new();
        let blob_store: Arc<dyn BlobStore + Send + Sync> = RegistryTestCase::blob_store(&fs_case);
        let metadata_store: Arc<dyn MetadataStore + Send + Sync> = Arc::new(ErroringMetadataStore);

        let checker = LinkReferencesChecker::new(blob_store, metadata_store, false);

        let namespace = "test-repo/error";
        let target = Digest::Sha256(
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
        );
        let referrer = Digest::Sha256(
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into(),
        );
        let link = LinkKind::Config(target.clone());

        let result = checker
            .ensure_referenced_by(namespace, &link, &target, &referrer)
            .await;

        assert!(
            matches!(result, Err(Error::Internal(_))),
            "StorageBackend error must propagate as Error::Internal, got: {result:?}"
        );
    }
}
