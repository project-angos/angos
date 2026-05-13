use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::Digest,
    registry::{
        blob_store::BlobStore,
        metadata_store::{self, MetadataStore, link_kind::LinkKind},
        parse_manifest_digests,
    },
};

pub struct LinkReferencesChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
}

impl LinkReferencesChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    async fn repair_referenced_by(
        &self,
        namespace: &str,
        revision: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let content = self.blob_store.read(revision).await?;
        let manifest = parse_manifest_digests(&content, None)?;

        if let Some(config) = &manifest.config {
            self.ensure_referenced_by(
                namespace,
                &LinkKind::Config(config.clone()),
                config,
                revision,
                sink,
            )
            .await?;
        }

        for layer in &manifest.layers {
            self.ensure_referenced_by(
                namespace,
                &LinkKind::Layer(layer.clone()),
                layer,
                revision,
                sink,
            )
            .await?;
        }

        for child in &manifest.manifests {
            self.ensure_referenced_by(
                namespace,
                &LinkKind::Manifest(revision.clone(), child.clone()),
                child,
                revision,
                sink,
            )
            .await?;
        }

        Ok(())
    }

    /// Returns `true` only when the link exists but its `referenced_by` set is
    /// missing `referrer` — the back-link is absent and must be added.
    ///
    /// A `ReferenceNotFound` error means the link itself does not exist, so
    /// there is nothing to update; orphan-link cleanup handles that case in a
    /// separate scrub stage.
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

    pub async fn ensure_referenced_by(
        &self,
        namespace: &str,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        if self
            .needs_referrer_update(namespace, link, referrer)
            .await?
        {
            sink.apply(Action::AddReferrer {
                namespace: namespace.to_string(),
                link: link.clone(),
                target: target.clone(),
                referrer: referrer.clone(),
            })
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for LinkReferencesChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking referenced_by field for namespace '{namespace}'");

        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(revision) = revisions.next().await {
            let revision = revision?;
            if let Err(e) = self.repair_referenced_by(namespace, &revision, sink).await {
                error!(
                    "Failed to fix referenced_by for '{namespace}' (revision '{revision}'): {e}"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        oci::{Descriptor, Namespace},
        registry::{
            Registry,
            metadata_store::{
                BlobIndex, BlobIndexOperation, LinkMetadata, LinkOperation, MetadataStoreExt,
            },
            test_utils::{self, FSRegistryTestCase, NoopMultipart, RegistryTestCase, backends},
        },
    };

    fn noop_executor(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        upload_store: Arc<dyn crate::registry::blob_store::UploadStore>,
    ) -> Executor {
        Executor::new(
            blob_store,
            metadata_store,
            upload_store,
            std::sync::Arc::new(NoopMultipart),
        )
    }

    async fn create_manifest_scenario(
        registry: &Registry,
        metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
        blob_store: &Arc<dyn BlobStore>,
        namespace: &Namespace,
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
            .create(manifest_content.as_bytes())
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
            let namespace = &Namespace::new("test-repo/app").unwrap();
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

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());

            let mut executor = noop_executor(
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
            );

            checker.check(namespace, &mut executor).await.unwrap();

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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_dry_run_makes_no_writes() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dry-run").unwrap();
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

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());

            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let config_link = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest), false)
                .await
                .unwrap();
            assert!(
                config_link.referenced_by.is_empty(),
                "Vec sink must not write: config referenced_by should remain empty"
            );

            let layer_link = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest), false)
                .await
                .unwrap();
            assert!(
                layer_link.referenced_by.is_empty(),
                "Vec sink must not write: layer referenced_by should remain empty"
            );
            test_case.cleanup().await;
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
                .create(manifest_content.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(manifest_digest.clone()), &manifest_digest)
                .add();
            tx.commit().await.unwrap();

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            let result = checker.check(namespace, &mut sink).await;
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
                "Config link should still not exist after check"
            );
            test_case.cleanup().await;
        }
    }

    // Stub used only to make read_link return a StorageBackend error.
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
    }

    #[tokio::test]
    async fn test_unexpected_error_is_propagated() {
        let fs_case = FSRegistryTestCase::new();
        let blob_store: Arc<dyn BlobStore + Send + Sync> = RegistryTestCase::blob_store(&fs_case);
        let metadata_store: Arc<dyn MetadataStore + Send + Sync> =
            std::sync::Arc::new(ErroringMetadataStore);

        let checker = LinkReferencesChecker::new(blob_store, metadata_store);

        let namespace = "test-repo/error";
        let target = Digest::Sha256(
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
        );
        let referrer = Digest::Sha256(
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into(),
        );
        let link = LinkKind::Config(target.clone());

        let mut sink: Vec<Action> = Vec::new();
        let result = checker
            .ensure_referenced_by(namespace, &link, &target, &referrer, &mut sink)
            .await;

        assert!(
            matches!(result, Err(Error::MetadataStore(_))),
            "StorageBackend error must propagate as Error::MetadataStore, got: {result:?}"
        );
    }
}
