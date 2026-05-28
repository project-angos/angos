use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        check::{NamespaceChecker, ensure_link, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::Digest,
    registry::{
        blob_store::BlobStore,
        metadata_store::{MetadataStore, link_kind::LinkKind},
        parse_manifest_digests,
    },
};

pub struct ManifestChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<MetadataStore>,
}

impl ManifestChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<MetadataStore>,
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
        ensure_link(
            &self.metadata_store,
            namespace,
            &LinkKind::Digest(revision.clone()),
            revision,
            sink,
        )
        .await?;

        let content = self.blob_store.read(revision).await?;
        let manifest = parse_manifest_digests(&content, None)?;

        for (link, target) in manifest.links_for_revision(revision) {
            ensure_link(&self.metadata_store, namespace, &link, &target, sink).await?;
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

        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(revision) = revisions.next().await {
            let revision = revision?;
            if let Err(e) = self.repair_manifest_links(namespace, &revision, sink).await {
                error!("Failed to check revision from '{namespace}' (revision '{revision}'): {e}");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        oci::Namespace,
        registry::{
            metadata_store::{LinkOperation, link_kind::LinkKind},
            test_utils::{self, backends, put_blob_direct},
        },
    };

    #[tokio::test]
    async fn test_scrub_revisions_validates_manifest_links() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
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

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
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
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn manifest_checker_repairs_digest_link_with_mismatched_target() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/digest-repair").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config for digest repair")
                    .await;
            let (layer_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"layer for digest repair").await;

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

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let wrong_digest = Digest::from_str(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap();

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        wrong_digest,
                    )],
                )
                .await
                .unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
            );

            let scrubber = ManifestChecker::new(blob_store.clone(), metadata_store.clone());
            scrubber.check(namespace, &mut executor).await.unwrap();

            let link_meta = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();

            assert_eq!(
                link_meta.target, manifest_digest,
                "revision link target must be corrected back to the path digest"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn manifest_checker_emits_recreate_link_action_for_target_mismatch_in_dry_run() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/digest-dry-run").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"config for dry-run check")
                    .await;
            let (layer_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"layer for dry-run check").await;

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

            let manifest_digest =
                put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let wrong_digest = Digest::from_str(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )
            .unwrap();

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        wrong_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut sink: Vec<Action> = Vec::new();
            let scrubber = ManifestChecker::new(blob_store.clone(), metadata_store.clone());
            scrubber.check(namespace, &mut sink).await.unwrap();

            let has_recreate = sink.iter().any(|a| {
                matches!(
                    a,
                    Action::RecreateLink {
                        link: LinkKind::Digest(d),
                        target: t,
                        ..
                    } if d == &manifest_digest && t == &manifest_digest
                )
            });
            assert!(
                has_recreate,
                "sink must contain RecreateLink for the corrupted revision digest link"
            );

            let stored_meta = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()), false)
                .await
                .unwrap();

            assert_eq!(
                stored_meta.target, wrong_digest,
                "Vec sink must not mutate storage"
            );
            test_case.cleanup().await;
        }
    }
}
