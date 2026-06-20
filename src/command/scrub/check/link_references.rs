use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::{Digest, Namespace},
    registry::{
        blob_store,
        metadata_store::{self, LinkKind, MetadataStore},
        parse_manifest_digests,
    },
};

pub struct LinkReferencesChecker {
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl LinkReferencesChecker {
    pub fn new(blob_store: Arc<blob_store::BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    async fn repair_referenced_by(
        &self,
        namespace: &Namespace,
        revision: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let content = match self.blob_store.read(revision).await {
            Ok(content) => content,
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                warn!("Manifest blob missing for revision {revision}; removing revision link");
                return sink
                    .apply(Action::DeleteOrphanManifest {
                        namespace: namespace.clone(),
                        digest: revision.clone(),
                    })
                    .await;
            }
            Err(e) => return Err(e.into()),
        };

        let manifest = parse_manifest_digests(&content, None)?;

        for (link, target) in manifest.referenced_links_for_revision(revision) {
            self.ensure_referenced_by(namespace, &link, &target, revision, sink)
                .await?;
        }

        Ok(())
    }

    /// Ensures the back-link from `link` to `referrer` is present, and prunes
    /// any entries in `link`'s `referenced_by` set that no longer have a
    /// corresponding `LinkKind::Digest` revision link in the namespace.
    ///
    /// A link with an entirely-phantom `referenced_by` that is unreachable from
    /// the current revision graph is not visited by this method; that case is
    /// left to future enhancements.
    pub async fn ensure_referenced_by(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        target: &Digest,
        referrer: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let metadata = match self.metadata_store.read_link(namespace, link).await {
            Ok(m) => m,
            Err(metadata_store::Error::ReferenceNotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        if !metadata.referenced_by.contains(referrer) {
            sink.apply(Action::AddReferrer {
                namespace: namespace.clone(),
                link: link.clone(),
                target: target.clone(),
                referrer: referrer.clone(),
            })
            .await?;
        }

        for stale in &metadata.referenced_by {
            if stale == referrer {
                continue;
            }
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(stale.clone()))
                .await
            {
                Ok(_) => {}
                Err(metadata_store::Error::ReferenceNotFound) => {
                    sink.apply(Action::RemoveReferrer {
                        namespace: namespace.clone(),
                        link: link.clone(),
                        referrer: stale.clone(),
                    })
                    .await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for LinkReferencesChecker {
    async fn check(
        &self,
        namespace: &Namespace,
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
    use super::*;

    use crate::{
        command::scrub::{action::Action, executor::Executor},
        oci::Namespace,
        registry::{
            Registry,
            metadata_store::{LinkKind, LinkOperation},
            test_utils::{self, FSRegistryTestCase, RegistryTestCase, backends, put_blob_direct},
        },
    };

    fn noop_executor(
        blob_store: Arc<blob_store::BlobStore>,
        metadata_store: Arc<MetadataStore>,
    ) -> Executor {
        Executor::new_for_test(blob_store, metadata_store)
    }

    async fn create_manifest_scenario(
        registry: &Registry,
        metadata_store: &Arc<MetadataStore>,
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

        let manifest_digest =
            put_blob_direct(metadata_store.store(), manifest_content.as_bytes()).await;

        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Config(config_digest.clone()),
                        config_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

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
                namespace,
                b"config content",
                b"layer content",
            )
            .await;

            let config_link_before = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest.clone()))
                .await
                .unwrap();
            assert!(
                config_link_before.referenced_by.is_empty(),
                "Config link should start with empty referenced_by"
            );

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());

            let mut executor = noop_executor(blob_store.clone(), metadata_store.clone());

            checker.check(namespace, &mut executor).await.unwrap();

            let config_link_after = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest.clone()))
                .await
                .unwrap();
            assert!(
                config_link_after.referenced_by.contains(&manifest_digest),
                "Config link should have manifest digest in referenced_by after check"
            );

            let layer_link_after = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
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
                namespace,
                b"dry-run config",
                b"dry-run layer",
            )
            .await;

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());

            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            let config_link = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest))
                .await
                .unwrap();
            assert!(
                config_link.referenced_by.is_empty(),
                "Vec sink must not write: config referenced_by should remain empty"
            );

            let layer_link = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest))
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
            let namespace = &Namespace::new("test-repo/not-found").unwrap();
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

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            let result = checker.check(namespace, &mut sink).await;
            assert!(
                result.is_ok(),
                "ReferenceNotFound must not propagate as an error"
            );

            let config_digest =
                Digest::sha256("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                    .unwrap();
            let config_result = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest))
                .await;
            assert!(
                matches!(config_result, Err(metadata_store::Error::ReferenceNotFound)),
                "Config link should still not exist after check"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn link_references_checker_emits_delete_orphan_manifest_when_blob_missing() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/missing-blob").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (manifest_digest, _, _) = create_manifest_scenario(
                registry,
                &metadata_store,
                namespace,
                b"config for missing-blob",
                b"layer for missing-blob",
            )
            .await;

            blob_store.delete_blob(&manifest_digest).await.unwrap();

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = noop_executor(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()))
                    .await
                    .is_err(),
                "revision link must be removed when its manifest blob is missing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn link_references_checker_dry_run_captures_delete_orphan_manifest() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/missing-blob-dry").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (manifest_digest, _, _) = create_manifest_scenario(
                registry,
                &metadata_store,
                namespace,
                b"config for dry-run missing",
                b"layer for dry-run missing",
            )
            .await;

            blob_store.delete_blob(&manifest_digest).await.unwrap();

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::DeleteOrphanManifest { digest, .. } if *digest == manifest_digest
                )),
                "Vec sink must capture DeleteOrphanManifest action"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()))
                    .await
                    .is_ok(),
                "revision link must not be touched under Vec sink"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_unexpected_error_is_propagated() {
        // Build a real FS-backed metadata store; then corrupt the link file so
        // that `read_link` fails with an `InvalidData` error, which propagates
        // as `Error::MetadataStore`.
        let fs_case = FSRegistryTestCase::new();
        let blob_store: Arc<blob_store::BlobStore> = RegistryTestCase::blob_store(&fs_case);
        let metadata_store = RegistryTestCase::metadata_store(&fs_case);

        let namespace = Namespace::new("test-repo/error").unwrap();
        let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let target = Digest::sha256(hash).unwrap();
        let referrer =
            Digest::sha256("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
                .unwrap();
        let link = LinkKind::Config(target.clone());

        // Compute the link file path directly (mirrors path_builder::link_path).
        let rel_link_path = format!("v2/repositories/{namespace}/_config/sha256/{hash}/link");
        let full_path = fs_case.temp_dir().path().join(&rel_link_path);
        std::fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        std::fs::write(&full_path, b"not-valid-json").unwrap();

        let checker = LinkReferencesChecker::new(blob_store, metadata_store);
        let mut sink: Vec<Action> = Vec::new();
        let result = checker
            .ensure_referenced_by(&namespace, &link, &target, &referrer, &mut sink)
            .await;

        assert!(
            matches!(result, Err(Error::MetadataStore(_))),
            "parse error must propagate as Error::MetadataStore, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn link_references_checker_prunes_phantom_referrer_from_layer() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/prune-phantom").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (manifest_digest, _config_digest, layer_digest) = create_manifest_scenario(
                registry,
                &metadata_store,
                namespace,
                b"config for prune-phantom",
                b"layer for prune-phantom",
            )
            .await;

            // Inject a phantom referrer: a digest that has no Digest link.
            let phantom =
                Digest::sha256("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                    .unwrap();
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                        phantom.clone(),
                    )],
                )
                .await
                .unwrap();

            // Confirm the phantom was injected.
            let before = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
                .await
                .unwrap();
            assert!(
                before.referenced_by.contains(&phantom),
                "phantom referrer must be present before check"
            );

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = noop_executor(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
                .await
                .unwrap();
            assert!(
                !after.referenced_by.contains(&phantom),
                "phantom referrer must be removed after check"
            );
            assert!(
                after.referenced_by.contains(&manifest_digest),
                "real referrer must be retained after check"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn link_references_checker_keeps_existing_valid_referrers() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/keep-valid").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            // Build two independent manifests that share the same layer content,
            // giving the layer link two real referrers A and B.
            let (manifest_a, _config_a, layer_digest) = create_manifest_scenario(
                registry,
                &metadata_store,
                namespace,
                b"config for keep-valid A",
                b"shared layer for keep-valid",
            )
            .await;
            let (manifest_b, _config_b, _) = create_manifest_scenario(
                registry,
                &metadata_store,
                namespace,
                b"config for keep-valid B",
                b"shared layer for keep-valid",
            )
            .await;

            // Establish both referrers on the layer link.
            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create_with_referrer(
                            LinkKind::Layer(layer_digest.clone()),
                            layer_digest.clone(),
                            manifest_a.clone(),
                        ),
                        LinkOperation::create_with_referrer(
                            LinkKind::Layer(layer_digest.clone()),
                            layer_digest.clone(),
                            manifest_b.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = noop_executor(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
                .await
                .unwrap();
            assert!(
                after.referenced_by.contains(&manifest_a),
                "referrer A must be retained"
            );
            assert!(
                after.referenced_by.contains(&manifest_b),
                "referrer B must be retained"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn link_references_checker_dry_run_captures_remove_referrer() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dry-remove").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (_manifest_digest, _config_digest, layer_digest) = create_manifest_scenario(
                registry,
                &metadata_store,
                namespace,
                b"config for dry-remove",
                b"layer for dry-remove",
            )
            .await;

            // Inject a phantom referrer.
            let phantom =
                Digest::sha256("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                    .unwrap();
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                        phantom.clone(),
                    )],
                )
                .await
                .unwrap();

            let checker = LinkReferencesChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::RemoveReferrer { referrer, .. } if *referrer == phantom
                )),
                "Vec sink must capture a RemoveReferrer action for the phantom"
            );

            // On-disk state must be unchanged under a Vec sink.
            let unchanged = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
                .await
                .unwrap();
            assert!(
                unchanged.referenced_by.contains(&phantom),
                "on-disk referenced_by must be unchanged under Vec sink"
            );
            test_case.cleanup().await;
        }
    }
}
