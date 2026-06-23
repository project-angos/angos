use std::{fmt, sync::Arc};

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error, warn};

use crate::{
    command::scrub::{
        check::{NamespaceChecker, ensure_link, list_all},
        error::Error,
        executor::ActionSink,
        report::Findings,
    },
    oci::{Digest, Namespace},
    registry::{
        ParsedManifestDigests,
        blob_store::{self, BlobStore},
        metadata_store::{LinkKind, MetadataStore},
        parse_manifest_digests,
    },
};

/// The kind of content a manifest references, for reporting a dangling reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceKind {
    Config,
    Layer,
    /// A child manifest of a multi-arch image index.
    ChildManifest,
}

impl fmt::Display for ReferenceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ReferenceKind::Config => "config blob",
            ReferenceKind::Layer => "layer blob",
            ReferenceKind::ChildManifest => "child manifest",
        })
    }
}

/// A manifest reference whose backing content is absent on this registry.
///
/// Report-only: a pull-through mirror or a partial pull legitimately lacks the
/// content, so scrub surfaces the finding but never deletes on this signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DanglingReference {
    pub kind: ReferenceKind,
    pub digest: Digest,
}

pub struct ManifestChecker {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    /// Shared report-only accumulator: each dangling reference is recorded here so
    /// it surfaces in the end-of-run summary. Recording is purely additive; it
    /// never changes which `Action`s are emitted. Tests pass `Findings::default()`
    /// (a no-op accumulator never read back).
    findings: Findings,
}

impl ManifestChecker {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        findings: Findings,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            findings,
        }
    }

    async fn repair_manifest_links(
        &self,
        namespace: &Namespace,
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

        // Report-only: surface references whose backing content is missing.
        // Never deleted: a pull-through mirror or a partial pull legitimately
        // lacks config/layer blobs or index children. The `warn!` is the
        // per-item line; the `findings` record feeds the end-of-run summary count
        // (additive, emits no `Action`).
        for dangling in self.find_dangling_references(&manifest).await? {
            warn!(
                "Manifest '{namespace}@{revision}' references missing {} '{}'",
                dangling.kind, dangling.digest
            );
            self.findings
                .record_dangling(namespace, revision, &dangling)
                .await;
        }

        Ok(())
    }

    /// Probe every blob and child manifest a revision references and collect the
    /// ones whose bytes are absent. Existence-only (`size`); a real backend error
    /// propagates. Report-only; see [`DanglingReference`].
    async fn find_dangling_references(
        &self,
        manifest: &ParsedManifestDigests,
    ) -> Result<Vec<DanglingReference>, Error> {
        let references = manifest
            .config
            .iter()
            .map(|digest| (ReferenceKind::Config, digest))
            .chain(
                manifest
                    .layers
                    .iter()
                    .map(|digest| (ReferenceKind::Layer, digest)),
            )
            .chain(
                manifest
                    .manifests
                    .iter()
                    .map(|digest| (ReferenceKind::ChildManifest, digest)),
            );

        let mut dangling = Vec::new();
        for (kind, digest) in references {
            if !self.blob_present(digest).await? {
                dangling.push(DanglingReference {
                    kind,
                    digest: digest.clone(),
                });
            }
        }
        Ok(dangling)
    }

    /// Whether `digest`'s bytes are present. A not-found is reported as absent; a
    /// real backend error propagates.
    async fn blob_present(&self, digest: &Digest) -> Result<bool, Error> {
        match self.blob_store.size(digest).await {
            Ok(_) => Ok(true),
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl NamespaceChecker for ManifestChecker {
    async fn check(
        &self,
        namespace: &Namespace,
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
            metadata_store::{LinkKind, LinkOperation},
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

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let scrubber = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
            scrubber.check(namespace, &mut executor).await.unwrap();

            let config_link = metadata_store
                .read_link(namespace, &LinkKind::Config(config_digest.clone()))
                .await;

            let layer_link = metadata_store
                .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
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

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let scrubber = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
            scrubber.check(namespace, &mut executor).await.unwrap();

            let link_meta = metadata_store
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()))
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
            let scrubber = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
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
                .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()))
                .await
                .unwrap();

            assert_eq!(
                stored_meta.target, wrong_digest,
                "Vec sink must not mutate storage"
            );
            test_case.cleanup().await;
        }
    }

    fn image_manifest(config: &Digest, layers: &[&Digest]) -> String {
        let layers = layers
            .iter()
            .map(|d| {
                format!(
                    r#"{{ "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "{d}", "size": 1 }}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {{ "mediaType": "application/vnd.oci.image.config.v1+json", "digest": "{config}", "size": 1 }},
            "layers": [{layers}]
        }}"#
        )
    }

    #[tokio::test]
    async fn find_dangling_references_reports_missing_layer() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dangling").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (config, _) =
                test_utils::create_test_blob(registry, namespace, b"present cfg").await;
            let (present_layer, _) =
                test_utils::create_test_blob(registry, namespace, b"present layer").await;
            let missing_layer = Digest::from_str(
                "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            )
            .unwrap();

            let body = image_manifest(&config, &[&present_layer, &missing_layer]);
            let manifest = parse_manifest_digests(body.as_bytes(), None).unwrap();

            let checker = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
            let dangling = checker.find_dangling_references(&manifest).await.unwrap();

            assert_eq!(
                dangling,
                vec![DanglingReference {
                    kind: ReferenceKind::Layer,
                    digest: missing_layer,
                }],
                "only the layer whose bytes are absent must be reported"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn find_dangling_references_reports_missing_index_child() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/dangling-index").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (present_child, _) =
                test_utils::create_test_blob(registry, namespace, b"present child").await;
            let missing_child = Digest::from_str(
                "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            )
            .unwrap();

            let body = format!(
                r#"{{
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": [
                    {{ "mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "{present_child}", "size": 1 }},
                    {{ "mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "{missing_child}", "size": 1 }}
                ]
            }}"#
            );
            let manifest = parse_manifest_digests(body.as_bytes(), None).unwrap();

            let checker = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
            let dangling = checker.find_dangling_references(&manifest).await.unwrap();

            assert_eq!(
                dangling,
                vec![DanglingReference {
                    kind: ReferenceKind::ChildManifest,
                    digest: missing_child,
                }],
                "only the index child whose manifest is absent must be reported"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn find_dangling_references_empty_when_all_present() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/all-present").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (config, _) = test_utils::create_test_blob(registry, namespace, b"cfg").await;
            let (layer, _) = test_utils::create_test_blob(registry, namespace, b"lyr").await;

            let body = image_manifest(&config, &[&layer]);
            let manifest = parse_manifest_digests(body.as_bytes(), None).unwrap();

            let checker = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
            assert!(
                checker
                    .find_dangling_references(&manifest)
                    .await
                    .unwrap()
                    .is_empty(),
                "a manifest whose references are all present must report nothing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn manifest_checker_reports_dangling_layer_without_deleting() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/no-delete").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (config_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"cfg present").await;
            let (layer_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"layer bytes").await;

            let body = image_manifest(&config_digest, &[&layer_digest]);
            let manifest_digest = put_blob_direct(metadata_store.store(), body.as_bytes()).await;

            // Seed every forward link so `ensure_link` is a no-op; the only fault
            // is the missing layer bytes below.
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

            // Dangling layer: still referenced and linked, but its bytes are gone.
            blob_store.delete_blob(&layer_digest).await.unwrap();

            let checker = ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Findings::default(),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                !sink
                    .iter()
                    .any(|a| matches!(a, Action::DeleteOrphanManifest { .. })),
                "a dangling layer is report-only and must never trigger a delete"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(manifest_digest.clone()))
                    .await
                    .is_ok(),
                "the revision link must remain intact"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Layer(layer_digest.clone()))
                    .await
                    .is_ok(),
                "the layer link must remain intact"
            );
            test_case.cleanup().await;
        }
    }
}
