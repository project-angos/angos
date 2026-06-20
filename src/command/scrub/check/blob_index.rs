//! [`BlobIndexChecker`]: rebuilds missing blob-index grants from the
//! authoritative manifest bodies.
//!
//! The blob index is a denormalised reverse map (blob digest -> {namespace ->
//! {`LinkKind`}}) maintained incrementally on every push/delete. If it is
//! corrupted or partially lost out-of-band (storage corruption, manual
//! tampering, a shard written under a namespace name a later validation change
//! rejects), nothing in the normal write path repairs it: a grant is only
//! re-issued when the *link* is recreated, never when an intact link's index
//! entry goes missing. A blob that loses its last index entry while a manifest
//! still references it is then seen as an orphan and its bytes are reclaimed,
//! silently breaking the image.
//!
//! This checker treats the per-namespace link trees as the source of truth and
//! re-derives the grants each manifest implies, emitting an idempotent
//! [`Action::GrantBlobIndexLink`] for any that the index is missing. It runs as
//! a [`NamespaceChecker`], i.e. before the store-wide [`BlobChecker`], so the
//! index is healed before orphan-blob GC reads it.
//!
//! Scope: this is the *additive* half of a reconcile. It restores entries the
//! index is missing; it does not remove stale entries whose backing link no
//! longer exists (a leak, not data loss). Bare self-ownership `Blob` grants are
//! also out of scope — they are transient and not derivable from manifest
//! content; a manifest-referenced blob is pinned by its tracked link regardless.

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
    oci::{Digest, Namespace},
    registry::{
        blob_store,
        metadata_store::{Error as MetadataError, LinkKind, MetadataStore},
        parse_manifest_digests,
    },
};

/// Rebuilds blob-index grants the index is missing relative to the manifests
/// that actually reference each blob. See the module docs for the failure mode
/// this guards against and the (additive) scope.
pub struct BlobIndexChecker {
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl BlobIndexChecker {
    pub fn new(blob_store: Arc<blob_store::BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    /// Re-derive every grant `revision`'s manifest implies and re-issue the ones
    /// the index is missing. A revision whose manifest bytes are gone yields no
    /// derivable references and is left to [`LinkReferencesChecker`]'s
    /// missing-blob repair.
    ///
    /// [`LinkReferencesChecker`]: super::link_references::LinkReferencesChecker
    async fn reconcile_revision(
        &self,
        namespace: &Namespace,
        revision: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let content = match self.blob_store.read(revision).await {
            Ok(content) => content,
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let manifest = parse_manifest_digests(&content, None)?;

        // The revision's own digest link pins the manifest blob; the body's
        // config/layer/sub-manifest (and referrer) links pin everything it
        // references. Each pair is the grant a fresh push would have written.
        self.ensure_grant(
            namespace,
            revision,
            &LinkKind::Digest(revision.clone()),
            sink,
        )
        .await?;
        for (link, target) in manifest.links_for_revision(revision) {
            self.ensure_grant(namespace, &target, &link, sink).await?;
        }
        Ok(())
    }

    /// Emit a grant for `link` on `blob` unless the index already records it.
    ///
    /// Only re-grants a reference to bytes that still exist: a manifest that
    /// references a missing blob is a broken-manifest problem, not an index one,
    /// and granting it would churn forever against the blob GC that strips
    /// entries for absent bytes.
    async fn ensure_grant(
        &self,
        namespace: &Namespace,
        blob: &Digest,
        link: &LinkKind,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        match self
            .metadata_store
            .read_blob_index_namespace(namespace, blob)
            .await
        {
            Ok(links) if links.contains(link) => return Ok(()),
            Ok(_) | Err(MetadataError::ReferenceNotFound) => {}
            Err(e) => return Err(e.into()),
        }

        match self.blob_store.size(blob).await {
            Ok(_) => {}
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                debug!("Skipping blob-index grant for missing blob '{namespace}/{blob}': '{link}'");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        sink.apply(Action::GrantBlobIndexLink {
            namespace: namespace.clone(),
            blob: blob.clone(),
            link: link.clone(),
        })
        .await
    }
}

#[async_trait]
impl NamespaceChecker for BlobIndexChecker {
    async fn check(
        &self,
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Reconciling blob index from manifest links for namespace '{namespace}'");

        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(revision) = revisions.next().await {
            let revision = revision?;
            if let Err(e) = self.reconcile_revision(namespace, &revision, sink).await {
                error!(
                    "Failed to reconcile blob index for '{namespace}' (revision '{revision}'): {e}"
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
        command::scrub::executor::Executor,
        oci::Namespace,
        registry::{
            Registry,
            metadata_store::{BlobIndexOperation, LinkKind, LinkOperation},
            test_utils::{self, backends, put_blob_direct},
        },
    };

    /// Push a config blob, a layer blob, and a manifest that references both,
    /// creating the revision/config/layer links (which grant the blob index).
    /// Returns `(manifest_digest, config_digest, layer_digest)`.
    async fn create_manifest_scenario(
        registry: &Registry,
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> (Digest, Digest, Digest) {
        let (config_digest, _) =
            test_utils::create_test_blob(registry, namespace, b"reconcile config").await;
        let (layer_digest, _) =
            test_utils::create_test_blob(registry, namespace, b"reconcile layer").await;

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

    /// True when the index records `Layer(layer)` for `namespace`, mapping a
    /// stripped (not-found) entry to absent.
    async fn layer_grant_present(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        layer: &Digest,
    ) -> bool {
        match metadata_store
            .read_blob_index_namespace(namespace, layer)
            .await
        {
            Ok(links) => links.contains(&LinkKind::Layer(layer.clone())),
            Err(_) => false,
        }
    }

    /// Drop the layer's blob-index entry while leaving its link and bytes intact:
    /// the exact shape of an index corrupted out-of-band.
    async fn strip_layer_grant(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        layer: &Digest,
    ) {
        for link in [
            LinkKind::Layer(layer.clone()),
            LinkKind::Blob(layer.clone()),
        ] {
            metadata_store
                .update_blob_index(namespace, layer, BlobIndexOperation::Remove(link))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn reconcile_restores_a_missing_layer_grant() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/reconcile").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (_manifest, _config, layer) =
                create_manifest_scenario(registry, &metadata_store, namespace).await;

            strip_layer_grant(&metadata_store, namespace, &layer).await;
            assert!(
                !layer_grant_present(&metadata_store, namespace, &layer).await,
                "precondition: the layer grant must be stripped"
            );

            let checker = BlobIndexChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            assert!(
                layer_grant_present(&metadata_store, namespace, &layer).await,
                "the layer grant must be re-derived from the manifest"
            );
            assert!(
                metadata_store.has_blob_references(&layer).await.unwrap(),
                "the restored grant must again pin the layer against orphan GC"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn reconcile_is_a_noop_when_the_index_is_consistent() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/consistent").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            create_manifest_scenario(registry, &metadata_store, namespace).await;

            let checker = BlobIndexChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "a fully-consistent index must produce no grant actions, got {} action(s)",
                sink.len()
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn dry_run_captures_the_grant_without_mutating() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/reconcile-dry").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (_manifest, _config, layer) =
                create_manifest_scenario(registry, &metadata_store, namespace).await;
            strip_layer_grant(&metadata_store, namespace, &layer).await;

            let checker = BlobIndexChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::GrantBlobIndexLink { blob, link, .. }
                        if *blob == layer && *link == LinkKind::Layer(layer.clone())
                )),
                "the Vec sink must capture the layer grant action"
            );
            assert!(
                !layer_grant_present(&metadata_store, namespace, &layer).await,
                "a Vec sink must not mutate the index"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn reconcile_skips_a_grant_for_a_blob_with_no_bytes() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/reconcile-missing").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (_manifest, _config, layer) =
                create_manifest_scenario(registry, &metadata_store, namespace).await;
            strip_layer_grant(&metadata_store, namespace, &layer).await;
            // The manifest still references the layer, but its bytes are gone:
            // granting here would churn against the blob GC, so it is skipped.
            blob_store.delete_blob(&layer).await.unwrap();

            let checker = BlobIndexChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                !sink.iter().any(|a| matches!(
                    a,
                    Action::GrantBlobIndexLink { blob, .. } if *blob == layer
                )),
                "no grant must be emitted for a referenced blob whose bytes are missing"
            );
            test_case.cleanup().await;
        }
    }
}
