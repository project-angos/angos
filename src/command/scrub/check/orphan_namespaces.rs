//! [`OrphanNamespaceChecker`]: clears content for namespaces no longer owned by
//! any configured repository.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{StoreChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::{Digest, Namespace},
    registry::{
        blob_store::BlobStore,
        metadata_store::{Error as MetadataError, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
};

/// Deletes all content (revisions, tags, in-flight uploads) for namespaces that
/// no longer resolve to a configured repository, then revokes their blob grants
/// to reclaim layer/config bytes. Does nothing when no repository is configured,
/// so an emptied config can never wipe the whole registry.
pub struct OrphanNamespaceChecker {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
}

impl OrphanNamespaceChecker {
    pub fn new(
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        resolver: Arc<RepositoryResolver>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            resolver,
        }
    }

    /// Emit the delete actions clearing the manifest content of `namespace`;
    /// in-flight uploads are swept separately by [`Self::clear_uploads`].
    async fn clear_namespace(
        &self,
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        // Each revision delete cascades its digest link, pointing tags, referrer
        // and layer/config ref entries; bytes are reclaimed later by the grant pass.
        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(digest) = revisions.next().await {
            sink.apply(Action::DeleteOrphanManifest {
                namespace: namespace.clone(),
                digest: digest?,
            })
            .await?;
        }
        // Sweep any dangling tag the revision pass above could not reach.
        let mut tags = list_all::tags(&self.metadata_store, namespace);
        while let Some(tag) = tags.next().await {
            sink.apply(Action::DeleteTag {
                namespace: namespace.clone(),
                tag: tag?,
            })
            .await?;
        }
        Ok(())
    }

    /// Sweep every in-flight upload of the dead `namespace`.
    async fn clear_uploads(
        &self,
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let mut uploads = list_all::uploads(&self.blob_store, namespace);
        while let Some(uuid) = uploads.next().await {
            sink.apply(Action::DeleteExpiredUpload {
                namespace: namespace.clone(),
                uuid: uuid?,
            })
            .await?;
        }
        Ok(())
    }

    /// Scan every blob and revoke each orphan namespace's ownership grant,
    /// reclaiming the layer/config bytes the clearing pass left pinned.
    async fn reclaim_orphan_grants(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        let mut blobs = list_all::blobs(&self.blob_store);
        while let Some(blob) = blobs.next().await {
            let blob = blob?;
            if let Err(e) = self.revoke_orphan_grants_for_blob(&blob, sink).await {
                error!("Failed to revoke orphan grants for blob {blob}: {e}");
            }
        }
        Ok(())
    }

    /// Emit a grant revoke for every namespace owning `blob` that no longer
    /// resolves to a configured repository. This pass has no age gate, unlike
    /// [`OrphanGrantChecker`](super::orphan_grants::OrphanGrantChecker); the
    /// executor's under-lock re-check still spares any blob a manifest references.
    async fn revoke_orphan_grants_for_blob(
        &self,
        blob: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let index = match self.metadata_store.read_blob_index(blob).await {
            Ok(index) => index,
            Err(MetadataError::ReferenceNotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for namespace in index.namespace.into_keys() {
            if self.resolver.resolve(&namespace).is_some() {
                continue;
            }
            sink.apply(Action::RemoveOrphanBlobGrant {
                namespace,
                blob: blob.clone(),
            })
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl StoreChecker for OrphanNamespaceChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        // No repositories configured means every namespace is an orphan; refuse
        // to delete the entire registry.
        if self.resolver.len() == 0 {
            return Ok(());
        }
        debug!("Clearing namespaces not owned by any configured repository");
        let mut namespaces = list_all::namespaces(&self.metadata_store);
        while let Some(namespace) = namespaces.next().await {
            let namespace = namespace?;
            if self.resolver.resolve(&namespace).is_some() {
                continue;
            }
            // A name that fails `Namespace` validation cannot form typed links,
            // yet its on-disk directory is exactly the out-of-band corruption
            // this checker exists to reclaim, so remove it by raw prefix.
            let Ok(namespace) = Namespace::new(&namespace) else {
                if let Err(e) = sink
                    .apply(Action::DeleteInvalidNamespace {
                        name: namespace.clone(),
                    })
                    .await
                {
                    error!("Failed to reclaim invalid orphan namespace '{namespace}': {e}");
                }
                continue;
            };
            if let Err(e) = self.clear_namespace(&namespace, sink).await {
                error!("Failed to clear orphan namespace '{namespace}': {e}");
            }
        }
        // Upload-only namespaces are absent from the catalog, so sweep them off
        // the upload tree.
        let mut upload_namespaces = list_all::upload_namespaces(&self.metadata_store);
        while let Some(namespace) = upload_namespaces.next().await {
            let namespace = namespace?;
            if self.resolver.resolve(&namespace).is_some() {
                continue;
            }
            let Ok(namespace) = Namespace::new(&namespace) else {
                if let Err(e) = sink
                    .apply(Action::DeleteInvalidUploadNamespace {
                        name: namespace.clone(),
                    })
                    .await
                {
                    error!("Failed to reclaim invalid orphan upload namespace '{namespace}': {e}");
                }
                continue;
            };
            if let Err(e) = self.clear_uploads(&namespace, sink).await {
                error!("Failed to clear orphan namespace uploads '{namespace}': {e}");
            }
        }
        // The clearing pass freed only links; revoke the grants to reclaim bytes.
        if let Err(e) = self.reclaim_orphan_grants(sink).await {
            error!("Failed to reclaim orphan namespace blob grants: {e}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use bytes::Bytes;

    use super::OrphanNamespaceChecker;
    use crate::{
        command::scrub::{action::Action, check::StoreChecker, executor::Executor},
        oci::{Digest, Namespace, Tag},
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Repository,
            metadata_store::{BlobIndexOperation, LinkKind, LinkOperation, MetadataStore},
            repository_resolver::RepositoryResolver,
            test_utils::{self, for_each_backend, put_blob_direct},
        },
    };

    /// A resolver configured with the given repository prefixes (minimal repos,
    /// no upstreams or downstreams).
    fn resolver(prefixes: &[&str]) -> Arc<RepositoryResolver> {
        let mut repositories = HashMap::new();
        for prefix in prefixes {
            repositories.insert(
                (*prefix).to_string(),
                Repository {
                    name: Namespace::new(prefix).unwrap(),
                    upstreams: Vec::new(),
                    replication: Vec::new(),
                    retention_policy: RetentionPolicy::new(
                        &RetentionPolicyConfig::default(),
                        Arc::new(SystemClock),
                    ),
                    immutable_tags: false,
                    immutable_tags_exclusions: Vec::new(),
                },
            );
        }
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    /// Seed a namespace with a revision (digest self-link) and a tag pointing at
    /// it, the shape a normal manifest push leaves. Returns the manifest digest.
    async fn seed_image(metadata_store: &Arc<MetadataStore>, namespace: &Namespace) -> Digest {
        let digest = put_blob_direct(metadata_store.store(), b"orphan manifest").await;
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        digest
    }

    #[tokio::test]
    async fn clears_orphan_namespace_content() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("ghost/app").unwrap();
            seed_image(&metadata_store, &namespace).await;

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            let (revisions, _) = metadata_store
                .list_revisions(&namespace, 100, None)
                .await
                .unwrap();
            assert!(revisions.is_empty(), "revisions must be cleared");
            let (tags, _) = metadata_store
                .list_tags(&namespace, 100, None)
                .await
                .unwrap();
            assert!(tags.is_empty(), "tags must be cleared");
            let (catalog, _) = metadata_store.list_namespaces(100, None).await.unwrap();
            assert!(
                !catalog.contains(&"ghost/app".to_string()),
                "the cleared namespace must drop out of the catalog; got: {catalog:?}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn leaves_configured_namespace_untouched() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("keep/app").unwrap();
            seed_image(&metadata_store, &namespace).await;

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "a configured namespace must not be touched"
            );
            let (tags, _) = metadata_store
                .list_tags(&namespace, 100, None)
                .await
                .unwrap();
            assert_eq!(
                tags,
                vec![Tag::new("latest").unwrap()],
                "content must remain"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn dry_run_captures_actions_without_mutating() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("ghost/app").unwrap();
            seed_image(&metadata_store, &namespace).await;

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::DeleteOrphanManifest { namespace, .. } if namespace == "ghost/app")),
                "the orphan revision must be captured"
            );
            assert!(
                sink.iter().any(
                    |a| matches!(a, Action::DeleteTag { namespace, .. } if namespace == "ghost/app")
                ),
                "the orphan tag must be captured"
            );
            let (revisions, _) = metadata_store
                .list_revisions(&namespace, 100, None)
                .await
                .unwrap();
            assert_eq!(
                revisions.len(),
                1,
                "a capturing sink must not mutate storage"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn empty_resolver_deletes_nothing() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("ghost/app").unwrap();
            seed_image(&metadata_store, &namespace).await;

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&[]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "an empty resolver must not delete the whole registry"
            );
            let (revisions, _) = metadata_store
                .list_revisions(&namespace, 100, None)
                .await
                .unwrap();
            assert_eq!(
                revisions.len(),
                1,
                "content must remain with no repos configured"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn sweeps_dangling_tag_without_revision() {
        for_each_backend(async |test_case| {
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();
            let namespace = Namespace::new("ghost/app").unwrap();
            // A tag with no revision self-link: the revision pass cannot reach it.
            let digest = put_blob_direct(metadata_store.store(), b"dangling").await;
            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest,
                    )],
                )
                .await
                .unwrap();

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::DeleteTag { namespace, tag } if namespace == "ghost/app" && tag.as_ref() == "latest"
                )),
                "a dangling tag must still be swept"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn end_to_end_clears_only_the_orphan_namespace() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            seed_image(&metadata_store, &Namespace::new("keep/app").unwrap()).await;
            seed_image(&metadata_store, &Namespace::new("ghost/app").unwrap()).await;

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            let (catalog, _) = metadata_store.list_namespaces(100, None).await.unwrap();
            assert!(
                catalog.contains(&"keep/app".to_string()),
                "the configured namespace must remain; got: {catalog:?}"
            );
            assert!(
                !catalog.contains(&"ghost/app".to_string()),
                "the orphan namespace must be cleared; got: {catalog:?}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn reclaims_orphan_namespace_layer_and_config_bytes() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("ghost/app").unwrap();

            // The end-state of a real push: manifest/config/layer blobs, their
            // links, the revision self-link, and the config/layer ownership grants.
            let (manifest_digest, config_digest, layer_digest) =
                test_utils::seed_manifest(metadata_store.store(), &metadata_store, &namespace)
                    .await;
            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    )],
                )
                .await
                .unwrap();
            for digest in [config_digest.clone(), layer_digest.clone()] {
                metadata_store
                    .update_blob_index(
                        &namespace,
                        &digest,
                        BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
                    )
                    .await
                    .unwrap();
            }

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            assert!(
                blob_store.read(&config_digest).await.is_err(),
                "the orphan config blob bytes must be reclaimed"
            );
            assert!(
                blob_store.read(&layer_digest).await.is_err(),
                "the orphan layer blob bytes must be reclaimed"
            );
            let (catalog, _) = metadata_store.list_namespaces(100, None).await.unwrap();
            assert!(
                !catalog.contains(&"ghost/app".to_string()),
                "the orphan namespace must drop out of the catalog; got: {catalog:?}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn configured_namespace_grant_is_spared() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // A grant-only blob owned by a *configured* namespace: the resolve
            // guard must spare it, the same as a live repo's bytes.
            let namespace = Namespace::new("keep/app").unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"live grant").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &blob,
                    BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                )
                .await
                .unwrap();

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_ok(),
                "a configured namespace's grant must be spared"
            );
            assert!(
                blob_store.read(&blob).await.is_ok(),
                "the configured namespace's bytes must remain"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn shared_blob_survives_when_configured_namespace_references_it() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // One blob owned by both a live and an orphan namespace. Revoking the
            // orphan's grant must remove only that grant; the configured
            // namespace's reference still pins the bytes.
            let blob = put_blob_direct(metadata_store.store(), b"shared blob").await;
            for namespace in ["keep/app", "ghost/app"] {
                let namespace = Namespace::new(namespace).unwrap();
                metadata_store
                    .update_blob_index(
                        &namespace,
                        &blob,
                        BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                    )
                    .await
                    .unwrap();
            }

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&Namespace::new("ghost/app").unwrap(), &blob)
                    .await
                    .is_err(),
                "the orphan namespace's grant must be revoked"
            );
            assert!(
                metadata_store
                    .read_blob_index_namespace(&Namespace::new("keep/app").unwrap(), &blob)
                    .await
                    .is_ok(),
                "the configured namespace's grant must remain"
            );
            assert!(
                blob_store.read(&blob).await.is_ok(),
                "the still-referenced bytes must not be reclaimed"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn reclaims_pure_grant_only_orphan_namespace() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // An orphan namespace whose sole footprint is a grant (no manifest),
            // the end-state of a push that lost last-writer-wins. The grant is
            // revoked and the now-unreferenced bytes reclaimed.
            let namespace = Namespace::new("ghost/app").unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"pure grant").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &blob,
                    BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                )
                .await
                .unwrap();

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_err(),
                "the orphan namespace's grant must be revoked"
            );
            assert!(
                blob_store.read(&blob).await.is_err(),
                "the now-unreferenced bytes must be reclaimed"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn dry_run_does_not_revoke_grants() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new("ghost/app").unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"dry-run grant").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &blob,
                    BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                )
                .await
                .unwrap();

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::RemoveOrphanBlobGrant { namespace, blob: b }
                        if namespace == "ghost/app" && *b == blob
                )),
                "the orphan grant revoke must be captured"
            );
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_ok(),
                "a capturing sink must not revoke the grant"
            );
            assert!(
                blob_store.read(&blob).await.is_ok(),
                "a capturing sink must not reclaim the bytes"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn deletes_in_flight_upload_of_orphan_namespace() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();
            let uuid = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&Namespace::new("ghost/app").unwrap(), &uuid)
                .await
                .unwrap();

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.iter().any(|a| matches!(
                    a,
                    Action::DeleteExpiredUpload { namespace, .. } if namespace == "ghost/app"
                )),
                "the in-flight upload of the orphan namespace must be swept"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn reclaims_invalidly_named_orphan_namespace() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // Seed a raw namespace whose name fails `Namespace` validation
            // (uppercase). A `_manifests` descendant makes the catalog walk
            // enumerate it; this is the out-of-band corruption scrub must reclaim.
            let key = "v2/repositories/BadNS/_manifests/revisions/sha256/dead/link";
            metadata_store
                .store()
                .put(key, Bytes::from_static(b"sha256:dead"))
                .await
                .unwrap();
            let (catalog, _) = metadata_store.list_namespaces(100, None).await.unwrap();
            assert!(
                catalog.iter().any(|n| n == "BadNS"),
                "the invalid namespace must enumerate before the sweep; got: {catalog:?}"
            );

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            assert!(
                metadata_store.store().get(key).await.is_err(),
                "the invalidly-named namespace directory must be reclaimed"
            );
            let (catalog, _) = metadata_store.list_namespaces(100, None).await.unwrap();
            assert!(
                !catalog.iter().any(|n| n == "BadNS"),
                "the reclaimed namespace must drop out of the catalog; got: {catalog:?}"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn dry_run_captures_invalid_namespace_reclaim_without_mutating() {
        for_each_backend(async |test_case| {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let key = "v2/repositories/BadNS/_manifests/revisions/sha256/dead/link";
            metadata_store
                .store()
                .put(key, Bytes::from_static(b"sha256:dead"))
                .await
                .unwrap();

            let checker = OrphanNamespaceChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                resolver(&["keep"]),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.iter().any(
                    |a| matches!(a, Action::DeleteInvalidNamespace { name } if name == "BadNS")
                ),
                "the invalid orphan namespace reclaim must be captured"
            );
            assert!(
                metadata_store.store().get(key).await.is_ok(),
                "a capturing sink must not mutate storage"
            );
        })
        .await;
    }
}
