//! Orphan-namespace clearing: every namespace not owned by any configured
//! repository loses its content. Runs on every `angos prune` (the
//! config-trusting command); grants of unresolved namespaces are revoked by
//! the grant sweep in `checker`, and invalid-name directories are scrub's
//! (structural) concern.

use std::sync::Arc;

use futures_util::StreamExt;
use tracing::{error, warn};

use crate::{
    command::scrub::{Error, action::Action, check::list_all, executor::ActionSink},
    oci::Namespace,
    registry::{
        blob_store::BlobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

/// Clear the manifest content and in-flight uploads of every namespace that
/// no longer resolves to a configured repository. Revision and tag deletes go
/// through the registry delete path, cascading child links; byte reclaim
/// follows via the grant sweep and scrub's blob GC.
pub async fn sweep_orphan_namespaces(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    // No repositories configured means every namespace is an orphan; refuse
    // to delete the entire registry.
    if resolver.len() == 0 {
        warn!("prune: no repositories configured; skipping orphan-namespace clearing");
        return Ok(());
    }

    let mut namespaces = list_all::namespaces(metadata_store);
    while let Some(namespace) = namespaces.next().await {
        let namespace = namespace?;
        if resolver.resolve(&namespace).is_some() {
            continue;
        }
        // An invalid name cannot form typed links; scrub reclaims such
        // directories structurally.
        let Ok(namespace) = Namespace::new(&namespace) else {
            continue;
        };
        if let Err(e) = clear_namespace(metadata_store, &namespace, sink).await {
            error!("prune: failed to clear orphan namespace '{namespace}': {e}");
        }
    }

    // Upload-only namespaces are absent from the catalog, so sweep them off
    // the blob store's upload tree.
    let mut upload_namespaces = list_all::upload_namespaces(blob_store);
    while let Some(namespace) = upload_namespaces.next().await {
        let namespace = namespace?;
        if resolver.resolve(&namespace).is_some() {
            continue;
        }
        let Ok(namespace) = Namespace::new(&namespace) else {
            continue;
        };
        if let Err(e) = clear_uploads(blob_store, &namespace, sink).await {
            error!("prune: failed to clear orphan uploads of '{namespace}': {e}");
        }
    }

    Ok(())
}

/// Emit the delete actions clearing the manifest content of `namespace`.
/// Each revision delete cascades its digest link, pointing tags, referrer and
/// layer/config entries; the tag pass sweeps any dangling tag the revision
/// cascade could not reach.
async fn clear_namespace(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let mut revisions = list_all::revisions(metadata_store, namespace);
    while let Some(digest) = revisions.next().await {
        sink.apply(Action::DeleteOrphanManifest {
            namespace: namespace.clone(),
            digest: digest?,
        })
        .await?;
    }
    let mut tags = list_all::tags(metadata_store, namespace);
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
    blob_store: &Arc<BlobStore>,
    namespace: &Namespace,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let mut uploads = list_all::uploads(blob_store, namespace);
    while let Some(uuid) = uploads.next().await {
        sink.apply(Action::DeleteExpiredUpload {
            namespace: namespace.clone(),
            uuid: uuid?,
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::scrub::executor::Executor,
        registry::{
            repository_resolver::RepositoryResolver,
            test_utils::{create_test_repositories, for_each_backend, seed_manifest},
        },
    };

    fn resolver() -> Arc<RepositoryResolver> {
        Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not overlap"),
        )
    }

    #[tokio::test]
    async fn orphan_namespace_content_is_cleared_and_owned_one_kept() {
        for_each_backend(async |test_case| {
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();
            // `ghost/app` resolves to no configured repository; `test-repo/app`
            // does.
            let ghost = Namespace::new("ghost/app").unwrap();
            let owned = Namespace::new("test-repo/app").unwrap();
            seed_manifest(metadata_store.store(), &metadata_store, &ghost).await;
            seed_manifest(metadata_store.store(), &metadata_store, &owned).await;
            let ghost_upload = uuid::Uuid::new_v4().to_string();
            blob_store
                .create_upload(&ghost, &ghost_upload)
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            sweep_orphan_namespaces(&blob_store, &metadata_store, &resolver(), &executor)
                .await
                .unwrap();

            let (ghost_tags, _) = metadata_store.list_tags(&ghost, 10, None).await.unwrap();
            assert!(
                ghost_tags.is_empty(),
                "the orphan namespace's tags must be gone"
            );
            assert!(
                blob_store
                    .upload_summary(&ghost, &ghost_upload)
                    .await
                    .is_err(),
                "the orphan namespace's uploads must be gone"
            );
            let (owned_tags, _) = metadata_store.list_tags(&owned, 10, None).await.unwrap();
            assert_eq!(owned_tags.len(), 1, "the owned namespace must be untouched");
        })
        .await;
    }

    #[tokio::test]
    async fn empty_config_refuses_to_clear_anything() {
        for_each_backend(async |test_case| {
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();
            let ghost = Namespace::new("ghost/app").unwrap();
            seed_manifest(metadata_store.store(), &metadata_store, &ghost).await;

            let empty = Arc::new(
                RepositoryResolver::new(Arc::new(std::collections::HashMap::new()))
                    .expect("empty resolver"),
            );
            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            sweep_orphan_namespaces(&blob_store, &metadata_store, &empty, &executor)
                .await
                .unwrap();

            let (tags, _) = metadata_store.list_tags(&ghost, 10, None).await.unwrap();
            assert_eq!(
                tags.len(),
                1,
                "an emptied config must never clear the registry"
            );
        })
        .await;
    }
}
