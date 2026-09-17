//! Orphan-namespace clearing: every namespace not owned by any configured
//! repository loses its content. Grants of unresolved namespaces are revoked
//! by the grant sweep in `checker`.

use std::pin::pin;
use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use tracing::{error, warn};

use angos_oci::Namespace;

use crate::{
    command::maintenance::{Error, action::Action, executor::ActionSink},
    registry::{
        blob_store::BlobStore,
        content_discovery::holds_manifest_content,
        metadata_store::{LIST_PAGE, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
};

/// Clear the manifest content and in-flight uploads of every namespace that
/// no longer resolves to a configured repository. Deletes go through the
/// registry path so child links cascade; byte reclaim follows via the grant
/// sweep and scrub's blob GC.
pub async fn sweep_orphan_namespaces(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    // With no repositories configured every namespace looks like an orphan;
    // refuse to delete the entire registry.
    if resolver.len() == 0 {
        warn!("prune: no repositories configured; skipping orphan-namespace clearing");
        return Ok(());
    }

    // Ownership is resolved in the walk, so a namespace a repository claims
    // costs no content probe at all.
    let unowned = |namespace: &Namespace| resolver.resolve(namespace).is_none();
    let mut orphans = pin!(metadata_store.stream_namespaces(
        None,
        LIST_PAGE,
        &unowned,
        |namespace| holds_manifest_content(metadata_store, namespace)
    ));
    while let Some(namespace) = orphans.try_next().await? {
        if let Err(e) = clear_namespace(metadata_store, &namespace, sink).await {
            error!("prune: failed to clear orphan namespace '{namespace}': {e}");
        }
    }

    // Upload-only namespaces are absent from the catalog, so sweep them off
    // the blob store's upload tree.
    for namespace in blob_store.collect_upload_namespaces(None).await? {
        if resolver.resolve(&namespace).is_some() {
            continue;
        }
        if let Err(e) = clear_uploads(blob_store, &namespace, sink).await {
            error!("prune: failed to clear orphan uploads of '{namespace}': {e}");
        }
    }

    Ok(())
}

/// Emit the delete actions clearing the manifest content of `namespace`. Each
/// revision delete cascades its links; the tag pass then sweeps any dangling
/// tag the cascade could not reach.
async fn clear_namespace(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let mut revisions = pin!(metadata_store.stream_revisions(namespace));
    while let Some(digest) = revisions.next().await {
        sink.apply(Action::DeleteOrphanManifest {
            namespace: namespace.clone(),
            digest: digest?,
        })
        .await?;
    }
    let mut tags = pin!(metadata_store.stream_live_tags(namespace, None));
    while let Some(tag) = tags.next().await {
        sink.apply(Action::DeleteTag {
            namespace: namespace.clone(),
            tag: tag?.0,
            // The namespace belongs to no configured repository, so every tag
            // in it goes whatever it points at.
            target: None,
        })
        .await?;
    }
    Ok(())
}

async fn clear_uploads(
    blob_store: &Arc<BlobStore>,
    namespace: &Namespace,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let mut uploads = pin!(blob_store.stream_uploads(namespace));
    while let Some(session_id) = uploads.next().await {
        sink.apply(Action::DeleteExpiredUpload {
            namespace: namespace.clone(),
            session_id: session_id?,
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use angos_oci::UploadSessionId;

    use crate::command::prune::orphan_namespaces::*;
    use crate::{
        command::maintenance::executor::Executor,
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
            // `ghost/app` resolves to no configured repository.
            let ghost = Namespace::new("ghost/app").unwrap();
            let owned = Namespace::new("test-repo/app").unwrap();
            seed_manifest(metadata_store.object_store(), &metadata_store, &ghost).await;
            seed_manifest(metadata_store.object_store(), &metadata_store, &owned).await;
            let ghost_upload = UploadSessionId::generate();
            blob_store
                .create_upload(&ghost, &ghost_upload, None)
                .await
                .unwrap();

            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            sweep_orphan_namespaces(&blob_store, &metadata_store, &resolver(), &executor)
                .await
                .unwrap();

            let ghost_tags = metadata_store
                .list_tags(&ghost, 10, None)
                .await
                .unwrap()
                .items;
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
            let owned_tags = metadata_store
                .list_tags(&owned, 10, None)
                .await
                .unwrap()
                .items;
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
            seed_manifest(metadata_store.object_store(), &metadata_store, &ghost).await;

            let empty = Arc::new(
                RepositoryResolver::new(Arc::new(std::collections::HashMap::new()))
                    .expect("empty resolver"),
            );
            let executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            sweep_orphan_namespaces(&blob_store, &metadata_store, &empty, &executor)
                .await
                .unwrap();

            let tags = metadata_store
                .list_tags(&ghost, 10, None)
                .await
                .unwrap()
                .items;
            assert_eq!(
                tags.len(),
                1,
                "an emptied config must never clear the registry"
            );
        })
        .await;
    }
}
