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
    oci::Digest,
    registry::{
        blob_store::BlobStore,
        metadata_store::{BlobIndex, Error as MetadataError, MetadataStore, link_kind::LinkKind},
    },
};

enum BlobVerdict {
    /// Blob has no recorded references in any namespace; safe to delete.
    Orphan,
    /// Blob is referenced; iterate references to probe each link's validity.
    Referenced(BlobIndex),
}

async fn classify_blob(
    metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
    blob: &Digest,
) -> Result<BlobVerdict, Error> {
    match metadata_store.read_blob_index(blob).await {
        Ok(index) if index.namespace.is_empty() => Ok(BlobVerdict::Orphan),
        Ok(index) => Ok(BlobVerdict::Referenced(index)),
        Err(MetadataError::ReferenceNotFound) => Ok(BlobVerdict::Orphan),
        Err(e) => Err(e.into()),
    }
}

pub struct BlobChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
}

impl BlobChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    async fn check_blob(
        &self,
        blob: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking blob index for blob '{blob}'");
        match classify_blob(&self.metadata_store, blob).await? {
            BlobVerdict::Orphan => {
                sink.apply(Action::DeleteOrphanBlob(blob.clone())).await?;
            }
            BlobVerdict::Referenced(blob_index) => {
                for (namespace, references) in blob_index.namespace {
                    for link in references {
                        self.probe_and_cleanup_link(&namespace, blob, &link, sink)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn probe_and_cleanup_link(
        &self,
        namespace: &str,
        blob: &Digest,
        link: &LinkKind,
        sink: &mut (dyn ActionSink + Send),
    ) {
        if let LinkKind::Blob(owned_digest) = link
            && owned_digest == blob
        {
            return;
        }

        if self
            .metadata_store
            .read_link(namespace, link, false)
            .await
            .is_err()
            && let Err(err) = sink
                .apply(Action::RemoveBlobIndexLink {
                    namespace: namespace.to_string(),
                    blob: blob.clone(),
                    link: link.clone(),
                })
                .await
        {
            error!(
                "Failed to remove invalid link '{link}' from blob index '{namespace}/{blob}': {err}"
            );
        }
    }
}

#[async_trait]
impl StoreChecker for BlobChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        debug!("Checking blobs");

        let mut blobs = list_all::blobs(&self.blob_store);
        while let Some(blob) = blobs.next().await {
            let blob = blob?;
            if let Err(e) = self.check_blob(&blob, sink).await {
                error!("Failed to process blob index for {blob}: {e}");
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
        oci::{Digest, Namespace},
        registry::{
            blob_store::MultipartCleanup,
            metadata_store::BlobIndexOperation,
            test_utils::{self, NoopMultipart, backends},
        },
    };

    #[tokio::test]
    async fn classify_blob_returns_orphan_when_no_index_entry() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_digest = blob_store.create(b"orphan content").await.unwrap();

            let verdict = classify_blob(&metadata_store, &orphan_digest)
                .await
                .unwrap();
            assert!(
                matches!(verdict, BlobVerdict::Orphan),
                "A blob with no index entry must be classified as Orphan"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn classify_blob_returns_orphan_when_namespace_map_is_empty() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let digest = blob_store
                .create(b"content with empty index")
                .await
                .unwrap();

            // Insert then immediately remove the only link so the namespace map exists but is empty.
            let namespace = "test-repo/empty";
            let link = LinkKind::Layer(digest.clone());
            metadata_store
                .update_blob_index(namespace, &digest, BlobIndexOperation::Insert(link.clone()))
                .await
                .unwrap();
            metadata_store
                .update_blob_index(namespace, &digest, BlobIndexOperation::Remove(link))
                .await
                .unwrap();

            let verdict = classify_blob(&metadata_store, &digest).await.unwrap();
            assert!(
                matches!(verdict, BlobVerdict::Orphan),
                "A blob whose namespace map is empty must be classified as Orphan"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn classify_blob_returns_referenced_when_index_has_links() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"referenced content").await;

            let verdict = classify_blob(&metadata_store, &blob_digest).await.unwrap();
            match verdict {
                BlobVerdict::Referenced(index) => {
                    assert!(
                        index.namespace.contains_key(namespace.as_ref()),
                        "Referenced verdict must carry the namespace in its BlobIndex"
                    );
                }
                BlobVerdict::Orphan => {
                    panic!("Expected Referenced, got Orphan");
                }
            }
            test_case.cleanup().await;
        }
    }

    fn noop_multipart() -> std::sync::Arc<dyn MultipartCleanup + Send + Sync> {
        std::sync::Arc::new(NoopMultipart)
    }

    #[tokio::test]
    async fn test_cleanup_orphan_blobs_removes_invalid_index_entries() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test content").await;

            let orphan_layer_link = LinkKind::Layer(
                Digest::from_str(
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap(),
            );
            metadata_store
                .update_blob_index(
                    namespace,
                    &blob_digest,
                    BlobIndexOperation::Insert(orphan_layer_link.clone()),
                )
                .await
                .unwrap();

            let blob_index_before = metadata_store.read_blob_index(&blob_digest).await.unwrap();

            let initial_refs = blob_index_before
                .namespace
                .get(namespace.as_ref())
                .map_or(0, std::collections::HashSet::len);

            let checker = BlobChecker::new(blob_store.clone(), metadata_store.clone());

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
                noop_multipart(),
            );

            checker.check_all(&mut executor).await.unwrap();

            let blob_index_after = metadata_store.read_blob_index(&blob_digest).await.unwrap();

            let final_refs = blob_index_after
                .namespace
                .get(namespace.as_ref())
                .map_or(0, std::collections::HashSet::len);

            assert!(
                final_refs < initial_refs,
                "Invalid blob index entry should be removed. Before: {initial_refs}, After: {final_refs}"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_cleanup_preserves_explicit_blob_ownership() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let blob_digest = blob_store.create(b"owned content").await.unwrap();
            let ownership_link = LinkKind::Blob(blob_digest.clone());
            metadata_store
                .update_blob_index(
                    namespace,
                    &blob_digest,
                    BlobIndexOperation::Insert(ownership_link.clone()),
                )
                .await
                .unwrap();

            let checker = BlobChecker::new(blob_store.clone(), metadata_store.clone());
            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
                noop_multipart(),
            );

            checker.check_all(&mut executor).await.unwrap();

            let blob_index = metadata_store.read_blob_index(&blob_digest).await.unwrap();
            let links = blob_index.namespace.get(namespace.as_ref()).unwrap();
            assert!(links.contains(&ownership_link));
            assert!(blob_store.read(&blob_digest).await.is_ok());
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_cleanup_deletes_orphan_blobs_without_index() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"orphan blob content";
            let orphan_digest = blob_store.create(orphan_content).await.unwrap();

            assert!(blob_store.read(&orphan_digest).await.is_ok());

            let checker = BlobChecker::new(blob_store.clone(), metadata_store.clone());

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store,
                test_case.upload_store(),
                noop_multipart(),
            );

            checker.check_all(&mut executor).await.unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_err(),
                "Orphan blob without index should be deleted after scrub"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_cleanup_dry_run_preserves_orphan_blobs() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let orphan_content = b"orphan blob content for dry run";
            let orphan_digest = blob_store.create(orphan_content).await.unwrap();

            assert!(blob_store.read(&orphan_digest).await.is_ok());

            let checker = BlobChecker::new(blob_store.clone(), metadata_store.clone());

            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                blob_store.read(&orphan_digest).await.is_ok(),
                "Vec sink must not mutate storage"
            );
            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::DeleteOrphanBlob(_))),
                "Vec sink must capture the DeleteOrphanBlob action"
            );
            test_case.cleanup().await;
        }
    }
}
