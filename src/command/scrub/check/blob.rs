use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, error};

use crate::{
    command::scrub::{action::Action, check::StoreChecker, error::Error, executor::ActionSink},
    oci::Digest,
    registry::{
        blob_store::{self, BlobStore},
        metadata_store::{self, MetadataStore, link_kind::LinkKind},
        pagination::collect_all_pages,
    },
};

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

        let blob_index = match self.metadata_store.read_blob_index(blob).await {
            Ok(index) => index,
            Err(metadata_store::Error::ReferenceNotFound) => {
                sink.apply(Action::DeleteOrphanBlob(blob.clone())).await?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        if blob_index.namespace.is_empty() {
            sink.apply(Action::DeleteOrphanBlob(blob.clone())).await?;
            return Ok(());
        }

        for (namespace, references) in blob_index.namespace {
            for link in references {
                self.probe_and_cleanup_link(&namespace, blob, &link, sink)
                    .await;
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

        let blobs: Vec<Digest> =
            collect_all_pages(|marker| async move { self.blob_store.list(100, marker).await })
                .await
                .map_err(|e: blob_store::Error| Error::from(e))?;

        for blob in &blobs {
            if let Err(e) = self.check_blob(blob, sink).await {
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
        oci::Digest,
        registry::{
            blob_store::MultipartCleanup,
            metadata_store::BlobIndexOperation,
            test_utils::{self, NoopMultipart, backends},
        },
    };

    fn noop_multipart() -> std::sync::Arc<dyn MultipartCleanup + Send + Sync> {
        std::sync::Arc::new(NoopMultipart)
    }

    #[tokio::test]
    async fn test_cleanup_orphan_blobs_removes_invalid_index_entries() {
        for test_case in backends() {
            let namespace = "test-repo/app";
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
                .get(namespace)
                .map_or(0, std::collections::HashSet::len);

            let checker = BlobChecker::new(blob_store.clone(), metadata_store.clone());

            let mut executor = Executor::new(
                false,
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
                noop_multipart(),
            );

            checker.check_all(&mut executor).await.unwrap();

            let blob_index_after = metadata_store.read_blob_index(&blob_digest).await.unwrap();

            let final_refs = blob_index_after
                .namespace
                .get(namespace)
                .map_or(0, std::collections::HashSet::len);

            assert!(
                final_refs < initial_refs,
                "Invalid blob index entry should be removed. Before: {initial_refs}, After: {final_refs}"
            );
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
                false,
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
        }
    }
}
