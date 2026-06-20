use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
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
        blob_store,
        metadata_store::{Error as MetadataError, LinkKind, MetadataStore},
    },
};

/// Reclaims per-namespace blob-ownership grants (`LinkKind::Blob`) stranded when
/// a replication push grants a blob's ownership but its manifest PUT then loses
/// last-writer-wins or dead-letters, so no manifest in that namespace ever
/// references the blob. Such a grant pins the bytes forever: the standard
/// blob-orphan GC keeps any blob whose namespace map is non-empty. A grant is
/// revoked only when the bytes are older than `min_age`, so an in-flight push
/// (which grants ownership before it links the manifest) is never reaped.
pub struct OrphanGrantChecker {
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
    min_age: Duration,
}

impl OrphanGrantChecker {
    pub fn new(
        blob_store: Arc<blob_store::BlobStore>,
        metadata_store: Arc<MetadataStore>,
        min_age: Duration,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            min_age,
        }
    }

    /// Emits a revoke for every namespace whose only entry for `blob` is the
    /// self-ownership grant (no manifest reference), once the bytes are older
    /// than `min_age`. The executor re-checks under the blob-data lock before
    /// revoking, so a manifest reference racing this scan is not clobbered.
    async fn check_blob(
        &self,
        blob: &Digest,
        now: DateTime<Utc>,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        // Age-gate on the bytes' mtime; without one (or with the bytes absent)
        // leave the blob to the standard blob-orphan GC.
        let last_modified = match self.blob_store.last_modified(blob).await {
            Ok(Some(ts)) => ts,
            Ok(None) => return Ok(()),
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        if now.signed_duration_since(last_modified) < self.min_age {
            return Ok(());
        }

        let index = match self.metadata_store.read_blob_index(blob).await {
            Ok(index) => index,
            Err(MetadataError::ReferenceNotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let grant = LinkKind::Blob(blob.clone());
        for (namespace, links) in index.namespace {
            // A tracked link (Layer/Config/Manifest) means a manifest references
            // the blob, so the grant is live; only a grant-only namespace leaks.
            if !links.contains(&grant) || links.iter().any(LinkKind::is_tracked) {
                continue;
            }
            if let Err(e) = sink
                .apply(Action::RemoveOrphanBlobGrant {
                    namespace: namespace.clone(),
                    blob: blob.clone(),
                })
                .await
            {
                error!("Failed to enqueue orphan grant revoke for '{namespace}/{blob}': {e}");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl StoreChecker for OrphanGrantChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        debug!("Checking for orphaned blob-ownership grants");
        let now = Utc::now();
        let mut blobs = list_all::blobs(&self.blob_store);
        while let Some(blob) = blobs.next().await {
            let blob = blob?;
            if let Err(e) = self.check_blob(&blob, now, sink).await {
                error!("Failed to check orphan grants for blob {blob}: {e}");
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
            metadata_store::BlobIndexOperation,
            test_utils::{self, backends, put_blob_direct},
        },
    };

    const NAMESPACE: &str = "test-repo/app";

    #[tokio::test]
    async fn revokes_grant_and_reclaims_bytes_when_no_manifest_references_it() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // A blob whose only index entry is the self-ownership grant: the
            // exact end-state of a push that uploaded the blob then lost LWW.
            let namespace = Namespace::new(NAMESPACE).unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"stranded grant").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &blob,
                    BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                )
                .await
                .unwrap();

            let checker = OrphanGrantChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Duration::zero(),
            );
            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());
            checker.check_all(&mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_err(),
                "the orphaned grant must be revoked"
            );
            assert!(
                blob_store.read(&blob).await.is_err(),
                "the now-unreferenced bytes must be reclaimed"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn preserves_grant_when_a_manifest_references_the_blob() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new(NAMESPACE).unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"referenced blob").await;
            for link in [LinkKind::Blob(blob.clone()), LinkKind::Layer(blob.clone())] {
                metadata_store
                    .update_blob_index(&namespace, &blob, BlobIndexOperation::Insert(link))
                    .await
                    .unwrap();
            }

            let checker = OrphanGrantChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Duration::zero(),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                !sink
                    .iter()
                    .any(|a| matches!(a, Action::RemoveOrphanBlobGrant { .. })),
                "a grant backing a manifest reference must not be revoked"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn skips_a_grant_younger_than_min_age() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // Just-written bytes: an in-flight push may still link a manifest.
            let namespace = Namespace::new(NAMESPACE).unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"fresh grant").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &blob,
                    BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                )
                .await
                .unwrap();

            let checker = OrphanGrantChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Duration::hours(1),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "a grant younger than min_age must not be touched"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn dry_run_captures_the_revoke_without_mutating() {
        for test_case in backends() {
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let namespace = Namespace::new(NAMESPACE).unwrap();
            let blob = put_blob_direct(metadata_store.store(), b"dry-run grant").await;
            metadata_store
                .update_blob_index(
                    &namespace,
                    &blob,
                    BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
                )
                .await
                .unwrap();

            let checker = OrphanGrantChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Duration::zero(),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                sink.iter().any(
                    |a| matches!(a, Action::RemoveOrphanBlobGrant { blob: b, .. } if *b == blob)
                ),
                "the Vec sink must capture the revoke action"
            );
            assert!(
                blob_store.read(&blob).await.is_ok(),
                "a Vec sink must not mutate storage"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn ignores_a_blob_with_only_a_real_manifest_reference() {
        // A normal pushed image (Layer reference, no stray grant) is untouched.
        for test_case in backends() {
            let namespace = &Namespace::new(NAMESPACE).unwrap();
            let registry = test_case.registry();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let _ = test_utils::create_test_blob(registry, namespace, b"normal blob").await;

            let checker = OrphanGrantChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                Duration::zero(),
            );
            let mut sink: Vec<Action> = Vec::new();
            checker.check_all(&mut sink).await.unwrap();

            assert!(
                !sink
                    .iter()
                    .any(|a| matches!(a, Action::RemoveOrphanBlobGrant { .. })),
                "a normally-referenced blob must not be touched"
            );
            test_case.cleanup().await;
        }
    }
}
