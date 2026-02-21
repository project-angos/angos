use std::sync::Arc;

use tracing::{debug, error};

use crate::{
    command::scrub::check::ensure_link,
    registry::{
        Error,
        metadata_store::{MetadataStore, link_kind::LinkKind},
    },
};

pub struct TagChecker {
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    dry_run: bool,
}

impl TagChecker {
    pub fn new(metadata_store: Arc<dyn MetadataStore + Send + Sync>, dry_run: bool) -> Self {
        Self {
            metadata_store,
            dry_run,
        }
    }

    pub async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking tags inconsistencies from namespace '{namespace}'");

        let mut marker = None;
        loop {
            let (tags, next_marker) = self
                .metadata_store
                .list_tags(namespace, 100, marker)
                .await?;

            for tag in &tags {
                if let Err(e) = self.repair_tag_digest_link(namespace, tag).await {
                    error!("Failed to check tag from '{namespace}' (tag '{tag}'): {e}");
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        Ok(())
    }

    async fn repair_tag_digest_link(&self, namespace: &str, tag: &str) -> Result<(), Error> {
        debug!("Checking digest link for tag '{namespace}:{tag}'");
        let tag_metadata = self
            .metadata_store
            .read_link(namespace, &LinkKind::Tag(tag.to_string()), false)
            .await?;

        let digest_link = LinkKind::Digest(tag_metadata.target.clone());
        ensure_link(
            &self.metadata_store,
            namespace,
            &digest_link,
            &tag_metadata.target,
            self.dry_run,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{metadata_store::MetadataStoreExt, test_utils, tests::backends};

    #[tokio::test]
    async fn test_scrub_tags_creates_missing_digest_links() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let tag_name = "v1.0.0";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest content").await;

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Tag(tag_name.to_string()), &blob_digest);
            tx.commit().await.unwrap();

            let scrubber = TagChecker::new(metadata_store.clone(), false);

            scrubber.check_namespace(namespace).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(blob_digest.clone()), false)
                .await;

            assert!(
                digest_link.is_ok(),
                "Digest link should be created if missing"
            );
            assert_eq!(digest_link.unwrap().target, blob_digest);
        }
    }

    #[tokio::test]
    async fn test_scrub_tags_creates_digest_links() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.delete_link(&LinkKind::Digest(blob_digest.clone()));
            tx.create_link(&LinkKind::Tag("v1.0.0".to_string()), &blob_digest);
            tx.commit().await.unwrap();

            let scrubber = TagChecker::new(metadata_store.clone(), false);

            scrubber.check_namespace(namespace).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(blob_digest.clone()), false)
                .await;

            assert!(
                digest_link.is_ok(),
                "scrub_tags should create missing digest links"
            );
        }
    }
}
