use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        check::{NamespaceChecker, ensure_link, list_all},
        error::Error,
        executor::ActionSink,
    },
    registry::metadata_store::{MetadataStore, link_kind::LinkKind},
};

pub struct TagChecker {
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
}

impl TagChecker {
    pub fn new(metadata_store: Arc<dyn MetadataStore + Send + Sync>) -> Self {
        Self { metadata_store }
    }

    async fn repair_tag_digest_link(
        &self,
        namespace: &str,
        tag: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
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
            sink,
        )
        .await
    }
}

#[async_trait]
impl NamespaceChecker for TagChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking tags inconsistencies from namespace '{namespace}'");

        let mut tags = list_all::tags(&self.metadata_store, namespace);
        while let Some(tag) = tags.next().await {
            let tag = tag?;
            if let Err(e) = self.repair_tag_digest_link(namespace, &tag, sink).await {
                error!("Failed to check tag from '{namespace}' (tag '{tag}'): {e}");
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
            metadata_store::LinkOperation,
            test_utils::{self, NoopMultipart, backends},
        },
    };

    #[tokio::test]
    async fn test_scrub_tags_creates_missing_digest_links() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let tag_name = "v1.0.0";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest content").await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(tag_name.to_string()),
                        blob_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut executor = Executor::new(
                test_case.blob_store(),
                metadata_store.clone(),
                test_case.upload_store(),
                std::sync::Arc::new(NoopMultipart),
            );

            let scrubber = TagChecker::new(metadata_store.clone());
            scrubber.check(namespace, &mut executor).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(blob_digest.clone()), false)
                .await;

            assert!(
                digest_link.is_ok(),
                "Digest link should be created if missing"
            );
            assert_eq!(digest_link.unwrap().target, blob_digest);
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_scrub_tags_creates_digest_links() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::delete(LinkKind::Digest(blob_digest.clone())),
                        LinkOperation::create(
                            LinkKind::Tag("v1.0.0".to_string()),
                            blob_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let mut executor = Executor::new(
                blob_store.clone(),
                metadata_store.clone(),
                test_case.upload_store(),
                std::sync::Arc::new(NoopMultipart),
            );

            let scrubber = TagChecker::new(metadata_store.clone());
            scrubber.check(namespace, &mut executor).await.unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(blob_digest.clone()), false)
                .await;

            assert!(
                digest_link.is_ok(),
                "scrub_tags should create missing digest links"
            );
            test_case.cleanup().await;
        }
    }
}
