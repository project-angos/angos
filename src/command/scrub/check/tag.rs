use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, ensure_link, list_all},
        error::Error,
        executor::ActionSink,
    },
    registry::{
        blob_store,
        metadata_store::{MetadataStore, link_kind::LinkKind},
    },
};

pub struct TagChecker {
    blob_store: Arc<blob_store::BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl TagChecker {
    pub fn new(blob_store: Arc<blob_store::BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
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

        match self.blob_store.size(&tag_metadata.target).await {
            Ok(_) => {
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
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                warn!(
                    "Tag '{namespace}:{tag}' targets missing blob '{}'; removing",
                    tag_metadata.target
                );
                sink.apply(Action::DeleteOrphanManifest {
                    namespace: namespace.to_string(),
                    digest: tag_metadata.target,
                })
                .await
            }
            Err(e) => Err(e.into()),
        }
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
        command::scrub::{action::Action, executor::Executor},
        oci::Namespace,
        registry::{
            metadata_store::{LinkOperation, link_kind::LinkKind},
            test_utils::{self, backends, put_blob_direct},
        },
    };

    #[tokio::test]
    async fn test_scrub_tags_creates_missing_digest_links() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let tag_name = "v1.0.0";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

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

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            let scrubber = TagChecker::new(blob_store.clone(), metadata_store.clone());
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

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            let scrubber = TagChecker::new(blob_store.clone(), metadata_store.clone());
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

    #[tokio::test]
    async fn tag_checker_emits_delete_orphan_manifest_when_blob_missing() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (blob_digest, _) = test_utils::create_test_blob(
                registry,
                namespace,
                b"manifest for missing-blob test",
            )
            .await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag("dangling".to_string()),
                        blob_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&blob_digest).await.unwrap();

            let mut executor = Executor::new(blob_store.clone(), metadata_store.clone());

            let scrubber = TagChecker::new(blob_store.clone(), metadata_store.clone());
            scrubber.check(namespace, &mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag("dangling".to_string()), false)
                    .await
                    .is_err(),
                "tag link must be removed when target blob is missing"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(blob_digest.clone()), false)
                    .await
                    .is_err(),
                "digest revision link must be removed when target blob is missing"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn tag_checker_dry_run_captures_delete_orphan_manifest_action() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            // Write the blob and manually create digest + tag links so both are present.
            let blob_digest =
                put_blob_direct(metadata_store.store(), b"manifest for dry-run test").await;
            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::create(
                            LinkKind::Digest(blob_digest.clone()),
                            blob_digest.clone(),
                        ),
                        LinkOperation::create(
                            LinkKind::Tag("dangling-dry".to_string()),
                            blob_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&blob_digest).await.unwrap();

            let scrubber = TagChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            scrubber.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::DeleteOrphanManifest { .. })),
                "Vec sink must capture DeleteOrphanManifest action"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag("dangling-dry".to_string()), false)
                    .await
                    .is_ok(),
                "tag link must not be touched under Vec sink"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(blob_digest.clone()), false)
                    .await
                    .is_ok(),
                "digest link must not be touched under Vec sink"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn tag_checker_does_not_emit_delete_when_blob_present() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"healthy manifest blob").await;

            metadata_store
                .update_links(
                    namespace,
                    &[
                        LinkOperation::delete(LinkKind::Digest(blob_digest.clone())),
                        LinkOperation::create(
                            LinkKind::Tag("present".to_string()),
                            blob_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let scrubber = TagChecker::new(blob_store.clone(), metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            scrubber.check(namespace, &mut sink).await.unwrap();

            assert!(
                !sink
                    .iter()
                    .any(|a| matches!(a, Action::DeleteOrphanManifest { .. })),
                "DeleteOrphanManifest must not be emitted when the blob is present"
            );
            test_case.cleanup().await;
        }
    }
}
