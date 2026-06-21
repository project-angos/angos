use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use crate::{
    command::scrub::{
        check::{TagChecker, ensure_link, orphan_on_missing_manifest},
        error::Error,
        executor::ActionSink,
    },
    oci::{Namespace, Tag},
    registry::{
        blob_store::BlobStore,
        metadata_store::{LinkKind, MetadataStore},
    },
};

/// Repairs a single tag's digest revision link: it recreates a missing digest
/// link when the target blob is present, and emits `DeleteOrphanManifest` when
/// the target blob is missing.
pub struct DigestLinkChecker {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl DigestLinkChecker {
    pub fn new(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }
}

#[async_trait]
impl TagChecker for DigestLinkChecker {
    async fn check_tag(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking digest link for tag '{namespace}:{tag}'");
        let target = self
            .metadata_store
            .read_link(namespace, &LinkKind::Tag(tag.clone()))
            .await?
            .target;

        // Existence is enough here, so probe with `size`; a missing manifest
        // blob makes the tag's revision an orphan.
        let probe = self.blob_store.size(&target).await;
        if orphan_on_missing_manifest(probe, namespace, &target, sink)
            .await?
            .is_none()
        {
            return Ok(());
        }

        let digest_link = LinkKind::Digest(target.clone());
        ensure_link(&self.metadata_store, namespace, &digest_link, &target, sink).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        oci::Namespace,
        registry::{
            metadata_store::{LinkKind, LinkOperation},
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
                        LinkKind::Tag(Tag::new(tag_name).unwrap()),
                        blob_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let scrubber = DigestLinkChecker::new(blob_store.clone(), metadata_store.clone());
            let tag = Tag::new(tag_name).unwrap();
            scrubber
                .check_tag(namespace, &tag, &mut executor)
                .await
                .unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(blob_digest.clone()))
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
                            LinkKind::Tag(Tag::new("v1.0.0").unwrap()),
                            blob_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let scrubber = DigestLinkChecker::new(blob_store.clone(), metadata_store.clone());
            let tag = Tag::new("v1.0.0").unwrap();
            scrubber
                .check_tag(namespace, &tag, &mut executor)
                .await
                .unwrap();

            let digest_link = metadata_store
                .read_link(namespace, &LinkKind::Digest(blob_digest.clone()))
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
                        LinkKind::Tag(Tag::new("dangling").unwrap()),
                        blob_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&blob_digest).await.unwrap();

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let scrubber = DigestLinkChecker::new(blob_store.clone(), metadata_store.clone());
            let tag = Tag::new("dangling").unwrap();
            scrubber
                .check_tag(namespace, &tag, &mut executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag(Tag::new("dangling").unwrap()))
                    .await
                    .is_err(),
                "tag link must be removed when target blob is missing"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(blob_digest.clone()))
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
                            LinkKind::Tag(Tag::new("dangling-dry").unwrap()),
                            blob_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            blob_store.delete_blob(&blob_digest).await.unwrap();

            let scrubber = DigestLinkChecker::new(blob_store.clone(), metadata_store.clone());
            let tag = Tag::new("dangling-dry").unwrap();
            let mut sink: Vec<Action> = Vec::new();
            scrubber
                .check_tag(namespace, &tag, &mut sink)
                .await
                .unwrap();

            assert!(
                sink.iter()
                    .any(|a| matches!(a, Action::DeleteOrphanManifest { .. })),
                "Vec sink must capture DeleteOrphanManifest action"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Tag(Tag::new("dangling-dry").unwrap()))
                    .await
                    .is_ok(),
                "tag link must not be touched under Vec sink"
            );
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(blob_digest.clone()))
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
                            LinkKind::Tag(Tag::new("present").unwrap()),
                            blob_digest.clone(),
                        ),
                    ],
                )
                .await
                .unwrap();

            let scrubber = DigestLinkChecker::new(blob_store.clone(), metadata_store.clone());
            let tag = Tag::new("present").unwrap();
            let mut sink: Vec<Action> = Vec::new();
            scrubber
                .check_tag(namespace, &tag, &mut sink)
                .await
                .unwrap();

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
