use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::Digest,
    registry::metadata_store::{self, LinkKind, MetadataStore},
};

pub struct ReferrerChecker {
    metadata_store: Arc<MetadataStore>,
}

impl ReferrerChecker {
    pub fn new(metadata_store: Arc<MetadataStore>) -> Self {
        Self { metadata_store }
    }

    async fn check_referrers_for_revision(
        &self,
        namespace: &str,
        subject: &Digest,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let referrers = self
            .metadata_store
            .list_referrers(namespace, subject, None)
            .await?;

        for descriptor in referrers {
            let referrer = descriptor.digest.clone();
            // list_referrers filters out entries with no cached descriptor and no readable blob,
            // so a Referrer link with neither cached descriptor nor blob is not detected here.
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(referrer.clone()))
                .await
            {
                Ok(_) => {}
                Err(metadata_store::Error::ReferenceNotFound) => {
                    sink.apply(Action::DeleteOrphanReferrer {
                        namespace: namespace.to_string(),
                        subject: subject.clone(),
                        referrer,
                    })
                    .await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for ReferrerChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking referrer links for namespace '{namespace}'");

        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(revision) = revisions.next().await {
            let revision = revision?;
            if let Err(e) = self
                .check_referrers_for_revision(namespace, &revision, sink)
                .await
            {
                error!("Failed to check referrers for '{namespace}' (revision '{revision}'): {e}");
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
        oci::{Descriptor, Digest, Namespace},
        registry::{
            metadata_store::{LinkKind, LinkOperation},
            test_utils::{backends, put_blob_direct},
        },
    };

    async fn push_referrer_link(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        subject: &Digest,
    ) -> Digest {
        let manifest_content = format!(
            r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {{
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": "sha256:44136fa355ba77b9ad7b35f047c4b8dd53b4f7e3e6a86e6f39e0b02ac8a4d2c8",
                "size": 2
            }},
            "layers": [],
            "subject": {{
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "{subject}",
                "size": 100
            }}
        }}"#
        );

        let manifest_bytes = manifest_content.as_bytes();
        let manifest_digest = put_blob_direct(metadata_store.store(), manifest_bytes).await;

        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(
                        LinkKind::Digest(manifest_digest.clone()),
                        manifest_digest.clone(),
                    ),
                    LinkOperation::create_with_descriptor(
                        LinkKind::Referrer(subject.clone(), manifest_digest.clone()),
                        manifest_digest.clone(),
                        Box::new(Descriptor {
                            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
                            digest: manifest_digest.clone(),
                            size: manifest_bytes.len() as u64,
                            annotations: std::collections::HashMap::new(),
                            artifact_type: None,
                            platform: None,
                        }),
                    ),
                ],
            )
            .await
            .unwrap();

        manifest_digest
    }

    #[tokio::test]
    async fn referrer_checker_emits_delete_for_referrer_with_no_revision_link() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/referrer-orphan").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let subject_digest = put_blob_direct(metadata_store.store(), b"subject manifest").await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(subject_digest.clone()),
                        subject_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let referrer_digest =
                push_referrer_link(&metadata_store, namespace, &subject_digest).await;

            // Verify the Referrer link exists.
            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone())
                    )
                    .await
                    .is_ok(),
                "Referrer link must exist before the test"
            );

            // Simulate orphan state: remove A's revision link.
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::delete(LinkKind::Digest(
                        referrer_digest.clone(),
                    ))],
                )
                .await
                .unwrap();

            let mut executor = Executor::new_for_test(blob_store.clone(), metadata_store.clone());

            let checker = ReferrerChecker::new(metadata_store.clone());
            checker.check(namespace, &mut executor).await.unwrap();

            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone())
                    )
                    .await
                    .is_err(),
                "Referrer link must be removed after checker runs"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn referrer_checker_no_action_when_referrer_revision_still_exists() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/referrer-present").unwrap();
            let metadata_store = test_case.metadata_store();

            let subject_digest =
                put_blob_direct(metadata_store.store(), b"subject manifest v2").await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(subject_digest.clone()),
                        subject_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let _referrer_digest =
                push_referrer_link(&metadata_store, namespace, &subject_digest).await;

            let checker = ReferrerChecker::new(metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert!(
                sink.is_empty(),
                "No action must be emitted when referrer revision link still exists"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn referrer_checker_dry_run_captures_action_without_mutation() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/referrer-dryrun").unwrap();
            let metadata_store = test_case.metadata_store();

            let subject_digest =
                put_blob_direct(metadata_store.store(), b"subject manifest dry").await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(subject_digest.clone()),
                        subject_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let referrer_digest =
                push_referrer_link(&metadata_store, namespace, &subject_digest).await;

            // Create orphan state.
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::delete(LinkKind::Digest(
                        referrer_digest.clone(),
                    ))],
                )
                .await
                .unwrap();

            let checker = ReferrerChecker::new(metadata_store.clone());
            let mut sink: Vec<Action> = Vec::new();
            checker.check(namespace, &mut sink).await.unwrap();

            assert_eq!(sink.len(), 1, "Exactly one action must be captured");
            assert!(
                matches!(
                    &sink[0],
                    Action::DeleteOrphanReferrer {
                        namespace: ns,
                        subject: s,
                        referrer: r,
                    } if ns == namespace.as_ref()
                        && s == &subject_digest
                        && r == &referrer_digest
                ),
                "Captured action must be DeleteOrphanReferrer with correct fields"
            );

            // Referrer link must still exist because we used a Vec sink (no I/O).
            assert!(
                metadata_store
                    .read_link(
                        namespace,
                        &LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone())
                    )
                    .await
                    .is_ok(),
                "Referrer link must persist when using a Vec (dry-run) sink"
            );
            test_case.cleanup().await;
        }
    }
}
