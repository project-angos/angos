use std::sync::Arc;

use tracing::{debug, error, info};

use crate::{
    oci::Digest,
    registry::{
        Error,
        metadata_store::{MetadataStore, MetadataStoreExt, link_kind::LinkKind},
    },
};

/// Backfills missing `descriptor` metadata on referrer links.
///
/// Pre-descriptor referrer links force `list_referrers` to fall back to a full
/// manifest GET every time they are listed. Walking them once during scrub and
/// persisting the computed descriptor eliminates the fallback on subsequent
/// lookups. `list_referrers` already performs the manifest read and exposes the
/// resulting `Descriptor`, so the checker only needs to read each referrer link
/// back to see whether the descriptor is already stored.
pub struct DescriptorChecker {
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    dry_run: bool,
}

impl DescriptorChecker {
    pub fn new(metadata_store: Arc<dyn MetadataStore + Send + Sync>, dry_run: bool) -> Self {
        Self {
            metadata_store,
            dry_run,
        }
    }

    pub async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking referrer descriptors for namespace '{namespace}'");

        let mut marker = None;
        loop {
            let (revisions, next_marker) = self
                .metadata_store
                .list_revisions(namespace, 100, marker)
                .await?;

            for subject in &revisions {
                if let Err(e) = self.backfill_subject(namespace, subject).await {
                    error!(
                        "Failed to backfill referrer descriptors for '{namespace}' (subject '{subject}'): {e}"
                    );
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        Ok(())
    }

    async fn backfill_subject(&self, namespace: &str, subject: &Digest) -> Result<(), Error> {
        if !self
            .metadata_store
            .has_referrers(namespace, subject)
            .await?
        {
            return Ok(());
        }

        // `list_referrers` computes the descriptor from the manifest blob when
        // the link is missing it; we reuse that computed value and only persist
        // it when the stored link still has no descriptor.
        let descriptors = self
            .metadata_store
            .list_referrers(namespace, subject, None)
            .await?;

        for descriptor in descriptors {
            let link = LinkKind::Referrer(subject.clone(), descriptor.digest.clone());
            let metadata = match self.metadata_store.read_link(namespace, &link, false).await {
                Ok(metadata) => metadata,
                Err(e) => {
                    debug!("Skipping referrer link {link}: {e}");
                    continue;
                }
            };

            if metadata.descriptor.is_some() {
                continue;
            }

            if self.dry_run {
                info!("DRY RUN: would backfill descriptor on referrer link '{namespace}':{link}");
                continue;
            }

            info!("Backfilling descriptor on referrer link '{namespace}':{link}");
            let target = descriptor.digest.clone();
            let mut tx = self.metadata_store.begin_transaction(namespace);
            tx.create_link_with_descriptor(&link, &target, descriptor);
            tx.commit().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{metadata_store::MetadataStoreExt, test_utils, tests::backends};

    #[tokio::test]
    async fn test_descriptor_checker_backfills_missing_descriptor() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (subject_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"subject manifest").await;

            let referrer_body = format!(
                r#"{{
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "subject": {{
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": "{subject_digest}",
                        "size": 123
                    }},
                    "artifactType": "application/vnd.example.test",
                    "config": {{
                        "mediaType": "application/vnd.oci.image.config.v1+json",
                        "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "size": 10
                    }},
                    "layers": []
                }}"#
            );
            let referrer_digest = blob_store
                .create_blob(referrer_body.as_bytes())
                .await
                .unwrap();

            // Create revisions for the subject and the referrer, plus the
            // Referrer link with no descriptor (the legacy shape).
            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(subject_digest.clone()), &subject_digest);
            tx.create_link(&LinkKind::Digest(referrer_digest.clone()), &referrer_digest);
            let referrer_link = LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone());
            tx.create_link(&referrer_link, &referrer_digest);
            tx.commit().await.unwrap();

            let before = metadata_store
                .read_link(namespace, &referrer_link, false)
                .await
                .unwrap();
            assert!(
                before.descriptor.is_none(),
                "pre-condition: referrer link starts without descriptor"
            );

            let checker = DescriptorChecker::new(metadata_store.clone(), false);
            checker.check_namespace(namespace).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &referrer_link, false)
                .await
                .unwrap();
            let descriptor = after
                .descriptor
                .expect("scrub should have backfilled descriptor");
            assert_eq!(descriptor.digest, referrer_digest);
            assert_eq!(
                descriptor.artifact_type.as_deref(),
                Some("application/vnd.example.test")
            );
        }
    }

    #[tokio::test]
    async fn test_descriptor_checker_dry_run_leaves_link_untouched() {
        for test_case in backends() {
            let namespace = "test-repo/dry";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let (subject_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"subject manifest dry").await;

            let referrer_body = format!(
                r#"{{
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "subject": {{
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": "{subject_digest}",
                        "size": 123
                    }},
                    "artifactType": "application/vnd.example.dry",
                    "config": {{
                        "mediaType": "application/vnd.oci.image.config.v1+json",
                        "digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                        "size": 10
                    }},
                    "layers": []
                }}"#
            );
            let referrer_digest = blob_store
                .create_blob(referrer_body.as_bytes())
                .await
                .unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(subject_digest.clone()), &subject_digest);
            tx.create_link(&LinkKind::Digest(referrer_digest.clone()), &referrer_digest);
            let referrer_link = LinkKind::Referrer(subject_digest.clone(), referrer_digest.clone());
            tx.create_link(&referrer_link, &referrer_digest);
            tx.commit().await.unwrap();

            let checker = DescriptorChecker::new(metadata_store.clone(), true);
            checker.check_namespace(namespace).await.unwrap();

            let after = metadata_store
                .read_link(namespace, &referrer_link, false)
                .await
                .unwrap();
            assert!(
                after.descriptor.is_none(),
                "dry-run must not mutate the link"
            );
        }
    }
}
