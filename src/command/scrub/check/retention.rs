use std::{collections::HashMap, sync::Arc};

use futures_util::future::join_all;
use tracing::{debug, info};

use crate::{
    oci::Digest,
    policy::{ManifestImage, RetentionPolicy},
    registry::{
        Error,
        blob_store::BlobStore,
        metadata_store::{LinkMetadata, MetadataStore, MetadataStoreExt, link_kind::LinkKind},
        parse_manifest_digests,
        repository::Repository,
    },
};

struct TagWithMetadata {
    name: String,
    metadata: LinkMetadata,
}

pub struct RetentionChecker {
    blob_store: Arc<dyn BlobStore + Send + Sync>,
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    repositories: Arc<HashMap<String, Repository>>,
    global_retention_policy: Option<Arc<RetentionPolicy>>,
    dry_run: bool,
}

impl RetentionChecker {
    pub fn new(
        blob_store: Arc<dyn BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        repositories: Arc<HashMap<String, Repository>>,
        global_retention_policy: Option<Arc<RetentionPolicy>>,
        dry_run: bool,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            repositories,
            global_retention_policy,
            dry_run,
        }
    }

    pub async fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        debug!("Checking retention policies on '{namespace}'");

        let tag_names = self.fetch_all_tag_names(namespace).await?;
        let tag_metadata = self.fetch_tag_metadata(namespace, &tag_names).await?;
        let (last_pushed, last_pulled) = Self::build_sorted_rankings(&tag_metadata);

        self.delete_eligible_tags(namespace, &tag_metadata, &last_pushed, &last_pulled)
            .await?;

        self.delete_orphan_manifests(namespace, &last_pushed, &last_pulled)
            .await
    }

    async fn fetch_all_tag_names(&self, namespace: &str) -> Result<Vec<String>, Error> {
        let mut tag_names = Vec::new();
        let mut marker = None;

        loop {
            let (tags, next_marker) = self
                .metadata_store
                .list_tags(namespace, 1000, marker)
                .await?;
            tag_names.extend(tags);

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }

        Ok(tag_names)
    }

    async fn fetch_tag_metadata(
        &self,
        namespace: &str,
        tag_names: &[String],
    ) -> Result<Vec<TagWithMetadata>, Error> {
        const BATCH_SIZE: usize = 100;

        let mut all_tags = Vec::new();
        for chunk in tag_names.chunks(BATCH_SIZE) {
            let batch = self.fetch_metadata_batch(namespace, chunk).await?;
            all_tags.extend(batch);
        }
        Ok(all_tags)
    }

    async fn fetch_metadata_batch(
        &self,
        namespace: &str,
        tag_names: &[String],
    ) -> Result<Vec<TagWithMetadata>, Error> {
        let futures = tag_names.iter().map(|tag| {
            let namespace = namespace.to_string();
            let tag_name = tag.clone();
            let metadata_store = self.metadata_store.clone();
            async move {
                let metadata = metadata_store
                    .read_link(&namespace, &LinkKind::Tag(tag_name.clone()), false)
                    .await?;
                Ok::<TagWithMetadata, Error>(TagWithMetadata {
                    name: tag_name,
                    metadata,
                })
            }
        });

        let results = join_all(futures).await;
        results.into_iter().collect()
    }

    fn build_sorted_rankings(tags: &[TagWithMetadata]) -> (Vec<String>, Vec<String>) {
        let mut pushed_indices: Vec<usize> = (0..tags.len()).collect();
        pushed_indices.sort_by(|&a, &b| {
            tags[b]
                .metadata
                .created_at
                .cmp(&tags[a].metadata.created_at)
        });
        let last_pushed = pushed_indices
            .iter()
            .map(|&i| tags[i].name.clone())
            .collect();

        let mut pulled_indices: Vec<usize> = (0..tags.len()).collect();
        pulled_indices.sort_by(|&a, &b| {
            tags[b]
                .metadata
                .accessed_at
                .cmp(&tags[a].metadata.accessed_at)
        });
        let last_pulled = pulled_indices
            .iter()
            .map(|&i| tags[i].name.clone())
            .collect();

        (last_pushed, last_pulled)
    }

    async fn delete_eligible_tags(
        &self,
        namespace: &str,
        tags: &[TagWithMetadata],
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<(), Error> {
        let tags_to_delete: Vec<&str> = tags
            .iter()
            .filter(|tag| {
                !self
                    .should_retain_tag(namespace, tag, last_pushed, last_pulled)
                    .unwrap_or(true)
            })
            .map(|tag| tag.name.as_str())
            .collect();

        if tags_to_delete.is_empty() {
            return Ok(());
        }

        if self.dry_run {
            for tag in &tags_to_delete {
                info!("DRY RUN: would delete tag '{namespace}:{tag}' (policy)");
            }
            return Ok(());
        }

        let mut tx = self.metadata_store.begin_transaction(namespace);
        for tag in &tags_to_delete {
            info!("Deleting tag '{namespace}:{tag}' (policy)");
            tx.delete_link(&LinkKind::Tag((*tag).to_string()));
        }
        tx.commit().await?;
        Ok(())
    }

    fn should_retain_tag(
        &self,
        namespace: &str,
        tag: &TagWithMetadata,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<bool, Error> {
        debug!("'{namespace}': Checking tag '{}' for retention", tag.name);

        let manifest = ManifestImage {
            tag: Some(tag.name.clone()),
            pushed_at: tag
                .metadata
                .created_at
                .map(|t| t.timestamp())
                .unwrap_or_default(),
            last_pulled_at: tag
                .metadata
                .accessed_at
                .map(|t| t.timestamp())
                .unwrap_or_default(),
        };

        self.evaluate_retention_policies(namespace, &tag.name, &manifest, last_pushed, last_pulled)
    }

    fn find_repository_for_namespace(&self, namespace: &str) -> Option<&Repository> {
        self.repositories
            .iter()
            .find(|(repository_name, _)| {
                namespace == repository_name.as_str()
                    || namespace.starts_with(&format!("{repository_name}/"))
            })
            .map(|(_, repository)| repository)
    }

    fn evaluate_retention_policies(
        &self,
        namespace: &str,
        tag: &str,
        manifest: &ManifestImage,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<bool, Error> {
        let has_global_policy = self.global_retention_policy.is_some();
        let repository = self.find_repository_for_namespace(namespace);
        let has_repo_policy = repository.is_some_and(|r| r.retention_policy.has_rules());

        if !has_global_policy && !has_repo_policy {
            debug!("No retention policies defined, keeping {namespace}:{tag} by default");
            return Ok(true);
        }

        if let Some(global_policy) = &self.global_retention_policy {
            debug!("Evaluating global retention policy for {namespace}:{tag}");
            if global_policy.should_retain(manifest, last_pushed, last_pulled)? {
                debug!("Global retention policy says to retain {namespace}:{tag}");
                return Ok(true);
            }
        }

        if let Some(repo) = repository
            && repo.retention_policy.has_rules()
        {
            debug!("Evaluating repository retention policy for {namespace}:{tag}");
            if repo
                .retention_policy
                .should_retain(manifest, last_pushed, last_pulled)?
            {
                debug!("Repository retention policy says to retain {namespace}:{tag}");
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn delete_orphan_manifests(
        &self,
        namespace: &str,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<(), Error> {
        let mut marker = None;
        loop {
            let (revisions, next_marker) = self
                .metadata_store
                .list_revisions(namespace, 100, marker)
                .await?;

            for digest in &revisions {
                if self.is_protected(namespace, digest).await? {
                    debug!("Skipping protected manifest '{namespace}@{digest}'");
                    continue;
                }

                if self.has_tags(namespace, digest).await? {
                    continue;
                }

                let Ok(metadata) = self
                    .metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                    .await
                else {
                    continue;
                };

                let manifest = ManifestImage {
                    tag: None,
                    pushed_at: metadata
                        .created_at
                        .map(|t| t.timestamp())
                        .unwrap_or_default(),
                    last_pulled_at: metadata
                        .accessed_at
                        .map(|t| t.timestamp())
                        .unwrap_or_default(),
                };

                let label = format!("{namespace}@{digest}");
                if !self.evaluate_retention_policies(
                    namespace,
                    &label,
                    &manifest,
                    last_pushed,
                    last_pulled,
                )? {
                    self.delete_manifest(namespace, digest).await?;
                }
            }

            if next_marker.is_none() {
                break;
            }
            marker = next_marker;
        }
        Ok(())
    }

    async fn is_protected(&self, namespace: &str, digest: &Digest) -> Result<bool, Error> {
        // Index child manifests are protected
        if let Ok(blob_index) = self.metadata_store.read_blob_index(digest).await
            && let Some(refs) = blob_index.namespace.get(namespace)
        {
            for link in refs {
                if matches!(link, LinkKind::Manifest(_, _)) {
                    return Ok(true);
                }
            }
        }

        // Referrer subjects are protected
        if self.metadata_store.has_referrers(namespace, digest).await? {
            return Ok(true);
        }

        Ok(false)
    }

    async fn has_tags(&self, namespace: &str, digest: &Digest) -> Result<bool, Error> {
        if let Ok(blob_index) = self.metadata_store.read_blob_index(digest).await
            && let Some(refs) = blob_index.namespace.get(namespace)
        {
            for link in refs {
                if matches!(link, LinkKind::Tag(_)) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn delete_manifest(&self, namespace: &str, digest: &Digest) -> Result<(), Error> {
        if self.dry_run {
            info!("DRY RUN: would delete orphan manifest '{namespace}@{digest}' (policy)");
            return Ok(());
        }

        info!("Deleting orphan manifest '{namespace}@{digest}' (policy)");

        let content = self.blob_store.read_blob(digest).await?;
        let manifest = parse_manifest_digests(&content, None)?;

        let mut tx = self.metadata_store.begin_transaction(namespace);

        if let Some(config) = &manifest.config {
            tx.delete_link(&LinkKind::Config(config.clone()));
        }

        for layer in &manifest.layers {
            tx.delete_link(&LinkKind::Layer(layer.clone()));
        }

        for child in &manifest.manifests {
            tx.delete_link(&LinkKind::Manifest(digest.clone(), child.clone()));
        }

        if let Some(subject) = &manifest.subject {
            tx.delete_link(&LinkKind::Referrer(subject.clone(), digest.clone()));
        }

        tx.delete_link(&LinkKind::Digest(digest.clone()));

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{RetentionPolicy, RetentionPolicyConfig},
        registry::{test_utils, tests::backends},
    };

    const TEST_MANIFEST: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;

    const TEST_INDEX: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:1fc08b525237c75b560cf0b8ab766fc363d4e5ff1537f4f3ae28a49ade78938b","size":0}]}"#;

    #[tokio::test]
    async fn test_enforce_retention_with_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Tag("v1.0.0".to_string()), &blob_digest);
            tx.commit().await.unwrap();

            let retention_config = RetentionPolicyConfig {
                rules: vec!["top_pushed(10)".to_string()],
            };

            let retention_policy = Arc::new(RetentionPolicy::new(&retention_config).unwrap());

            let repositories = test_utils::create_test_repositories();
            let scrubber = RetentionChecker::new(
                blob_store,
                metadata_store.clone(),
                repositories,
                Some(retention_policy),
                false,
            );

            scrubber.check_namespace(namespace).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("v1.0.0".to_string()), false)
                .await;

            assert!(
                tag_link.is_ok(),
                "enforce_retention should keep tags matching the top 10 policy"
            );
        }
    }

    #[tokio::test]
    async fn test_enforce_retention_no_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Tag("any-tag".to_string()), &blob_digest);
            tx.commit().await.unwrap();

            let repositories = test_utils::create_test_repositories();
            let scrubber = RetentionChecker::new(
                blob_store,
                metadata_store.clone(),
                repositories,
                None,
                false,
            );

            scrubber.check_namespace(namespace).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("any-tag".to_string()), false)
                .await;

            assert!(
                tag_link.is_ok(),
                "enforce_retention without policy should keep all tags"
            );
        }
    }

    #[tokio::test]
    async fn test_orphan_manifest_deleted_with_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let digest = blob_store.create_blob(TEST_MANIFEST).await.unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest);
            tx.commit().await.unwrap();

            let policy = Arc::new(
                RetentionPolicy::new(&RetentionPolicyConfig {
                    rules: vec!["image.tag != null".to_string()],
                })
                .unwrap(),
            );

            RetentionChecker::new(
                blob_store,
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                Some(policy),
                false,
            )
            .check_namespace(namespace)
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest), false)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn test_orphan_manifest_kept_without_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let digest = blob_store.create_blob(TEST_MANIFEST).await.unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest);
            tx.commit().await.unwrap();

            RetentionChecker::new(
                blob_store,
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                None,
                false,
            )
            .check_namespace(namespace)
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest), false)
                    .await
                    .is_ok()
            );
        }
    }

    #[tokio::test]
    async fn test_index_child_manifest_protected() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let child_digest = blob_store.create_blob(TEST_MANIFEST).await.unwrap();
            let index_digest = blob_store.create_blob(TEST_INDEX).await.unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(child_digest.clone()), &child_digest);
            tx.create_link(&LinkKind::Digest(index_digest.clone()), &index_digest);
            tx.create_link(&LinkKind::Tag("latest".to_string()), &index_digest);
            tx.create_link(
                &LinkKind::Manifest(index_digest.clone(), child_digest.clone()),
                &child_digest,
            );
            tx.commit().await.unwrap();

            let policy = Arc::new(
                RetentionPolicy::new(&RetentionPolicyConfig {
                    rules: vec!["image.tag != null".to_string()],
                })
                .unwrap(),
            );

            RetentionChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                Some(policy.clone()),
                false,
            )
            .check_namespace(namespace)
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(child_digest.clone()), false)
                    .await
                    .is_ok()
            );

            // Remove the index manifest - child is no longer protected
            let mut tx = metadata_store.begin_transaction(namespace);
            tx.delete_link(&LinkKind::Tag("latest".to_string()));
            tx.delete_link(&LinkKind::Manifest(
                index_digest.clone(),
                child_digest.clone(),
            ));
            tx.delete_link(&LinkKind::Digest(index_digest));
            tx.commit().await.unwrap();

            RetentionChecker::new(
                blob_store,
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                Some(policy),
                false,
            )
            .check_namespace(namespace)
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(child_digest), false)
                    .await
                    .is_err()
            );
        }
    }
}
