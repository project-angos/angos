use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt, future::join_all};
use tracing::debug;

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::{Digest, namespace_belongs_to},
    policy::{EpochSeconds, ManifestImage, RetentionPolicy},
    registry::{
        metadata_store::{BlobIndex, LinkMetadata, MetadataStore, link_kind::LinkKind},
        pagination::collect_all_pages,
        repository::Repository,
    },
};

struct TagWithMetadata {
    name: String,
    metadata: LinkMetadata,
}

pub struct RetentionChecker {
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    repositories: Arc<HashMap<String, Repository>>,
    global_retention_policy: Option<Arc<RetentionPolicy>>,
}

fn has_link_kind(
    blob_index: &BlobIndex,
    namespace: &str,
    predicate: impl Fn(&LinkKind) -> bool,
) -> bool {
    blob_index
        .namespace
        .get(namespace)
        .is_some_and(|refs| refs.iter().any(predicate))
}

async fn fetch_single_tag_metadata(
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    namespace: String,
    tag_name: String,
) -> Result<TagWithMetadata, Error> {
    let metadata = metadata_store
        .read_link(&namespace, &LinkKind::Tag(tag_name.clone()), false)
        .await?;
    Ok(TagWithMetadata {
        name: tag_name,
        metadata,
    })
}

impl RetentionChecker {
    pub fn new(
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        repositories: Arc<HashMap<String, Repository>>,
        global_retention_policy: Option<Arc<RetentionPolicy>>,
    ) -> Self {
        Self {
            metadata_store,
            repositories,
            global_retention_policy,
        }
    }
}

#[async_trait]
impl NamespaceChecker for RetentionChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        debug!("Checking retention policies on '{namespace}'");

        let tag_names = collect_all_pages(|marker| async move {
            self.metadata_store.list_tags(namespace, 1000, marker).await
        })
        .await
        .map_err(Error::from)?;

        let tag_metadata = self.fetch_tag_metadata(namespace, &tag_names).await?;
        let (last_pushed, last_pulled) = Self::build_sorted_rankings(&tag_metadata);

        let tags = self.get_deletable_tags(namespace, &tag_metadata, &last_pushed, &last_pulled);
        self.emit_delete_tags(namespace, &tags, sink).await?;

        self.emit_delete_orphan_manifests(namespace, &last_pushed, &last_pulled, sink)
            .await
    }
}

impl RetentionChecker {
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
        join_all(tag_names.iter().map(|tag| {
            fetch_single_tag_metadata(
                self.metadata_store.clone(),
                namespace.to_string(),
                tag.clone(),
            )
        }))
        .await
        .into_iter()
        .collect()
    }

    fn build_sorted_rankings(tags: &[TagWithMetadata]) -> (Vec<String>, Vec<String>) {
        let last_pushed = Self::rank_by(tags, |m| m.created_at);
        let last_pulled = Self::rank_by(tags, |m| m.accessed_at);
        (last_pushed, last_pulled)
    }

    fn rank_by<K: Ord>(tags: &[TagWithMetadata], key: impl Fn(&LinkMetadata) -> K) -> Vec<String> {
        let mut indices: Vec<usize> = (0..tags.len()).collect();
        indices.sort_by_cached_key(|&i| std::cmp::Reverse(key(&tags[i].metadata)));
        indices.iter().map(|&i| tags[i].name.clone()).collect()
    }

    fn get_deletable_tags<'a>(
        &self,
        namespace: &str,
        tags: &'a [TagWithMetadata],
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Vec<&'a str> {
        tags.iter()
            .filter(|tag| {
                !self
                    .should_retain_tag(namespace, tag, last_pushed, last_pulled)
                    .unwrap_or(true)
            })
            .map(|tag| tag.name.as_str())
            .collect()
    }

    async fn emit_delete_tags(
        &self,
        namespace: &str,
        tags_to_delete: &[&str],
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        for tag in tags_to_delete {
            sink.apply(Action::DeleteTag {
                namespace: namespace.to_string(),
                tag: (*tag).to_string(),
            })
            .await?;
        }
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
            pushed_at: EpochSeconds::from_seconds(
                tag.metadata.created_at.map_or(0, |t| t.timestamp()),
            ),
            last_pulled_at: EpochSeconds::from_seconds(
                tag.metadata.accessed_at.map_or(0, |t| t.timestamp()),
            ),
        };

        self.evaluate_retention_policies(namespace, &tag.name, &manifest, last_pushed, last_pulled)
    }

    fn find_repository_for_namespace(&self, namespace: &str) -> Option<&Repository> {
        self.repositories
            .iter()
            .find(|(repository_name, _)| namespace_belongs_to(namespace, repository_name))
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

    async fn emit_delete_orphan_manifests(
        &self,
        namespace: &str,
        last_pushed: &[String],
        last_pulled: &[String],
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let mut revisions = list_all::revisions(&self.metadata_store, namespace);
        while let Some(digest) = revisions.next().await {
            let digest = digest?;
            self.process_orphan_revision(namespace, &digest, last_pushed, last_pulled, sink)
                .await?;
        }

        Ok(())
    }

    async fn process_orphan_revision(
        &self,
        namespace: &str,
        digest: &Digest,
        last_pushed: &[String],
        last_pulled: &[String],
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        if self.is_protected(namespace, digest).await? {
            debug!("Skipping protected manifest '{namespace}@{digest}'");
            return Ok(());
        }

        if self.has_tags(namespace, digest).await? {
            return Ok(());
        }

        let Ok(metadata) = self
            .metadata_store
            .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
            .await
        else {
            return Ok(());
        };

        let manifest = ManifestImage {
            tag: None,
            pushed_at: EpochSeconds::from_seconds(metadata.created_at.map_or(0, |t| t.timestamp())),
            last_pulled_at: EpochSeconds::from_seconds(
                metadata.accessed_at.map_or(0, |t| t.timestamp()),
            ),
        };

        let label = format!("{namespace}@{digest}");
        if !self.evaluate_retention_policies(
            namespace,
            &label,
            &manifest,
            last_pushed,
            last_pulled,
        )? {
            sink.apply(Action::DeleteOrphanManifest {
                namespace: namespace.to_string(),
                digest: digest.clone(),
            })
            .await?;
        }

        Ok(())
    }

    async fn is_protected(&self, namespace: &str, digest: &Digest) -> Result<bool, Error> {
        if let Ok(blob_index) = self.metadata_store.read_blob_index(digest).await
            && has_link_kind(&blob_index, namespace, |link| {
                matches!(link, LinkKind::Manifest(_, _))
            })
        {
            return Ok(true);
        }

        if self.metadata_store.has_referrers(namespace, digest).await? {
            return Ok(true);
        }

        Ok(false)
    }

    async fn has_tags(&self, namespace: &str, digest: &Digest) -> Result<bool, Error> {
        if let Ok(blob_index) = self.metadata_store.read_blob_index(digest).await
            && has_link_kind(&blob_index, namespace, |link| {
                matches!(link, LinkKind::Tag(_))
            })
        {
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr};

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::{
        command::scrub::{action::Action, executor::Executor},
        oci::Digest,
        policy::{CelRule, RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            metadata_store::{
                LockStrategy, MetadataStoreExt,
                fs::{Backend as FsMetadataStore, BackendConfig as FsMetadataConfig},
            },
            test_utils::{self, NoopMultipart, backends},
        },
    };

    fn dummy_digest() -> Digest {
        Digest::from_str("sha256:0000000000000000000000000000000000000000000000000000000000000000")
            .unwrap()
    }

    fn tag_with_times(
        name: &str,
        created: Option<chrono::DateTime<Utc>>,
        accessed: Option<chrono::DateTime<Utc>>,
    ) -> TagWithMetadata {
        TagWithMetadata {
            name: name.to_string(),
            metadata: LinkMetadata {
                target: dummy_digest(),
                created_at: created,
                accessed_at: accessed,
                referenced_by: HashSet::default(),
                media_type: None,
                descriptor: None,
            },
        }
    }

    fn make_checker_with_repos(
        repositories: Arc<HashMap<String, crate::registry::Repository>>,
    ) -> RetentionChecker {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        std::mem::forget(dir);
        let metadata_store = Arc::new(
            FsMetadataStore::new(&FsMetadataConfig {
                root_dir: path,
                sync_to_disk: false,
                lock_strategy: LockStrategy::Memory,
            })
            .unwrap(),
        );
        RetentionChecker::new(metadata_store, repositories, None)
    }

    fn make_executor(
        blob_store: Arc<dyn crate::registry::blob_store::BlobStore + Send + Sync>,
        metadata_store: Arc<dyn MetadataStore + Send + Sync>,
        upload_store: Arc<dyn crate::registry::blob_store::UploadStore>,
    ) -> Executor {
        Executor::new(
            false,
            blob_store,
            metadata_store,
            upload_store,
            Arc::new(NoopMultipart),
        )
    }

    // --- rank_by ---

    #[test]
    fn rank_by_sorts_descending_by_key() {
        let t1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = vec![
            tag_with_times("oldest", Some(t1), None),
            tag_with_times("newest", Some(t3), None),
            tag_with_times("middle", Some(t2), None),
        ];

        let ranked = RetentionChecker::rank_by(&tags, |m| m.created_at);

        assert_eq!(ranked, vec!["newest", "middle", "oldest"]);
    }

    #[test]
    fn rank_by_empty_slice_returns_empty() {
        let ranked = RetentionChecker::rank_by(&[], |m| m.created_at);
        assert!(ranked.is_empty());
    }

    #[test]
    fn rank_by_all_none_timestamps_returns_all_names() {
        let tags = vec![
            tag_with_times("alpha", None, None),
            tag_with_times("beta", None, None),
        ];
        let ranked = RetentionChecker::rank_by(&tags, |m| m.created_at);
        assert_eq!(ranked.len(), 2);
        assert!(ranked.contains(&"alpha".to_string()));
        assert!(ranked.contains(&"beta".to_string()));
    }

    // --- build_sorted_rankings ---

    #[test]
    fn build_sorted_rankings_pushed_order() {
        let t1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = vec![
            tag_with_times("old", Some(t1), None),
            tag_with_times("new", Some(t3), None),
        ];

        let (last_pushed, last_pulled) = RetentionChecker::build_sorted_rankings(&tags);

        assert_eq!(last_pushed[0], "new");
        assert_eq!(last_pushed[1], "old");
        assert_eq!(last_pulled.len(), 2);
    }

    #[test]
    fn build_sorted_rankings_empty_input() {
        let (last_pushed, last_pulled) = RetentionChecker::build_sorted_rankings(&[]);
        assert!(last_pushed.is_empty());
        assert!(last_pulled.is_empty());
    }

    #[test]
    fn build_sorted_rankings_pulled_independent_of_pushed() {
        let pushed_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let pulled_time = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = vec![
            tag_with_times("a", Some(pulled_time), Some(pushed_time)),
            tag_with_times("b", Some(pushed_time), Some(pulled_time)),
        ];

        let (last_pushed, last_pulled) = RetentionChecker::build_sorted_rankings(&tags);

        assert_eq!(last_pushed[0], "a");
        assert_eq!(last_pulled[0], "b");
    }

    // --- find_repository_for_namespace ---

    #[test]
    fn find_repository_for_namespace_exact_match() {
        let repos = test_utils::create_test_repositories();
        let checker = make_checker_with_repos(repos);

        let found = checker.find_repository_for_namespace("test-repo");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "test-repo");
    }

    #[test]
    fn find_repository_for_namespace_prefix_match() {
        let repos = test_utils::create_test_repositories();
        let checker = make_checker_with_repos(repos);

        let found = checker.find_repository_for_namespace("test-repo/images/app");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "test-repo");
    }

    #[test]
    fn find_repository_for_namespace_unknown_returns_none() {
        let repos = test_utils::create_test_repositories();
        let checker = make_checker_with_repos(repos);

        let found = checker.find_repository_for_namespace("completely-different");
        assert!(found.is_none());
    }

    const TEST_MANIFEST: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;

    const TEST_INDEX: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:1fc08b525237c75b560cf0b8ab766fc363d4e5ff1537f4f3ae28a49ade78938b","size":0}]}"#;

    #[tokio::test]
    async fn test_enforce_retention_with_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Tag("v1.0.0".to_string()), &blob_digest)
                .add();
            tx.commit().await.unwrap();

            let retention_config = RetentionPolicyConfig {
                rules: vec![CelRule::compile("top_pushed(10)").unwrap()],
            };

            let retention_policy = Arc::new(RetentionPolicy::new(
                &retention_config,
                Arc::new(SystemClock),
            ));

            let repositories = test_utils::create_test_repositories();
            let scrubber =
                RetentionChecker::new(metadata_store.clone(), repositories, Some(retention_policy));

            let mut executor = make_executor(
                test_case.blob_store(),
                test_case.metadata_store(),
                test_case.upload_store(),
            );
            scrubber.check(namespace, &mut executor).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("v1.0.0".to_string()), false)
                .await;

            assert!(tag_link.is_ok());
        }
    }

    #[tokio::test]
    async fn test_enforce_retention_no_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Tag("any-tag".to_string()), &blob_digest)
                .add();
            tx.commit().await.unwrap();

            let repositories = test_utils::create_test_repositories();
            let scrubber = RetentionChecker::new(metadata_store.clone(), repositories, None);

            let mut executor = make_executor(
                test_case.blob_store(),
                test_case.metadata_store(),
                test_case.upload_store(),
            );
            scrubber.check(namespace, &mut executor).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("any-tag".to_string()), false)
                .await;

            assert!(tag_link.is_ok());
        }
    }

    #[tokio::test]
    async fn test_orphan_manifest_deleted_with_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            let digest = blob_store.create(TEST_MANIFEST).await.unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
                .add();
            tx.commit().await.unwrap();

            let policy = Arc::new(RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: vec![CelRule::compile("image.tag != null").unwrap()],
                },
                Arc::new(SystemClock),
            ));

            let mut executor = make_executor(
                test_case.blob_store(),
                test_case.metadata_store(),
                test_case.upload_store(),
            );
            RetentionChecker::new(
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                Some(policy),
            )
            .check(namespace, &mut executor)
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

            let digest = blob_store.create(TEST_MANIFEST).await.unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(digest.clone()), &digest)
                .add();
            tx.commit().await.unwrap();

            let mut executor = make_executor(
                test_case.blob_store(),
                test_case.metadata_store(),
                test_case.upload_store(),
            );
            RetentionChecker::new(
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                None,
            )
            .check(namespace, &mut executor)
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

            let child_digest = blob_store.create(TEST_MANIFEST).await.unwrap();
            let index_digest = blob_store.create(TEST_INDEX).await.unwrap();

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.create_link(&LinkKind::Digest(child_digest.clone()), &child_digest)
                .add();
            tx.create_link(&LinkKind::Digest(index_digest.clone()), &index_digest)
                .add();
            tx.create_link(&LinkKind::Tag("latest".to_string()), &index_digest)
                .add();
            tx.create_link(
                &LinkKind::Manifest(index_digest.clone(), child_digest.clone()),
                &child_digest,
            )
            .add();
            tx.commit().await.unwrap();

            let policy = Arc::new(RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: vec![CelRule::compile("image.tag != null").unwrap()],
                },
                Arc::new(SystemClock),
            ));

            let mut executor = make_executor(
                test_case.blob_store(),
                test_case.metadata_store(),
                test_case.upload_store(),
            );
            RetentionChecker::new(
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                Some(policy.clone()),
            )
            .check(namespace, &mut executor)
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(child_digest.clone()), false)
                    .await
                    .is_ok()
            );

            let mut tx = metadata_store.begin_transaction(namespace);
            tx.delete_link(&LinkKind::Tag("latest".to_string()));
            tx.delete_link(&LinkKind::Manifest(
                index_digest.clone(),
                child_digest.clone(),
            ));
            tx.delete_link(&LinkKind::Digest(index_digest));
            tx.commit().await.unwrap();

            let mut executor2 = make_executor(
                test_case.blob_store(),
                test_case.metadata_store(),
                test_case.upload_store(),
            );
            RetentionChecker::new(
                metadata_store.clone(),
                test_utils::create_test_repositories(),
                Some(policy),
            )
            .check(namespace, &mut executor2)
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

    #[tokio::test]
    async fn test_delete_tag_action_emitted_without_storage_mutation() {
        let test_case = backends().into_iter().next().unwrap();
        let namespace = "test-repo/app";
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();

        let (blob_digest, _) =
            test_utils::create_test_blob(registry, namespace, b"test manifest").await;

        let mut tx = metadata_store.begin_transaction(namespace);
        tx.create_link(&LinkKind::Tag("v0.0.1".to_string()), &blob_digest)
            .add();
        tx.commit().await.unwrap();

        let policy = Arc::new(RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        ));

        let scrubber = RetentionChecker::new(
            metadata_store.clone(),
            test_utils::create_test_repositories(),
            Some(policy),
        );

        let mut sink: Vec<Action> = Vec::new();
        scrubber.check(namespace, &mut sink).await.unwrap();

        assert!(
            sink.iter().any(|a| matches!(a, Action::DeleteTag { .. })),
            "Vec sink must capture a DeleteTag action"
        );

        assert!(
            metadata_store
                .read_link(namespace, &LinkKind::Tag("v0.0.1".to_string()), false)
                .await
                .is_ok(),
            "Vec sink must not delete the tag"
        );
    }
}
