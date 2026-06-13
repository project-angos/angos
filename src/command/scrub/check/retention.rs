use std::{cmp::Reverse, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, future::join_all};
use tracing::{debug, error};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::Digest,
    policy::{EpochSeconds, ManifestImage, RetentionPolicy},
    registry::{
        Repository,
        metadata_store::{BlobIndex, LinkMetadata, MetadataStore, link_kind::LinkKind},
        pagination::collect_all_pages,
        repository_resolver::RepositoryResolver,
    },
};

struct TagWithMetadata {
    name: String,
    metadata: LinkMetadata,
}

pub struct RetentionChecker {
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
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

fn to_epoch(timestamp: Option<DateTime<Utc>>) -> i64 {
    timestamp.map_or(0, |t| t.timestamp())
}

#[derive(Debug, PartialEq, Eq)]
enum PolicyDecision {
    Retain,
    Delete,
    NoOpinion,
}

fn check_global_policy(
    policy: Option<&RetentionPolicy>,
    manifest: &ManifestImage,
    last_pushed: &[String],
    last_pulled: &[String],
) -> Result<PolicyDecision, Error> {
    let Some(policy) = policy else {
        return Ok(PolicyDecision::NoOpinion);
    };
    if policy.should_retain(manifest, last_pushed, last_pulled)? {
        Ok(PolicyDecision::Retain)
    } else {
        Ok(PolicyDecision::Delete)
    }
}

fn check_repo_policy(
    repository: Option<&Repository>,
    manifest: &ManifestImage,
    last_pushed: &[String],
    last_pulled: &[String],
) -> Result<PolicyDecision, Error> {
    let Some(repo) = repository else {
        return Ok(PolicyDecision::NoOpinion);
    };
    if !repo.retention_policy.has_rules() {
        return Ok(PolicyDecision::NoOpinion);
    }
    if repo
        .retention_policy
        .should_retain(manifest, last_pushed, last_pulled)?
    {
        Ok(PolicyDecision::Retain)
    } else {
        Ok(PolicyDecision::Delete)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Fate {
    /// Manifest is protected by another reference, has tags, or its link
    /// metadata is missing: leave it alone.
    Skip,
    /// Retention policy says to keep this manifest.
    Retain,
    /// Retention policy says to delete this manifest.
    Delete,
}

/// Pure decision over pre-loaded inputs: should this orphan manifest be skipped,
/// retained, or deleted? The caller is responsible for fetching `is_protected`,
/// `has_tags`, and `metadata` and for resolving the relevant `Repository` /
/// global policy from the checker's state.
fn decide_orphan_fate(
    is_protected: bool,
    has_tags: bool,
    metadata: Option<&LinkMetadata>,
    repository: Option<&Repository>,
    global_policy: Option<&RetentionPolicy>,
    last_pushed: &[String],
    last_pulled: &[String],
) -> Result<Fate, Error> {
    if is_protected || has_tags {
        return Ok(Fate::Skip);
    }
    let Some(metadata) = metadata else {
        return Ok(Fate::Skip);
    };

    let manifest = ManifestImage {
        tag: None,
        pushed_at: EpochSeconds::from_seconds(to_epoch(metadata.created_at)),
        last_pulled_at: EpochSeconds::from_seconds(to_epoch(metadata.accessed_at)),
    };

    let global = check_global_policy(global_policy, &manifest, last_pushed, last_pulled)?;
    let repo = check_repo_policy(repository, &manifest, last_pushed, last_pulled)?;

    Ok(match (global, repo) {
        (PolicyDecision::Retain, _)
        | (_, PolicyDecision::Retain)
        | (PolicyDecision::NoOpinion, PolicyDecision::NoOpinion) => Fate::Retain,
        _ => Fate::Delete,
    })
}

impl RetentionChecker {
    pub fn new(
        metadata_store: Arc<MetadataStore>,
        resolver: Arc<RepositoryResolver>,
        global_retention_policy: Option<Arc<RetentionPolicy>>,
    ) -> Self {
        Self {
            metadata_store,
            resolver,
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
        join_all(
            tag_names
                .iter()
                .map(|tag| self.fetch_single_tag_metadata(namespace, tag.clone())),
        )
        .await
        .into_iter()
        .collect()
    }

    async fn fetch_single_tag_metadata(
        &self,
        namespace: &str,
        tag_name: String,
    ) -> Result<TagWithMetadata, Error> {
        let metadata = self
            .metadata_store
            .read_link(namespace, &LinkKind::Tag(tag_name.clone()), false)
            .await?;
        Ok(TagWithMetadata {
            name: tag_name,
            metadata,
        })
    }

    fn build_sorted_rankings(tags: &[TagWithMetadata]) -> (Vec<String>, Vec<String>) {
        let last_pushed = Self::rank_by(tags, |m| m.created_at);
        let last_pulled = Self::rank_by(tags, |m| m.accessed_at);
        (last_pushed, last_pulled)
    }

    fn rank_by<K: Ord>(tags: &[TagWithMetadata], key: impl Fn(&LinkMetadata) -> K) -> Vec<String> {
        let mut indices: Vec<usize> = (0..tags.len()).collect();
        indices.sort_by_cached_key(|&i| Reverse(key(&tags[i].metadata)));
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
            pushed_at: EpochSeconds::from_seconds(to_epoch(tag.metadata.created_at)),
            last_pulled_at: EpochSeconds::from_seconds(to_epoch(tag.metadata.accessed_at)),
        };

        self.evaluate_retention_policies(namespace, &tag.name, &manifest, last_pushed, last_pulled)
    }

    fn evaluate_retention_policies(
        &self,
        namespace: &str,
        tag: &str,
        manifest: &ManifestImage,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<bool, Error> {
        let global = check_global_policy(
            self.global_retention_policy.as_deref(),
            manifest,
            last_pushed,
            last_pulled,
        )?;
        let repo = check_repo_policy(
            self.resolver.resolve(namespace),
            manifest,
            last_pushed,
            last_pulled,
        )?;

        match (global, repo) {
            (PolicyDecision::Retain, _) => {
                debug!("Global retention policy says to retain {namespace}:{tag}");
                Ok(true)
            }
            (_, PolicyDecision::Retain) => {
                debug!("Repository retention policy says to retain {namespace}:{tag}");
                Ok(true)
            }
            (PolicyDecision::NoOpinion, PolicyDecision::NoOpinion) => {
                debug!("No retention policies defined, keeping {namespace}:{tag} by default");
                Ok(true)
            }
            _ => Ok(false),
        }
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
            if let Err(e) = self
                .process_orphan_revision(namespace, &digest, last_pushed, last_pulled, sink)
                .await
            {
                error!("Failed to check revision from '{namespace}' (revision '{digest}'): {e}");
            }
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
        let is_protected = self.is_protected(namespace, digest).await?;
        if is_protected {
            debug!("Skipping protected manifest '{namespace}@{digest}'");
        }

        let has_tags = !is_protected && self.has_tags(namespace, digest).await?;

        let metadata = if is_protected || has_tags {
            None
        } else {
            self.metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                .await
                .ok()
        };

        let fate = decide_orphan_fate(
            is_protected,
            has_tags,
            metadata.as_ref(),
            self.resolver.resolve(namespace),
            self.global_retention_policy.as_deref(),
            last_pushed,
            last_pulled,
        )?;

        self.apply_fate(namespace, digest, fate, sink).await
    }

    async fn apply_fate(
        &self,
        namespace: &str,
        digest: &Digest,
        fate: Fate,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        match fate {
            Fate::Skip | Fate::Retain => Ok(()),
            Fate::Delete => {
                sink.apply(Action::DeleteOrphanManifest {
                    namespace: namespace.to_string(),
                    digest: digest.clone(),
                })
                .await
            }
        }
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
        oci::{Digest, Namespace},
        policy::{CelRule, RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            blob_store,
            metadata_store::LinkOperation,
            repository_resolver::RepositoryResolver,
            test_utils::{self, backends, put_blob_direct},
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

    fn make_executor(
        blob_store: Arc<blob_store::BlobStore>,
        metadata_store: Arc<MetadataStore>,
    ) -> Executor {
        Executor::new_for_test(blob_store, metadata_store)
    }

    async fn setup_index_scenario(
        metadata_store: &Arc<MetadataStore>,
        namespace: &str,
        index_digest: &Digest,
        child_digest: &Digest,
    ) {
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(
                        LinkKind::Digest(child_digest.clone()),
                        child_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Digest(index_digest.clone()),
                        index_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Tag("latest".to_string()),
                        index_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Manifest(index_digest.clone(), child_digest.clone()),
                        child_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
    }

    async fn teardown_index_scenario(
        metadata_store: &Arc<MetadataStore>,
        namespace: &str,
        index_digest: Digest,
        child_digest: &Digest,
    ) {
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::delete(LinkKind::Tag("latest".to_string())),
                    LinkOperation::delete(LinkKind::Manifest(
                        index_digest.clone(),
                        child_digest.clone(),
                    )),
                    LinkOperation::delete(LinkKind::Digest(index_digest)),
                ],
            )
            .await
            .unwrap();
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

    const TEST_MANIFEST: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;

    const TEST_INDEX: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:1fc08b525237c75b560cf0b8ab766fc363d4e5ff1537f4f3ae28a49ade78938b","size":0}]}"#;

    #[tokio::test]
    async fn test_enforce_retention_with_policy() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag("v1.0.0".to_string()),
                        blob_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let retention_config = RetentionPolicyConfig {
                rules: vec![CelRule::compile("top_pushed(10)").unwrap()],
            };

            let retention_policy = Arc::new(RetentionPolicy::new(
                &retention_config,
                Arc::new(SystemClock),
            ));

            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            let scrubber =
                RetentionChecker::new(metadata_store.clone(), resolver, Some(retention_policy));

            let mut executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            scrubber.check(namespace, &mut executor).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("v1.0.0".to_string()), false)
                .await;

            assert!(tag_link.is_ok());
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_enforce_retention_no_policy() {
        for test_case in backends() {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag("any-tag".to_string()),
                        blob_digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            let scrubber = RetentionChecker::new(metadata_store.clone(), resolver, None);

            let mut executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            scrubber.check(namespace, &mut executor).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag("any-tag".to_string()), false)
                .await;

            assert!(tag_link.is_ok());
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_orphan_manifest_deleted_with_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let metadata_store = test_case.metadata_store();

            let digest = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let policy = Arc::new(RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: vec![CelRule::compile("image.tag != null").unwrap()],
                },
                Arc::new(SystemClock),
            ));

            let mut executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, Some(policy))
                .check(namespace, &mut executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest), false)
                    .await
                    .is_err()
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_orphan_manifest_kept_without_policy() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let metadata_store = test_case.metadata_store();

            let digest = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, None)
                .check(namespace, &mut executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest), false)
                    .await
                    .is_ok()
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_index_child_manifest_protected() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let metadata_store = test_case.metadata_store();

            let child_digest = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;
            let index_digest = put_blob_direct(metadata_store.store(), TEST_INDEX).await;

            setup_index_scenario(&metadata_store, namespace, &index_digest, &child_digest).await;

            let policy = Arc::new(RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: vec![CelRule::compile("image.tag != null").unwrap()],
                },
                Arc::new(SystemClock),
            ));

            let mut executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, Some(policy.clone()))
                .check(namespace, &mut executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(child_digest.clone()), false)
                    .await
                    .is_ok()
            );

            teardown_index_scenario(&metadata_store, namespace, index_digest, &child_digest).await;

            let mut executor2 = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver2 = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver2, Some(policy))
                .check(namespace, &mut executor2)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(child_digest), false)
                    .await
                    .is_err()
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_delete_tag_action_emitted_without_storage_mutation() {
        let test_case = backends().into_iter().next().unwrap();
        let namespace = &Namespace::new("test-repo/app").unwrap();
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();

        let (blob_digest, _) =
            test_utils::create_test_blob(registry, namespace, b"test manifest").await;

        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create(
                    LinkKind::Tag("v0.0.1".to_string()),
                    blob_digest.clone(),
                )],
            )
            .await
            .unwrap();

        let policy = Arc::new(RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        ));

        let resolver = Arc::new(
            RepositoryResolver::new(test_utils::create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let scrubber = RetentionChecker::new(metadata_store.clone(), resolver, Some(policy));

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
        test_case.cleanup().await;
    }

    #[tokio::test]
    async fn retention_checker_continues_after_missing_blob_in_one_revision() {
        for test_case in backends() {
            let namespace = "test-repo/app";
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // First revision: write blob, then delete it so the executor encounters a missing blob.
            let digest_missing = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest_missing.clone()),
                        digest_missing.clone(),
                    )],
                )
                .await
                .unwrap();
            blob_store.delete_blob(&digest_missing).await.unwrap();

            // Second revision: healthy manifest blob with a digest link.
            let digest_healthy = put_blob_direct(metadata_store.store(), TEST_INDEX).await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest_healthy.clone()),
                        digest_healthy.clone(),
                    )],
                )
                .await
                .unwrap();

            let policy = Arc::new(RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: vec![CelRule::compile("image.tag != null").unwrap()],
                },
                Arc::new(SystemClock),
            ));

            let mut executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, Some(policy))
                .check(namespace, &mut executor)
                .await
                .unwrap();

            // The healthy revision must be cleaned up: the broken one did not block the loop.
            assert!(
                metadata_store
                    .read_link(namespace, &LinkKind::Digest(digest_healthy), false)
                    .await
                    .is_err(),
                "healthy revision after the broken one must still be processed"
            );
            test_case.cleanup().await;
        }
    }

    fn make_manifest(tag: &str) -> ManifestImage {
        ManifestImage {
            tag: Some(tag.to_string()),
            pushed_at: EpochSeconds::from_seconds(0),
            last_pulled_at: EpochSeconds::from_seconds(0),
        }
    }

    #[test]
    fn check_global_policy_returns_no_opinion_when_policy_absent() {
        let manifest = make_manifest("v1");
        let result = check_global_policy(None, &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_global_policy_returns_retain_when_policy_keeps() {
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'v1'").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let manifest = make_manifest("v1");
        let result = check_global_policy(Some(&policy), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Retain);
    }

    #[test]
    fn check_global_policy_returns_delete_when_policy_drops() {
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let manifest = make_manifest("v1");
        let result = check_global_policy(Some(&policy), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Delete);
    }

    fn make_repo(name: &str, rules: Vec<CelRule>) -> Repository {
        Repository {
            name: name.to_string(),
            upstreams: Vec::new(),
            replication: Vec::new(),
            retention_policy: RetentionPolicy::new(
                &RetentionPolicyConfig { rules },
                Arc::new(SystemClock),
            ),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
        }
    }

    #[test]
    fn check_repo_policy_returns_no_opinion_when_repository_absent() {
        let manifest = make_manifest("v1");
        let result = check_repo_policy(None, &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_repo_policy_returns_no_opinion_when_repo_has_no_rules() {
        let repo = make_repo("r", vec![]);
        let manifest = make_manifest("v1");
        let result = check_repo_policy(Some(&repo), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_repo_policy_returns_retain_when_repo_policy_keeps() {
        let repo = make_repo("r", vec![CelRule::compile("image.tag == 'v1'").unwrap()]);
        let manifest = make_manifest("v1");
        let result = check_repo_policy(Some(&repo), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Retain);
    }

    #[test]
    fn check_repo_policy_returns_delete_when_repo_policy_drops() {
        let repo = make_repo(
            "r",
            vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
        );
        let manifest = make_manifest("v1");
        let result = check_repo_policy(Some(&repo), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Delete);
    }

    fn dummy_metadata() -> LinkMetadata {
        LinkMetadata {
            target: dummy_digest(),
            created_at: None,
            accessed_at: None,
            referenced_by: HashSet::default(),
            media_type: None,
            descriptor: None,
        }
    }

    #[test]
    fn decide_orphan_fate_skips_protected_manifest() {
        let metadata = dummy_metadata();
        let fate = decide_orphan_fate(true, false, Some(&metadata), None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Skip);
    }

    #[test]
    fn decide_orphan_fate_skips_manifest_with_tags() {
        let metadata = dummy_metadata();
        let fate = decide_orphan_fate(false, true, Some(&metadata), None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Skip);
    }

    #[test]
    fn decide_orphan_fate_skips_when_metadata_missing() {
        let fate = decide_orphan_fate(false, false, None, None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Skip);
    }

    #[test]
    fn decide_orphan_fate_retains_when_no_policies_apply() {
        let metadata = dummy_metadata();
        let fate = decide_orphan_fate(false, false, Some(&metadata), None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Retain);
    }

    #[test]
    fn decide_orphan_fate_retains_when_global_policy_keeps() {
        let metadata = dummy_metadata();
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == null").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let fate = decide_orphan_fate(false, false, Some(&metadata), None, Some(&policy), &[], &[])
            .unwrap();
        assert_eq!(fate, Fate::Retain);
    }

    #[test]
    fn decide_orphan_fate_deletes_when_all_applicable_policies_drop() {
        let metadata = dummy_metadata();
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let fate = decide_orphan_fate(false, false, Some(&metadata), None, Some(&policy), &[], &[])
            .unwrap();
        assert_eq!(fate, Fate::Delete);
    }
}
