use std::{cmp::Reverse, pin::pin, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStreamExt};
use tracing::{debug, error};

use chrono::Duration;

use crate::{
    command::maintenance::{Error, action::Action, check::NamespaceChecker, executor::ActionSink},
    oci::{Digest, Namespace, Tag},
    policy::{EpochSeconds, ManifestImage, RetentionPolicy},
    registry::{
        Error as RegistryError, Repository,
        blob_store::BlobStore,
        metadata_store::{BlobIndex, LinkKind, LinkMetadata, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
};

/// Fan-out for the per-tag link-metadata reads feeding the retention rankings.
const TAG_METADATA_CONCURRENCY: usize = 16;

struct TagWithMetadata {
    name: Tag,
    metadata: LinkMetadata,
}

pub struct RetentionChecker {
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    global_retention_policy: Option<Arc<RetentionPolicy>>,
    tag_metadata_concurrency: usize,
}

fn has_link_kind(
    blob_index: &BlobIndex,
    namespace: &Namespace,
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

/// The combined `(global, repo)` retention verdict: any `Retain` wins, two
/// `NoOpinion`s retain by default, anything else deletes.
fn policies_retain(global: &PolicyDecision, repo: &PolicyDecision) -> bool {
    matches!(
        (global, repo),
        (PolicyDecision::Retain, _)
            | (_, PolicyDecision::Retain)
            | (PolicyDecision::NoOpinion, PolicyDecision::NoOpinion)
    )
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

    Ok(if policies_retain(&global, &repo) {
        Fate::Retain
    } else {
        Fate::Delete
    })
}

/// Revoke per-namespace blob-ownership grants with no manifest reference,
/// letting the retention policies decide their fate like any other untagged
/// content: the subject carries no tag and `pushed_at` is the bytes' mtime,
/// so time-based keep rules apply uniformly. With no policies configured the
/// grant is retained, matching the orphan-manifest default.
///
/// `in_flight_window` guards the push race, not the decision: a push grants
/// ownership before it links the manifest, so bytes younger than the window
/// are never considered. The executor still re-checks under the blob-data
/// lock before revoking.
pub async fn sweep_orphan_grants(
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
    global_policy: Option<&RetentionPolicy>,
    in_flight_window: Duration,
    sink: &dyn ActionSink,
    concurrency: usize,
) -> Result<(), Error> {
    let ctx = GrantSweep {
        blob_store,
        metadata_store,
        resolver,
        global_policy,
        in_flight_window,
        now: Utc::now(),
        sink,
    };
    let ctx = &ctx;
    blob_store
        .stream_blobs()
        .err_into()
        .try_for_each_concurrent(concurrency, |blob| async move {
            if let Err(e) = sweep_grants_for_blob(ctx, &blob).await {
                error!("prune: failed to check grants for blob {blob}: {e}");
            }
            Ok(())
        })
        .await
}

/// The state a single grant-only blob check shares across the whole sweep.
struct GrantSweep<'a> {
    blob_store: &'a Arc<BlobStore>,
    metadata_store: &'a Arc<MetadataStore>,
    resolver: &'a Arc<RepositoryResolver>,
    global_policy: Option<&'a RetentionPolicy>,
    in_flight_window: Duration,
    now: DateTime<Utc>,
    sink: &'a dyn ActionSink,
}

async fn sweep_grants_for_blob(ctx: &GrantSweep<'_>, blob: &Digest) -> Result<(), Error> {
    // In-flight guard on the bytes' mtime; without one (or with the bytes
    // absent) leave the blob to scrub's orphan GC.
    let last_modified = match ctx.blob_store.last_modified(blob).await {
        Ok(Some(ts)) => ts,
        Ok(None) | Err(RegistryError::BlobUnknown | RegistryError::NotFound) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if ctx.now.signed_duration_since(last_modified) < ctx.in_flight_window {
        return Ok(());
    }

    let index = match ctx.metadata_store.read_blob_index(blob).await {
        Ok(index) => index,
        Err(RegistryError::NotFound) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let subject = ManifestImage {
        tag: None,
        pushed_at: EpochSeconds::from_seconds(last_modified.timestamp()),
        last_pulled_at: EpochSeconds::from_seconds(0),
    };
    let grant = LinkKind::Blob(blob.clone());
    for (namespace, links) in index.namespace {
        // A namespace that no longer resolves to any configured repository is
        // being cleared by the orphan-namespace sweep; revoke its grants
        // outright (the executor's under-lock re-check still spares a blob a
        // not-yet-cascaded manifest references).
        let repository = ctx.resolver.resolve(&namespace);
        if repository.is_none() && ctx.resolver.len() > 0 {
            ctx.sink
                .apply(Action::RemoveOrphanBlobGrant {
                    namespace,
                    blob: blob.clone(),
                })
                .await?;
            continue;
        }
        // A tracked link (Layer/Config/Manifest) means a manifest references
        // the blob, so the grant is live; only a grant-only namespace is a
        // retention subject.
        if !links.contains(&grant) || links.iter().any(LinkKind::is_tracked) {
            continue;
        }
        let global = check_global_policy(ctx.global_policy, &subject, &[], &[])?;
        let repo = check_repo_policy(repository, &subject, &[], &[])?;
        if policies_retain(&global, &repo) {
            continue;
        }
        ctx.sink
            .apply(Action::RemoveOrphanBlobGrant {
                namespace,
                blob: blob.clone(),
            })
            .await?;
    }
    Ok(())
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
            tag_metadata_concurrency: TAG_METADATA_CONCURRENCY,
        }
    }

    /// Override the per-tag link-read fan-out; the prune command derives it from
    /// its `--concurrency` option.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.tag_metadata_concurrency = concurrency.max(1);
        self
    }
}

#[async_trait]
impl NamespaceChecker for RetentionChecker {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
        debug!("Checking retention policies on '{namespace}'");

        let tag_metadata = self.fetch_tag_metadata(namespace).await?;
        let (last_pushed, last_pulled) = Self::build_sorted_rankings(&tag_metadata);

        let tags = self.get_deletable_tags(namespace, &tag_metadata, &last_pushed, &last_pulled);
        self.emit_delete_tags(namespace, &tags, sink).await?;

        self.emit_delete_orphan_manifests(namespace, &last_pushed, &last_pulled, sink)
            .await
    }
}

impl RetentionChecker {
    /// Reads every tag's link metadata, the tag listing streaming into up to
    /// `TAG_METADATA_CONCURRENCY` concurrent link reads.
    async fn fetch_tag_metadata(
        &self,
        namespace: &Namespace,
    ) -> Result<Vec<TagWithMetadata>, Error> {
        self.metadata_store
            .stream_tags(namespace)
            .err_into::<Error>()
            .map_ok(|tag| async move {
                let metadata = self
                    .metadata_store
                    .read_link(namespace, &LinkKind::Tag(tag.clone()))
                    .await?;
                Ok(TagWithMetadata {
                    name: tag,
                    metadata,
                })
            })
            .try_buffered(self.tag_metadata_concurrency)
            .try_collect()
            .await
    }

    fn build_sorted_rankings(tags: &[TagWithMetadata]) -> (Vec<String>, Vec<String>) {
        let last_pushed = Self::rank_by(tags, |m| m.created_at);
        let last_pulled = Self::rank_by(tags, |m| m.accessed_at);
        (last_pushed, last_pulled)
    }

    fn rank_by<K: Ord>(tags: &[TagWithMetadata], key: impl Fn(&LinkMetadata) -> K) -> Vec<String> {
        let mut indices: Vec<usize> = (0..tags.len()).collect();
        indices.sort_by_cached_key(|&i| Reverse(key(&tags[i].metadata)));
        indices.iter().map(|&i| tags[i].name.to_string()).collect()
    }

    fn get_deletable_tags<'a>(
        &self,
        namespace: &Namespace,
        tags: &'a [TagWithMetadata],
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Vec<&'a Tag> {
        tags.iter()
            .filter(
                |tag| match self.should_retain_tag(namespace, tag, last_pushed, last_pulled) {
                    Ok(retain) => !retain,
                    Err(e) => {
                        error!(
                            "Retention evaluation failed for '{namespace}:{}', retaining: {e}",
                            tag.name
                        );
                        false
                    }
                },
            )
            .map(|tag| &tag.name)
            .collect()
    }

    /// Per-item tolerance: a failed deletion (or its required event delivery)
    /// is logged and skipped; the item is retried on the next run.
    async fn emit_delete_tags(
        &self,
        namespace: &Namespace,
        tags_to_delete: &[&Tag],
        sink: &dyn ActionSink,
    ) -> Result<(), Error> {
        for tag in tags_to_delete {
            let action = Action::DeleteTag {
                namespace: namespace.clone(),
                tag: (*tag).clone(),
            };
            if let Err(e) = sink.apply(action).await {
                error!("Failed to delete tag '{namespace}:{tag}' for retention: {e}");
            }
        }
        Ok(())
    }

    fn should_retain_tag(
        &self,
        namespace: &Namespace,
        tag: &TagWithMetadata,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<bool, Error> {
        debug!("'{namespace}': Checking tag '{}' for retention", tag.name);

        let manifest = ManifestImage {
            tag: Some(tag.name.to_string()),
            pushed_at: EpochSeconds::from_seconds(to_epoch(tag.metadata.created_at)),
            last_pulled_at: EpochSeconds::from_seconds(to_epoch(tag.metadata.accessed_at)),
        };

        self.evaluate_retention_policies(namespace, &tag.name, &manifest, last_pushed, last_pulled)
    }

    fn evaluate_retention_policies(
        &self,
        namespace: &Namespace,
        tag: &Tag,
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

        let retain = policies_retain(&global, &repo);
        debug!(
            "Retention verdict for {namespace}:{tag}: global={global:?} repo={repo:?} retain={retain}"
        );
        Ok(retain)
    }

    async fn emit_delete_orphan_manifests(
        &self,
        namespace: &Namespace,
        last_pushed: &[String],
        last_pulled: &[String],
        sink: &dyn ActionSink,
    ) -> Result<(), Error> {
        let mut revisions = pin!(self.metadata_store.stream_revisions(namespace));
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
        namespace: &Namespace,
        digest: &Digest,
        last_pushed: &[String],
        last_pulled: &[String],
        sink: &dyn ActionSink,
    ) -> Result<(), Error> {
        // A missing blob index means "no links"; any other read failure must
        // propagate rather than pass for "unprotected" on a delete path.
        let blob_index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => Some(index),
            Err(RegistryError::NotFound) => None,
            Err(e) => return Err(e.into()),
        };

        let is_protected = self
            .is_protected(namespace, digest, blob_index.as_ref())
            .await?;
        if is_protected {
            debug!("Skipping protected manifest '{namespace}@{digest}'");
        }

        let has_tags = !is_protected
            && blob_index.as_ref().is_some_and(|index| {
                has_link_kind(index, namespace, |link| matches!(link, LinkKind::Tag(_)))
            });

        let metadata = if is_protected || has_tags {
            None
        } else {
            self.metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()))
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
        namespace: &Namespace,
        digest: &Digest,
        fate: Fate,
        sink: &dyn ActionSink,
    ) -> Result<(), Error> {
        match fate {
            Fate::Skip | Fate::Retain => Ok(()),
            Fate::Delete => {
                sink.apply(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await
            }
        }
    }

    async fn is_protected(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        blob_index: Option<&BlobIndex>,
    ) -> Result<bool, Error> {
        if blob_index.is_some_and(|index| {
            has_link_kind(index, namespace, |link| {
                matches!(link, LinkKind::Manifest(_, _))
            })
        }) {
            return Ok(true);
        }

        Ok(self.metadata_store.has_referrers(namespace, digest).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    };

    use chrono::{TimeZone, Utc};
    use url::Url;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;
    use crate::{
        command::maintenance::{
            action::Action,
            executor::{Executor, RETENTION_ACTOR},
        },
        event_webhook::{
            config::{DeliveryPolicy, EventWebhookConfig},
            dispatcher::EventDispatcher,
            event::EventKind,
        },
        jobs::store::JobStore,
        oci::{Digest, Namespace},
        policy::{CelRule, RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Registry, RegistryConfig,
            blob_store::BlobStore,
            metadata_store::{BlobIndexOperation, LinkOperation},
            repository_resolver::RepositoryResolver,
            test_utils::{
                self, FSRegistryTestCase, RegistryTestCase, for_each_backend, put_blob_direct,
            },
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
            name: Tag::new(name).unwrap(),
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

    fn make_executor(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Executor {
        Executor::new_for_test(blob_store, metadata_store)
    }

    async fn setup_index_scenario(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
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
                        LinkKind::Tag(Tag::new("latest").unwrap()),
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
        namespace: &Namespace,
        index_digest: Digest,
        child_digest: &Digest,
    ) {
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::delete(LinkKind::Tag(Tag::new("latest").unwrap())),
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
        for_each_backend(async |test_case| {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("v1.0.0").unwrap()),
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

            let executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            scrubber.check(namespace, &executor).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("v1.0.0").unwrap()))
                .await;

            assert!(tag_link.is_ok());
        })
        .await;
    }

    #[tokio::test]
    async fn test_enforce_retention_no_policy() {
        for_each_backend(async |test_case| {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();

            let (blob_digest, _) =
                test_utils::create_test_blob(registry, namespace, b"test manifest").await;

            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new("any-tag").unwrap()),
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

            let executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            scrubber.check(namespace, &executor).await.unwrap();

            let tag_link = metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("any-tag").unwrap()))
                .await;

            assert!(tag_link.is_ok());
        })
        .await;
    }

    #[tokio::test]
    async fn test_orphan_manifest_deleted_with_policy() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();

            let digest = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;

            metadata_store
                .update_links(
                    &namespace,
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

            let executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, Some(policy))
                .check(&namespace, &executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(digest))
                    .await
                    .is_err()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_orphan_manifest_kept_without_policy() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();

            let digest = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;

            metadata_store
                .update_links(
                    &namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, None)
                .check(&namespace, &executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(digest))
                    .await
                    .is_ok()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_index_child_manifest_protected() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();

            let child_digest = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;
            let index_digest = put_blob_direct(metadata_store.store(), TEST_INDEX).await;

            setup_index_scenario(&metadata_store, &namespace, &index_digest, &child_digest).await;

            let policy = Arc::new(RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: vec![CelRule::compile("image.tag != null").unwrap()],
                },
                Arc::new(SystemClock),
            ));

            let executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, Some(policy.clone()))
                .check(&namespace, &executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(child_digest.clone()))
                    .await
                    .is_ok()
            );

            teardown_index_scenario(&metadata_store, &namespace, index_digest, &child_digest).await;

            let executor2 = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver2 = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver2, Some(policy))
                .check(&namespace, &executor2)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(child_digest))
                    .await
                    .is_err()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_delete_tag_action_emitted_without_storage_mutation() {
        let test_case = FSRegistryTestCase::new();
        let namespace = &Namespace::new("test-repo/app").unwrap();
        let registry = test_case.registry();
        let metadata_store = test_case.metadata_store();

        let (blob_digest, _) =
            test_utils::create_test_blob(registry, namespace, b"test manifest").await;

        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        scrubber.check(namespace, &sink).await.unwrap();

        assert!(
            sink.lock()
                .unwrap()
                .iter()
                .any(|a| matches!(a, Action::DeleteTag { .. })),
            "Vec sink must capture a DeleteTag action"
        );

        assert!(
            metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_ok(),
            "Vec sink must not delete the tag"
        );
        test_case.cleanup().await;
    }

    /// Retention deletions run through the registry, so they emit
    /// `manifest.delete` and `tag.delete` events carrying the internal
    /// `prune` actor.
    #[tokio::test]
    async fn retention_deletion_emits_events_with_internal_actor() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let webhook = EventWebhookConfig {
            url: Url::parse(&server.uri()).unwrap(),
            policy: DeliveryPolicy::Required,
            token: None,
            timeout_ms: 5_000,
            max_retries: 0,
            events: vec![EventKind::ManifestDelete, EventKind::TagDelete],
            repository_filter: None,
        };
        let mut webhooks = HashMap::new();
        webhooks.insert("retention-hook".to_string(), webhook);
        let dispatcher = EventDispatcher::new(webhooks).expect("dispatcher build");

        let test_case = FSRegistryTestCase::new();
        let namespace = &Namespace::new("test-repo/app").unwrap();
        let metadata_store = test_case.metadata_store();

        let (blob_digest, _) =
            test_utils::create_test_blob(test_case.registry(), namespace, b"retained content")
                .await;
        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
                    blob_digest.clone(),
                )],
            )
            .await
            .unwrap();

        let resolver = Arc::new(
            RepositoryResolver::new(test_utils::create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "retention-test"));
        let registry = Registry::new(
            test_case.blob_store(),
            metadata_store.clone(),
            resolver.clone(),
            RegistryConfig {
                job_queue: Some(job_store.clone()),
                event_dispatcher: Some(Arc::new(dispatcher)),
                ..RegistryConfig::default()
            },
        );
        let executor = Executor::new(test_case.blob_store(), metadata_store.clone(), job_store)
            .with_registry(registry);

        let policy = Arc::new(RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        ));
        RetentionChecker::new(metadata_store.clone(), resolver, Some(policy))
            .check(namespace, &executor)
            .await
            .unwrap();

        assert!(
            metadata_store
                .read_link(namespace, &LinkKind::Tag(Tag::new("v0.0.1").unwrap()))
                .await
                .is_err(),
            "the tag must be deleted"
        );

        let requests = server.received_requests().await.unwrap();
        let events: Vec<serde_json::Value> = requests
            .iter()
            .map(|r| serde_json::from_slice(&r.body).unwrap())
            .collect();
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| e["kind"].as_str().unwrap_or_default())
            .collect();
        assert!(
            kinds.contains(&"manifest.delete") && kinds.contains(&"tag.delete"),
            "retention must emit manifest.delete and tag.delete; got {kinds:?}"
        );
        for event in &events {
            assert_eq!(
                event["actor"]["internal"], RETENTION_ACTOR,
                "retention events must carry the internal actor; got {event}"
            );
        }
        test_case.cleanup().await;
    }

    #[tokio::test]
    async fn retention_checker_continues_after_missing_blob_in_one_revision() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();
            let metadata_store = test_case.metadata_store();

            // First revision: write blob, then delete it so the executor encounters a missing blob.
            let digest_missing = put_blob_direct(metadata_store.store(), TEST_MANIFEST).await;
            metadata_store
                .update_links(
                    &namespace,
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
                    &namespace,
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

            let executor = make_executor(test_case.blob_store(), test_case.metadata_store());
            let resolver = Arc::new(
                RepositoryResolver::new(test_utils::create_test_repositories())
                    .expect("test repositories must not have overlapping prefixes"),
            );
            RetentionChecker::new(metadata_store.clone(), resolver, Some(policy))
                .check(&namespace, &executor)
                .await
                .unwrap();

            // The healthy revision must be cleaned up: the broken one did not block the loop.
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(digest_healthy))
                    .await
                    .is_err(),
                "healthy revision after the broken one must still be processed"
            );
        })
        .await;
    }

    fn make_manifest(tag: &Tag) -> ManifestImage {
        ManifestImage {
            tag: Some(tag.to_string()),
            pushed_at: EpochSeconds::from_seconds(0),
            last_pulled_at: EpochSeconds::from_seconds(0),
        }
    }

    #[test]
    fn check_global_policy_returns_no_opinion_when_policy_absent() {
        let manifest = make_manifest(&Tag::new("v1").unwrap());
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
        let manifest = make_manifest(&Tag::new("v1").unwrap());
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
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_global_policy(Some(&policy), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Delete);
    }

    fn make_repo(name: &str, rules: Vec<CelRule>) -> Repository {
        Repository {
            name: Namespace::new(name).unwrap(),
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
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_repo_policy(None, &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_repo_policy_returns_no_opinion_when_repo_has_no_rules() {
        let repo = make_repo("r", vec![]);
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_repo_policy(Some(&repo), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_repo_policy_returns_retain_when_repo_policy_keeps() {
        let repo = make_repo("r", vec![CelRule::compile("image.tag == 'v1'").unwrap()]);
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_repo_policy(Some(&repo), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Retain);
    }

    #[test]
    fn check_repo_policy_returns_delete_when_repo_policy_drops() {
        let repo = make_repo(
            "r",
            vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
        );
        let manifest = make_manifest(&Tag::new("v1").unwrap());
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

    fn keep_nothing_policy() -> RetentionPolicy {
        RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        )
    }

    fn keep_recent_policy() -> RetentionPolicy {
        RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.pushed_at > now() - days(1)").unwrap()],
            },
            Arc::new(SystemClock),
        )
    }

    /// Seed a blob whose only index entry is its self-ownership grant.
    async fn seed_grant_only_blob(
        test_case: &dyn RegistryTestCase,
        namespace: &Namespace,
    ) -> Digest {
        let metadata_store = test_case.metadata_store();
        let blob = put_blob_direct(metadata_store.store(), b"granted-but-unreferenced").await;
        metadata_store
            .update_blob_index(
                namespace,
                &blob,
                BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
            )
            .await
            .unwrap();
        blob
    }

    fn grant_resolver() -> Arc<RepositoryResolver> {
        Arc::new(
            RepositoryResolver::new(test_utils::create_test_repositories())
                .expect("test repositories must not overlap"),
        )
    }

    #[tokio::test]
    async fn orphan_grant_is_retained_without_policies() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/no-rules").unwrap();
            let blob = seed_grant_only_blob(test_case, &namespace).await;
            let metadata_store = test_case.metadata_store();
            let executor = Executor::new_for_test(test_case.blob_store(), metadata_store.clone());

            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &grant_resolver(),
                None,
                chrono::Duration::zero(),
                &executor,
                4,
            )
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_ok(),
                "with no retention policies a grant must be retained, like any untagged content"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn orphan_grant_is_revoked_when_no_rule_retains_it() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/dropped").unwrap();
            let blob = seed_grant_only_blob(test_case, &namespace).await;
            let metadata_store = test_case.metadata_store();
            let executor = Executor::new_for_test(test_case.blob_store(), metadata_store.clone());
            let policy = keep_nothing_policy();

            // Within the in-flight window: never considered, whatever policy says.
            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &grant_resolver(),
                Some(&policy),
                chrono::Duration::days(1),
                &executor,
                4,
            )
            .await
            .unwrap();
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_ok(),
                "a grant within the in-flight window must never be considered"
            );

            // Past the window: the policy decides, and nothing retains it.
            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &grant_resolver(),
                Some(&policy),
                chrono::Duration::zero(),
                &executor,
                4,
            )
            .await
            .unwrap();
            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_err(),
                "a grant no rule retains must be revoked past the window"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn orphan_grant_is_retained_by_a_matching_time_rule() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/kept").unwrap();
            let blob = seed_grant_only_blob(test_case, &namespace).await;
            let metadata_store = test_case.metadata_store();
            let executor = Executor::new_for_test(test_case.blob_store(), metadata_store.clone());
            let policy = keep_recent_policy();

            // The bytes were just written, so `pushed_at > now() - days(1)`
            // retains them even though the in-flight window is zero.
            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &grant_resolver(),
                Some(&policy),
                chrono::Duration::zero(),
                &executor,
                4,
            )
            .await
            .unwrap();

            assert!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await
                    .is_ok(),
                "a rule matching the subject must retain the grant"
            );
        })
        .await;
    }
}
