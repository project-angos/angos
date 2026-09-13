use std::{cmp::Reverse, collections::VecDeque, pin::pin, sync::Arc};

use async_trait::async_trait;
use chrono::Duration;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStreamExt};
use tracing::{debug, error};

use angos_oci::{Digest, Namespace, Tag};

use crate::{
    command::maintenance::{
        Error,
        action::Action,
        check::NamespaceChecker,
        executor::{ActionSink, object_younger_than_grace},
    },
    policy::{ManifestImage, RetentionPolicy},
    registry::{
        Error as RegistryError, Repository,
        blob_store::BlobStore,
        keys::DigestKeys,
        manifest::{link_plan, read_manifest},
        metadata_store::{BlobIndex, LinkKind, LinkMetadata, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
};

/// Fan-out for the per-tag link-metadata reads feeding the retention rankings.
/// Fixed rather than derived from `--concurrency`, which already bounds the
/// namespace walk: one knob for both would square the in-flight reads.
const TAG_METADATA_CONCURRENCY: usize = 16;

struct TagWithMetadata {
    name: Tag,
    metadata: LinkMetadata,
    /// The tag's last pull, read from its access entries rather than carried
    /// on the metadata the tag resolves to.
    pulled_at: Option<DateTime<Utc>>,
}

pub struct RetentionChecker {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    global_retention_policy: Option<Arc<RetentionPolicy>>,
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

fn check_policy(
    policy: Option<&RetentionPolicy>,
    manifest: &ManifestImage,
    last_pushed: &[String],
    last_pulled: &[String],
) -> Result<PolicyDecision, Error> {
    let Some(policy) = policy else {
        return Ok(PolicyDecision::NoOpinion);
    };
    Ok(
        if policy.should_retain(manifest, last_pushed, last_pulled)? {
            PolicyDecision::Retain
        } else {
            PolicyDecision::Delete
        },
    )
}

/// The repository's own retention policy, when it declares rules.
fn repository_policy(repository: Option<&Repository>) -> Option<&RetentionPolicy> {
    let policy = &repository?.retention_policy;
    policy.has_rules().then_some(policy)
}

#[derive(Debug, PartialEq, Eq)]
enum Fate {
    /// Protected by another reference, tagged, or missing its link metadata.
    Skip,
    Retain,
    Delete,
}

/// Pure decision over pre-loaded inputs: should this orphan manifest be
/// skipped, retained, or deleted?
fn decide_orphan_fate(
    is_protected: bool,
    has_tags: bool,
    manifest: Option<&ManifestImage>,
    repository: Option<&Repository>,
    global_policy: Option<&RetentionPolicy>,
    last_pushed: &[String],
    last_pulled: &[String],
) -> Result<Fate, Error> {
    if is_protected || has_tags {
        return Ok(Fate::Skip);
    }
    let Some(manifest) = manifest else {
        return Ok(Fate::Skip);
    };

    let global = check_policy(global_policy, manifest, last_pushed, last_pulled)?;
    let repo = check_policy(
        repository_policy(repository),
        manifest,
        last_pushed,
        last_pulled,
    )?;

    Ok(if policies_retain(&global, &repo) {
        Fate::Retain
    } else {
        Fate::Delete
    })
}

/// Revoke per-namespace blob-ownership grants with no manifest reference. The
/// retention policies decide, with the bytes' mtime as `pushed_at` and no tag,
/// so an unconfigured registry retains the grant like an orphan manifest.
///
/// `in_flight_window` guards the push race, not the decision: a push grants
/// ownership before it links the manifest, so neither bytes nor a grant
/// younger than the window is considered. The executor re-checks the
/// references and the own key's age at apply time before revoking.
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
        in_flight_window_secs: u64::try_from(in_flight_window.num_seconds()).unwrap_or(0),
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

struct GrantSweep<'a> {
    blob_store: &'a Arc<BlobStore>,
    metadata_store: &'a Arc<MetadataStore>,
    resolver: &'a Arc<RepositoryResolver>,
    global_policy: Option<&'a RetentionPolicy>,
    in_flight_window: Duration,
    /// [`Self::in_flight_window`] in seconds, for the key-age helper.
    in_flight_window_secs: u64,
    now: DateTime<Utc>,
    sink: &'a dyn ActionSink,
}

async fn sweep_grants_for_blob(ctx: &GrantSweep<'_>, blob: &Digest) -> Result<(), Error> {
    // With no mtime or no bytes, leave the blob to scrub's orphan GC. The
    // mtime spares a whole freshly uploaded blob, and dates the retention
    // subject below; a grant's own in-flight guard is per namespace, since a
    // mount writes a fresh ownership key over bytes of any age.
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
    let subject = ManifestImage::new(None, Some(last_modified), None, Utc::now());
    let grant = LinkKind::Blob(blob.clone());
    for (namespace, links) in index.namespace {
        // An unresolved namespace is being cleared by the orphan-namespace
        // sweep, so revoke its grants outright; the executor's re-check still
        // spares a blob a not-yet-cascaded manifest references.
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
        // A tracked link means a manifest references the blob, so the grant is
        // live; only a grant-only namespace is a retention subject.
        if !links.contains(&grant) || links.iter().any(LinkKind::is_tracked) {
            continue;
        }
        // A cross-repository mount grants ownership of bytes that may be weeks
        // old, so the window that shields an in-flight push is measured on the
        // grant, not on what it points at. A gone key is already revoked.
        if object_younger_than_grace(
            ctx.metadata_store.object_store().as_ref(),
            &blob.blob_ref_own_path(&namespace),
            ctx.in_flight_window_secs,
        )
        .await
        .map_err(RegistryError::from)?
        .unwrap_or(true)
        {
            continue;
        }
        let global = check_policy(ctx.global_policy, &subject, &[], &[])?;
        let repo = check_policy(repository_policy(repository), &subject, &[], &[])?;
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
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        resolver: Arc<RepositoryResolver>,
        global_retention_policy: Option<Arc<RetentionPolicy>>,
    ) -> Self {
        Self {
            blob_store,
            metadata_store,
            resolver,
            global_retention_policy,
        }
    }
}

#[async_trait]
impl NamespaceChecker for RetentionChecker {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
        debug!("Checking retention policies on '{namespace}'");

        let tag_metadata = self.fetch_tag_metadata(namespace).await?;
        let last_pushed = Self::rank_by(&tag_metadata, |t| t.metadata.created_at);
        let last_pulled = Self::rank_by(&tag_metadata, |t| t.pulled_at);

        let tags = self.get_deletable_tags(namespace, &tag_metadata, &last_pushed, &last_pulled);
        self.emit_delete_tags(namespace, &tags, sink).await?;

        self.emit_delete_orphan_manifests(namespace, &last_pushed, &last_pulled, sink)
            .await
    }
}

impl RetentionChecker {
    /// Reads every tag's link metadata, up to `TAG_METADATA_CONCURRENCY` at a
    /// time.
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
                let pulled_at = self
                    .metadata_store
                    .read_tag_access_time(namespace, &tag)
                    .await?;
                Ok(TagWithMetadata {
                    name: tag,
                    metadata,
                    pulled_at,
                })
            })
            .try_buffered(TAG_METADATA_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Ranks tags most recent first, leaving out those carrying no such time.
    /// A never-pulled tag is not one of the "n most recently pulled", and
    /// ranking it would make `top_pulled(n)` retain untouched tags forever.
    fn rank_by(
        tags: &[TagWithMetadata],
        key: impl Fn(&TagWithMetadata) -> Option<DateTime<Utc>>,
    ) -> Vec<String> {
        let mut ranked: Vec<(Reverse<DateTime<Utc>>, String)> = tags
            .iter()
            .filter_map(|t| Some((Reverse(key(t)?), t.name.to_string())))
            .collect();
        ranked.sort_by_key(|t| t.0);
        ranked.into_iter().map(|(_, name)| name).collect()
    }

    fn get_deletable_tags<'a>(
        &self,
        namespace: &Namespace,
        tags: &'a [TagWithMetadata],
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Vec<&'a TagWithMetadata> {
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
            .collect()
    }

    /// A failed deletion (or its required event delivery) is logged and
    /// skipped, leaving the tag for the next run.
    async fn emit_delete_tags(
        &self,
        namespace: &Namespace,
        tags_to_delete: &[&TagWithMetadata],
        sink: &dyn ActionSink,
    ) -> Result<(), Error> {
        for tag in tags_to_delete {
            let action = Action::DeleteTag {
                namespace: namespace.clone(),
                tag: tag.name.clone(),
                // What the tag resolved to when it was judged: a re-push
                // between here and the delete must not be swept away.
                target: Some(tag.metadata.target.clone()),
            };
            if let Err(e) = sink.apply(action).await {
                error!(
                    "Failed to delete tag '{namespace}:{}' for retention: {e}",
                    tag.name
                );
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

        let manifest = ManifestImage::new(
            Some(tag.name.to_string()),
            tag.metadata.created_at,
            tag.pulled_at,
            Utc::now(),
        );
        let global = check_policy(
            self.global_retention_policy.as_deref(),
            &manifest,
            last_pushed,
            last_pulled,
        )?;
        let repo = check_policy(
            repository_policy(self.resolver.resolve(namespace)),
            &manifest,
            last_pushed,
            last_pulled,
        )?;

        let retain = policies_retain(&global, &repo);
        debug!(
            "Retention verdict for {namespace}:{}: global={global:?} repo={repo:?} retain={retain}",
            tag.name
        );
        Ok(retain)
    }

    /// Collects orphan revisions, requeueing what each delete unpins so one
    /// skipped while still pinned is revisited in the same run. Requeues
    /// cannot loop, since a digest names its children by content hash.
    async fn emit_delete_orphan_manifests(
        &self,
        namespace: &Namespace,
        last_pushed: &[String],
        last_pulled: &[String],
        sink: &dyn ActionSink,
    ) -> Result<(), Error> {
        // Requeues first: the queue then spans one subtree, not the listing.
        let mut requeued: VecDeque<Digest> = VecDeque::new();

        let mut revisions = pin!(self.metadata_store.stream_revisions(namespace));
        loop {
            let digest = match requeued.pop_front() {
                Some(digest) => digest,
                None => match revisions.next().await {
                    Some(digest) => digest?,
                    None => break,
                },
            };

            match self
                .process_orphan_revision(namespace, &digest, last_pushed, last_pulled, sink)
                .await
            {
                Ok(unpinned) => {
                    for candidate in unpinned {
                        if !requeued.contains(&candidate) {
                            requeued.push_back(candidate);
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to check revision from '{namespace}' (revision '{digest}'): {e}"
                    );
                }
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
    ) -> Result<Vec<Digest>, Error> {
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
            && self
                .has_backed_link(namespace, digest, blob_index.as_ref(), |link| {
                    matches!(link, LinkKind::Tag(_))
                })
                .await?;

        let manifest = if is_protected || has_tags {
            None
        } else {
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()))
                .await
            {
                // A revision's last pull lives in its access entries.
                Ok(metadata) => {
                    let pulled_at = self
                        .metadata_store
                        .read_revision_access_time(namespace, digest)
                        .await?;
                    Some(ManifestImage::new(
                        None,
                        metadata.created_at,
                        pulled_at,
                        Utc::now(),
                    ))
                }
                Err(_) => None,
            }
        };

        let fate = decide_orphan_fate(
            is_protected,
            has_tags,
            manifest.as_ref(),
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
    ) -> Result<Vec<Digest>, Error> {
        match fate {
            Fate::Skip | Fate::Retain => Ok(Vec::new()),
            Fate::Delete => {
                // Read before the delete reclaims the body naming them.
                let mut unpinned = match read_manifest(&self.blob_store, digest).await? {
                    Some(manifest) => link_plan::unpinned_by_delete(&manifest),
                    None => Vec::new(),
                };
                // Its referrers lose their subject and are judged in this run.
                let referrers: Vec<Digest> = self
                    .metadata_store
                    .stream_referrer_digests(namespace, digest)
                    .try_collect()
                    .await?;
                unpinned.extend(referrers);
                sink.apply(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await?;
                Ok(unpinned)
            }
        }
    }

    /// Pinned by a parent, so reclaimed with it rather than judged on its own:
    /// an index child while the index resolves, or a referrer while its
    /// subject does.
    async fn is_protected(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        blob_index: Option<&BlobIndex>,
    ) -> Result<bool, Error> {
        if self
            .has_backed_link(namespace, digest, blob_index, |link| {
                matches!(link, LinkKind::Manifest { .. } | LinkKind::ReferencedBy(_))
            })
            .await?
        {
            return Ok(true);
        }

        let Some(links) = blob_index.and_then(|index| index.namespace.get(namespace)) else {
            return Ok(false);
        };
        for link in links {
            let LinkKind::Referrer { subject, .. } = link else {
                continue;
            };
            // Bypasses the link cache like `reference_backed`, so a subject
            // deleted elsewhere cannot keep its referrers skipped.
            match self
                .metadata_store
                .read_link_reference(namespace, &LinkKind::Digest(subject.clone()))
                .await
            {
                Ok(_) => return Ok(true),
                Err(RegistryError::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    /// Any index entry matching `predicate` whose backing link still resolves.
    /// Writers never remove reference entries, so a bare membership test would
    /// let stale ones steer retention.
    async fn has_backed_link(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        blob_index: Option<&BlobIndex>,
        predicate: impl Fn(&LinkKind) -> bool,
    ) -> Result<bool, Error> {
        let Some(refs) = blob_index.and_then(|index| index.namespace.get(namespace)) else {
            return Ok(false);
        };
        for link in refs {
            if predicate(link)
                && self
                    .metadata_store
                    .reference_backed(namespace, link, digest)
                    .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs::{File, FileTimes},
        str::FromStr,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration as StdDuration, SystemTime},
    };

    use chrono::{TimeZone, Utc};
    use tokio::time::sleep;
    use url::Url;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use angos_oci::{Digest, Namespace};
    use angos_storage::{
        Error as StorageError, ObjectStore,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use crate::command::prune::checker::*;
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
        jobs::store::{ClaimMode, JobStore},
        policy::{CelRule, RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Registry, RegistryConfig,
            blob_store::BlobStore,
            metadata_store::{BlobIndexOperation, LinkOperation},
            repository_resolver::RepositoryResolver,
            test_utils::{
                self, FSRegistryTestCase, RegistryTestCase, for_each_backend, metadata_store_over,
                put_blob_direct,
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
        pulled: Option<chrono::DateTime<Utc>>,
    ) -> TagWithMetadata {
        TagWithMetadata {
            name: Tag::new(name).unwrap(),
            metadata: LinkMetadata {
                target: dummy_digest(),
                created_at: created,
                media_type: None,
                descriptor: None,
            },
            pulled_at: pulled,
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
                    LinkOperation::create_with_referrer(
                        LinkKind::Manifest {
                            index: index_digest.clone(),
                            child: child_digest.clone(),
                        },
                        child_digest.clone(),
                        index_digest.clone(),
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
                    LinkOperation::delete(LinkKind::Manifest {
                        index: index_digest.clone(),
                        child: child_digest.clone(),
                    }),
                    LinkOperation::delete(LinkKind::Digest(index_digest)),
                ],
            )
            .await
            .unwrap();
    }

    #[test]
    fn rank_by_sorts_descending_by_key() {
        let t1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = [
            tag_with_times("oldest", Some(t1), None),
            tag_with_times("newest", Some(t3), None),
            tag_with_times("middle", Some(t2), None),
        ];

        let ranked = RetentionChecker::rank_by(&tags, |t| t.metadata.created_at);

        assert_eq!(ranked, vec!["newest", "middle", "oldest"]);
    }

    #[test]
    fn rank_by_empty_slice_returns_empty() {
        let ranked = RetentionChecker::rank_by(&[], |t| t.metadata.created_at);
        assert!(ranked.is_empty());
    }

    /// An unranked tag cannot be retained by `top_pushed(n)` / `top_pulled(n)`
    /// on a slot it never earned.
    #[test]
    fn rank_by_drops_tags_without_the_timestamp() {
        let pushed = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let tags = [
            tag_with_times("alpha", None, None),
            tag_with_times("beta", Some(pushed), None),
        ];

        let ranked = RetentionChecker::rank_by(&tags, |t| t.metadata.created_at);

        assert_eq!(ranked, vec!["beta".to_string()]);
    }

    #[test]
    fn build_sorted_rankings_pushed_order() {
        let t1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = vec![
            tag_with_times("old", Some(t1), None),
            tag_with_times("new", Some(t3), None),
        ];

        let last_pushed = RetentionChecker::rank_by(&tags, |t| t.metadata.created_at);
        let last_pulled = RetentionChecker::rank_by(&tags, |t| t.pulled_at);

        assert_eq!(last_pushed[0], "new");
        assert_eq!(last_pushed[1], "old");
        assert!(last_pulled.is_empty());
    }

    /// Without this, `top_pulled(n)` keeps every tag forever in a namespace
    /// holding n tags or fewer, whatever their pull times.
    #[test]
    fn build_sorted_rankings_skips_never_pulled() {
        let pushed = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let pulled = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = vec![
            tag_with_times("never-pulled", Some(pushed), None),
            tag_with_times("pulled", Some(pushed), Some(pulled)),
        ];

        let last_pushed = RetentionChecker::rank_by(&tags, |t| t.metadata.created_at);
        let last_pulled = RetentionChecker::rank_by(&tags, |t| t.pulled_at);

        assert_eq!(last_pushed.len(), 2);
        assert_eq!(last_pulled, vec!["pulled".to_string()]);
    }

    #[test]
    fn build_sorted_rankings_pulled_independent_of_pushed() {
        let pushed_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let pulled_time = Utc.with_ymd_and_hms(2024, 12, 1, 0, 0, 0).unwrap();

        let tags = vec![
            tag_with_times("a", Some(pulled_time), Some(pushed_time)),
            tag_with_times("b", Some(pushed_time), Some(pulled_time)),
        ];

        let last_pushed = RetentionChecker::rank_by(&tags, |t| t.metadata.created_at);
        let last_pulled = RetentionChecker::rank_by(&tags, |t| t.pulled_at);

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
            let scrubber = RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver,
                Some(retention_policy),
            );

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
            let scrubber = RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver,
                None,
            );

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

            let digest = put_blob_direct(metadata_store.object_store(), TEST_MANIFEST).await;

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
            RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver,
                Some(policy),
            )
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

            let digest = put_blob_direct(metadata_store.object_store(), TEST_MANIFEST).await;

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
            RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver,
                None,
            )
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

            let child_digest = put_blob_direct(metadata_store.object_store(), TEST_MANIFEST).await;
            let index_digest = put_blob_direct(metadata_store.object_store(), TEST_INDEX).await;

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
            RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver,
                Some(policy.clone()),
            )
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
            RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver2,
                Some(policy),
            )
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

    /// One `check` collects a tagged index and the child it pinned.
    async fn assert_index_and_child_collected_in_one_pass(test_case: &dyn RegistryTestCase) {
        let namespace = Namespace::new("test-repo/app").unwrap();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let child_digest = test_utils::put_blob_body(&blob_store, TEST_MANIFEST).await;
        let index_digest = test_utils::put_blob_body(&blob_store, TEST_INDEX).await;
        assert!(
            child_digest.to_string() < index_digest.to_string(),
            "fixture must keep the child ahead of the index in the listing"
        );

        setup_index_scenario(&metadata_store, &namespace, &index_digest, &child_digest).await;

        // Retains nothing, so the tag goes, then the index, then the child.
        let policy = Arc::new(RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        ));

        let executor = make_executor(blob_store.clone(), metadata_store.clone());
        let resolver = Arc::new(
            RepositoryResolver::new(test_utils::create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        RetentionChecker::new(blob_store, metadata_store.clone(), resolver, Some(policy))
            .check(&namespace, &executor)
            .await
            .unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(index_digest))
                .await
                .is_err(),
            "the untagged index must be collected"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(child_digest))
                .await
                .is_err(),
            "the child must be collected in the same pass, not left for the next run"
        );
    }

    /// The child sorts before the index in the revision listing, the ordering
    /// that risks leaving the child for a second run.
    #[tokio::test]
    async fn index_child_collected_in_the_same_pass() {
        for_each_backend(async |test_case| {
            assert_index_and_child_collected_in_one_pass(test_case).await;
        })
        .await;
    }

    /// The same on separate roots, which `for_each_backend` shares and so
    /// cannot catch a read of the wrong store.
    #[tokio::test]
    async fn index_child_collected_in_the_same_pass_across_split_backends() {
        assert_index_and_child_collected_in_one_pass(&FSRegistryTestCase::with_split_backends())
            .await;
    }

    /// A referrer body naming `subject`, salted until its digest lists before
    /// the subject's: the ordering that leaves it for a second run unless the
    /// subject's deletion requeues it.
    fn referrer_body_listing_before(subject: &Digest) -> Vec<u8> {
        let mut salt = 0u32;
        loop {
            let body = format!(
                r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","artifactType":"application/vnd.example.report+json","config":{{"mediaType":"application/vnd.oci.empty.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0}},"layers":[],"subject":{{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"{subject}","size":0}},"annotations":{{"salt":"{salt}"}}}}"#
            );
            if Digest::sha256_of_bytes(&body).to_string() < subject.to_string() {
                return body.into_bytes();
            }
            salt += 1;
        }
    }

    /// Records `subject` and `referrer` as revisions, the referrer's back-link
    /// on `subject`, and `tag` on the digest it names.
    async fn setup_referrer_scenario(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
        tag: Option<(&str, &Digest)>,
    ) {
        let mut ops = vec![
            LinkOperation::create(LinkKind::Digest(subject.clone()), subject.clone()),
            LinkOperation::create(LinkKind::Digest(referrer.clone()), referrer.clone()),
            LinkOperation::create(
                LinkKind::Referrer {
                    subject: subject.clone(),
                    referrer: referrer.clone(),
                },
                referrer.clone(),
            ),
        ];
        if let Some((tag, target)) = tag {
            ops.push(LinkOperation::create(
                LinkKind::Tag(Tag::new(tag).unwrap()),
                target.clone(),
            ));
        }
        metadata_store.update_links(namespace, &ops).await.unwrap();
    }

    fn keep_tagged_checker(test_case: &dyn RegistryTestCase) -> RetentionChecker {
        let policy = Arc::new(RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag != null").unwrap()],
            },
            Arc::new(SystemClock),
        ));
        let resolver = Arc::new(
            RepositoryResolver::new(test_utils::create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        RetentionChecker::new(
            test_case.blob_store(),
            test_case.metadata_store(),
            resolver,
            Some(policy),
        )
    }

    /// An untagged report follows its tagged subject instead of being judged
    /// as untagged content of its own.
    #[tokio::test]
    async fn referrer_of_a_tagged_subject_survives_a_keep_tagged_policy() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let subject = test_utils::put_blob_body(&blob_store, TEST_MANIFEST).await;
            let referrer =
                test_utils::put_blob_body(&blob_store, &referrer_body_listing_before(&subject))
                    .await;
            setup_referrer_scenario(
                &metadata_store,
                &namespace,
                &subject,
                &referrer,
                Some(("latest", &subject)),
            )
            .await;

            let executor = make_executor(blob_store, metadata_store.clone());
            keep_tagged_checker(test_case)
                .check(&namespace, &executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(referrer))
                    .await
                    .is_ok(),
                "the report of a live tagged image must survive"
            );
        })
        .await;
    }

    /// A tagged referrer, such as a cosign fallback tag, no longer pins the
    /// untagged image it signs.
    #[tokio::test]
    async fn referrer_does_not_pin_its_subject() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let subject = test_utils::put_blob_body(&blob_store, TEST_MANIFEST).await;
            let referrer =
                test_utils::put_blob_body(&blob_store, &referrer_body_listing_before(&subject))
                    .await;
            setup_referrer_scenario(
                &metadata_store,
                &namespace,
                &subject,
                &referrer,
                Some(("sha256-abc.sig", &referrer)),
            )
            .await;

            let executor = make_executor(blob_store, metadata_store.clone());
            keep_tagged_checker(test_case)
                .check(&namespace, &executor)
                .await
                .unwrap();

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(subject))
                    .await
                    .is_err(),
                "an untagged image is judged by the rules whatever refers to it"
            );
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(referrer))
                    .await
                    .is_ok(),
                "the tagged referrer is retained by the rules"
            );
        })
        .await;
    }

    /// One `check` collects an untagged subject and the referrer it pinned,
    /// with the referrer listed first so only the requeue can reach it.
    async fn assert_subject_and_referrer_collected_in_one_pass(test_case: &dyn RegistryTestCase) {
        let namespace = Namespace::new("test-repo/app").unwrap();
        let metadata_store = test_case.metadata_store();
        let blob_store = test_case.blob_store();

        let subject = test_utils::put_blob_body(&blob_store, TEST_MANIFEST).await;
        let referrer =
            test_utils::put_blob_body(&blob_store, &referrer_body_listing_before(&subject)).await;
        setup_referrer_scenario(&metadata_store, &namespace, &subject, &referrer, None).await;

        let executor = make_executor(blob_store, metadata_store.clone());
        keep_tagged_checker(test_case)
            .check(&namespace, &executor)
            .await
            .unwrap();

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(subject))
                .await
                .is_err(),
            "the untagged subject must be collected"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(referrer))
                .await
                .is_err(),
            "the referrer must be collected in the same pass, not left for the next run"
        );
    }

    #[tokio::test]
    async fn subject_and_referrer_collected_in_the_same_pass() {
        for_each_backend(async |test_case| {
            assert_subject_and_referrer_collected_in_one_pass(test_case).await;
        })
        .await;
    }

    #[tokio::test]
    async fn subject_and_referrer_collected_in_the_same_pass_across_split_backends() {
        assert_subject_and_referrer_collected_in_one_pass(
            &FSRegistryTestCase::with_split_backends(),
        )
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
        let scrubber = RetentionChecker::new(
            test_case.blob_store(),
            metadata_store.clone(),
            resolver,
            Some(policy),
        );

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
            max_retries: Some(0),
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
        let job_store = Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "retention-test",
            ClaimMode::Atomic,
        ));
        let registry = Registry::new(
            test_case.blob_store(),
            metadata_store.clone(),
            resolver.clone(),
            RegistryConfig {
                event_dispatcher: Some(Arc::new(dispatcher)),
                ..RegistryConfig::new(job_store.clone())
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
        RetentionChecker::new(
            test_case.blob_store(),
            metadata_store.clone(),
            resolver,
            Some(policy),
        )
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

            // First revision: deleted blob, so the executor hits a missing one.
            let digest_missing =
                put_blob_direct(metadata_store.object_store(), TEST_MANIFEST).await;
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

            let digest_healthy = put_blob_direct(metadata_store.object_store(), TEST_INDEX).await;
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
            RetentionChecker::new(
                test_case.blob_store(),
                metadata_store.clone(),
                resolver,
                Some(policy),
            )
            .check(&namespace, &executor)
            .await
            .unwrap();

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
        ManifestImage::new(Some(tag.to_string()), None, None, DateTime::UNIX_EPOCH)
    }

    #[test]
    fn check_global_policy_returns_no_opinion_when_policy_absent() {
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_policy(None, &manifest, &[], &[]).unwrap();
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
        let result = check_policy(Some(&policy), &manifest, &[], &[]).unwrap();
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
        let result = check_policy(Some(&policy), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Delete);
    }

    fn make_repo(name: &str, rules: Vec<CelRule>) -> Repository {
        Repository {
            name: Namespace::new(name).unwrap(),
            namespace: None,
            upstreams: Vec::new(),
            replication: Vec::new(),
            retention_policy: RetentionPolicy::new(
                &RetentionPolicyConfig { rules },
                Arc::new(SystemClock),
            ),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            scan: false,
            index: false,
        }
    }

    #[test]
    fn check_repo_policy_returns_no_opinion_when_repository_absent() {
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_policy(repository_policy(None), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_repo_policy_returns_no_opinion_when_repo_has_no_rules() {
        let repo = make_repo("r", vec![]);
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_policy(repository_policy(Some(&repo)), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::NoOpinion);
    }

    #[test]
    fn check_repo_policy_returns_retain_when_repo_policy_keeps() {
        let repo = make_repo("r", vec![CelRule::compile("image.tag == 'v1'").unwrap()]);
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_policy(repository_policy(Some(&repo)), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Retain);
    }

    #[test]
    fn check_repo_policy_returns_delete_when_repo_policy_drops() {
        let repo = make_repo(
            "r",
            vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
        );
        let manifest = make_manifest(&Tag::new("v1").unwrap());
        let result = check_policy(repository_policy(Some(&repo)), &manifest, &[], &[]).unwrap();
        assert_eq!(result, PolicyDecision::Delete);
    }

    /// An image with no push or pull time, the shape a revision record with
    /// no `created_at` resolves to.
    fn dummy_image() -> ManifestImage {
        ManifestImage::new(None, None, None, Utc::now())
    }

    #[test]
    fn decide_orphan_fate_skips_protected_manifest() {
        let manifest = dummy_image();
        let fate = decide_orphan_fate(true, false, Some(&manifest), None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Skip);
    }

    #[test]
    fn decide_orphan_fate_skips_manifest_with_tags() {
        let manifest = dummy_image();
        let fate = decide_orphan_fate(false, true, Some(&manifest), None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Skip);
    }

    #[test]
    fn decide_orphan_fate_skips_when_metadata_missing() {
        let fate = decide_orphan_fate(false, false, None, None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Skip);
    }

    #[test]
    fn decide_orphan_fate_retains_when_no_policies_apply() {
        let manifest = dummy_image();
        let fate = decide_orphan_fate(false, false, Some(&manifest), None, None, &[], &[]).unwrap();
        assert_eq!(fate, Fate::Retain);
    }

    #[test]
    fn decide_orphan_fate_retains_unknown_push_time_under_age_policy() {
        // An age rule must not read a missing `created_at` as the Unix epoch
        // and delete recent content.
        let manifest = dummy_image();
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.pushed_at > now() - days(30)").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let fate = decide_orphan_fate(false, false, Some(&manifest), None, Some(&policy), &[], &[])
            .unwrap();
        assert_eq!(
            fate,
            Fate::Retain,
            "content with an unknown push time must not be age-deleted"
        );
    }

    #[test]
    fn decide_orphan_fate_retains_when_global_policy_keeps() {
        let manifest = dummy_image();
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == null").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let fate = decide_orphan_fate(false, false, Some(&manifest), None, Some(&policy), &[], &[])
            .unwrap();
        assert_eq!(fate, Fate::Retain);
    }

    #[test]
    fn decide_orphan_fate_deletes_when_all_applicable_policies_drop() {
        let manifest = dummy_image();
        let policy = RetentionPolicy::new(
            &RetentionPolicyConfig {
                rules: vec![CelRule::compile("image.tag == 'keep-me'").unwrap()],
            },
            Arc::new(SystemClock),
        );
        let fate = decide_orphan_fate(false, false, Some(&manifest), None, Some(&policy), &[], &[])
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
        let blob =
            put_blob_direct(metadata_store.object_store(), b"granted-but-unreferenced").await;
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

    /// A cross-repository mount grants ownership of bytes that may be weeks
    /// old, so the window that shields an in-flight push has to be measured on
    /// the grant. Measured on the bytes it never fires, and a deleting policy
    /// revokes the mount mid-push.
    #[tokio::test]
    async fn a_fresh_grant_over_old_bytes_stays_inside_the_in_flight_window() {
        let test_case = FSRegistryTestCase::new();
        let namespace = Namespace::new("test-repo/mounted").unwrap();
        let metadata_store = test_case.metadata_store();
        let blob = seed_grant_only_blob(&test_case, &namespace).await;

        // Age the bytes a week; the grant naming them was written just now.
        let week_ago = SystemTime::now() - StdDuration::from_hours(24 * 7);
        File::options()
            .write(true)
            .open(test_case.temp_dir().path().join(blob.blob_path()))
            .unwrap()
            .set_times(FileTimes::new().set_modified(week_ago))
            .unwrap();

        sweep_orphan_grants(
            &test_case.blob_store(),
            &metadata_store,
            &grant_resolver(),
            Some(&keep_nothing_policy()),
            chrono::Duration::days(1),
            &Executor::new_for_test(test_case.blob_store(), metadata_store.clone()),
            4,
        )
        .await
        .unwrap();

        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &blob)
                .await
                .is_ok(),
            "a grant written inside the in-flight window must survive, however old its bytes"
        );
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

            // Just-written bytes: `pushed_at > now() - days(1)` retains them
            // even though the in-flight window is zero.
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

    /// Records the peak number of overlapping per-tag metadata reads, holding
    /// each one open long enough for the whole buffered batch to pile up.
    struct PeakTagLinkReads {
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreHook for PeakTagLinkReads {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            // A tag resolves from an entry listing and its last pull from an
            // atime listing, so the reads to bound are lists, not gets.
            let StoreOp::List { prefix } = op else {
                return Ok(());
            };
            if !(prefix.contains("!tag/") || prefix.contains("!atime/")) {
                return Ok(());
            }
            let overlapping = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(overlapping, Ordering::SeqCst);
            sleep(StdDuration::from_millis(20)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// The namespace walk is already parallel, so deriving this fan-out from
    /// the same `--concurrency` would square the in-flight reads.
    #[tokio::test]
    async fn tag_metadata_reads_stay_within_the_fixed_fan_out() {
        let case = FSRegistryTestCase::new();
        let namespace = Namespace::new("test-repo/fanout").unwrap();
        let links: Vec<LinkOperation> = (0..TAG_METADATA_CONCURRENCY * 3)
            .map(|i| {
                LinkOperation::create(
                    LinkKind::Tag(Tag::new(&format!("v{i}")).unwrap()),
                    dummy_digest(),
                )
            })
            .collect();
        case.metadata_store()
            .update_links(&namespace, &links)
            .await
            .unwrap();

        let peak = Arc::new(AtomicUsize::new(0));
        let inner: Arc<dyn ObjectStore> = case.metadata_store().object_store().clone();
        let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
            inner,
            PeakTagLinkReads {
                in_flight: Arc::new(AtomicUsize::new(0)),
                peak: peak.clone(),
            },
        ));
        let checker = RetentionChecker::new(
            case.blob_store(),
            metadata_store_over(hooked),
            Arc::new(RepositoryResolver::new(Arc::new(HashMap::new())).unwrap()),
            None,
        );

        let tags = checker.fetch_tag_metadata(&namespace).await.unwrap();

        assert_eq!(tags.len(), links.len(), "every tag must be read");
        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak > 1,
            "the reads must overlap, otherwise the bound is vacuous, got {peak}"
        );
        assert!(
            peak <= TAG_METADATA_CONCURRENCY,
            "tag reads must stay within the fixed fan-out, got {peak}"
        );
    }
}
