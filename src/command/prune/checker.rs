use std::collections::VecDeque;
use std::pin::pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures_util::{StreamExt, TryStreamExt};
use tracing::{debug, error};

use angos_oci::{Content, Digest, Namespace};

use crate::command::maintenance::Error;
use crate::command::maintenance::action::Action;
use crate::command::maintenance::check::NamespaceChecker;
use crate::command::maintenance::executor::{ActionSink, object_younger_than_grace};
use crate::command::maintenance::tags::Rankings;
use crate::policy::{ManifestImage, RetentionPolicy};
use crate::registry::blob_store::BlobStore;
use crate::registry::keys::DigestExt;
use crate::registry::manifest::read_manifest;
use crate::registry::metadata_store::{BlobIndex, LinkKind, MetadataStore};
use crate::registry::repository_resolver::RepositoryResolver;
use crate::registry::{Error as RegistryError, Repository};
use crate::scan::{is_angos_report, scan_reports};

pub struct RetentionChecker {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    global_retention_policy: Option<Arc<RetentionPolicy>>,
}

/// Whether the global and repository policies keep `image`: either retaining
/// wins, and with neither having an opinion (absent, or a repository policy
/// without rules) the image stays.
fn retained(
    global: Option<&RetentionPolicy>,
    repository: Option<&Repository>,
    image: &ManifestImage,
    last_pushed: &[String],
    last_pulled: &[String],
) -> Result<bool, Error> {
    let global = global
        .map(|policy| policy.should_retain(image, last_pushed, last_pulled))
        .transpose()?;
    let repository = repository
        .map(|repository| &repository.retention_policy)
        .filter(|policy| policy.has_rules())
        .map(|policy| policy.should_retain(image, last_pushed, last_pulled))
        .transpose()?;
    Ok(match (global, repository) {
        (None, None) => true,
        (global, repository) => global == Some(true) || repository == Some(true),
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
    let grant = LinkKind::Blob(blob.clone());
    for (namespace, links) in index {
        // An unresolved namespace is being cleared by the orphan-namespace
        // sweep, so revoke its grants outright; the executor's re-check still
        // spares a blob a not-yet-cascaded manifest references.
        let repository = ctx.resolver.resolve(&namespace);
        if repository.is_none() && !ctx.resolver.is_empty() {
            ctx.sink
                .apply(Action::RemoveOrphanBlobGrant {
                    namespace,
                    blob: blob.clone(),
                })
                .await?;
            continue;
        }
        // A manifest still referencing the blob keeps the grant live; only a
        // grant-only namespace is a retention subject.
        if !links.contains(&grant)
            || ctx
                .metadata_store
                .manifest_references_live(&namespace, blob, &links)
                .await?
        {
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
        let subject = ManifestImage::new(&namespace, None, Some(last_modified), None, ctx.now);
        if retained(ctx.global_policy, repository, &subject, &[], &[])? {
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

#[async_trait]
impl NamespaceChecker for RetentionChecker {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
        debug!("Checking retention policies on '{namespace}'");

        let rankings = Rankings::read(&self.metadata_store, namespace).await?;
        let repository = self.resolver.resolve(namespace);
        for tag in &rankings.tags {
            let image = ManifestImage::new(
                namespace,
                Some(tag.name.to_string()),
                tag.metadata.created_at,
                tag.pulled_at,
                Utc::now(),
            );
            match retained(
                self.global_retention_policy.as_deref(),
                repository,
                &image,
                &rankings.last_pushed,
                &rankings.last_pulled,
            ) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    error!(
                        "Retention evaluation failed for '{namespace}:{}', retaining: {e}",
                        tag.name
                    );
                    continue;
                }
            }
            let action = Action::DeleteTag {
                namespace: namespace.clone(),
                tag: tag.name.clone(),
                // What the tag resolved to when it was judged: a re-push
                // between here and the delete must not be swept away.
                target: Some(tag.metadata.target.clone()),
            };
            // A failed deletion (or its required event delivery) leaves the
            // tag for the next run.
            if let Err(e) = sink.apply(action).await {
                error!(
                    "Failed to delete tag '{namespace}:{}' for retention: {e}",
                    tag.name
                );
            }
        }

        self.emit_delete_orphan_manifests(namespace, &rankings, sink)
            .await
    }
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

    /// Collects orphan revisions, requeueing what each delete unpins so one
    /// skipped while still pinned is revisited in the same run. Requeues
    /// cannot loop, since a digest names its children by content hash.
    async fn emit_delete_orphan_manifests(
        &self,
        namespace: &Namespace,
        rankings: &Rankings,
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
                .process_orphan_revision(namespace, &digest, rankings, sink)
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
        rankings: &Rankings,
        sink: &dyn ActionSink,
    ) -> Result<Vec<Digest>, Error> {
        // A missing blob index means "no links"; any other read failure must
        // propagate rather than pass for "unprotected" on a delete path.
        let blob_index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => Some(index),
            Err(RegistryError::NotFound) => None,
            Err(e) => return Err(e.into()),
        };
        if self
            .is_protected(namespace, digest, blob_index.as_ref())
            .await?
        {
            debug!("Skipping protected manifest '{namespace}@{digest}'");
            return Ok(Vec::new());
        }
        if self
            .has_backed_link(namespace, digest, blob_index.as_ref(), |link| {
                matches!(link, LinkKind::Tag(_))
            })
            .await?
        {
            return Ok(Vec::new());
        }
        let revision = LinkKind::Digest(digest.clone());
        let Ok(metadata) = self.metadata_store.read_link(namespace, &revision).await else {
            return Ok(Vec::new());
        };
        // A revision's last pull lives in its access entries.
        let pulled_at = self
            .metadata_store
            .read_access_time(namespace, &revision)
            .await?;
        let image = ManifestImage::new(namespace, None, metadata.created_at, pulled_at, Utc::now());
        if retained(
            self.global_retention_policy.as_deref(),
            self.resolver.resolve(namespace),
            &image,
            &rankings.last_pushed,
            &rankings.last_pulled,
        )? {
            return Ok(Vec::new());
        }

        // Read before the delete reclaims the body naming them: an index's
        // children and a subject lose the pin it held.
        let mut unpinned: Vec<Digest> = match read_manifest(&self.blob_store, digest).await? {
            Some(manifest) => {
                let children = match &manifest.content {
                    Content::Index { manifests } => manifests.as_slice(),
                    Content::Image { .. } => &[],
                };
                children
                    .iter()
                    .chain(manifest.subject.as_ref())
                    .map(|descriptor| descriptor.digest.clone())
                    .collect()
            }
            None => Vec::new(),
        };
        // Its referrers lose their subject and are judged in this run.
        let referrers: Vec<Digest> = self
            .metadata_store
            .stream_referrer_digests(namespace, digest, None)
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

    /// Pinned by a parent, so reclaimed with it rather than judged on its own:
    /// an index child while the index resolves, or a referrer while its
    /// subject does. A scan report a newer angos report has superseded is the
    /// exception: its subject lives on, and the rules judge the old report
    /// like any untagged manifest.
    async fn is_protected(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        blob_index: Option<&BlobIndex>,
    ) -> Result<bool, Error> {
        if self
            .has_backed_link(namespace, digest, blob_index, |link| {
                matches!(link, LinkKind::ReferencedBy(_))
            })
            .await?
        {
            return Ok(true);
        }

        let Some(links) = blob_index.and_then(|index| index.get(namespace)) else {
            return Ok(false);
        };
        for link in links {
            let LinkKind::Referrer { subject, .. } = link else {
                continue;
            };
            match self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(subject.clone()))
                .await
            {
                Ok(_) => {
                    if !self
                        .is_superseded_report(namespace, subject, digest)
                        .await?
                    {
                        return Ok(true);
                    }
                }
                Err(RegistryError::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }

    /// Whether `referrer` is a scan report angos attached to `subject` that a
    /// newer one has superseded. Its own record says whether it is a report
    /// at all, so only a report pays for listing its siblings.
    async fn is_superseded_report(
        &self,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<bool, Error> {
        let link = LinkKind::Referrer {
            subject: subject.clone(),
            referrer: referrer.clone(),
        };
        let is_report = match self.metadata_store.read_link(namespace, &link).await {
            Ok(metadata) => metadata
                .descriptor
                .is_some_and(|descriptor| is_angos_report(&descriptor)),
            Err(RegistryError::NotFound) => false,
            Err(e) => return Err(e.into()),
        };
        if !is_report {
            return Ok(false);
        }
        let reports = scan_reports(&self.metadata_store, namespace, subject).await?;
        Ok(reports
            .iter()
            .skip(1)
            .any(|report| report.digest == *referrer))
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
        let Some(refs) = blob_index.and_then(|index| index.get(namespace)) else {
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
    use std::collections::HashMap;
    use std::fs::{File, FileTimes};
    use std::str::FromStr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration as StdDuration, SystemTime};

    use chrono::{TimeZone, Utc};
    use serde_json::Value;
    use tokio::time::sleep;
    use url::Url;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use angos_mtls_client::ClientTls;
    use angos_oci::{Digest, Namespace, Tag};
    use angos_storage::test_util::{HookedStore, StoreHook, StoreOp};
    use angos_storage::{Error as StorageError, ObjectStore};

    use super::*;
    use crate::command::maintenance::action::Action;
    use crate::command::maintenance::executor::{Executor, RETENTION_ACTOR};
    use crate::command::maintenance::tags::{TAG_METADATA_CONCURRENCY, TagWithMetadata, rank_by};
    use crate::command::maintenance::test_helper::test_executor;
    use crate::event_webhook::config::{DeliveryPolicy, EventWebhookConfig};
    use crate::event_webhook::dispatcher::EventDispatcher;
    use crate::event_webhook::event::EventKind;
    use crate::jobs::store::{ClaimMode, JobStore};
    use crate::policy::{CelRule, RetentionPolicy};
    use crate::registry::metadata_store::LinkMetadata;
    use crate::registry::repository_resolver::RepositoryResolver;
    use crate::registry::test_utils::{
        self, FSRegistryTestCase, RegistryTestCase, angos_report, drop_links, for_each_backend,
        metadata_store_over, put_blob_direct, seed_links,
    };
    use crate::registry::{Registry, RegistryConfig};

    fn dummy_digest() -> Digest {
        Digest::from_str("sha256:0000000000000000000000000000000000000000000000000000000000000000")
            .unwrap()
    }

    fn tag_with_times(
        name: &str,
        created: Option<DateTime<Utc>>,
        pulled: Option<DateTime<Utc>>,
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

    async fn setup_index_scenario(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        index_digest: &Digest,
        child_digest: &Digest,
    ) {
        seed_links(
            metadata_store,
            namespace,
            &[
                (LinkKind::Digest(child_digest.clone()), child_digest.clone()),
                (LinkKind::Digest(index_digest.clone()), index_digest.clone()),
                (
                    LinkKind::Tag(Tag::new("latest").unwrap()),
                    index_digest.clone(),
                ),
            ],
        )
        .await
        .unwrap();
        metadata_store
            .pin_references(
                namespace,
                &[(
                    child_digest.clone(),
                    LinkKind::ReferencedBy(index_digest.clone()),
                )],
            )
            .await
            .unwrap();
    }

    async fn teardown_index_scenario(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        index_digest: Digest,
    ) {
        drop_links(
            metadata_store,
            namespace,
            &[
                LinkKind::Tag(Tag::new("latest").unwrap()),
                LinkKind::ReferencedBy(index_digest.clone()),
                LinkKind::Digest(index_digest),
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

        let ranked = rank_by(&tags, |t| t.metadata.created_at);

        assert_eq!(ranked, vec!["newest", "middle", "oldest"]);
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

        let ranked = rank_by(&tags, |t| t.metadata.created_at);

        assert_eq!(ranked, vec!["beta".to_string()]);
    }

    const TEST_MANIFEST: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","size":0},"layers":[]}"#;

    const TEST_INDEX: &[u8] = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:1fc08b525237c75b560cf0b8ab766fc363d4e5ff1537f4f3ae28a49ade78938b","size":0}]}"#;

    const KEEP_NOTHING: &str = "image.tag == 'keep-me'";
    const KEEP_TAGGED: &str = "image.tag != null";

    fn policy(rule: &str) -> RetentionPolicy {
        RetentionPolicy::new(vec![CelRule::compile(rule).unwrap()])
    }

    fn resolver() -> Arc<RepositoryResolver> {
        Arc::new(RepositoryResolver::new(test_utils::create_test_repositories()).unwrap())
    }

    /// A checker over the test repositories with `rule` as its global policy.
    fn checker(test_case: &dyn RegistryTestCase, rule: Option<&str>) -> RetentionChecker {
        RetentionChecker::new(
            test_case.blob_store(),
            test_case.metadata_store(),
            resolver(),
            rule.map(|rule| Arc::new(policy(rule))),
        )
    }

    /// Runs `checker` over `namespace` through a real executor.
    async fn run(test_case: &dyn RegistryTestCase, rule: Option<&str>, namespace: &Namespace) {
        let executor = test_executor(test_case.blob_store(), test_case.metadata_store());
        checker(test_case, rule)
            .check(namespace, &executor)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tag_is_retained_by_a_matching_rule_or_no_policy() {
        for_each_backend(async |test_case| {
            let namespace = &Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let (blob_digest, _) =
                test_utils::create_test_blob(test_case.registry(), namespace, b"test manifest")
                    .await;
            let tag = LinkKind::Tag(Tag::new("v1.0.0").unwrap());
            seed_links(&metadata_store, namespace, &[(tag.clone(), blob_digest)])
                .await
                .unwrap();

            for rule in [Some("top_pushed(10)"), None] {
                run(test_case, rule, namespace).await;
                assert!(
                    metadata_store.read_link(namespace, &tag).await.is_ok(),
                    "{rule:?} must retain the tag"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn orphan_manifest_is_kept_without_policy_and_deleted_with_one() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let digest = put_blob_direct(&test_case.blob_store(), TEST_MANIFEST).await;
            let revision = LinkKind::Digest(digest.clone());
            seed_links(&metadata_store, &namespace, &[(revision.clone(), digest)])
                .await
                .unwrap();

            run(test_case, None, &namespace).await;
            assert!(
                metadata_store
                    .read_link(&namespace, &revision)
                    .await
                    .is_ok()
            );

            run(test_case, Some(KEEP_TAGGED), &namespace).await;
            assert!(
                metadata_store
                    .read_link(&namespace, &revision)
                    .await
                    .is_err()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_index_child_manifest_protected() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();

            let child_digest = put_blob_direct(&test_case.blob_store(), TEST_MANIFEST).await;
            let index_digest = put_blob_direct(&test_case.blob_store(), TEST_INDEX).await;

            setup_index_scenario(&metadata_store, &namespace, &index_digest, &child_digest).await;
            let child = LinkKind::Digest(child_digest);

            run(test_case, Some(KEEP_TAGGED), &namespace).await;
            assert!(metadata_store.read_link(&namespace, &child).await.is_ok());

            teardown_index_scenario(&metadata_store, &namespace, index_digest).await;

            run(test_case, Some(KEEP_TAGGED), &namespace).await;
            assert!(metadata_store.read_link(&namespace, &child).await.is_err());
        })
        .await;
    }

    /// One `check` collects a tagged index and the child it pinned, though the
    /// child sorts before the index in the revision listing.
    #[tokio::test]
    async fn index_child_collected_in_the_same_pass() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let child_digest = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let index_digest = put_blob_direct(&blob_store, TEST_INDEX).await;
            assert!(
                child_digest.to_string() < index_digest.to_string(),
                "fixture must keep the child ahead of the index in the listing"
            );
            setup_index_scenario(&metadata_store, &namespace, &index_digest, &child_digest).await;

            // Retains nothing, so the tag goes, then the index, then the child.
            run(test_case, Some(KEEP_NOTHING), &namespace).await;

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
        })
        .await;
    }

    /// A child the index delete requeues is skipped when the namespace holds
    /// no revision record for it.
    #[tokio::test]
    async fn a_requeued_child_without_a_revision_record_is_skipped() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let blob_store = test_case.blob_store();
            let child_digest = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let index_digest = put_blob_direct(&blob_store, TEST_INDEX).await;
            seed_links(
                &test_case.metadata_store(),
                &namespace,
                &[(LinkKind::Digest(index_digest.clone()), index_digest.clone())],
            )
            .await
            .unwrap();

            let sink: Mutex<Vec<Action>> = Mutex::new(Vec::new());
            checker(test_case, Some(KEEP_NOTHING))
                .check(&namespace, &sink)
                .await
                .unwrap();

            let deleted: Vec<Digest> = sink
                .lock()
                .unwrap()
                .iter()
                .filter_map(|action| match action {
                    Action::DeleteOrphanManifest { digest, .. } => Some(digest.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                deleted,
                [index_digest],
                "child {child_digest} must be skipped"
            );
        })
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
            (LinkKind::Digest(subject.clone()), subject.clone()),
            (LinkKind::Digest(referrer.clone()), referrer.clone()),
            (
                LinkKind::Referrer {
                    subject: subject.clone(),
                    referrer: referrer.clone(),
                },
                referrer.clone(),
            ),
        ];
        if let Some((tag, target)) = tag {
            ops.push((LinkKind::Tag(Tag::new(tag).unwrap()), target.clone()));
        }
        seed_links(metadata_store, namespace, &ops).await.unwrap();
    }

    /// An untagged report follows its tagged subject instead of being judged
    /// as untagged content of its own.
    #[tokio::test]
    async fn referrer_of_a_tagged_subject_survives_a_keep_tagged_policy() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let subject = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let referrer =
                put_blob_direct(&blob_store, &referrer_body_listing_before(&subject)).await;
            setup_referrer_scenario(
                &metadata_store,
                &namespace,
                &subject,
                &referrer,
                Some(("latest", &subject)),
            )
            .await;

            run(test_case, Some(KEEP_TAGGED), &namespace).await;

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

    /// Of a live image's scan reports only the newest stays shielded; an
    /// older one is judged by the rules like any untagged manifest.
    #[tokio::test]
    async fn a_superseded_scan_report_is_judged_by_the_rules() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let subject = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let mut reports = Vec::new();
            for created in ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"] {
                let (body, descriptor) = angos_report(&subject, created);
                let report = put_blob_direct(&blob_store, &body).await;
                // The record is create-if-absent, so the descriptor lands
                // before the scenario's bare one.
                metadata_store
                    .put_referrer(&namespace, &subject, &report, Some(&descriptor))
                    .await
                    .unwrap();
                setup_referrer_scenario(
                    &metadata_store,
                    &namespace,
                    &subject,
                    &report,
                    Some(("latest", &subject)),
                )
                .await;
                reports.push(report);
            }

            run(test_case, Some(KEEP_TAGGED), &namespace).await;

            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(reports[0].clone()))
                    .await
                    .is_err(),
                "the superseded report is untagged content the rules reclaim"
            );
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(reports[1].clone()))
                    .await
                    .is_ok(),
                "the newest report follows its live image"
            );
            assert!(
                metadata_store
                    .read_link(&namespace, &LinkKind::Digest(subject))
                    .await
                    .is_ok(),
                "the tagged image is retained"
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

            let subject = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let referrer =
                put_blob_direct(&blob_store, &referrer_body_listing_before(&subject)).await;
            setup_referrer_scenario(
                &metadata_store,
                &namespace,
                &subject,
                &referrer,
                Some(("sha256-abc.sig", &referrer)),
            )
            .await;

            run(test_case, Some(KEEP_TAGGED), &namespace).await;

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
    #[tokio::test]
    async fn subject_and_referrer_collected_in_the_same_pass() {
        for_each_backend(async |test_case| {
            let namespace = Namespace::new("test-repo/app").unwrap();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let subject = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let referrer =
                put_blob_direct(&blob_store, &referrer_body_listing_before(&subject)).await;
            setup_referrer_scenario(&metadata_store, &namespace, &subject, &referrer, None).await;

            run(test_case, Some(KEEP_TAGGED), &namespace).await;

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
        })
        .await;
    }

    #[tokio::test]
    async fn test_delete_tag_action_emitted_without_storage_mutation() {
        let test_case = FSRegistryTestCase::new();
        let namespace = &Namespace::new("test-repo/app").unwrap();
        let metadata_store = test_case.metadata_store();
        let (blob_digest, _) =
            test_utils::create_test_blob(test_case.registry(), namespace, b"test manifest").await;
        let tag = LinkKind::Tag(Tag::new("v0.0.1").unwrap());
        seed_links(&metadata_store, namespace, &[(tag.clone(), blob_digest)])
            .await
            .unwrap();

        let sink: Mutex<Vec<Action>> = Mutex::new(Vec::new());
        checker(&test_case, Some(KEEP_NOTHING))
            .check(namespace, &sink)
            .await
            .unwrap();

        assert!(
            sink.lock()
                .unwrap()
                .iter()
                .any(|a| matches!(a, Action::DeleteTag { .. })),
            "Vec sink must capture a DeleteTag action"
        );
        assert!(
            metadata_store.read_link(namespace, &tag).await.is_ok(),
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
            tls: ClientTls::default(),
        };
        let mut webhooks = HashMap::new();
        webhooks.insert("retention-hook".to_string(), webhook);
        let dispatcher =
            EventDispatcher::new(webhooks, vec!["retention-hook".to_string()], HashMap::new())
                .expect("dispatcher build");

        let test_case = FSRegistryTestCase::new();
        let namespace = &Namespace::new("test-repo/app").unwrap();
        let metadata_store = test_case.metadata_store();

        let (blob_digest, _) =
            test_utils::create_test_blob(test_case.registry(), namespace, b"retained content")
                .await;
        seed_links(
            &metadata_store,
            namespace,
            &[(
                LinkKind::Tag(Tag::new("v0.0.1").unwrap()),
                blob_digest.clone(),
            )],
        )
        .await
        .unwrap();

        let resolver = resolver();
        let job_store = Arc::new(JobStore::new(
            &metadata_store,
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
        let executor = Executor::new(
            test_case.blob_store(),
            metadata_store.clone(),
            job_store,
            Some(registry),
        );

        RetentionChecker::new(
            test_case.blob_store(),
            metadata_store.clone(),
            resolver,
            Some(Arc::new(policy(KEEP_NOTHING))),
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
        let events: Vec<Value> = requests
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
            let digest_missing = put_blob_direct(&blob_store, TEST_MANIFEST).await;
            let digest_healthy = put_blob_direct(&blob_store, TEST_INDEX).await;
            seed_links(
                &metadata_store,
                &namespace,
                &[
                    (
                        LinkKind::Digest(digest_missing.clone()),
                        digest_missing.clone(),
                    ),
                    (
                        LinkKind::Digest(digest_healthy.clone()),
                        digest_healthy.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
            blob_store.delete_blob(&digest_missing).await.unwrap();

            run(test_case, Some(KEEP_TAGGED), &namespace).await;

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

    /// An image with no push or pull time, the shape a revision record with
    /// no `created_at` resolves to.
    fn dummy_image() -> ManifestImage {
        ManifestImage::new(
            &Namespace::new("app").unwrap(),
            None,
            None,
            None,
            Utc::now(),
        )
    }

    #[test]
    fn retained_follows_the_policies() {
        let image = dummy_image();
        let keep_nothing = policy(KEEP_NOTHING);
        // An age rule must not read a missing `created_at` as the Unix epoch
        // and delete recent content.
        let age = policy("image.pushed_at > now() - days(30)");
        let keep_untagged = policy("image.tag == null");
        let cases = [
            ("no policy", None, true),
            ("age rule", Some(&age), true),
            ("keep untagged", Some(&keep_untagged), true),
            ("keep nothing", Some(&keep_nothing), false),
        ];
        for (name, global, expected) in cases {
            assert_eq!(
                retained(global, None, &image, &[], &[]).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    /// A repository policy without rules has no opinion, so a deleting global
    /// policy decides alone.
    #[test]
    fn a_repository_policy_without_rules_has_no_opinion() {
        let repo = Repository {
            name: Namespace::new("r").unwrap(),
            namespace: None,
            upstreams: Vec::new(),
            replication: Vec::new(),
            retention_policy: RetentionPolicy::new(Vec::new()),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            scan: None,
            index: None,
        };
        let keep_nothing = policy(KEEP_NOTHING);
        assert!(!retained(Some(&keep_nothing), Some(&repo), &dummy_image(), &[], &[]).unwrap());
    }

    /// Seed a blob whose only index entry is its self-ownership grant.
    async fn seed_grant_only_blob(
        test_case: &dyn RegistryTestCase,
        namespace: &Namespace,
    ) -> Digest {
        let metadata_store = test_case.metadata_store();
        let blob = put_blob_direct(&test_case.blob_store(), b"granted-but-unreferenced").await;
        metadata_store
            .insert_reference(namespace, &blob, &LinkKind::Blob(blob.clone()))
            .await
            .unwrap();
        blob
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
            .open(test_case.blob_root().join(blob.blob_path()))
            .unwrap()
            .set_times(FileTimes::new().set_modified(week_ago))
            .unwrap();

        sweep_orphan_grants(
            &test_case.blob_store(),
            &metadata_store,
            &resolver(),
            Some(&policy(KEEP_NOTHING)),
            Duration::days(1),
            &test_executor(test_case.blob_store(), metadata_store.clone()),
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
            let executor = test_executor(test_case.blob_store(), metadata_store.clone());

            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &resolver(),
                None,
                Duration::zero(),
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
            let executor = test_executor(test_case.blob_store(), metadata_store.clone());
            let policy = policy(KEEP_NOTHING);

            // Within the in-flight window: never considered, whatever policy says.
            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &resolver(),
                Some(&policy),
                Duration::days(1),
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
                &resolver(),
                Some(&policy),
                Duration::zero(),
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

    /// A deleted manifest's reference key outlives it, so only a backed one may
    /// keep a grant out of the sweep, or reclaiming a pruned image takes a
    /// second prune.
    #[tokio::test]
    async fn only_a_live_manifest_reference_keeps_a_grant_from_the_sweep() {
        for_each_backend(async |test_case| {
            let live = Namespace::new("test-repo/live-ref").unwrap();
            let stale = Namespace::new("test-repo/stale-ref").unwrap();
            let blob = seed_grant_only_blob(test_case, &live).await;
            let metadata_store = test_case.metadata_store();
            let grant = LinkKind::Blob(blob.clone());
            let manifest = Digest::sha256_of_bytes(b"live-manifest");
            metadata_store
                .put_revision(&live, &manifest, None, None)
                .await
                .unwrap();
            let pins = [
                (&live, LinkKind::ReferencedBy(manifest)),
                (&stale, grant.clone()),
                (&stale, LinkKind::ReferencedBy(dummy_digest())),
            ];
            for (namespace, link) in pins {
                metadata_store
                    .insert_reference(namespace, &blob, &link)
                    .await
                    .unwrap();
            }

            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &resolver(),
                Some(&policy(KEEP_NOTHING)),
                Duration::zero(),
                &test_executor(test_case.blob_store(), metadata_store.clone()),
                4,
            )
            .await
            .unwrap();

            let holds_grant = async |namespace| {
                metadata_store
                    .read_blob_index_namespace(namespace, &blob)
                    .await
                    .unwrap()
                    .contains(&grant)
            };
            assert!(
                holds_grant(&live).await,
                "a backed reference must keep the grant"
            );
            assert!(
                !holds_grant(&stale).await,
                "a stale reference must not keep the grant"
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
            let executor = test_executor(test_case.blob_store(), metadata_store.clone());
            let policy = policy("image.pushed_at > now() - days(1)");

            // Just-written bytes: `pushed_at > now() - days(1)` retains them
            // even though the in-flight window is zero.
            sweep_orphan_grants(
                &test_case.blob_store(),
                &metadata_store,
                &resolver(),
                Some(&policy),
                Duration::zero(),
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
        let links: Vec<(LinkKind, Digest)> = (0..TAG_METADATA_CONCURRENCY * 3)
            .map(|i| {
                (
                    LinkKind::Tag(Tag::new(&format!("v{i}")).unwrap()),
                    dummy_digest(),
                )
            })
            .collect();
        seed_links(case.metadata_store().as_ref(), &namespace, &links)
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
        let tags = Rankings::read(&metadata_store_over(hooked), &namespace)
            .await
            .unwrap()
            .tags;

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
