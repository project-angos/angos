//! The namespace / tag / revision / referrer enumeration endpoints, served
//! from the `v2/cat` index and the namespace's own `v2/ns` listings.

use std::collections::HashMap;
use std::pin::pin;

use bytes::Bytes;
use futures_util::future::ready;
use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use tracing::{instrument, warn};

use angos_oci::{Digest, Namespace, Tag, namespace_belongs_to};
use angos_storage::{Page, paginated};

use crate::registry::keys::{
    CAT_ROOT, DigestExt, NamespaceExt, TagEntry, parse_ref_digest, parse_referrer_record,
    parse_revision_record, ref_digest_name,
};
use crate::registry::metadata_store::tag::{beats, live_metadata};
use crate::registry::metadata_store::{LIST_PAGE, LinkKind, LinkMetadata, MetadataStore};
use crate::registry::{Error, pagination};

/// Candidate tags a digest delete resolves at once.
const TAG_RESOLVE_CONCURRENCY: usize = 16;

/// Folds a sorted stream of (group, entry-file) pairs into each group's live
/// tag, yielding none for a tombstoned group or a malformed group name. A
/// sorted listing delivers each group's entries contiguously, newest ordinal
/// first, so every group resolves by the same rule as a point read.
///
/// A group is resolved as the next one opens rather than at the end, so a
/// caller reading in key order can stop once it has the names it needs.
#[derive(Default)]
struct WinnerFold {
    current: Option<(String, TagEntry)>,
}

impl WinnerFold {
    /// Folds one entry file in, returning the live tag this entry closed: the
    /// previous group's, whenever the entry opens a new group.
    fn push(&mut self, group: &str, file: &str) -> Option<(Tag, LinkMetadata)> {
        let entry = file.parse::<TagEntry>().ok()?;
        let mut closed = None;
        self.current = Some(match self.current.take() {
            Some((cur, winner)) if cur == group => (
                cur,
                if beats(&entry, &winner) {
                    entry
                } else {
                    winner
                },
            ),
            previous => {
                closed = previous.and_then(Self::resolve);
                (group.to_string(), entry)
            }
        });
        closed
    }

    /// The group's tag, named by its `<tag>!` directory, with the metadata a
    /// point read would resolve it to. `None` for a tombstoned winner, which
    /// holds no content to list.
    fn resolve((group, entry): (String, TagEntry)) -> Option<(Tag, LinkMetadata)> {
        let tag = Tag::new(group.strip_suffix('!')?).ok()?;
        Some((tag, live_metadata(entry)?))
    }
}

impl MetadataStore {
    /// One page of the namespaces `allowed` admits that hold manifest content
    /// (at least one revision or live tag); an `_uploads`-only namespace is
    /// not a catalog entry and is discovered through the blob store instead.
    ///
    /// The page holds `n` admitted names whenever that many remain, since the
    /// walk applies `allowed` as it goes rather than trimming a finished page.
    #[instrument(skip(self, allowed))]
    pub async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
        allowed: &(dyn Fn(&Namespace) -> bool + Sync),
    ) -> Result<Page<Namespace>, Error> {
        // One name past the page decides whether a next one is advertised; the
        // index listing is already in `Namespace` order, so no sort here.
        let wanted = usize::from(n).saturating_add(1);
        let namespaces: Vec<Namespace> = self
            .stream_namespaces(
                last.as_deref(),
                u16::try_from(wanted).unwrap_or(u16::MAX),
                allowed,
            )
            .take(wanted)
            .try_collect()
            .await?;

        Ok(pagination::slice_page(&namespaces, n))
    }

    /// Record the namespace in the `v2/cat` index, the key the unscoped
    /// catalog listing enumerates. Written by whoever creates content in it,
    /// so a namespace is listable as soon as it holds anything; a failed put
    /// only costs the namespace its listing until scrub's own probe repairs it.
    pub async fn index_namespace(&self, namespace: &Namespace) {
        if let Err(error) = self
            .object_store()
            .put(&namespace.catalog_index_path(), Bytes::new())
            .await
        {
            warn!("failed to write catalog index for '{namespace}': {error}");
        }
    }

    /// Every indexed namespace name in `v2/cat` key order, without the content
    /// probe [`Self::stream_namespaces`] runs. `scope` reads only that
    /// repository's key range: a prefix's keys are contiguous, so the scan
    /// starts at the scope and stops at the first key past it.
    ///
    /// A key whose namespace was emptied but not yet reaped still lists, so
    /// this serves a caller that tolerates a stale name over paying one probe
    /// per namespace. Callers needing content-checked names use
    /// [`Self::stream_namespaces`].
    #[instrument(skip(self))]
    pub async fn list_indexed_namespaces(
        &self,
        scope: Option<&str>,
    ) -> Result<Vec<Namespace>, Error> {
        // Keys carry a trailing `!`, so the bare scope sorts below every key
        // in its range and skips everything before it.
        let mut start_after = scope.map(str::to_string);
        paginated(|token| {
            let start_after = start_after.take();
            async move {
                Ok::<_, Error>(
                    self.object_store()
                        .list_after(CAT_ROOT, LIST_PAGE, token, start_after)
                        .await?,
                )
            }
        })
        // Ordered keys: past the prefix range, nothing else matches.
        .try_take_while(|key| ready(Ok(scope.is_none_or(|scope| key.starts_with(scope)))))
        .try_filter_map(|key| {
            let namespace = key
                .strip_suffix('!')
                .filter(|name| scope.is_none_or(|scope| namespace_belongs_to(name, scope)))
                .and_then(|name| Namespace::new(name).ok());
            ready(Ok(namespace))
        })
        .try_collect()
        .await
    }

    /// Whether the catalog names `namespace` as a repository: it holds at least
    /// one manifest revision, or one tag that resolves live. A namespace holding
    /// nothing but tombstones or in-progress uploads is not a repository.
    pub async fn holds_manifest_content(&self, namespace: &Namespace) -> Result<bool, Error> {
        if self.any_revision(namespace).await? {
            return Ok(true);
        }
        let mut tags = pin!(self.stream_live_tags(namespace, None));
        Ok(tags.try_next().await?.is_some())
    }

    /// Streams the `v2/cat` index above `last` in lexical key order, which is
    /// `Namespace` order because the trailing `!` sorts below every namespace
    /// character. Only namespaces that [`Self::holds_manifest_content`] list, so
    /// a stale index key of an emptied namespace does not.
    ///
    /// `allowed` decides who may see a name and runs before the content probe
    /// costs a round trip; `page` bounds both the listing and the probes in
    /// flight, so a caller after two namespaces never pays to probe a thousand.
    pub fn stream_namespaces<'a>(
        &'a self,
        last: Option<&str>,
        page: u16,
        allowed: &'a (dyn Fn(&Namespace) -> bool + Sync),
    ) -> impl Stream<Item = Result<Namespace, Error>> + Send + 'a {
        let page = page.clamp(1, LIST_PAGE);
        let fan_out = self
            .settings
            .namespace_walk_concurrency
            .get()
            .min(usize::from(page));
        // Index keys carry a trailing `!`, so the cursor's own key sorts below
        // the suffixed cursor and the listing resumes at the next namespace.
        let start_after = last.map(|last| format!("{last}!"));
        stream::try_unfold(Some((start_after, None)), move |state| async move {
            let Some((start_after, token)) = state else {
                return Ok::<_, Error>(None);
            };
            let listed = self
                .object_store()
                .list_after(CAT_ROOT, page, token, start_after)
                .await?;
            let mut probes = Vec::new();
            for key in &listed.items {
                let Some(name) = key.strip_suffix('!') else {
                    continue;
                };
                let Ok(namespace) = Namespace::new(name) else {
                    continue;
                };
                if !allowed(&namespace) {
                    continue;
                }
                probes.push(async move {
                    self.holds_manifest_content(&namespace)
                        .await
                        .map(|listable| listable.then_some(namespace))
                });
            }
            // Fanned out, but ordered, so the listing's lexical order survives
            // and a caller taking one page probes little beyond it.
            let probing = stream::iter(probes)
                .buffered(fan_out)
                .try_filter_map(|found| ready(Ok(found)));
            Ok(Some((
                probing,
                listed.next_token.map(|token| (None, Some(token))),
            )))
        })
        .try_flatten()
    }

    /// One page of `namespace`'s live tags in tag order, advertising the next
    /// while the walk has more.
    #[instrument(skip(self))]
    pub async fn list_tags(
        &self,
        namespace: &Namespace,
        n: u16,
        last: Option<String>,
    ) -> Result<Page<Tag>, Error> {
        // One tag past the page decides whether a next one is advertised, and
        // the walk stops there.
        let wanted = usize::from(n).saturating_add(1);
        let tags: Vec<Tag> = self
            .stream_live_tags(namespace, last.as_deref())
            .map_ok(|(tag, _)| tag)
            .take(wanted)
            .try_collect()
            .await?;

        Ok(pagination::slice_page(&tags, n))
    }

    /// Streams `namespace`'s live tags above `last`, each with the metadata a
    /// point read resolves it to, in tag order. One listing page at a time,
    /// and a group closes as the next opens, so taking a page of tags reads a
    /// page of entries rather than every tag in the namespace. A tombstone
    /// winner drops its tag, and a malformed name is skipped silently; scrub
    /// reports and removes those.
    pub fn stream_live_tags(
        &self,
        namespace: &Namespace,
        last: Option<&str>,
    ) -> impl Stream<Item = Result<(Tag, LinkMetadata), Error>> + Send + '_ {
        let root = namespace.tag_entries_root();
        // Entry keys are `<tag>!/<file>`, and `0` sorts above `/`, so `<last>!0`
        // sorts past every entry of `last` and below every later tag's.
        let start_after = last.map(|last| format!("{last}!0"));
        stream::try_unfold(
            Some((start_after, None, WinnerFold::default())),
            move |state| {
                let root = root.clone();
                async move {
                    let Some((start_after, token, mut fold)) = state else {
                        return Ok::<_, Error>(None);
                    };
                    let page = self
                        .object_store()
                        .list_after(&root, LIST_PAGE, token, start_after)
                        .await?;
                    let mut live = Vec::new();
                    for key in &page.items {
                        let Some((group, file)) = key.split_once('/') else {
                            continue;
                        };
                        live.extend(fold.push(group, file));
                    }
                    let next = match page.next_token {
                        // The listing itself closes the last group.
                        None => {
                            live.extend(fold.current.and_then(WinnerFold::resolve));
                            None
                        }
                        token => Some((None, token, fold)),
                    };
                    Ok(Some((stream::iter(live.into_iter().map(Ok)), next)))
                }
            },
        )
        .try_flatten()
    }

    /// The tags in `namespace` currently pointing at `digest`, in tag order,
    /// with their winning entries resolved from the tag entries alone. Every
    /// tag write pins its reference key on the digest first, and only scrub
    /// removes one once stale, so the namespace's tag keys for `digest` hold
    /// every candidate, a tag re-pointed on another replica included: the
    /// result gates the digest-delete LWW guard.
    #[instrument(skip(self))]
    pub async fn find_tags_pointing_at(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<Vec<(Tag, LinkMetadata)>, Error> {
        let candidates: Vec<Tag> = self
            .object_store()
            .list_in_order(&digest.blob_ref_namespace_dir(namespace))
            .try_filter_map(|entry| {
                ready(Ok(match digest.parse_blob_ref_entry(&entry) {
                    Some(LinkKind::Tag(tag)) => Some(tag),
                    _ => None,
                }))
            })
            .try_collect()
            .await?;
        stream::iter(candidates)
            .map(|tag| async move {
                match self.resolve_tag(namespace, &tag).await {
                    Ok(metadata) if metadata.target == *digest => Ok(Some((tag, metadata))),
                    Ok(_) | Err(Error::NotFound) => Ok(None),
                    Err(error) => Err(error),
                }
            })
            .buffered(TAG_RESOLVE_CONCURRENCY)
            .try_filter_map(|pointing| ready(Ok(pointing)))
            .try_collect()
            .await
    }

    /// Streams `digest`'s candidate referrer manifest digests above `after`,
    /// unresolved and in digest order, a listing page at a time, so a caller
    /// resuming at a cursor reads from there. Callers resolve each candidate
    /// to a descriptor at registry altitude, where the blob store holding
    /// manifest bodies is in reach.
    pub fn stream_referrer_digests(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        after: Option<&Digest>,
    ) -> impl Stream<Item = Result<Digest, Error>> + Send + '_ {
        let record_dir = namespace.referrer_record_dir(digest);
        let start_after = after.map(ref_digest_name);
        paginated(move |token| {
            let (record_dir, start_after) = (record_dir.clone(), start_after.clone());
            async move {
                Ok::<_, Error>(
                    self.object_store()
                        .list_after(&record_dir, LIST_PAGE, token, start_after)
                        .await?,
                )
            }
        })
        .try_filter_map(|key| ready(Ok(parse_ref_digest(&key))))
    }

    /// Every referrer record in `namespace`, keyed by subject: one walk of the
    /// `!sub` tree, where a listing per subject would cost one round trip each.
    pub async fn collect_referrers(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<Digest, Vec<Digest>>, Error> {
        let root = namespace.referrer_records_root();
        let mut keys = self.object_store().list_all(&root);
        let mut by_subject: HashMap<Digest, Vec<Digest>> = HashMap::new();
        while let Some(key) = keys.try_next().await? {
            if let Some((subject, referrer)) = parse_referrer_record(&key) {
                by_subject.entry(subject).or_default().push(referrer);
            }
        }
        Ok(by_subject)
    }

    /// Whether `namespace` holds any revision record. One key answers it, so
    /// it never pages.
    pub async fn any_revision(&self, namespace: &Namespace) -> Result<bool, Error> {
        let revisions = self
            .object_store()
            .list(&namespace.revision_records_root(), 1, None)
            .await?;
        Ok(!revisions.items.is_empty())
    }

    /// Streams every manifest revision digest in `namespace`, from its
    /// revision records.
    pub fn stream_revisions<'a>(
        &'a self,
        namespace: &'a Namespace,
    ) -> impl Stream<Item = Result<Digest, Error>> + Send + 'a {
        paginated(move |token| async move {
            let root = namespace.revision_records_root();
            Ok::<_, Error>(self.object_store().list(&root, LIST_PAGE, token).await?)
        })
        .try_filter_map(|key| ready(Ok(parse_revision_record(&key))))
    }
}

#[cfg(test)]
mod tests {
    use std::slice;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use chrono::Utc;
    use futures_util::{StreamExt, TryStreamExt};

    use angos_oci::{Algorithm, Digest, Namespace, Tag, UploadSessionId};
    use angos_storage::Page;
    use angos_storage::test_util::StoreOp;

    use crate::registry::keys::{NamespaceExt, TagEntryKind};
    use crate::registry::metadata_store::{LIST_PAGE, LinkKind, LinkMetadata, MetadataStore};
    use crate::registry::test_utils::{
        FSRegistryTestCase, RegistryTestCase, counting_metadata_store, create_link,
        create_test_blob, drop_links, for_each_backend, put_blob_direct,
    };

    /// A namespace lists exactly while it holds at least one revision or tag, and
    /// disappears once all are deleted with no scrub or rebuild in between.
    #[tokio::test]
    async fn list_namespaces_is_derived_from_content() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = &Namespace::new("derived-catalog/repo").unwrap();

            let (digest, _) = create_test_blob(registry, namespace, b"content").await;

            let listed = metadata_store
                .list_namespaces(1000, None, &|_| true)
                .await
                .unwrap()
                .items;
            assert!(
                listed.contains(namespace),
                "a namespace with content must appear in the catalog; got: {listed:?}"
            );

            drop_links(
                &metadata_store,
                namespace,
                &[
                    LinkKind::Tag(Tag::new("latest").unwrap()),
                    LinkKind::ReferencedBy(digest.clone()),
                ],
            )
            .await
            .unwrap();

            let listed = metadata_store
                .list_namespaces(1000, None, &|_| true)
                .await
                .unwrap()
                .items;
            assert!(
                !listed.contains(namespace),
                "a namespace whose revisions and tags were all deleted must \
                 disappear from the catalog; got: {listed:?}"
            );
        })
        .await;
    }

    /// One walk of the `!sub` tree yields every referrer record keyed by its
    /// subject, and a namespace whose name extends this one's stays out of it:
    /// the `!` after the name is what keeps the two prefixes apart.
    #[tokio::test]
    async fn collect_referrers_groups_records_by_subject() {
        for_each_backend(async |test_case| {
            let metadata_store = test_case.metadata_store();
            let namespace = Namespace::new("collect/img").unwrap();
            let neighbour = Namespace::new("collect/img2").unwrap();
            let subject_a = Digest::sha256_of_bytes(b"subject a");
            let subject_b = Digest::sha256_of_bytes(b"subject b");
            let (r1, r2, r3) = (
                Digest::sha256_of_bytes(b"referrer 1"),
                Digest::sha256_of_bytes(b"referrer 2"),
                Digest::sha256_of_bytes(b"referrer 3"),
            );
            for (ns, subject, referrer) in [
                (&namespace, &subject_a, &r1),
                (&namespace, &subject_a, &r2),
                (&namespace, &subject_b, &r3),
                (&neighbour, &subject_a, &r3),
            ] {
                metadata_store
                    .put_referrer(ns, subject, referrer, None)
                    .await
                    .unwrap();
            }

            let mut collected = metadata_store.collect_referrers(&namespace).await.unwrap();
            for referrers in collected.values_mut() {
                referrers.sort();
            }
            let mut of_a = vec![r1.clone(), r2.clone()];
            of_a.sort();

            assert_eq!(
                collected.len(),
                2,
                "two subjects hold records; got: {collected:?}"
            );
            assert_eq!(collected.get(&subject_a), Some(&of_a));
            assert_eq!(collected.get(&subject_b), Some(&vec![r3.clone()]));
        })
        .await;
    }

    /// `collect_upload_namespaces` keys off `_uploads` and `list_namespaces` off
    /// `_manifests`, so each surfaces what the other omits.
    #[tokio::test]
    async fn collect_upload_namespaces_keys_off_uploads_not_manifests() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let blob_store = test_case.blob_store();

            let manifest_only = &Namespace::new("upload-marker/manifest-only").unwrap();
            let upload_only = &Namespace::new("upload-marker/upload-only").unwrap();
            let mixed = &Namespace::new("upload-marker/mixed").unwrap();

            create_test_blob(registry, manifest_only, b"manifest-only").await;

            blob_store
                .create_upload(upload_only, &UploadSessionId::generate(), None)
                .await
                .unwrap();

            create_test_blob(registry, mixed, b"mixed").await;
            blob_store
                .create_upload(mixed, &UploadSessionId::generate(), None)
                .await
                .unwrap();

            let upload_listed = blob_store.collect_upload_namespaces(None).await.unwrap();
            assert!(
                upload_listed.contains(upload_only),
                "an upload-only namespace must appear in collect_upload_namespaces; got: {upload_listed:?}"
            );
            assert!(
                upload_listed.contains(mixed),
                "a namespace with an upload must appear in collect_upload_namespaces; got: {upload_listed:?}"
            );
            assert!(
                !upload_listed.contains(manifest_only),
                "a manifest-only namespace must not appear in collect_upload_namespaces; got: {upload_listed:?}"
            );

            let manifest_listed = metadata_store.list_namespaces(1000, None, &|_| true).await.unwrap().items;
            assert!(
                manifest_listed.contains(manifest_only),
                "a manifest-only namespace must appear in the catalog; got: {manifest_listed:?}"
            );
            assert!(
                manifest_listed.contains(mixed),
                "a namespace with content must appear in the catalog; got: {manifest_listed:?}"
            );
            assert!(
                !manifest_listed.contains(upload_only),
                "an upload-only namespace must not appear in the catalog; got: {manifest_listed:?}"
            );

        })
        .await;
    }

    /// On FS the catalog index key's `!` terminator keeps `a`'s leaf beside
    /// `a/b`'s directory, so nested repositories coexist.
    #[tokio::test]
    async fn nested_namespaces_coexist_in_the_catalog_on_fs() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        seed_namespaces(&store, &["cat-nest", "cat-nest/b"]).await;
        for name in ["cat-nest", "cat-nest/b"] {
            store
                .object_store()
                .head(&Namespace::new(name).unwrap().catalog_index_path())
                .await
                .expect("the catalog index key must exist beside the nested directory");
        }

        let listed = store
            .list_namespaces(10, None, &|_| true)
            .await
            .unwrap()
            .items;
        let names: Vec<&str> = listed.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["cat-nest", "cat-nest/b"]);
    }

    /// Catalog pages come off the index's ordered listing in lexical order
    /// (`-` < `.` < `/`), paginated by `n` plus `last`.
    #[tokio::test]
    async fn catalog_pages_serve_lexical_order_from_the_index() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        // Seeded out of lexical order on purpose.
        seed_namespaces(&store, &["cat-z", "cat-p/b", "cat-p", "cat-p-b", "cat-p.c"]).await;

        let page = store.list_namespaces(2, None, &|_| true).await.unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["cat-p", "cat-p-b"]);
        assert!(page.next_token.is_some(), "more pages must be signalled");

        let page = store
            .list_namespaces(2, Some("cat-p-b".to_string()), &|_| true)
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["cat-p.c", "cat-p/b"]);

        let page = store
            .list_namespaces(2, Some("cat-p/b".to_string()), &|_| true)
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["cat-z"]);
        assert!(page.next_token.is_none(), "the last page ends the chain");
    }

    /// A scope reads only its own key range. `cat-p-b` and `cat-p.c` share the
    /// `cat-p` prefix and sort between `cat-p!` and `cat-p/b!` (`-` < `.` < `/`),
    /// so the scan cannot stop at the first non-member, and neither may be
    /// mistaken for a namespace of the `cat-p` repository.
    #[tokio::test]
    async fn a_scoped_index_listing_reads_only_its_own_range() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        seed_namespaces(&store, &["cat-z", "cat-p/b", "cat-p", "cat-p-b", "cat-p.c"]).await;

        let scoped = store.list_indexed_namespaces(Some("cat-p")).await.unwrap();
        let names: Vec<&str> = scoped.iter().map(AsRef::as_ref).collect();
        assert_eq!(
            names,
            ["cat-p", "cat-p/b"],
            "only the repository itself and its sub-namespaces belong to the scope"
        );

        let all = store.list_indexed_namespaces(None).await.unwrap();
        let names: Vec<&str> = all.iter().map(AsRef::as_ref).collect();
        assert_eq!(
            names,
            ["cat-p", "cat-p-b", "cat-p.c", "cat-p/b", "cat-z"],
            "unscoped listing stays in index order"
        );

        let missing = store.list_indexed_namespaces(Some("cat-q")).await.unwrap();
        assert!(
            missing.is_empty(),
            "a repository with no namespaces lists nothing; got: {missing:?}"
        );
    }

    /// Seed one revision in each of `names`, which indexes the namespace.
    async fn seed_namespaces(store: &MetadataStore, names: &[&str]) {
        for name in names {
            let digest = Digest::sha256_of_bytes(name.as_bytes());
            store
                .put_revision(&Namespace::new(name).unwrap(), &digest, None, None)
                .await
                .unwrap();
        }
    }

    /// The tag walk streams: it resolves each group as the next opens and
    /// yields it, so a caller taking two tags of a namespace whose entries
    /// span several listing pages pays one listing rather than all of them.
    #[tokio::test]
    async fn taking_two_tags_reads_one_listing_of_a_namespace_that_spans_pages() {
        let case = FSRegistryTestCase::new();
        let namespace = Namespace::new("wide-tag-ns").unwrap();
        let digest = Digest::sha256_of_bytes(b"wide");
        let seeding = case.metadata_store();
        // One entry past a full listing page, so draining the walk would take
        // a second round trip that serving two tags must not pay for.
        let entries = usize::from(LIST_PAGE) + 1;
        for i in 0..entries {
            let tag = Tag::new(&format!("t{i:05}")).unwrap();
            seeding
                .object_store()
                .put(
                    &namespace.tag_entry_path(&tag, Utc::now(), TagEntryKind::Set, &digest),
                    Bytes::new(),
                )
                .await
                .unwrap();
        }

        let root = namespace.tag_entries_root();
        let (store, count) = counting_metadata_store(
            seeding.object_store().clone(),
            move |op| matches!(op, StoreOp::List { prefix } if *prefix == root),
        );

        let taken: Vec<Tag> = store
            .stream_live_tags(&namespace, None)
            .map_ok(|(tag, _)| tag)
            .take(2)
            .try_collect()
            .await
            .unwrap();
        let names: Vec<&str> = taken.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["t00000", "t00001"]);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "taking two tags must not drain the namespace's entries"
        );
    }

    /// The tags pointing at a digest come off its reference keys, each
    /// confirmed by a point read, so the namespace's tag entries are never
    /// listed; a tag moved off the digest or deleted is not among them.
    #[tokio::test]
    async fn tags_pointing_at_a_digest_skip_the_namespace_tag_walk() {
        for_each_backend(async |test_case| {
            let seeding = test_case.metadata_store();
            let namespace = &Namespace::new("pointing-ns").unwrap();
            let target = Digest::sha256_of_bytes(b"pointed at");
            let other = Digest::sha256_of_bytes(b"pointed elsewhere");
            for (name, digest) in [
                ("kept", &target),
                ("moved", &target),
                ("deleted", &target),
                ("other", &other),
                ("moved", &other),
            ] {
                let tag = LinkKind::Tag(Tag::new(name).unwrap());
                create_link(&seeding, namespace, &tag, digest).await;
            }
            seeding
                .put_tag_tombstone(namespace, &Tag::new("deleted").unwrap(), None)
                .await
                .unwrap();

            let root = namespace.tag_entries_root();
            let (store, count) = counting_metadata_store(
                seeding.object_store().clone(),
                move |op| matches!(op, StoreOp::List { prefix } if *prefix == root),
            );
            let pointing = store
                .find_tags_pointing_at(namespace, &target)
                .await
                .unwrap();

            let names: Vec<&str> = pointing.iter().map(|(tag, _)| tag.as_ref()).collect();
            assert_eq!(names, ["kept"]);
            assert_eq!(
                count.load(Ordering::SeqCst),
                0,
                "the namespace's tag entries must not be listed"
            );
        })
        .await;
    }

    /// A listed tag carries what a point read of it resolves: the walk reads
    /// key names alone, so a caller that lists needs no read per tag.
    #[tokio::test]
    async fn a_listed_tag_resolves_as_its_point_read_does() {
        for_each_backend(async |test_case| {
            let store = test_case.metadata_store();
            let namespace = &Namespace::new("listed-resolve-ns").unwrap();
            for (i, name) in ["a", "b", "c"].iter().enumerate() {
                let digest = Digest::sha256_of_bytes(format!("target-{i}").as_bytes());
                create_link(
                    &store,
                    namespace,
                    &LinkKind::Tag(Tag::new(name).unwrap()),
                    &digest,
                )
                .await;
            }

            let listed: Vec<(Tag, LinkMetadata)> = store
                .stream_live_tags(namespace, None)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(listed.len(), 3);
            for (tag, metadata) in listed {
                let read = store
                    .read_link(namespace, &LinkKind::Tag(tag.clone()))
                    .await
                    .unwrap();
                assert_eq!(metadata.target, read.target, "{tag}");
                assert_eq!(metadata.created_at, read.created_at, "{tag}");
            }
        })
        .await;
    }

    /// Serving a page probes only the namespaces it returns. The index is read
    /// from the cursor and probing stops once the page is full, so a paginated
    /// walk costs one probe per name served rather than one per name stored.
    #[tokio::test]
    async fn a_catalog_page_probes_only_the_namespaces_it_serves() {
        let case = FSRegistryTestCase::new();
        let store = case.metadata_store();
        let names: Vec<String> = (0..10).map(|i| format!("probe-{i}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        seed_namespaces(&store, &names).await;

        // `holds_manifest_content` answers from the first revision key, so one
        // revision listing is one content probe.
        let (paged, count) = counting_metadata_store(
            store.object_store().clone(),
            |op| matches!(op, StoreOp::List { prefix } if prefix.contains("!rev")),
        );

        let page = paged.list_namespaces(2, None, &|_| true).await.unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["probe-0", "probe-1"]);
        assert_eq!(
            page.next_token.as_deref(),
            Some("probe-1"),
            "more namespaces remain, so the page advertises the next"
        );
        // Two served plus the one that proves a next page exists.
        assert!(
            count.load(Ordering::SeqCst) <= 3,
            "a page of 2 out of 10 namespaces must not probe them all; probed {}",
            count.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_list_tags() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("test-repo").unwrap();
            let digest = put_blob_direct(&test_case.blob_store(), b"test blob content").await;

            let tags = ["latest", "v1.0", "v2.0"];
            for tag in tags {
                let tag_link = LinkKind::Tag(Tag::new(tag).unwrap());
                create_link(&m, namespace, &tag_link, &digest).await;
            }

            let Page {
                items: all_tags,
                next_token: token,
            } = m.list_tags(namespace, 10, None).await.unwrap();
            assert_eq!(all_tags.len(), tags.len());
            for tag in tags {
                assert!(all_tags.contains(&Tag::new(tag).unwrap()));
            }
            assert!(token.is_none());

            let Page {
                items: page1,
                next_token: token1,
            } = m.list_tags(namespace, 2, None).await.unwrap();
            assert_eq!(page1.len(), 2);
            assert!(token1.is_some());

            let Page {
                items: page2,
                next_token: token2,
            } = m.list_tags(namespace, 2, token1).await.unwrap();
            assert_eq!(page2.len(), 1);
            assert!(token2.is_none());

            let delete_tag = "v1.0";
            let tag_link = LinkKind::Tag(Tag::new(delete_tag).unwrap());
            drop_links(&m, namespace, slice::from_ref(&tag_link))
                .await
                .unwrap();

            let tags_after_delete = m.list_tags(namespace, 10, None).await.unwrap().items;
            assert_eq!(tags_after_delete.len(), tags.len() - 1);
            assert!(!tags_after_delete.contains(&Tag::new(delete_tag).unwrap()));
        })
        .await;
    }

    #[tokio::test]
    async fn test_stream_revisions_across_algorithms() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("multi-algo-repo").unwrap();

            // sha256 and sha512 live under separate prefixes; the stream must chain
            // them in global sort order, each digest exactly once.
            let mut expected = Vec::new();
            for content in [b"a".as_slice(), b"b".as_slice()] {
                for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
                    let digest = Digest::from_bytes(algorithm, content);
                    create_link(&m, namespace, &LinkKind::Digest(digest.clone()), &digest).await;
                    expected.push(digest);
                }
            }
            expected.sort();

            let all: Vec<Digest> = m.stream_revisions(namespace).try_collect().await.unwrap();
            assert_eq!(all, expected);
        })
        .await;
    }
}
