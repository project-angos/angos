//! Tag state as ordered write-once entries.
//!
//! A tag is the set of entries under its [`NamespaceKeys::tag_entry_dir`], each
//! named `<ord>.<kind>.<algo>.<hash>` with an inverted-timestamp `<ord>` so a
//! listing yields newest first. Writers only append, so last-writer-wins is a
//! property of the key names and concurrent writers never contend; a tag with
//! no entries does not exist.

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};

use angos_oci::{Digest, Namespace, Tag};
use angos_storage::Error as StorageError;

use crate::registry::{
    Error,
    keys::{NamespaceKeys, TagEntry},
    metadata_store::{LIST_PAGE, LinkMetadata, MetadataStore},
};

/// The timestamp a locally authored entry carries: this replica's clock,
/// truncated to the millisecond its key encodes and floored one millisecond
/// above the entry it supersedes.
///
/// Entry order is the tag's order, and two replicas sharing a backend can have
/// skewed clocks. Without the floor a push or delete stamped below the current
/// winner lands as the loser: the tag does not move, and the client is told it
/// did. The truncation is part of that floor, since a stamp landing later
/// inside the superseded millisecond encodes the very same ordinal. A
/// replicated write keeps its author's timestamp instead, since that is what
/// the last-writer-wins gate compares.
fn local_entry_ts(now: DateTime<Utc>, superseded: Option<DateTime<Utc>>) -> DateTime<Utc> {
    let now = DateTime::from_timestamp_millis(now.timestamp_millis()).unwrap_or(now);
    match superseded {
        // Equal counts: a same-ordinal tie resolves on the digest, not on who
        // wrote last.
        Some(superseded) if superseded >= now => superseded + TimeDelta::milliseconds(1),
        _ => now,
    }
}

impl MetadataStore {
    /// Append one entry to `tag`, the only write of an entry key. The name
    /// carries the ordinal, the kind and the target, which is the whole
    /// record, so the body is empty.
    async fn write_tag_entry(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        target: &Digest,
        deletion: bool,
        created_at: DateTime<Utc>,
    ) -> Result<(), Error> {
        let key = namespace.tag_entry_path(tag, created_at, deletion, target);
        self.object_store()
            .put(&key, Bytes::new())
            .await
            .map_err(Error::from)
    }

    /// Point `tag` at `target` with one entry put, returning whether that
    /// moved the tag: `false` for a re-push of the digest it already holds,
    /// which keeps its timestamp so the same key stays the same write.
    ///
    /// A replicated write carries its author's time; anything else stamps this
    /// replica's clock floored above the newest entry, tombstone included, so
    /// the entry wins resolution whatever the skew between replicas.
    pub async fn put_tag_entry(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        target: &Digest,
        authored_at: Option<DateTime<Utc>>,
    ) -> Result<bool, Error> {
        let winner = self.resolve_tag_winner(namespace, tag).await?;
        let same_target = matches!(
            winner.as_ref(),
            Some(TagEntry::Set { digest, .. }) if digest == target
        );
        let newest = winner.and_then(|winner| winner.authored_at());
        let created_at = if same_target { newest } else { None }
            .or(authored_at)
            .unwrap_or_else(|| local_entry_ts(Utc::now(), newest));

        self.write_tag_entry(namespace, tag, target, false, created_at)
            .await?;
        if !same_target {
            self.index_namespace(namespace).await;
        }
        Ok(!same_target)
    }

    /// End `tag` with one tombstone entry naming the digest it held, which
    /// tag history requires. A tag with no live entry has nothing to end, and
    /// reads as untouched.
    pub async fn put_tag_tombstone(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        authored_at: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let current = match self.resolve_tag(namespace, tag).await {
            Ok(current) => current,
            Err(Error::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        let created_at =
            authored_at.unwrap_or_else(|| local_entry_ts(Utc::now(), current.created_at));
        self.write_tag_entry(namespace, tag, &current.target, true, created_at)
            .await
    }

    /// Resolve `tag` to link-shaped metadata: the complete newest entry group
    /// decides, the highest digest winning a same-millisecond tie and a
    /// deletion never beating an equal-timestamped push of the same digest. A
    /// tombstone winner reads as `NotFound`, and `media_type` is always `None`
    /// because the winner comes from key names alone.
    pub async fn resolve_tag(
        &self,
        namespace: &Namespace,
        tag: &Tag,
    ) -> Result<LinkMetadata, Error> {
        match self.resolve_tag_winner(namespace, tag).await? {
            Some(TagEntry::Set {
                authored_at,
                digest,
                ..
            }) => Ok(LinkMetadata {
                target: digest,
                created_at: authored_at,
                media_type: None,
                descriptor: None,
            }),
            Some(TagEntry::Deletion { .. }) | None => Err(Error::NotFound),
        }
    }

    /// The winner of the tag's complete lowest-ordinal entry group, or `None`
    /// when the tag has no entries, a tombstone winner included, since a local
    /// write floors its own timestamp above whatever is newest. Pages
    /// until the ordinal changes, so a same-millisecond pair straddling a page
    /// boundary is never split.
    async fn resolve_tag_winner(
        &self,
        namespace: &Namespace,
        tag: &Tag,
    ) -> Result<Option<TagEntry>, Error> {
        let dir = namespace.tag_entry_dir(tag);
        let mut group: Vec<TagEntry> = Vec::new();
        let mut token = None;
        'pages: loop {
            let page = self.object_store().list(&dir, LIST_PAGE, token).await?;
            for name in &page.items {
                let Ok(entry) = name.parse::<TagEntry>() else {
                    continue;
                };
                match group.first() {
                    Some(first) if entry.ord() != first.ord() => break 'pages,
                    _ => group.push(entry),
                }
            }
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }
        // Highest digest first; a `set` beats a `del` of the same digest.
        group.sort_by(|a, b| {
            b.digest().cmp(a.digest()).then_with(|| {
                matches!(a, TagEntry::Deletion { .. }).cmp(&matches!(b, TagEntry::Deletion { .. }))
            })
        });
        Ok(group.into_iter().next())
    }

    /// Move one superseded tag entry under the `!hist/` prefix, re-reading the
    /// body so a racing writer's entry is copied as it stands. The hist key
    /// lands before the delete, so an interruption duplicates the entry rather
    /// than losing it, and a source entry already gone means an earlier run
    /// finished the move.
    pub async fn demote_tag_entry(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        entry_name: &str,
    ) -> Result<(), Error> {
        let entry_key = format!("{}/{entry_name}", namespace.tag_entry_dir(tag));
        let body = match self.object_store().get(&entry_key).await {
            Ok(body) => body,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        // An interrupted earlier demotion may already have written the copy;
        // the delete below finishes the move either way.
        self.object_store()
            .create_if_absent(&namespace.tag_hist_path(tag, entry_name), Bytes::from(body))
            .await?;
        match self.object_store().delete(&entry_key).await {
            Ok(()) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::{DateTime, TimeDelta, Utc};
    use tempfile::TempDir;

    use angos_oci::{Digest, Namespace, Tag};
    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use super::*;
    use crate::registry::{
        keys::NamespaceKeys,
        metadata_store::LinkKind,
        test_utils::{FSRegistryTestCase, RegistryTestCase, drop_links, metadata_store_over},
    };

    /// The entry key carries only the millisecond, so a stamp landing later
    /// inside the superseded millisecond must still be floored above it: it
    /// would otherwise encode the same ordinal, leaving the group tie-break
    /// rather than the clock to decide the tag.
    #[test]
    fn a_stamp_inside_the_superseded_millisecond_is_floored_above_it() {
        let entry = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        let later_same_ms = entry + TimeDelta::microseconds(700);
        assert_eq!(
            local_entry_ts(later_same_ms, Some(entry)),
            entry + TimeDelta::milliseconds(1)
        );
    }

    #[test]
    fn a_local_entry_is_floored_above_the_one_it_supersedes() {
        let now = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let ms = TimeDelta::milliseconds(1);

        assert_eq!(local_entry_ts(now, None), now, "nothing to supersede");
        assert_eq!(
            local_entry_ts(now, Some(now - ms)),
            now,
            "an older entry leaves the clock alone"
        );
        assert_eq!(
            local_entry_ts(now, Some(now)),
            now + ms,
            "a same-ordinal tie resolves on the digest, so equal must be bumped"
        );
        assert_eq!(
            local_entry_ts(now, Some(now + TimeDelta::seconds(30))),
            now + TimeDelta::seconds(30) + ms,
            "a peer's clock 30s ahead must not pin the tag"
        );
    }

    fn entry_ms(ts: DateTime<Utc>) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ts.timestamp_millis()).unwrap()
    }

    /// Two processes pushing the same tag write disjoint entry keys, so neither
    /// write is lost and both resolve the same winner (equal timestamps
    /// tie-break on the higher digest).
    #[tokio::test]
    async fn concurrent_same_tag_pushes_from_two_processes_lose_nothing() {
        let dir = TempDir::new().unwrap();
        let store_for = || {
            let backend: Arc<dyn ObjectStore> = Arc::new(
                StorageFsBackend::builder(dir.path())
                    .sync_to_disk(false)
                    .build(),
            );
            metadata_store_over(backend)
        };
        let a = store_for();
        let b = store_for();
        let namespace = Namespace::new("two-writers").unwrap();
        let tag = Tag::new("latest").unwrap();
        let link = LinkKind::Tag(tag.clone());
        let ts = entry_ms(Utc::now());
        let digest_a = Digest::sha256_of_bytes(b"writer-a");
        let digest_b = Digest::sha256_of_bytes(b"writer-b");

        let (ra, rb) = tokio::join!(
            a.put_tag_entry(&namespace, &tag, &digest_a, Some(ts)),
            b.put_tag_entry(&namespace, &tag, &digest_b, Some(ts)),
        );
        ra.unwrap();
        rb.unwrap();

        let entries = a
            .object_store()
            .list(
                &namespace.tag_entry_dir(&Tag::new("latest").unwrap()),
                10,
                None,
            )
            .await
            .unwrap();
        assert_eq!(entries.items.len(), 2, "neither concurrent write is lost");

        let winner = std::cmp::max(digest_a.clone(), digest_b.clone());
        for store in [&a, &b] {
            let resolved = store.read_link(&namespace, &link).await.unwrap();
            assert_eq!(
                resolved.target, winner,
                "both processes must resolve the same deterministic winner"
            );
        }
    }

    /// Tag history needs a delete to name the digest the tag held, so the
    /// tombstone repeats the winner's target rather than standing alone.
    #[tokio::test]
    async fn a_tombstone_names_the_digest_the_tag_held() {
        let test_case = FSRegistryTestCase::new();
        let store = test_case.metadata_store();
        let namespace = Namespace::new("tombstone-target").unwrap();
        let tag = Tag::new("v1").unwrap();
        let target = Digest::sha256_of_bytes(b"tombstoned-manifest");

        store
            .put_tag_entry(&namespace, &tag, &target, None)
            .await
            .unwrap();
        store
            .put_tag_tombstone(&namespace, &tag, None)
            .await
            .unwrap();

        let entries = store
            .object_store()
            .list(&namespace.tag_entry_dir(&tag), 10, None)
            .await
            .unwrap();
        let tombstone = entries
            .items
            .iter()
            .find(|name| name.contains(".del."))
            .expect("the delete must append a tombstone entry");
        let entry = tombstone
            .parse::<TagEntry>()
            .expect("the tombstone must parse as an entry");
        assert!(
            matches!(entry, TagEntry::Deletion { held, .. } if held == target),
            "the tombstone must name the digest the tag held"
        );
        assert!(
            matches!(
                store.read_link(&namespace, &LinkKind::Tag(tag)).await,
                Err(Error::NotFound)
            ),
            "the tombstone must win resolution over the entry it supersedes"
        );
    }

    /// An older angos wrote descriptor fields into the entry body. Resolution
    /// reads key names alone, so such an entry still resolves and its body is
    /// simply ignored.
    #[tokio::test]
    async fn an_entry_written_by_an_older_angos_still_resolves() {
        let test_case = FSRegistryTestCase::new();
        let store = test_case.metadata_store();
        let namespace = Namespace::new("old-shape-entry").unwrap();
        let tag = Tag::new("v1").unwrap();
        let target = Digest::sha256_of_bytes(b"old-shape-manifest");
        let ts = entry_ms(Utc::now());

        let key = namespace.tag_entry_path(&tag, ts, false, &target);
        store
            .object_store()
            .put(
                &key,
                Bytes::from_static(
                    br#"{"media_type":"application/vnd.oci.image.manifest.v1+json","size":42}"#,
                ),
            )
            .await
            .unwrap();

        let resolved = store
            .read_link(&namespace, &LinkKind::Tag(tag))
            .await
            .unwrap();
        assert_eq!(resolved.target, target);
        assert_eq!(resolved.created_at, Some(ts));
    }

    /// A peer replica sharing the backend can stamp an entry ahead of this
    /// replica's clock. A local push must still move the tag: answering 201 for a
    /// write that lands as the loser tells the client something untrue.
    #[tokio::test]
    async fn a_local_push_outranks_an_entry_from_a_faster_peer() {
        let dir = TempDir::new().unwrap();
        let backend: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder(dir.path())
                .sync_to_disk(false)
                .build(),
        );
        let store = metadata_store_over(backend);
        let namespace = Namespace::new("skewed-peer").unwrap();
        let tag = Tag::new("latest").unwrap();
        let peer = Digest::sha256_of_bytes(b"what the faster peer pushed");
        let local = Digest::sha256_of_bytes(b"what this replica pushed");

        // The peer's clock runs a minute ahead, and its write replicates here
        // carrying its own author time.
        store
            .put_tag_entry(
                &namespace,
                &tag.clone(),
                &peer.clone(),
                Some(entry_ms(Utc::now()) + TimeDelta::seconds(60)),
            )
            .await
            .unwrap();

        // A local push, stamped by this replica's own clock.
        store
            .put_tag_entry(&namespace, &tag.clone(), &local.clone(), None)
            .await
            .unwrap();

        assert_eq!(
            store
                .read_link(&namespace, &LinkKind::Tag(tag.clone()))
                .await
                .unwrap()
                .target,
            local,
            "a local push must move the tag it reported as pushed"
        );

        // The same holds for the tombstone a local delete writes.
        drop_links(&store, &namespace, &[LinkKind::Tag(tag.clone())])
            .await
            .unwrap();

        let tags: Vec<String> = store
            .list_tags(&namespace, 10, None)
            .await
            .unwrap()
            .items
            .iter()
            .map(ToString::to_string)
            .collect();
        assert!(
            tags.is_empty(),
            "a local delete must remove the tag it reported as deleted, got {tags:?}"
        );
    }

    /// A tag page starts strictly after its cursor. The `!` suffix on a tag's
    /// entry directory is what keeps a name that is a prefix of another sorting
    /// first, so the page can be served off the entry listing itself, and the
    /// superseded entries beside a winner must not shift it.
    #[tokio::test]
    async fn tag_pages_start_after_their_cursor() {
        let dir = TempDir::new().unwrap();
        let backend: Arc<dyn ObjectStore> = Arc::new(
            StorageFsBackend::builder(dir.path())
                .sync_to_disk(false)
                .build(),
        );
        let store = metadata_store_over(backend);
        let namespace = Namespace::new("paged-tags").unwrap();

        // Seeded out of lexical order, each tag pushed twice so a superseded entry
        // sits beside its winner in the listing.
        let base = entry_ms(Utc::now());
        for name in ["v2", "v10", "v1", "v1.1"] {
            let tag = Tag::new(name).unwrap();
            for revision in 0..2i64 {
                let digest = Digest::sha256_of_bytes(format!("{name}-{revision}").as_bytes());
                store
                    .put_tag_entry(
                        &namespace,
                        &tag.clone(),
                        &digest,
                        Some(base + TimeDelta::milliseconds(revision)),
                    )
                    .await
                    .unwrap();
            }
        }

        let page = store.list_tags(&namespace, 2, None).await.unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(
            names,
            ["v1", "v1.1"],
            "`v1!` sorts below `v1.1!`, so a prefix name pages first"
        );
        assert_eq!(page.next_token.as_deref(), Some("v1.1"));

        let page = store
            .list_tags(&namespace, 2, Some("v1.1".to_string()))
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["v10", "v2"]);
        assert!(page.next_token.is_none(), "the last page ends the chain");

        // The cursor names a whole tag, not a prefix: `v1` must not swallow `v1.1`.
        let page = store
            .list_tags(&namespace, 10, Some("v1".to_string()))
            .await
            .unwrap();
        let names: Vec<&str> = page.items.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, ["v1.1", "v10", "v2"]);
    }
}
