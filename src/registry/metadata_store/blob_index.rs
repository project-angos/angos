//! Cross-namespace blob reference tracking.
//!
//! One write-once, empty reference key per (namespace, link) under
//! [`DigestKeys::blob_ref_dir`] records that a namespace references a blob.
//!
//! The two `read_blob_index*` readers report an absent index as
//! [`Error::NotFound`], which a collector must tell apart from an index that
//! exists and is empty; [`MetadataStore::referencing_namespaces`] and
//! [`MetadataStore::can_read`] serve the callers that read absence as
//! "nothing references it".

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use bytes::Bytes;
use chrono::Utc;
use futures_util::future::try_join_all;
use tracing::instrument;

use angos_oci::{Digest, Namespace};
use angos_storage::Error as StorageError;

use crate::registry::{
    Error,
    keys::DigestKeys,
    metadata_store::{LIST_PAGE, LinkKind, MetadataStore},
};

/// Every namespace referencing one blob, with the links each references it
/// through.
pub type BlobIndex = HashMap<Namespace, HashSet<LinkKind>>;

impl MetadataStore {
    /// `namespace`'s reference entries for `digest`: the `!own` leaf plus the
    /// `!r/` subtree.
    async fn namespace_ref_entries(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let store = self.object_store();
        let mut links = HashSet::new();
        if store.exists(&digest.blob_ref_own_path(namespace)).await? {
            links.insert(LinkKind::Blob(digest.clone()));
        }
        let dir = digest.blob_ref_namespace_dir(namespace);
        let mut token = None;
        loop {
            let page = store.list(&dir, LIST_PAGE, token).await?;
            links.extend(
                page.items
                    .iter()
                    .filter_map(|entry| digest.parse_blob_ref_entry(entry)),
            );
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }
        Ok(links)
    }

    /// Write one reference key per `(digest, link)` and confirm no collector
    /// run covers those digests. The keys land before anything that makes them
    /// reachable, so a crash leaves only an over-approximated reference; the
    /// clearance vouches for one grace period counted from the first key, and
    /// a caller slower than that cannot be vouched for at all, so it fails
    /// closed and the client retries.
    pub async fn pin_references(
        &self,
        namespace: &Namespace,
        pins: &[(Digest, LinkKind)],
    ) -> Result<(), Error> {
        if pins.is_empty() {
            return Ok(());
        }
        let started_at = Instant::now();
        try_join_all(
            pins.iter()
                .map(|(digest, link)| self.insert_reference(namespace, digest, link)),
        )
        .await?;

        let digests: Vec<&Digest> = pins.iter().map(|(digest, _)| digest).collect();
        if !self.gc_clear(&digests).await? {
            return Err(Error::ReclamationInProgress(
                "blob reclamation in progress for a referenced blob; retry".to_string(),
            ));
        }
        if started_at.elapsed().as_secs() > self.gc_grace_secs {
            return Err(Error::ReclamationInProgress(
                "reference wave outlasted the reclamation grace period; retry".to_string(),
            ));
        }
        Ok(())
    }

    /// Record that `namespace` references `digest` through `link`. Idempotent:
    /// the key's existence is the whole record.
    #[instrument(skip(self))]
    pub async fn insert_reference(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        link: &LinkKind,
    ) -> Result<(), Error> {
        self.object_store()
            .put(&digest.blob_ref_path(namespace, link), Bytes::new())
            .await
            .map_err(Error::from)
    }

    /// Drop one of `namespace`'s reference keys for `digest`; deleting a key
    /// that is already gone is a no-op.
    #[instrument(skip(self))]
    pub async fn remove_reference(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        link: &LinkKind,
    ) -> Result<(), Error> {
        self.object_store()
            .delete(&digest.blob_ref_path(namespace, link))
            .await
            .map_err(Error::from)
    }

    /// Revoke `namespace`'s ownership of `digest`, the only reference removal
    /// a writer ever performs. The bytes are the collector's to reclaim once
    /// every reference is stale.
    pub async fn revoke_blob_ownership(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(), Error> {
        self.remove_reference(namespace, digest, &LinkKind::Blob(digest.clone()))
            .await
    }

    /// Insert `namespace`'s blob ownership reference with one idempotent put.
    /// Correct on its own only for freshly written bytes, which the grace
    /// period covers; pre-existing bytes need
    /// [`blob_ownership::grant_existing`](crate::registry::blob_ownership::grant_existing).
    pub async fn grant(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        self.insert_reference(namespace, digest, &LinkKind::Blob(digest.clone()))
            .await
    }

    /// Whether `namespace` holds a live reference to `digest`: read
    /// authorization for its bytes, and the ownership a push must already
    /// hold to reference them. Writers never remove reference keys, so a raw
    /// entry is not ownership: the `own` key grants directly, and one head
    /// answers that common case before the fuller listing; anything else
    /// counts only while [`Self::reference_backed`] vouches for it, or a stale
    /// manifest reference would resurrect a blob the namespace deleted.
    pub async fn can_read(&self, namespace: &Namespace, digest: &Digest) -> Result<bool, Error> {
        if self
            .object_store()
            .exists(&digest.blob_ref_own_path(namespace))
            .await?
        {
            return Ok(true);
        }
        let entries = self.namespace_ref_entries(namespace, digest).await?;
        // A grant that landed between the head and the listing.
        if entries.contains(&LinkKind::Blob(digest.clone())) {
            return Ok(true);
        }
        for entry in &entries {
            if self.reference_backed(namespace, entry, digest).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Every local namespace referencing `digest`, per the blob index; empty
    /// when none do, a missing index entry included.
    pub async fn referencing_namespaces(&self, digest: &Digest) -> Result<Vec<Namespace>, Error> {
        match self.read_blob_index(digest).await {
            Ok(index) => Ok(index.into_keys().collect()),
            Err(Error::NotFound) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// The lexicographically-smallest namespace referencing `digest`, excluding
    /// `exclude`; `None` when no other namespace references it.
    pub async fn smallest_referencing_namespace(
        &self,
        digest: &Digest,
        exclude: &str,
    ) -> Result<Option<Namespace>, Error> {
        Ok(self
            .referencing_namespaces(digest)
            .await?
            .into_iter()
            .filter(|namespace| namespace.as_ref() != exclude)
            .min())
    }

    /// Every namespace referencing `digest` with the links it references
    /// through. [`Error::NotFound`] when no key does, which a collector must
    /// tell apart from an index that exists and is empty.
    #[instrument(skip(self))]
    pub async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        let mut index = BlobIndex::default();
        let dir = digest.blob_ref_dir();
        let mut token = None;
        loop {
            let page = self.object_store().list(&dir, LIST_PAGE, token).await?;
            for key in &page.items {
                let Some((raw, link)) = digest.parse_blob_ref(key) else {
                    continue;
                };
                let Ok(namespace) = Namespace::new(&raw) else {
                    continue;
                };
                index.entry(namespace).or_default().insert(link);
            }
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        if index.is_empty() {
            return Err(Error::NotFound);
        }
        Ok(index)
    }

    /// The links `namespace` references `digest` through, and
    /// [`Error::NotFound`] when it references it through none.
    #[instrument(skip(self))]
    pub async fn read_blob_index_namespace(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let links = self.namespace_ref_entries(namespace, digest).await?;
        if links.is_empty() {
            return Err(Error::NotFound);
        }
        Ok(links)
    }

    /// Whether the link behind a reference entry still backs it: a tag,
    /// revision, or referrer while it resolves to `blob`, a per-referrer entry
    /// while the referring manifest's revision resolves.
    pub async fn reference_backed(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        blob: &Digest,
    ) -> Result<bool, Error> {
        let backing = match link {
            LinkKind::Tag(_) | LinkKind::Digest(_) | LinkKind::Referrer { .. } => link.clone(),
            LinkKind::ReferencedBy(referrer) => LinkKind::Digest(referrer.clone()),
            // Ownership is answered before this, so an `own` key never
            // reaches here.
            LinkKind::Blob(_) => return Ok(false),
        };
        match self.read_link(namespace, &backing).await {
            // The referring revision resolving is all a per-referrer entry needs.
            Ok(metadata) => {
                Ok(matches!(link, LinkKind::ReferencedBy(_)) || &metadata.target == blob)
            }
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Collector-side liveness over the reference index: `own` pins
    /// unconditionally, and any other key pins while it is younger than the
    /// grace period or its backing link resolves. The blob-data age gate is
    /// the caller's, since the bytes live in the blob store.
    pub async fn blob_references_live(&self, digest: &Digest) -> Result<bool, Error> {
        let dir = digest.blob_ref_dir();
        let mut token = None;
        loop {
            let page = self.object_store().list(&dir, LIST_PAGE, token).await?;
            for key in &page.items {
                let Some((raw, link)) = digest.parse_blob_ref(key) else {
                    continue;
                };
                if matches!(link, LinkKind::Blob(_)) {
                    return Ok(true);
                }
                let full_key = format!("{dir}/{key}");
                match self.object_store().head(&full_key).await {
                    Ok(meta) => {
                        // No timestamp to gate on, so never guess in favour
                        // of deletion.
                        let Some(modified) = meta.last_modified else {
                            return Ok(true);
                        };
                        let age = Utc::now().signed_duration_since(modified);
                        if age.num_seconds() < i64::try_from(self.gc_grace_secs).unwrap_or(i64::MAX)
                        {
                            return Ok(true);
                        }
                    }
                    Err(StorageError::NotFound) => continue,
                    Err(e) => return Err(e.into()),
                }
                let Ok(namespace) = Namespace::new(&raw) else {
                    // A key angos cannot address is left to quarantine, and
                    // pins until then.
                    return Ok(true);
                };
                if self.reference_backed(&namespace, &link, digest).await? {
                    return Ok(true);
                }
            }
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }
        Ok(false)
    }

    /// Delete every reference key of a reclaimed blob. Only the collector
    /// calls this, and only after the marker protocol has fenced the
    /// blob-data delete.
    pub async fn delete_blob_references(&self, digest: &Digest) -> Result<(), Error> {
        self.object_store()
            .delete_prefix(&digest.blob_ref_dir())
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use angos_oci::{Digest, Namespace, Tag};

    use crate::registry::{
        Error,
        keys::DigestKeys,
        metadata_store::LinkKind,
        test_utils::{
            create_link, drop_links, for_each_backend, put_blob_direct, s3_metadata_store,
            seed_links,
        },
    };

    /// A link write lands as one key under `v2/ref/`.
    #[tokio::test]
    async fn a_write_lands_as_one_reference_key() {
        let backend = s3_metadata_store();
        let namespace = Namespace::new("ref-key-shape-test").unwrap();
        let digest = Digest::from_str(
            "sha256:abab000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let link = LinkKind::Tag(Tag::new("v1").unwrap());
        let ops = [(link.clone(), digest.clone())];
        seed_links(&backend, &namespace, &ops).await.unwrap();

        backend
            .object_store()
            .head(&digest.blob_ref_path(&namespace, &link))
            .await
            .expect("the tag's reference key must exist");
    }

    /// A dropped tag stops resolving while its reference key stays for the
    /// collector, and the tags around it are untouched.
    #[tokio::test]
    async fn a_dropped_tag_goes_but_its_reference_key_stays() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("parallel-mixed-ns").unwrap();

            let digest_a = put_blob_direct(m.object_store(), b"content-a").await;
            let digest_b = put_blob_direct(m.object_store(), b"content-b").await;
            let digest_c = put_blob_direct(m.object_store(), b"content-c").await;

            create_link(
                &m,
                namespace,
                &LinkKind::Tag(Tag::new("v1").unwrap()),
                &digest_a,
            )
            .await;
            create_link(
                &m,
                namespace,
                &LinkKind::Tag(Tag::new("v2").unwrap()),
                &digest_b,
            )
            .await;

            drop_links(&m, namespace, &[LinkKind::Tag(Tag::new("v1").unwrap())])
                .await
                .unwrap();
            seed_links(
                &m,
                namespace,
                &[(LinkKind::Tag(Tag::new("v3").unwrap()), digest_c.clone())],
            )
            .await
            .unwrap();

            let err = m
                .read_link(namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::NotFound),
                "Tag v1 should not exist after deletion but got error: {err:?}"
            );

            let meta_v2 = m
                .read_link(namespace, &LinkKind::Tag(Tag::new("v2").unwrap()))
                .await
                .unwrap();
            assert_eq!(
                meta_v2.target, digest_b,
                "Tag v2 should still point to digest_b"
            );

            let meta_v3 = m
                .read_link(namespace, &LinkKind::Tag(Tag::new("v3").unwrap()))
                .await
                .unwrap();
            assert_eq!(meta_v3.target, digest_c, "Tag v3 should point to digest_c");

            let tag_v1 = LinkKind::Tag(Tag::new("v1").unwrap());
            let index_a = m.read_blob_index(&digest_a).await.unwrap();
            assert!(
                index_a
                    .get(namespace)
                    .is_some_and(|links| links.contains(&tag_v1)),
                "the stale entry is the collector's to prune, not the writer's"
            );

            let index_c = m.read_blob_index(&digest_c).await.unwrap();
            let links_c = index_c
                .get(namespace)
                .expect("Blob index for digest_c should have an entry for namespace");
            assert!(
                links_c.contains(&LinkKind::Tag(Tag::new("v3").unwrap())),
                "Blob index for digest_c should contain Tag(v3)"
            );
        })
        .await;
    }

    /// Each digest's index carries its own links and no other digest's.
    #[tokio::test]
    async fn each_digest_indexes_only_its_own_links() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("parallel-blob-index-ns").unwrap();

            let mut digests = Vec::new();
            for i in 0..4 {
                let digest = put_blob_direct(m.object_store(), format!("content-{i}").as_bytes()).await;
                digests.push(digest);
            }

            let tags: Vec<(LinkKind, Digest)> = digests
                .iter()
                .enumerate()
                .map(|(i, digest)| {
                    (
                        LinkKind::Tag(Tag::try_from(format!("tag-{i}")).unwrap()),
                        digest.clone(),
                    )
                })
                .collect();
            seed_links(&m, namespace, &tags).await.unwrap();

            for (i, digest) in digests.iter().enumerate() {
                let expected_tag = LinkKind::Tag(Tag::try_from(format!("tag-{i}")).unwrap());
                let blob_index = m.read_blob_index(digest).await.unwrap();

                let links = blob_index.get(namespace).unwrap_or_else(|| {
                    panic!("Blob index for digest {digest} should have an entry for namespace {namespace}")
                });

                assert!(
                    links.contains(&expected_tag),
                    "Blob index for digest {digest} should contain tag-{i}"
                );

                for (j, other_digest) in digests.iter().enumerate() {
                    if j != i {
                        let other_tag = LinkKind::Tag(Tag::try_from(format!("tag-{j}")).unwrap());
                        assert!(
                            !links.contains(&other_tag),
                            "Blob index for digest {digest} (tag-{i}) should NOT contain tag-{j}"
                        );
                        let other_index = m.read_blob_index(other_digest).await.unwrap();
                        let other_links = other_index.get(namespace);
                        assert!(
                            other_links.is_some_and(|s| !s.contains(&expected_tag)),
                            "Blob index for digest {other_digest} (tag-{j}) should NOT contain tag-{i}"
                        );
                    }
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_duplicated_layer_keeps_other_referrers() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("tracked-duplicate-layer-ns").unwrap();

            let layer_digest = put_blob_direct(m.object_store(), b"layer listed twice").await;
            let first_manifest_digest =
                put_blob_direct(m.object_store(), b"manifest c content").await;
            let second_manifest_digest =
                put_blob_direct(m.object_store(), b"manifest d content").await;

            m.pin_references(
                namespace,
                &[(
                    layer_digest.clone(),
                    LinkKind::ReferencedBy(first_manifest_digest.clone()),
                )],
            )
            .await
            .unwrap();

            // A manifest listing the same layer twice pins the same key twice.
            let duplicated = vec![
                (
                    layer_digest.clone(),
                    LinkKind::ReferencedBy(second_manifest_digest.clone()),
                );
                2
            ];
            m.pin_references(namespace, &duplicated).await.unwrap();

            let blob_index = m.read_blob_index(&layer_digest).await.unwrap();
            let links = blob_index
                .get(namespace)
                .expect("Blob index should have an entry for the namespace");
            assert!(
                links.contains(&LinkKind::ReferencedBy(first_manifest_digest.clone()))
                    && links.contains(&LinkKind::ReferencedBy(second_manifest_digest.clone())),
                "the duplicated push must leave one per-referrer entry per manifest"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_shared_blob_pin_survives_other_manifest_delete() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("shared-config-delete-ns").unwrap();

            let config_digest = put_blob_direct(m.object_store(), b"shared config bytes").await;
            let first_manifest = put_blob_direct(m.object_store(), b"first sharing manifest").await;
            let second_manifest =
                put_blob_direct(m.object_store(), b"second sharing manifest").await;

            for manifest in [&first_manifest, &second_manifest] {
                m.pin_references(
                    namespace,
                    &[(
                        config_digest.clone(),
                        LinkKind::ReferencedBy(manifest.clone()),
                    )],
                )
                .await
                .unwrap();
                m.put_revision(namespace, manifest, None, None)
                    .await
                    .unwrap();
            }

            // The first manifest's delete drops its record; its per-referrer key on
            // the shared config is the collector's, so the pin survives.
            drop_links(&m, namespace, &[LinkKind::Digest(first_manifest.clone())])
                .await
                .unwrap();

            let links = m
                .read_blob_index_namespace(namespace, &config_digest)
                .await
                .unwrap();
            let surviving = LinkKind::ReferencedBy(second_manifest.clone());
            let deleted = LinkKind::ReferencedBy(first_manifest.clone());
            assert!(
                links.contains(&surviving),
                "the surviving manifest's entry must remain"
            );
            assert!(
                m.reference_backed(namespace, &surviving, &config_digest)
                    .await
                    .unwrap(),
                "the surviving manifest's entry must still be backed"
            );
            assert!(
                !m.reference_backed(namespace, &deleted, &config_digest)
                    .await
                    .unwrap(),
                "the deleted manifest's entry must read as unbacked"
            );
        })
        .await;
    }

    /// One write batch mixing record-backed links with reference-only pins
    /// leaves every kind's key in place.
    #[tokio::test]
    async fn links_of_every_kind_land_in_one_write_pass() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("mixed-tracked-untracked-ns").unwrap();

            let tag_digest = put_blob_direct(m.object_store(), b"tag content").await;
            let layer_digest = put_blob_direct(m.object_store(), b"layer content mixed").await;
            let digest_link_digest =
                put_blob_direct(m.object_store(), b"digest link content").await;
            let manifest_digest =
                put_blob_direct(m.object_store(), b"manifest content mixed").await;

            seed_links(
                &m,
                namespace,
                &[
                    (LinkKind::Tag(Tag::new("v1").unwrap()), tag_digest.clone()),
                    (
                        LinkKind::Digest(digest_link_digest.clone()),
                        digest_link_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
            m.pin_references(
                namespace,
                &[(
                    layer_digest.clone(),
                    LinkKind::ReferencedBy(manifest_digest.clone()),
                )],
            )
            .await
            .unwrap();

            let tag_meta = m
                .read_link(namespace, &LinkKind::Tag(Tag::new("v1").unwrap()))
                .await
                .unwrap();
            assert_eq!(
                tag_meta.target, tag_digest,
                "Tag v1 should target tag_digest"
            );
            let digest_meta = m
                .read_link(namespace, &LinkKind::Digest(digest_link_digest.clone()))
                .await
                .unwrap();
            assert_eq!(
                digest_meta.target, digest_link_digest,
                "Digest link should target digest_link_digest"
            );
            let tag_index = m.read_blob_index(&tag_digest).await.unwrap();
            let tag_links = tag_index
                .get(namespace)
                .expect("Blob index for tag_digest should have namespace entry");
            assert!(
                tag_links.contains(&LinkKind::Tag(Tag::new("v1").unwrap())),
                "Blob index for tag_digest should contain Tag(v1)"
            );

            let layer_index = m.read_blob_index(&layer_digest).await.unwrap();
            let layer_links = layer_index
                .get(namespace)
                .expect("Blob index for layer_digest should have namespace entry");
            assert!(
                layer_links.contains(&LinkKind::ReferencedBy(manifest_digest.clone())),
                "Blob index for layer_digest should contain the per-referrer entry"
            );

            let digest_index = m.read_blob_index(&digest_link_digest).await.unwrap();
            let digest_links = digest_index
                .get(namespace)
                .expect("Blob index for digest_link_digest should have namespace entry");
            assert!(
                digest_links.contains(&LinkKind::Digest(digest_link_digest.clone())),
                "Blob index for digest_link_digest should contain the Digest link"
            );
        })
        .await;
    }

    /// Two namespaces pin one blob independently: neither write clobbers the
    /// other's entry, which a shared index body could not guarantee.
    #[tokio::test]
    async fn two_namespaces_pin_one_blob_independently() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let other_ns = &Namespace::new("other-ns").unwrap();
            let my_ns = &Namespace::new("my-ns").unwrap();
            let digest = put_blob_direct(m.object_store(), b"shared content").await;

            let other_tag = LinkKind::Tag(Tag::new("stable").unwrap());
            create_link(&m, other_ns, &other_tag, &digest).await;

            let blob_index = m.read_blob_index(&digest).await.unwrap();
            assert!(
                blob_index.contains_key(other_ns),
                "Blob index should have entry for other-ns"
            );

            let my_tag = LinkKind::Tag(Tag::new("latest").unwrap());
            create_link(&m, my_ns, &my_tag, &digest).await;

            let blob_index = m.read_blob_index(&digest).await.unwrap();
            let other_links = blob_index
                .get(other_ns)
                .expect("Blob index should still have entry for other-ns");
            assert!(
                other_links.contains(&other_tag),
                "other-ns should still contain Tag(stable)"
            );

            let my_links = blob_index
                .get(my_ns)
                .expect("Blob index should have entry for my-ns");
            assert!(
                my_links.contains(&my_tag),
                "my-ns should contain Tag(latest)"
            );
        })
        .await;
    }
}
