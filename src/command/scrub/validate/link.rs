//! Link-key validation: one visit per key covers manifest reference repair,
//! `referenced_by` back-links, blob-index grant reconciliation, tag targets,
//! referrer liveness, and the invalid-name gates.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use tracing::{debug, warn};

use angos_oci::{Digest, Manifest, Namespace, Tag};
use angos_storage::Error as StorageError;

use crate::registry::keys::{NamespaceKeys, TagEntry, atime_entry_name, parse_atime_entry};
use crate::{
    command::{
        maintenance::{
            Error,
            action::{Action, AtimeCompaction, WalkedStore},
        },
        scrub::validate::Validator,
    },
    registry::{
        Error as RegistryError,
        content_discovery::holds_manifest_content,
        manifest::referenced_digests,
        metadata_store::{
            AccessEntry, LIST_PAGE, LinkKind, PullHistoryConfig, access_time::ATIME_CHUNK_CAP,
        },
    },
};

/// What [`Validator::ensure_grant`] did about one (blob, link) pin.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GrantState {
    /// The index records the entry, already or through this run's grant.
    Recorded,
    /// The blob has no bytes, so there is no pin to record.
    Byteless,
    /// A concurrent write took the decision; this run leaves it alone.
    Declined,
}

impl Validator {
    /// One tag's entry directory, validated once per (namespace, tag): the
    /// resolved winner must target existing bytes and a resolvable revision.
    pub async fn validate_tag_entries(
        &self,
        namespace_raw: &str,
        tag_raw: &str,
    ) -> Result<(), Error> {
        if !self.claim(format!("tag-entries:{namespace_raw}:{tag_raw}")) {
            return Ok(());
        }
        let (Ok(namespace), Ok(tag)) = (Namespace::new(namespace_raw), Tag::new(tag_raw)) else {
            return Ok(());
        };
        self.demote_superseded_entries(&namespace, &tag).await?;
        let metadata = match self
            .metadata_store
            .read_link(&namespace, &LinkKind::Tag(tag.clone()))
            .await
        {
            Ok(metadata) => metadata,
            // Tombstoned: the entries are history now.
            Err(RegistryError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        self.validate_tag_target(&namespace, &tag, metadata.target, metadata.created_at)
            .await
    }

    /// Demote entries superseded by the tag's winner group to the `!hist/`
    /// prefix. The whole lowest-ordinal group stays, so a same-millisecond tie
    /// is never split, and each strictly older candidate is age-gated so a
    /// racing push's entry is out of scope.
    async fn demote_superseded_entries(
        &self,
        namespace: &Namespace,
        tag: &Tag,
    ) -> Result<(), Error> {
        let dir = namespace.tag_entry_dir(tag);
        let mut winner_ord = None;
        let mut token = None;
        loop {
            let page = self
                .metadata_store
                .object_store()
                .list(&dir, 1000, token)
                .await
                .map_err(RegistryError::from)?;
            for name in &page.items {
                let Ok(entry) = name.parse::<TagEntry>() else {
                    continue;
                };
                // The listing sorts newest first, so the first parseable
                // ordinal is the winner group's.
                let winner = *winner_ord.get_or_insert(entry.ord());
                if entry.ord() <= winner {
                    continue;
                }
                if self.younger_than_grace(&format!("{dir}/{name}")).await? {
                    continue;
                }
                self.emit(Action::DemoteTagEntry {
                    namespace: namespace.clone(),
                    tag: tag.clone(),
                    entry_name: name.clone(),
                })
                .await?;
            }
            token = page.next_token;
            if token.is_none() {
                return Ok(());
            }
        }
    }

    /// One tag's access entries, compacted once per (namespace, tag).
    pub async fn collect_tag_atime_entries(
        &self,
        namespace_raw: &str,
        tag_raw: &str,
    ) -> Result<(), Error> {
        let (Ok(namespace), Ok(tag)) = (Namespace::new(namespace_raw), Tag::new(tag_raw)) else {
            return Ok(());
        };
        self.collect_atime_entries(&namespace, &LinkKind::Tag(tag))
            .await
    }

    /// One revision's access entries, compacted once per (namespace, digest).
    pub async fn collect_revision_atime_entries(
        &self,
        namespace_raw: &str,
        digest: &Digest,
    ) -> Result<(), Error> {
        let Ok(namespace) = Namespace::new(namespace_raw) else {
            return Ok(());
        };
        self.collect_atime_entries(&namespace, &LinkKind::Digest(digest.clone()))
            .await
    }

    /// Compact one target's access entries. The newest decodable entry always
    /// stays, since retention needs the last access durably, and an
    /// undecodable body goes. Every other entry past a gate is packed into one
    /// new chunk, written before those entries retire; an entry past the
    /// history bounds is deleted outright, and older chunks are trimmed to
    /// them.
    async fn collect_atime_entries(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<(), Error> {
        let (Some(dir), Some(compacted)) = (
            namespace.atime_dir(link),
            namespace.atime_compacted_dir(link),
        ) else {
            return Ok(());
        };
        if !self.claim(format!("atime-entries:{dir}")) {
            return Ok(());
        }
        let history = self.metadata_store.pull_history;
        let compact_after_secs = history.compaction_age_secs();
        let now = Utc::now();
        let limit = usize::try_from(history.max_pulls.get()).unwrap_or(usize::MAX);

        let chunks = self.metadata_store.list_names(&compacted).await?;
        // A chunk is named after its newest entry, and entries list newest
        // first, so every entry at or past the newest chunk's name is already
        // packed: a leftover of a run whose retires did not all land.
        let packed_through = chunks.first().cloned();

        let mut kept = 0_usize;
        // Packing stops at the history limit, which bounds both.
        let mut packed = Vec::new();
        let mut packed_keys = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .metadata_store
                .object_store()
                .list(&dir, LIST_PAGE, token)
                .await
                .map_err(RegistryError::from)?;
            // Nothing the new chunk holds, so these go at once.
            let mut dropped = Vec::new();
            for name in &page.items {
                let Some(at) = parse_atime_entry(name) else {
                    continue;
                };
                let key = format!("{dir}/{name}");
                let raw = match self.metadata_store.object_store().get(&key).await {
                    Ok(raw) => raw,
                    Err(StorageError::NotFound) => continue,
                    Err(e) => return Err(RegistryError::from(e).into()),
                };
                let Ok(entry) = serde_json::from_slice::<AccessEntry>(&raw) else {
                    warn!("scrub: access entry '{key}' does not parse; deleting");
                    self.delete_corrupt(WalkedStore::Metadata, &key).await?;
                    continue;
                };
                let leftover = packed_through
                    .as_ref()
                    .is_some_and(|through| name >= through);
                let ranked_out = history
                    .compact_after_pulls
                    .is_some_and(|max| kept >= usize::try_from(max.get()).unwrap_or(usize::MAX));
                if kept == 0 || !(leftover || older_than(now, at, compact_after_secs) || ranked_out)
                {
                    kept += 1;
                    continue;
                }
                if !leftover
                    && !older_than(now, at, history.max_age_secs)
                    && kept + packed.len() < limit
                {
                    packed.push(entry);
                    packed_keys.push(key);
                } else {
                    dropped.push(key);
                }
            }
            if !dropped.is_empty() {
                self.emit(Action::CompactAtime(AtimeCompaction {
                    chunks: Vec::new(),
                    retired: dropped,
                }))
                .await?;
            }
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        self.consolidate_chunks(&compacted, chunks, kept, packed, packed_keys, now)
            .await
    }

    /// Store the packed entries, merged into the newest chunk while it is
    /// under the cap, as chunks of at most the cap, then retire the packed
    /// keys and the chunk merged in. The older chunks are then trimmed to the
    /// history bounds once `live` newer pulls are counted: a chunk wholly past
    /// them goes, the one straddling them is rewritten with its fitting
    /// entries, and one no older than a newer chunk's oldest entry is the
    /// stale source of a merge whose retire did not land.
    async fn consolidate_chunks(
        &self,
        compacted: &str,
        chunks: Vec<String>,
        live: usize,
        mut head: Vec<AccessEntry>,
        mut retired: Vec<String>,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        let PullHistoryConfig {
            max_pulls,
            max_age_secs,
            ..
        } = self.metadata_store.pull_history;
        let limit = usize::try_from(max_pulls.get()).unwrap_or(usize::MAX);
        let mut chunks = chunks.into_iter().peekable();

        if !head.is_empty()
            && let Some(name) = chunks.peek()
        {
            let key = format!("{compacted}/{name}");
            match self.read_chunk(&key).await? {
                Some(newest) if newest.len() < ATIME_CHUNK_CAP => {
                    head.extend(newest);
                    retired.push(key);
                    chunks.next();
                }
                Some(_) => {}
                None => {
                    chunks.next();
                }
            }
        }
        head.truncate(fitting(
            &head,
            now,
            max_age_secs,
            limit.saturating_sub(live),
        ));
        let mut total = live + head.len();
        let mut covered = head.last().map(entry_name);
        if !head.is_empty() || !retired.is_empty() {
            self.emit(Action::CompactAtime(AtimeCompaction {
                chunks: chunk_writes(compacted, &head)?,
                retired,
            }))
            .await?;
        }

        let mut dropped = Vec::new();
        for name in chunks {
            let Some(at) = parse_atime_entry(&name) else {
                continue;
            };
            let key = format!("{compacted}/{name}");
            let stale = covered.as_ref().is_some_and(|covered| name <= *covered);
            if stale || total >= limit || older_than(now, at, max_age_secs) {
                dropped.push(key);
                continue;
            }
            let Some(mut chunk) = self.read_chunk(&key).await? else {
                continue;
            };
            covered = chunk.last().map(entry_name);
            let fits = fitting(&chunk, now, max_age_secs, limit - total);
            total += fits;
            if fits < chunk.len() {
                chunk.truncate(fits);
                self.emit(Action::CompactAtime(AtimeCompaction {
                    chunks: chunk_writes(compacted, &chunk)?,
                    retired: Vec::new(),
                }))
                .await?;
            }
        }
        if dropped.is_empty() {
            return Ok(());
        }
        self.emit(Action::CompactAtime(AtimeCompaction {
            chunks: Vec::new(),
            retired: dropped,
        }))
        .await
    }

    /// One compacted chunk, `None` once it is gone or, not parsing, deleted.
    async fn read_chunk(&self, key: &str) -> Result<Option<Vec<AccessEntry>>, Error> {
        let raw = match self.metadata_store.object_store().get(key).await {
            Ok(raw) => raw,
            Err(StorageError::NotFound) => return Ok(None),
            Err(e) => return Err(RegistryError::from(e).into()),
        };
        let Ok(chunk) = serde_json::from_slice(&raw) else {
            warn!("scrub: compacted access chunk '{key}' does not parse; deleting");
            self.delete_corrupt(WalkedStore::Metadata, key).await?;
            return Ok(None);
        };
        Ok(Some(chunk))
    }

    /// The shared tail of both tag shapes: the target must have blob bytes,
    /// else its orphan manifest is removed, and its revision link is re-issued
    /// when missing. A winning entry inside the grace period is left alone,
    /// since repairing mid-delete would resurrect a deleted manifest.
    async fn validate_tag_target(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        target: Digest,
        entry_created_at: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.ensure_catalog(namespace).await?;
        if let Some(created_at) = entry_created_at {
            let grace = i64::try_from(self.metadata_store.gc_grace_secs).unwrap_or(i64::MAX);
            if Utc::now().signed_duration_since(created_at).num_seconds() < grace {
                return Ok(());
            }
        }
        match self.blob_store.size(&target).await {
            Ok(_) => {
                self.ensure_link(namespace, &LinkKind::Digest(target.clone()), &target)
                    .await
            }
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                warn!("scrub: tag '{namespace}:{tag}' targets missing blob '{target}'; removing");
                self.emit(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: target,
                })
                .await
            }
            Err(e) => Err(e.into()),
        }
    }

    /// A revision record, anchored once per (namespace, digest).
    pub async fn validate_revision_record(
        &self,
        namespace_raw: &str,
        revision: &Digest,
    ) -> Result<(), Error> {
        // `categorize` rejects an unaddressable namespace before dispatch.
        let Ok(namespace) = Namespace::new(namespace_raw) else {
            return Ok(());
        };
        self.validate_revision_content(&namespace, revision)
            .await
            .map(|_| ())
    }

    /// The anchor of the derivable state, shared by both revision shapes: one
    /// manifest read drives child-link repair, back-links, and grant
    /// reconciliation, returning whether the manifest blob is present. Runs
    /// once per (namespace, revision); a repeat visit only re-probes health.
    async fn validate_revision_content(
        &self,
        namespace: &Namespace,
        revision: &Digest,
    ) -> Result<bool, Error> {
        self.ensure_catalog(namespace).await?;
        if !self.claim(format!("revision:{namespace}:{revision}")) {
            return Ok(self.blob_store.size(revision).await.is_ok());
        }

        // A revision younger than the grace period may belong to a push whose
        // later waves are still in flight, so repairs derived from it would
        // race them.
        if self
            .younger_than_grace(&namespace.revision_record_path(revision))
            .await?
        {
            return Ok(false);
        }
        let content = match self.blob_store.read(revision).await {
            Ok(content) => content,
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                warn!(
                    "scrub: manifest blob missing for revision '{namespace}@{revision}'; removing revision"
                );
                self.emit(Action::DeleteOrphanManifest {
                    namespace: namespace.clone(),
                    digest: revision.clone(),
                })
                .await?;
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        };
        let manifest =
            Manifest::from_slice(&content).map_err(|e| RegistryError::manifest_invalid(&e))?;

        // The revision's own grant pins the manifest blob.
        self.ensure_grant(namespace, revision, &LinkKind::Digest(revision.clone()))
            .await?;

        // Only repair references the namespace already holds: a permissive push
        // withholds the link and grant for a digest it does not own, and
        // re-deriving them from the manifest body would hand back exactly the
        // cross-namespace read access the write path refused.

        // A referenced digest is pinned by this revision's per-referrer entry,
        // never by a key of its own, so ownership of the target is the whole
        // gate and the repair is that entry.
        for target in referenced_digests(&manifest) {
            if !self.metadata_store.can_read(namespace, &target).await? {
                debug!(
                    "scrub: '{namespace}' holds no reference to '{target}'; \
                     leaving the link from revision '{revision}' unrepaired"
                );
                continue;
            }
            self.ensure_grant(
                namespace,
                &target,
                &LinkKind::ReferencedBy(revision.clone()),
            )
            .await?;
        }

        // A subject-bearing manifest also links the referrer back to its
        // subject, which carries a referrer record of its own.
        if let Some(subject) = &manifest.subject {
            let back_link = LinkKind::Referrer {
                subject: subject.digest.clone(),
                referrer: revision.clone(),
            };
            if self
                .holds_reference(namespace, revision, &back_link)
                .await?
            {
                self.ensure_link(namespace, &back_link, revision).await?;
                self.ensure_grant(namespace, revision, &back_link).await?;
            } else {
                debug!(
                    "scrub: '{namespace}' holds no reference to '{revision}'; \
                     leaving its subject back-link unrepaired"
                );
            }
        }
        Ok(true)
    }

    /// Whether the revision's record exists, the shape that makes a digest
    /// resolvable.
    async fn revision_exists(&self, namespace: &Namespace, digest: &Digest) -> Result<bool, Error> {
        let record_key = namespace.revision_record_path(digest);
        Ok(self
            .metadata_store
            .object_store()
            .exists(&record_key)
            .await
            .map_err(RegistryError::from)?)
    }

    /// Emit the namespace's catalog index key when it is missing, once per
    /// namespace per run.
    async fn ensure_catalog(&self, namespace: &Namespace) -> Result<(), Error> {
        if !self.claim(format!("catalog:{namespace}")) {
            return Ok(());
        }
        let key = namespace.catalog_index_path();
        if self
            .metadata_store
            .object_store()
            .exists(&key)
            .await
            .map_err(RegistryError::from)?
        {
            return Ok(());
        }
        self.emit(Action::EnsureCatalogIndex {
            namespace: namespace.clone(),
        })
        .await
    }

    /// A catalog index key is live only while its namespace holds a revision
    /// or a tag. An emptied namespace's key is reaped once past the grace
    /// window, so the admin listings stop naming it. A push landing in the
    /// meantime rewrites the key, and a race against that is repaired by the
    /// next run's `ensure_catalog`.
    pub async fn validate_catalog_index(&self, namespace_raw: &str) -> Result<(), Error> {
        // `categorize` rejects an unaddressable namespace before dispatch.
        let Ok(namespace) = Namespace::new(namespace_raw) else {
            return Ok(());
        };
        let key = namespace.catalog_index_path();
        if !self
            .metadata_store
            .object_store()
            .exists(&key)
            .await
            .map_err(RegistryError::from)?
        {
            return Ok(());
        }
        if self.younger_than_grace(&key).await? {
            return Ok(());
        }
        if holds_manifest_content(&self.metadata_store, namespace.clone()).await? {
            return Ok(());
        }
        self.emit(Action::ReapCatalogIndex { namespace }).await
    }

    /// Whether `namespace` already holds `target`. Raw key existence is not
    /// ownership: the namespace's own blob-index key counts directly, every
    /// other entry only while `reference_backed` vouches for it, else a
    /// manifest delete's leftovers would mint back the refused access.
    async fn holds_reference(
        &self,
        namespace: &Namespace,
        target: &Digest,
        link: &LinkKind,
    ) -> Result<bool, Error> {
        if self.metadata_store.can_read(namespace, target).await? {
            return Ok(true);
        }
        Ok(self
            .metadata_store
            .reference_backed(namespace, link, target)
            .await?)
    }

    /// A referrer record is live only while its referrer manifest is a current
    /// revision.
    pub async fn validate_referrer_record(
        &self,
        namespace_raw: &str,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        // `categorize` rejects an unaddressable namespace before dispatch.
        let Ok(namespace) = Namespace::new(namespace_raw) else {
            return Ok(());
        };
        let key = namespace.referrer_record_path(subject, referrer);
        if !self
            .metadata_store
            .object_store()
            .exists(&key)
            .await
            .map_err(RegistryError::from)?
        {
            return Ok(());
        }
        // A young record may precede its referrer's revision inside a push, or
        // follow a delete the walk raced; either way pruning waits.
        if self.younger_than_grace(&key).await? {
            return Ok(());
        }
        if self.revision_exists(&namespace, referrer).await? {
            return Ok(());
        }
        self.remove_dead_referrer(&namespace, subject, referrer)
            .await
    }

    /// The shared removal tail of both referrer shapes: confirm the referrer
    /// manifest is durably gone, then emit the orphan-referrer deletion.
    async fn remove_dead_referrer(
        &self,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        if self.revision_exists(namespace, referrer).await? {
            return Ok(());
        }
        self.emit(Action::DeleteOrphanReferrer {
            namespace: namespace.clone(),
            subject: subject.clone(),
            referrer: referrer.clone(),
        })
        .await
    }

    /// Whether the referrer's record key exists, the shape the write path
    /// creates for a referrer link.
    async fn referrer_record_exists(
        &self,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<bool, Error> {
        let key = namespace.referrer_record_path(subject, referrer);
        Ok(self
            .metadata_store
            .object_store()
            .exists(&key)
            .await
            .map_err(RegistryError::from)?)
    }

    /// Recreate `link -> expected` when its record is confirmed missing, and
    /// only then: a repair write must not be based on a read that never
    /// succeeded, nor race a delete removing the record it would write back.
    async fn ensure_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        expected: &Digest,
    ) -> Result<(), Error> {
        let missing = move || async move {
            match link {
                LinkKind::Digest(_) => {
                    Ok::<_, Error>(!self.revision_exists(namespace, expected).await?)
                }
                LinkKind::Referrer { subject, referrer } => Ok(!self
                    .referrer_record_exists(namespace, subject, referrer)
                    .await?),
                // Every other kind is pinned by its reference entry alone.
                _ => Ok(false),
            }
        };
        if !missing().await? {
            return Ok(());
        }
        if !missing().await? {
            return Ok(());
        }
        self.emit(Action::RecreateLink {
            namespace: namespace.clone(),
            link: link.clone(),
            target: expected.clone(),
        })
        .await
    }

    /// Emit a grant for `link` on `blob` unless the index already records it,
    /// and only for bytes that still exist.
    async fn ensure_grant(
        &self,
        namespace: &Namespace,
        blob: &Digest,
        link: &LinkKind,
    ) -> Result<GrantState, Error> {
        match self
            .metadata_store
            .read_blob_index_namespace(namespace, blob)
            .await
        {
            Ok(links) if links.contains(link) => return Ok(GrantState::Recorded),
            Ok(_) | Err(RegistryError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        match self.blob_store.size(blob).await {
            Ok(_) => {}
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                // A manifest referencing missing bytes is broken; granting it
                // would only churn against the blob GC.
                return Ok(GrantState::Byteless);
            }
            Err(e) => return Err(e.into()),
        }
        let recorded = match self
            .metadata_store
            .read_blob_index_namespace(namespace, blob)
            .await
        {
            Ok(links) => links.contains(link),
            Err(RegistryError::NotFound) => false,
            Err(e) => return Err(e.into()),
        };
        if recorded {
            return Ok(GrantState::Declined);
        }
        self.emit(Action::GrantBlobIndexLink {
            namespace: namespace.clone(),
            blob: blob.clone(),
            link: link.clone(),
        })
        .await?;
        Ok(GrantState::Recorded)
    }
}

/// How many of `entries`, newest first, are younger than `max_age_secs` and
/// fit in `room`.
fn fitting(
    entries: &[AccessEntry],
    now: DateTime<Utc>,
    max_age_secs: Option<u64>,
    room: usize,
) -> usize {
    entries
        .iter()
        .take_while(|entry| !older_than(now, entry.at, max_age_secs))
        .count()
        .min(room)
}

/// The key name `entry` was stored under while live.
fn entry_name(entry: &AccessEntry) -> String {
    atime_entry_name(entry.at, &entry.client)
}

/// `entries`, newest first, split into chunks of at most the cap and listed
/// oldest first, so a run that fails midway never leaves a newer chunk past
/// an unwritten older one. The remainder lands in the newest chunk, keeping
/// every older one full, and each is named after its newest entry.
pub fn chunk_writes(
    compacted: &str,
    entries: &[AccessEntry],
) -> Result<Vec<(String, Bytes)>, Error> {
    entries
        .rchunks(ATIME_CHUNK_CAP)
        .filter_map(|chunk| Some((chunk.first()?, chunk)))
        .map(|(newest, chunk)| {
            let body = serde_json::to_vec(chunk).map_err(RegistryError::from)?;
            Ok((
                format!("{compacted}/{}", entry_name(newest)),
                Bytes::from(body),
            ))
        })
        .collect()
}

/// Whether `at` is at least `secs` before `now`; never, without a bound.
fn older_than(now: DateTime<Utc>, at: DateTime<Utc>, secs: Option<u64>) -> bool {
    secs.is_some_and(|secs| {
        now.signed_duration_since(at).num_seconds() >= i64::try_from(secs).unwrap_or(i64::MAX)
    })
}
