//! The consolidated link-transaction planner.
//!
//! [`MetadataStore::execute_links_tx`] is the single planner behind every
//! transactional public method (`update_links`, `delete_links`,
//! `store_manifest`, `delete_manifest`, `revoke_blob_ownership`): each passes a
//! [`LinksTx`] kind, and the planner builds the [`Transaction`], runs the retry
//! loop, and performs post-apply cleanup. Single-link primitives live in
//! [`super::storage`].

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry_payload},
    store::Store,
    transaction::{Mutation, Transaction, TransactionBuilder},
};

use crate::{
    oci::{Descriptor, Digest, MediaType, Namespace},
    registry::{
        Error,
        metadata_store::{
            BlobIndexOperation, LinkKind, LinkMetadata, LinkOperation, MetadataStore,
            blob_index::shard::{
                any_other_namespace_references_blob, append_shard_for_digest, ops_for_digest,
                shard_will_be_empty,
            },
        },
        path_builder,
    },
};

// Consolidated transaction planner

/// The kind of link transaction the planner runs: one variant per public entry
/// point, each carrying exactly the blob-data / blob-index side effects and
/// timestamps that operation needs. Modelling it as an enum (not a struct of
/// optional fields) makes invalid combinations unrepresentable.
pub enum LinksTx<'a> {
    /// Plain link create/delete batch (`update_links`): no blob-data or
    /// blob-index side effects and no replication timestamps.
    UpdateLinks,
    /// Link delete batch carrying a replicated delete's `source_ts` for the
    /// last-writer-wins gate (`delete_links`); `None` is a plain local delete.
    /// Unlike [`Self::DeleteManifest`] it does no blob-data reclamation.
    DeleteLinks { source_ts: Option<DateTime<Utc>> },
    /// `store_manifest`: the link writes for a manifest push. The manifest
    /// blob-data is written separately to the blob store by the registry before
    /// this runs. `created_at` stamps new link metadata; a replicated write
    /// passes the author's `source_ts` for LWW.
    StoreManifest { created_at: Option<DateTime<Utc>> },
    /// `delete_manifest`: removes the links and reports via `reclaim_blob`
    /// whether the blob became unreferenced (the shard is empty and no other
    /// namespace references it), leaving the blob-data reclaim to the caller.
    /// `source_ts` gates each deleted tag via LWW; the caller's
    /// `blob-data:{digest}` lock keeps the unreferenced-check from racing a
    /// concurrent grant.
    DeleteManifest {
        blob: &'a Digest,
        source_ts: Option<DateTime<Utc>>,
    },
    /// `revoke_blob_ownership`: removes `namespace`'s shard ownership entry and
    /// reports via `reclaim_blob` whether the blob became unreferenced. The
    /// caller holds the `blob-data:{digest}` lock across the call and reclaims
    /// the blob-data from the blob store.
    RevokeBlobOwnership {
        blob: &'a Digest,
        ops: Vec<BlobIndexOperation>,
    },
}

impl<'a> LinksTx<'a> {
    /// Blob-data digest to reclaim when it becomes unreferenced.
    fn blob_data_delete_if_unreferenced(&self) -> Option<&'a Digest> {
        match self {
            LinksTx::DeleteManifest { blob, .. } | LinksTx::RevokeBlobOwnership { blob, .. } => {
                Some(*blob)
            }
            _ => None,
        }
    }

    /// Direct blob-index shard ops applied alongside the link-derived ops.
    fn blob_index_ops(&self) -> Option<(&'a Digest, &[BlobIndexOperation])> {
        match self {
            LinksTx::RevokeBlobOwnership { blob, ops } => Some((*blob, ops.as_slice())),
            _ => None,
        }
    }

    /// Creation timestamp stamped on newly-written link metadata (`None` =
    /// stamp the current time).
    fn created_at(&self) -> Option<DateTime<Utc>> {
        match self {
            LinksTx::StoreManifest { created_at, .. } => *created_at,
            _ => None,
        }
    }

    /// Author timestamp of a replicated delete, gating each deleted tag via LWW.
    fn delete_source_ts(&self) -> Option<DateTime<Utc>> {
        match self {
            LinksTx::DeleteLinks { source_ts } | LinksTx::DeleteManifest { source_ts, .. } => {
                *source_ts
            }
            _ => None,
        }
    }

    /// Whether this transaction touches blob-data or the blob-index beyond its
    /// link operations, so the empty-no-op short-circuit must not fire.
    fn has_blob_side_effects(&self) -> bool {
        !matches!(self, LinksTx::UpdateLinks | LinksTx::DeleteLinks { .. })
    }
}

/// Data captured from a successful link-transaction attempt, used for
/// post-apply cache/cleanup steps outside the engine lock.
#[derive(Default)]
struct LinksTxCaptured {
    /// Link writes that were committed (both tracked and non-tracked creates,
    /// plus tracked deletes where references remain).
    written_links: Vec<(LinkKind, LinkMetadata)>,
    /// Links that were fully removed.
    deleted_links: Vec<LinkKind>,
    /// Prior target per `Create` op's link (`None` = absent), as read by the
    /// committed attempt.
    prior_targets: Vec<(LinkKind, Option<Digest>)>,
    /// `Some(message)` when the attempt's last-writer-wins guard rejected the
    /// write; the attempt committed an empty transaction and the caller maps
    /// this to [`Error::ReplicationSuperseded`].
    superseded: Option<String>,
    /// Whether this transaction left the manifest blob unreferenced, so the
    /// caller should reclaim its blob-data from the blob store under the
    /// blob-data lock it already holds.
    reclaim_blob: bool,
}

/// Prior link state captured by a committed link transaction. The retry loop
/// re-reads each `Create` op's link on every attempt and the commit validates
/// those exact bytes, so this is the state the commit was actually validated
/// against, never a stale pre-write read.
#[derive(Default)]
pub struct LinksCommit {
    /// Prior target per `Create` op's link; `None` = the link did not exist.
    pub prior_targets: Vec<(LinkKind, Option<Digest>)>,
    /// Whether the committed transaction left the manifest blob unreferenced, so
    /// the caller should reclaim its blob-data from the blob store.
    pub reclaim_blob: bool,
}

impl LinksCommit {
    /// Whether the commit changed `link`: it was absent or pointed at a
    /// different digest before. Fails open (`true`) when the transaction had
    /// no `Create` op for `link`, so a genuine write is never suppressed.
    #[must_use]
    pub fn changed(&self, link: &LinkKind, target: &Digest) -> bool {
        self.prior_targets
            .iter()
            .find(|(l, _)| l == link)
            .is_none_or(|(_, prior)| prior.as_ref() != Some(target))
    }
}

/// One operation's snapshot from the single per-attempt read pass. Field
/// types mirror the borrows of the originating [`LinkOperation`], so the planning
/// phases can pass these around without re-reading.
enum OpSnapshot<'a> {
    /// A create with the link's current target (`None` = the link does not
    /// exist).
    Create {
        link: &'a LinkKind,
        target: &'a Digest,
        old_target: Option<Digest>,
        referrer: &'a Option<Digest>,
        media_type: &'a Option<MediaType>,
        descriptor: &'a Option<Box<Descriptor>>,
    },
    /// A delete with the link's currently-stored metadata (`None` = already
    /// absent). Boxed because `LinkMetadata` dwarfs the common `Create` variant.
    Delete {
        link: &'a LinkKind,
        metadata: Option<Box<LinkMetadata>>,
        referrer: &'a Option<Digest>,
    },
}

/// The single per-attempt read pass over every operation's link.
struct LinksSnapshot<'a> {
    ops: Vec<OpSnapshot<'a>>,
    /// Current metadata per present `Create` link, consumed by the mutation
    /// builders when merging tracked-link referrers.
    link_cache: HashMap<LinkKind, LinkMetadata>,
    /// The observed bytes per link path (empty = absent), joined to the
    /// transaction read set: any racing write to a touched link fails the
    /// commit's prepare validation and the retry re-plans against fresh state.
    reads: Vec<(String, Bytes)>,
}

/// The link-derived part of a transaction: the in-progress builder plus the
/// blob-index ops, written links and deleted links the later phases consume.
struct LinkMutations {
    builder: TransactionBuilder,
    pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    written_links: Vec<(LinkKind, LinkMetadata)>,
    deleted_links: Vec<LinkKind>,
}

impl LinkMutations {
    /// Queue a blob-index shard op for `digest`.
    fn push_blob_op(&mut self, digest: &Digest, op: BlobIndexOperation) {
        self.pending_blob_ops
            .entry(digest.clone())
            .or_default()
            .push(op);
    }

    /// `Put` `metadata` for `link` and record it among the written links.
    fn put_link(
        &mut self,
        namespace: &Namespace,
        link: &LinkKind,
        metadata: LinkMetadata,
    ) -> Result<(), TxError> {
        let body = serde_json::to_vec(&metadata)
            .map(Bytes::from)
            .map_err(TxError::Serde)?;
        self.builder = std::mem::take(&mut self.builder).mutation(Mutation::Put {
            key: path_builder::link_path(link, namespace),
            body,
            expected: None,
        });
        self.written_links.push((link.clone(), metadata));
        Ok(())
    }

    /// `Delete` `link`, queue its blob-index `Remove` against `target`, and
    /// record it among the deleted links.
    fn delete_link(&mut self, namespace: &Namespace, link: &LinkKind, target: &Digest) {
        self.builder = std::mem::take(&mut self.builder).mutation(Mutation::Delete {
            key: path_builder::link_path(link, namespace),
            expected: None,
        });
        self.push_blob_op(target, BlobIndexOperation::Remove(link.clone()));
        self.deleted_links.push(link.clone());
    }
}

impl MetadataStore {
    /// Engine-backed implementation of `update_links`.
    ///
    /// Thin wrapper over [`Self::execute_links_tx`].
    pub async fn update_links(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        if operations.is_empty() {
            return Ok(());
        }
        self.execute_links_tx(namespace, operations, LinksTx::UpdateLinks)
            .await
            .map(|_| ())
    }

    /// Delete links (e.g. a tag) carrying a replicated delete's `source_ts` for
    /// the last-writer-wins gate. Unlike [`Self::delete_manifest`] it does no
    /// blob-data reclamation. A `None` `source_ts` is a plain unconditional
    /// delete (a genuine client request).
    pub async fn delete_links(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        if operations.is_empty() {
            return Ok(());
        }
        self.execute_links_tx(namespace, operations, LinksTx::DeleteLinks { source_ts })
            .await
            .map(|_| ())
    }

    /// Run the retry loop, build the link-update transaction (plus any blob-data
    /// / blob-index side effects the [`LinksTx`] kind carries), commit it, and
    /// perform post-apply cleanup. Every public entry point shares this body,
    /// differing only in the `tx` kind; the per-attempt planning is split into
    /// the named phases below.
    pub async fn execute_links_tx(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
        tx: LinksTx<'_>,
    ) -> Result<LinksCommit, Error> {
        let (_, result) = execute_with_retry_payload(
            self.executor(),
            || async {
                // Phase 1: one read pass over every operation's link. The
                // observed bytes join the transaction read set in phase 3, so
                // a racing write to any touched link aborts this attempt at
                // prepare and the retry re-plans against fresh state.
                let snapshot = self.snapshot_links(namespace, operations).await?;

                // Empty no-op short-circuit: no creates, every delete target
                // already missing, and no extras to apply.
                if is_empty_noop(&snapshot.ops, &tx) {
                    return Ok((Transaction::builder().build(), LinksTxCaptured::default()));
                }

                // Phase 2: last-writer-wins gate for replicated writes/deletes.
                if let Some(message) = lww_superseded(&snapshot, &tx) {
                    return Ok((
                        Transaction::builder().build(),
                        LinksTxCaptured {
                            superseded: Some(message),
                            ..LinksTxCaptured::default()
                        },
                    ));
                }

                let LinksSnapshot {
                    ops,
                    mut link_cache,
                    reads,
                } = snapshot;

                // Phase 3: build the link mutations over the snapshot state.
                let LinkMutations {
                    mut builder,
                    pending_blob_ops,
                    written_links,
                    deleted_links,
                } = build_link_mutations(namespace, &ops, &mut link_cache, &tx, reads)?;

                // Phase 4: decide whether this transaction leaves the manifest
                // blob unreferenced (its shard becomes empty AND no other
                // namespace references it). The blob-data itself lives in the
                // blob store; the caller reclaims it under the blob-data lock.
                let store = self.store_arc();
                let reclaim_blob =
                    blob_will_be_unreferenced(store.as_ref(), namespace, &tx, &pending_blob_ops)
                        .await?;

                // Phase 5: append blob-index shard mutations and finalize.
                for (digest, shard_ops) in &pending_blob_ops {
                    builder = append_shard_for_digest(
                        store.as_ref(),
                        namespace,
                        digest,
                        shard_ops,
                        builder,
                    )
                    .await
                    .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                }

                Ok((
                    builder.build(),
                    LinksTxCaptured {
                        written_links,
                        deleted_links,
                        prior_targets: capture_prior_targets(&ops),
                        superseded: None,
                        reclaim_blob,
                    },
                ))
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map_err(Error::from)?;

        if let Some(message) = result.superseded {
            return Err(Error::ReplicationSuperseded(message));
        }

        // Post-apply cleanup (best-effort, outside the engine lock)
        for link in &result.deleted_links {
            let container = path_builder::link_container_path(link, namespace);
            let _ = self.store().object_store().delete_prefix(&container).await;
            if matches!(link, LinkKind::Tag(_))
                && let Some((parent, _)) = container.rsplit_once('/')
            {
                let _ = self.store().object_store().delete_prefix(parent).await;
            }
        }

        for (link, metadata) in &result.written_links {
            self.cache_put(namespace, link, metadata).await;
        }
        for link in &result.deleted_links {
            self.cache_invalidate(namespace, link).await;
        }

        Ok(LinksCommit {
            prior_targets: result.prior_targets,
            reclaim_blob: result.reclaim_blob,
        })
    }

    /// Phase 1: read each operation's link once, in parallel, capturing the
    /// raw bytes (for the transaction read set) and the parsed metadata (for
    /// planning) together. A non-`NotFound` read error fails the attempt
    /// rather than being planned around as an absent link.
    async fn snapshot_links<'a>(
        &self,
        namespace: &Namespace,
        operations: &'a [LinkOperation],
    ) -> Result<LinksSnapshot<'a>, TxError> {
        let results = join_all(operations.iter().map(|op| async move {
            let link = match op {
                LinkOperation::Create { link, .. } | LinkOperation::Delete { link, .. } => link,
            };
            let link_path = path_builder::link_path(link, namespace);
            let found = self.read_link_raw(&link_path).await?;
            Ok::<_, TxError>((op, link_path, found))
        }))
        .await;

        let mut snapshot = LinksSnapshot {
            ops: Vec::with_capacity(operations.len()),
            link_cache: HashMap::new(),
            reads: Vec::new(),
        };
        let mut seen_paths = HashSet::new();
        for result in results {
            let (op, link_path, found) = result?;
            if seen_paths.insert(link_path.clone()) {
                let bytes = found
                    .as_ref()
                    .map_or_else(Bytes::new, |(bytes, _)| bytes.clone());
                snapshot.reads.push((link_path, bytes));
            }
            let metadata = found.map(|(_, metadata)| metadata);
            snapshot.ops.push(match op {
                LinkOperation::Create {
                    link,
                    target,
                    referrer,
                    media_type,
                    descriptor,
                } => {
                    let old_target = metadata.as_ref().map(|m| m.target.clone());
                    if let Some(metadata) = metadata {
                        snapshot.link_cache.insert(link.clone(), metadata);
                    }
                    OpSnapshot::Create {
                        link,
                        target,
                        old_target,
                        referrer,
                        media_type,
                        descriptor,
                    }
                }
                LinkOperation::Delete { link, referrer } => OpSnapshot::Delete {
                    link,
                    metadata: metadata.map(Box::new),
                    referrer,
                },
            });
        }
        Ok(snapshot)
    }

    /// Read a link's exact stored bytes and parsed metadata, or `None` when
    /// absent. The snapshot pass needs the raw bytes for the read-set
    /// fingerprint alongside the parsed metadata.
    async fn read_link_raw(
        &self,
        link_path: &str,
    ) -> Result<Option<(Bytes, LinkMetadata)>, TxError> {
        match self.store().object_store().get(link_path).await {
            Ok(data) => {
                let bytes = Bytes::from(data.clone());
                let metadata: LinkMetadata = serde_json::from_slice(&data)
                    .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                Ok(Some((bytes, metadata)))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(TxError::Storage(e)),
        }
    }
}

/// Phase 2: the last-writer-wins gate for replicated writes and deletes.
/// Returns `Some(message)` when a local tag is newer than the replicated
/// source, so the attempt commits an empty transaction and the caller maps it
/// to [`Error::ReplicationSuperseded`]. The comparison runs against the same
/// snapshot the read set validates, so a racing tag write aborts the commit
/// rather than gating LWW on stale state.
fn lww_superseded(snapshot: &LinksSnapshot<'_>, tx: &LinksTx<'_>) -> Option<String> {
    for op in &snapshot.ops {
        match op {
            OpSnapshot::Create { link, target, .. } => {
                if !matches!(link, LinkKind::Tag(_)) {
                    continue;
                }
                if let (Some(source_ts), Some(metadata)) =
                    (tx.created_at(), snapshot.link_cache.get(*link))
                    && let Some(created_at) = metadata.supersedes(source_ts, Some(target))
                {
                    return Some(format!(
                        "local {link} (created {created_at}) is newer \
                         than the replicated source ({source_ts})"
                    ));
                }
            }
            OpSnapshot::Delete {
                link,
                metadata: Some(metadata),
                ..
            } => {
                if !matches!(link, LinkKind::Tag(_)) {
                    continue;
                }
                if let Some(source_ts) = tx.delete_source_ts()
                    && let Some(created_at) = metadata.supersedes(source_ts, None)
                {
                    return Some(format!(
                        "local {link} (created {created_at}) is newer \
                         than the replicated delete ({source_ts})"
                    ));
                }
            }
            OpSnapshot::Delete { .. } => {}
        }
    }
    None
}

/// Empty no-op short-circuit predicate: no creates, no blob side effects, and
/// every delete target already missing.
fn is_empty_noop(ops: &[OpSnapshot<'_>], tx: &LinksTx<'_>) -> bool {
    let had_creates = ops.iter().any(|op| matches!(op, OpSnapshot::Create { .. }));
    let all_deletes_absent = ops.iter().all(|op| match op {
        OpSnapshot::Create { .. } => true,
        OpSnapshot::Delete { metadata, .. } => metadata.is_none(),
    });
    !had_creates && !tx.has_blob_side_effects() && all_deletes_absent
}

/// Phase 3: turn the snapshot's creates and deletes into transaction mutations,
/// accumulating the blob-index ops and the written / deleted link sets. Seeds the
/// builder with the snapshot reads and direct blob-index ops, then threads a
/// [`LinkMutations`] accumulator through the create/delete processors.
fn build_link_mutations(
    namespace: &Namespace,
    ops: &[OpSnapshot<'_>],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    tx: &LinksTx<'_>,
    reads: Vec<(String, Bytes)>,
) -> Result<LinkMutations, TxError> {
    let mut builder = Transaction::builder();
    for (key, body) in reads {
        builder = builder.read(key, body);
    }
    let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();

    // Seed direct blob-index ops (e.g. `revoke_blob_ownership`'s ownership
    // revoke) so the unreferenced check and the shard mutations below treat them
    // like link-derived ops.
    if let Some((digest, ops)) = tx.blob_index_ops() {
        pending_blob_ops
            .entry(digest.clone())
            .or_default()
            .extend(ops.iter().cloned());
    }

    let acc = LinkMutations {
        builder,
        pending_blob_ops,
        written_links: Vec::new(),
        deleted_links: Vec::new(),
    };
    let acc = build_create_mutations(namespace, ops, link_cache, tx, acc)?;
    let acc = build_delete_mutations(namespace, ops, acc)?;
    Ok(acc)
}

/// Phase 3 (creates): append a link `Put` per `Create` op, recording the
/// inserted / moved blob-index entries and the written link metadata.
fn build_create_mutations(
    namespace: &Namespace,
    ops: &[OpSnapshot<'_>],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    tx: &LinksTx<'_>,
    mut acc: LinkMutations,
) -> Result<LinkMutations, TxError> {
    for op in ops {
        let OpSnapshot::Create {
            link,
            target,
            old_target,
            referrer,
            media_type,
            descriptor,
        } = op
        else {
            continue;
        };

        if link.is_tracked() && referrer.is_some() {
            // Tracked link: merge referrer into existing or new metadata.
            let mut metadata = link_cache.remove(*link).unwrap_or_else(|| {
                LinkMetadata::from_digest_at(
                    (*target).clone(),
                    tx.created_at().unwrap_or_else(Utc::now),
                )
                .with_media_type((*media_type).clone())
                .with_descriptor(descriptor.as_ref().map(|b| b.as_ref().clone()))
            });

            if let Some(manifest_digest) = referrer {
                metadata.add_referrer((*manifest_digest).clone());
            }

            if old_target.is_none() {
                acc.push_blob_op(target, BlobIndexOperation::Insert((*link).clone()));
            }
            acc.put_link(namespace, link, metadata)?;
        } else {
            // Non-tracked link.
            let same_target = old_target.as_ref() == Some(*target);
            if !same_target {
                acc.push_blob_op(target, BlobIndexOperation::Insert((*link).clone()));
                if let Some(old) = old_target
                    && *old != **target
                {
                    acc.push_blob_op(old, BlobIndexOperation::Remove((*link).clone()));
                }
            }

            // A same-digest re-push keeps the existing `created_at`: the binding
            // is unchanged so dispatch is suppressed, and bumping the timestamp
            // would let an interleaved peer write lose locally yet win on peers.
            // A real binding change stamps the new write time.
            let created_at = if same_target {
                link_cache.get(*link).and_then(|m| m.created_at)
            } else {
                None
            }
            .or(tx.created_at())
            .unwrap_or_else(Utc::now);
            let metadata = LinkMetadata::from_digest_at((*target).clone(), created_at)
                .with_media_type((*media_type).clone())
                .with_descriptor(descriptor.as_ref().map(|b| b.as_ref().clone()));
            acc.put_link(namespace, link, metadata)?;
        }
    }
    Ok(acc)
}

/// Phase 3 (deletes): for each `Delete` op whose link exists, either prune one
/// referrer (a tracked link with references left becomes a `Put`) or remove
/// the link outright (a `Delete` plus the blob-index `Remove`).
fn build_delete_mutations(
    namespace: &Namespace,
    ops: &[OpSnapshot<'_>],
    mut acc: LinkMutations,
) -> Result<LinkMutations, TxError> {
    for op in ops {
        let OpSnapshot::Delete {
            link,
            metadata: Some(metadata),
            referrer,
        } = op
        else {
            continue;
        };

        if link.is_tracked() && referrer.is_some() {
            let mut pruned = (**metadata).clone();
            if let Some(manifest_digest) = referrer {
                pruned.remove_referrer(manifest_digest);
            }

            // References remain: keep the link with the referrer pruned;
            // otherwise remove it outright.
            if pruned.has_references() {
                acc.put_link(namespace, link, pruned)?;
            } else {
                acc.delete_link(namespace, link, &metadata.target);
            }
        } else {
            acc.delete_link(namespace, link, &metadata.target);
        }
    }
    Ok(acc)
}

/// Phase 6: decide whether this transaction leaves the manifest blob
/// unreferenced (its namespace shard becomes empty and no other namespace
/// references it). The caller reclaims the blob-data from the blob store; the
/// blob's existence is not probed here (the reclaim is an idempotent
/// blob-store delete).
async fn blob_will_be_unreferenced(
    store: &Store,
    namespace: &Namespace,
    tx: &LinksTx<'_>,
    pending_blob_ops: &HashMap<Digest, Vec<BlobIndexOperation>>,
) -> Result<bool, TxError> {
    let Some(digest) = tx.blob_data_delete_if_unreferenced() else {
        return Ok(false);
    };
    if !pending_blob_ops.contains_key(digest) {
        return Ok(false);
    }

    let shard_path_ns = path_builder::blob_index_shard_path(digest, namespace);
    let our_shard_will_be_empty = shard_will_be_empty(
        store,
        ops_for_digest(pending_blob_ops, digest),
        &shard_path_ns,
    )
    .await?;
    if !our_shard_will_be_empty {
        return Ok(false);
    }

    let refs_prefix = path_builder::blob_index_refs_dir(digest);
    let other_refs_exist = any_other_namespace_references_blob(store, namespace, &refs_prefix)
        .await
        .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
    Ok(!other_refs_exist)
}

/// Prior target per `Create` op, from this attempt's read-set-validated
/// snapshot.
fn capture_prior_targets(ops: &[OpSnapshot<'_>]) -> Vec<(LinkKind, Option<Digest>)> {
    ops.iter()
        .filter_map(|op| match op {
            OpSnapshot::Create {
                link, old_target, ..
            } => Some(((*link).clone(), old_target.clone())),
            OpSnapshot::Delete { .. } => None,
        })
        .collect()
}

// store_manifest / delete_manifest: thin wrappers over the planner above.

impl MetadataStore {
    /// Persist a manifest's link metadata and blob-index shard updates as a
    /// single atomic transaction. The manifest blob-data itself is content and
    /// is written separately to the blob store by the caller. Returns the
    /// [`LinksCommit`] carrying each created link's commit-validated prior
    /// target.
    pub async fn store_manifest(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
        created_at: Option<DateTime<Utc>>,
    ) -> Result<LinksCommit, Error> {
        let tx = LinksTx::StoreManifest { created_at };
        self.execute_links_tx(namespace, operations, tx).await
    }

    /// Delete a manifest: removes link metadata and cleans up blob-index shards
    /// as a single atomic transaction. Returns whether the manifest blob became
    /// unreferenced, so the caller reclaims its blob-data from the blob store
    /// under the blob-data lock it must hold across this call (so a concurrent
    /// reference grant isn't missed; the `ManifestBlobUnknown` race).
    pub async fn delete_manifest(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        operations: &[LinkOperation],
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<bool, Error> {
        let tx = LinksTx::DeleteManifest {
            blob: digest,
            source_ts,
        };
        self.execute_links_tx(namespace, operations, tx)
            .await
            .map(|c| c.reclaim_blob)
    }
}
