//! The consolidated link-transaction planner.
//!
//! [`MetadataStore::execute_links_tx`] is the single planner behind every
//! transactional public method; each passes a [`LinksTx`] kind, and the planner
//! builds the transaction, runs the retry loop, and does post-apply cleanup.
//! Single-link primitives live in [`super::storage`].

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
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error, LinkKind, LinkMetadata, LinkOperation,
            MetadataStore,
            blob_index::shard::{
                any_other_namespace_references_blob, append_shard_for_digest, ops_for_digest,
                shard_will_be_empty,
            },
        },
        path_builder,
    },
};

// Error mapping

/// Map a tx-engine error to a metadata-store error.
pub fn tx_error_to_meta(err: TxError) -> Error {
    match err {
        TxError::Storage(e) => Error::from(e),
        TxError::Lock(e) => Error::from(e),
        TxError::Serde(e) => Error::from(e),
        TxError::Conflict | TxError::Precondition | TxError::PartialCommit => {
            Error::Coordination("transaction conflict: retry budget exhausted".to_string())
        }
        TxError::Build(msg) => Error::Coordination(format!("engine build error: {msg}")),
    }
}

// Consolidated transaction planner

/// The kind of link transaction the planner runs: one variant per public entry
/// point, each carrying exactly the blob-data / blob-index side effects and
/// timestamps it needs. An enum (not a struct of optionals) makes invalid
/// combinations unrepresentable.
pub enum LinksTx<'a> {
    /// Plain link create/delete batch (`update_links`): no blob side effects.
    UpdateLinks,
    /// Guarded metadata restamp (`restamp_links`): each non-tracked create
    /// commits only while the stored link still targets the op's target,
    /// reusing the stored metadata wholesale and updating only
    /// `media_type`/`descriptor`. A moved or vanished link drops the op, so
    /// the concurrent writer wins and no timestamp is ever manufactured.
    RestampLinks,
    /// Link delete batch carrying a replicated delete's `source_ts` for the LWW
    /// gate (`delete_links`); `None` is a plain local delete.
    DeleteLinks { source_ts: Option<DateTime<Utc>> },
    /// `store_manifest`: the link writes for a manifest push. `created_at` stamps
    /// new link metadata; a replicated write passes the author's `source_ts` for
    /// LWW. The blob-data is written separately by the registry beforehand.
    /// `granted` holds the digests whose `blob-data` locks the caller holds; a
    /// tracked grant insert outside it commits nothing and reports
    /// [`LinksCommit::needs_locks`].
    StoreManifest {
        created_at: Option<DateTime<Utc>>,
        granted: &'a HashSet<Digest>,
    },
    /// `delete_manifest`: removes the links and reports via `reclaim_blob` whether
    /// the blob became unreferenced, leaving the blob-data reclaim to the caller.
    /// `source_ts` gates each deleted tag via LWW.
    DeleteManifest {
        blob: &'a Digest,
        source_ts: Option<DateTime<Utc>>,
    },
    /// `revoke_blob_ownership`: removes `namespace`'s ownership entry and reports
    /// via `reclaim_blob` whether the blob became unreferenced. The caller holds
    /// the `blob-data:{digest}` lock and reclaims the blob-data.
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
    fn direct_shard_ops(&self) -> Option<(&'a Digest, &[BlobIndexOperation])> {
        match self {
            LinksTx::RevokeBlobOwnership { blob, ops } => Some((*blob, ops.as_slice())),
            _ => None,
        }
    }

    /// Creation timestamp for newly-written link metadata (`None` = now).
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
        !matches!(
            self,
            LinksTx::UpdateLinks | LinksTx::RestampLinks | LinksTx::DeleteLinks { .. }
        )
    }
}

/// Data captured from a committed link-transaction attempt, for post-apply
/// cache/cleanup steps outside the engine lock.
#[derive(Default)]
struct LinksTxCaptured {
    /// Committed link writes (creates plus tracked deletes with references left).
    written_links: Vec<(LinkKind, LinkMetadata)>,
    /// Links fully removed.
    deleted_links: Vec<LinkKind>,
    /// Prior target per `Create` op's link (`None` = absent).
    prior_targets: Vec<(LinkKind, Option<Digest>)>,
    /// `Some(message)` when the LWW guard rejected the write; the attempt
    /// committed an empty transaction and the caller maps this to
    /// [`Error::ReplicationSuperseded`].
    superseded: Option<String>,
    /// Tracked grant inserts the attempt refused for lack of a blob-data lock;
    /// non-empty means an empty transaction was committed.
    needs_locks: Vec<Digest>,
    /// Whether the manifest blob became unreferenced, so the caller reclaims its
    /// blob-data under the lock it holds.
    reclaim_blob: bool,
}

/// Prior link state from a committed transaction, validated against the retry
/// loop's per-attempt re-read (never a stale pre-write read).
#[derive(Default)]
pub struct LinksCommit {
    /// Prior target per `Create` op's link; `None` = the link did not exist.
    pub prior_targets: Vec<(LinkKind, Option<Digest>)>,
    /// Whether the committed transaction left the manifest blob unreferenced.
    pub reclaim_blob: bool,
    /// Digests whose `blob-data` locks a `store_manifest` must hold before its
    /// tracked grant inserts may commit. Non-empty means NOTHING was committed;
    /// the caller acquires the locks and retries.
    pub needs_locks: Vec<Digest>,
}

impl LinksCommit {
    /// Whether the commit changed `link` (absent or a different prior digest).
    /// Fails open (`true`) when the transaction had no `Create` op for `link`, so
    /// a genuine write is never suppressed.
    #[must_use]
    pub fn changed(&self, link: &LinkKind, target: &Digest) -> bool {
        self.prior_targets
            .iter()
            .find(|(l, _)| l == link)
            .is_none_or(|(_, prior)| prior.as_ref() != Some(target))
    }
}

/// One operation's pre-lock snapshot, captured once per retry attempt, so the
/// planning phases can pass it around without re-reading.
enum PrelockOp<'a> {
    /// A create with the link's prior target as read before locking (`None` =
    /// the link did not exist).
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
        expected_target: &'a Option<Digest>,
    },
}

/// Outcome of the commit-validate last-writer-wins phase.
struct LwwValidation {
    /// Tag-link bytes joined to the transaction read set so a racing re-put or
    /// re-delete aborts this attempt at prepare.
    reads: Vec<(String, Bytes)>,
    /// `Some(message)` when a local link is newer than the replicated write and
    /// the attempt should commit an empty transaction instead.
    superseded: Option<String>,
}

/// The link-derived part of a transaction: the builder plus the blob-index ops
/// and written/deleted link sets the later phases consume.
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
    /// Engine-backed `update_links`; a thin wrapper over
    /// [`Self::execute_links_tx`].
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

    /// Guarded metadata restamp: each non-tracked `Create` commits only while
    /// the stored link still targets the op's target, reusing the stored
    /// metadata (timestamps, referrers) and updating only
    /// `media_type`/`descriptor`. A moved or vanished link drops the op so the
    /// concurrent writer wins; the same-target LWW fingerprint read aborts a
    /// racing write at prepare. Scrub's tag restamp rides this.
    pub async fn restamp_links(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        if operations.is_empty() {
            return Ok(());
        }
        self.execute_links_tx(namespace, operations, LinksTx::RestampLinks)
            .await
            .map(|_| ())
    }

    /// Delete links carrying a replicated delete's `source_ts` for the LWW gate;
    /// `None` is a plain client delete. Unlike [`Self::delete_manifest`] it does
    /// no blob-data reclamation.
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

    /// Run the retry loop, build the link-update transaction (plus any blob side
    /// effects the [`LinksTx`] kind carries), commit it, and do post-apply
    /// cleanup. Every public entry point shares this, differing only in `tx`.
    pub async fn execute_links_tx(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
        tx: LinksTx<'_>,
    ) -> Result<LinksCommit, Error> {
        let (_, result) = execute_with_retry_payload(
            self.executor(),
            || async {
                let prelock = self.prelock_read_links(namespace, operations).await;

                if is_empty_noop(&prelock, &tx) {
                    return Ok((Transaction::builder().build(), LinksTxCaptured::default()));
                }

                // Re-read link state inside the retry closure for conflict
                // detection and metadata merging.
                let (mut link_cache, raw_links) =
                    self.reread_link_cache(namespace, &prelock).await?;

                detect_create_conflicts(&prelock, &link_cache)?;

                // Tracked grant inserts commit only under their blob-data lock;
                // like the superseded short-circuit below, a refusal commits an
                // empty transaction and reports through the captured payload.
                let needs_locks = refused_tracked_inserts(&prelock, &tx);
                if !needs_locks.is_empty() {
                    return Ok((
                        Transaction::builder().build(),
                        LinksTxCaptured {
                            needs_locks,
                            ..LinksTxCaptured::default()
                        },
                    ));
                }

                let lww = self.validate_lww_reads(namespace, &prelock, &tx).await?;
                if let Some(message) = lww.superseded {
                    return Ok((
                        Transaction::builder().build(),
                        LinksTxCaptured {
                            superseded: Some(message),
                            ..LinksTxCaptured::default()
                        },
                    ));
                }

                let LinkMutations {
                    mut builder,
                    pending_blob_ops,
                    written_links,
                    deleted_links,
                } = build_link_mutations(
                    namespace,
                    &prelock,
                    &mut link_cache,
                    &raw_links,
                    &tx,
                    lww.reads,
                )?;

                // Whether the manifest blob becomes unreferenced; the caller
                // reclaims its blob-data under the blob-data lock.
                let store = self.store_arc();
                let reclaim_blob =
                    blob_will_be_unreferenced(store.as_ref(), namespace, &tx, &pending_blob_ops)
                        .await?;

                for (digest, ops) in &pending_blob_ops {
                    builder =
                        append_shard_for_digest(store.as_ref(), namespace, digest, ops, builder)
                            .await
                            .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                }

                Ok((
                    builder.build(),
                    LinksTxCaptured {
                        written_links,
                        deleted_links,
                        prior_targets: capture_prior_targets(&prelock),
                        superseded: None,
                        needs_locks: Vec::new(),
                        reclaim_blob,
                    },
                ))
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map_err(tx_error_to_meta)?;

        if let Some(message) = result.superseded {
            return Err(Error::ReplicationSuperseded(message));
        }

        if !result.needs_locks.is_empty() {
            // `store_manifest` grows its lock set and retries; every other tx
            // kind has no lock protocol, so the refusal is a hard error.
            return match tx {
                LinksTx::StoreManifest { .. } => Ok(LinksCommit {
                    needs_locks: result.needs_locks,
                    ..LinksCommit::default()
                }),
                _ => Err(Error::TrackedInsertWithoutLock(result.needs_locks)),
            };
        }

        self.post_apply_cleanup(namespace, &result).await;

        Ok(LinksCommit {
            prior_targets: result.prior_targets,
            reclaim_blob: result.reclaim_blob,
            needs_locks: Vec::new(),
        })
    }

    /// Post-apply cleanup, best-effort outside the engine lock: prune emptied
    /// link containers and sync the link cache.
    async fn post_apply_cleanup(&self, namespace: &Namespace, result: &LinksTxCaptured) {
        for link in &result.deleted_links {
            let container = path_builder::link_container_path(link, namespace);
            let _ = self.store().delete_prefix(&container).await;
            if matches!(link, LinkKind::Tag(_))
                && let Some((parent, _)) = container.rsplit_once('/')
            {
                let _ = self.store().delete_prefix(parent).await;
            }
        }

        for (link, metadata) in &result.written_links {
            self.cache_put(namespace, link, metadata).await;
        }
        for link in &result.deleted_links {
            self.cache_invalidate(namespace, link).await;
        }
    }

    /// Read each operation's current link state before the engine lock, in
    /// parallel.
    async fn prelock_read_links<'a>(
        &self,
        namespace: &Namespace,
        operations: &'a [LinkOperation],
    ) -> Vec<PrelockOp<'a>> {
        join_all(operations.iter().map(|op| async move {
            match op {
                LinkOperation::Create {
                    link,
                    target,
                    referrer,
                    media_type,
                    descriptor,
                } => {
                    let old_target = self
                        .read_link_reference(namespace, link)
                        .await
                        .ok()
                        .map(|m| m.target);
                    PrelockOp::Create {
                        link,
                        target,
                        old_target,
                        referrer,
                        media_type,
                        descriptor,
                    }
                }
                LinkOperation::Delete {
                    link,
                    referrer,
                    expected_target,
                } => {
                    let metadata = self
                        .read_link_reference(namespace, link)
                        .await
                        .ok()
                        .map(Box::new);
                    PrelockOp::Delete {
                        link,
                        metadata,
                        referrer,
                        expected_target,
                    }
                }
            }
        }))
        .await
    }

    /// Re-read each operation's link inside the retry closure so conflict
    /// detection and metadata merging run against current state. Also returns
    /// the raw bytes per present link, backing the conditioning reads
    /// [`build_link_mutations`] joins to the transaction (absent = no entry).
    async fn reread_link_cache(
        &self,
        namespace: &Namespace,
        prelock: &[PrelockOp<'_>],
    ) -> Result<(HashMap<LinkKind, LinkMetadata>, HashMap<LinkKind, Bytes>), TxError> {
        let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
        let mut raw_links: HashMap<LinkKind, Bytes> = HashMap::new();
        for op in prelock {
            let link = match op {
                PrelockOp::Create { link, .. } | PrelockOp::Delete { link, .. } => *link,
            };
            let link_path = path_builder::link_path(link, namespace);
            if let Some((bytes, meta)) = self.read_link_raw(&link_path).await? {
                link_cache.insert(link.clone(), meta);
                raw_links.insert(link.clone(), bytes);
            }
        }
        Ok((link_cache, raw_links))
    }

    /// Read a link's exact stored bytes and parsed metadata, or `None` when
    /// absent. LWW validation needs the raw bytes for the read-set fingerprint.
    async fn read_link_raw(
        &self,
        link_path: &str,
    ) -> Result<Option<(Bytes, LinkMetadata)>, TxError> {
        match self.store().get(link_path).await {
            Ok(data) => {
                let bytes = Bytes::from(data.clone());
                let metadata = LinkMetadata::from_bytes(data)
                    .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                Ok(Some((bytes, metadata)))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(TxError::Storage(e)),
        }
    }

    /// Commit-validate tag-create and replicated-delete reads by joining each tag
    /// link's current bytes to the read set, so a racing same-tag write aborts at
    /// prepare and the retry re-reads. Guards LWW comparisons, no-op re-pushes,
    /// and replicated deletes; binding-changing writes skip it.
    async fn validate_lww_reads(
        &self,
        namespace: &Namespace,
        prelock: &[PrelockOp<'_>],
        tx: &LinksTx<'_>,
    ) -> Result<LwwValidation, TxError> {
        let mut lww_reads: Vec<(String, Bytes)> = Vec::new();

        for op in prelock {
            let PrelockOp::Create {
                link,
                target,
                old_target,
                ..
            } = op
            else {
                continue;
            };
            if !matches!(link, LinkKind::Tag(_)) {
                continue;
            }
            let same_target = old_target.as_ref() == Some(*target);
            if tx.created_at().is_none() && !same_target {
                continue;
            }

            let link_path = path_builder::link_path(link, namespace);
            let found = self.read_link_raw(&link_path).await?;
            let metadata = found.as_ref().map(|(_, m)| m);

            // `old_target` drives the committed dispatch decision, so abort if a
            // racing write moved the tag and let the retry re-read the real prior.
            if metadata.map(|m| &m.target) != old_target.as_ref() {
                return Err(TxError::Conflict);
            }

            // A replicated write gates LWW on this read.
            if let (Some(source_ts), Some(metadata)) = (tx.created_at(), metadata)
                && let Some(created_at) = metadata.supersedes(source_ts, Some(target))
            {
                return Ok(LwwValidation {
                    reads: Vec::new(),
                    superseded: Some(format!(
                        "local {link} (created {created_at}) is newer \
                         than the replicated source ({source_ts})"
                    )),
                });
            }

            lww_reads.push((link_path, found.map_or_else(Bytes::new, |(b, _)| b)));
        }

        if let Some(source_ts) = tx.delete_source_ts() {
            for op in prelock {
                let PrelockOp::Delete { link, .. } = op else {
                    continue;
                };
                if !matches!(link, LinkKind::Tag(_)) {
                    continue;
                }
                let link_path = path_builder::link_path(link, namespace);
                // Absent links need neither deletion nor validation.
                if let Some((bytes, metadata)) = self.read_link_raw(&link_path).await? {
                    if let Some(created_at) = metadata.supersedes(source_ts, None) {
                        return Ok(LwwValidation {
                            reads: Vec::new(),
                            superseded: Some(format!(
                                "local {link} (created {created_at}) is newer \
                                 than the replicated delete ({source_ts})"
                            )),
                        });
                    }
                    lww_reads.push((link_path, bytes));
                }
            }
        }

        Ok(LwwValidation {
            reads: lww_reads,
            superseded: None,
        })
    }
}

/// No-op short-circuit: no creates, no blob side effects, every delete target
/// already missing.
fn is_empty_noop(prelock: &[PrelockOp<'_>], tx: &LinksTx<'_>) -> bool {
    let had_creates = prelock
        .iter()
        .any(|op| matches!(op, PrelockOp::Create { .. }));
    let all_deletes_absent = prelock.iter().all(|op| match op {
        PrelockOp::Create { .. } => true,
        PrelockOp::Delete { metadata, .. } => metadata.is_none(),
    });
    !had_creates && !tx.has_blob_side_effects() && all_deletes_absent
}

/// Tracked grant inserts (a tracked create whose validated prior link is
/// absent) the transaction may not commit: every target outside
/// `store_manifest`'s granted set, and all of them for any other tx kind.
/// Mechanical enforcement of the `blob-data:{digest}` insert invariant.
fn refused_tracked_inserts(prelock: &[PrelockOp<'_>], tx: &LinksTx<'_>) -> Vec<Digest> {
    let mut refused: Vec<Digest> = Vec::new();
    for op in prelock {
        let PrelockOp::Create {
            link,
            target,
            old_target,
            referrer,
            ..
        } = op
        else {
            continue;
        };
        if !(link.is_tracked() && referrer.is_some() && old_target.is_none()) {
            continue;
        }
        let granted = match tx {
            LinksTx::StoreManifest { granted, .. } => granted.contains(*target),
            _ => false,
        };
        if !granted && !refused.contains(*target) {
            refused.push((*target).clone());
        }
    }
    refused
}

/// A create aborts the attempt when its live target diverged from the pre-lock
/// read.
fn detect_create_conflicts(
    prelock: &[PrelockOp<'_>],
    link_cache: &HashMap<LinkKind, LinkMetadata>,
) -> Result<(), TxError> {
    for op in prelock {
        let PrelockOp::Create {
            link, old_target, ..
        } = op
        else {
            continue;
        };
        let current_target = link_cache.get(*link).map(|m| &m.target);
        if current_target != old_target.as_ref() {
            return Err(TxError::Conflict);
        }
    }
    Ok(())
}

/// Turn the validated creates and deletes into transaction mutations. Seeds the
/// builder with the LWW and conditioning reads and the direct blob-index ops,
/// then threads a [`LinkMutations`] accumulator through the create/delete
/// processors.
fn build_link_mutations(
    namespace: &Namespace,
    prelock: &[PrelockOp<'_>],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    raw_links: &HashMap<LinkKind, Bytes>,
    tx: &LinksTx<'_>,
    lww_reads: Vec<(String, Bytes)>,
) -> Result<LinkMutations, TxError> {
    let mut builder = Transaction::builder();
    let mut read_keys: HashSet<String> = HashSet::new();
    for (key, body) in lww_reads {
        read_keys.insert(key.clone());
        builder = builder.read(key, body);
    }

    // Condition every tracked-link write and tag delete on the raw bytes (or
    // absence) this attempt observed, so the insert-versus-merge decision and
    // referrer merges/prunes hold through Apply on both executors. Non-tracked
    // self-links are serialized by the caller's own blob-data lock already.
    for op in prelock {
        let link = match op {
            PrelockOp::Create { link, .. } if link.is_tracked() => *link,
            PrelockOp::Delete {
                link,
                metadata: Some(_),
                ..
            } if link.is_tracked() || matches!(link, LinkKind::Tag(_)) => *link,
            _ => continue,
        };
        let key = path_builder::link_path(link, namespace);
        if !read_keys.insert(key.clone()) {
            continue;
        }
        let body = raw_links.get(link).cloned().unwrap_or_else(Bytes::new);
        builder = builder.read(key, body);
    }

    let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();

    // Seed direct blob-index ops so the unreferenced check and shard mutations
    // treat them like link-derived ops.
    if let Some((digest, ops)) = tx.direct_shard_ops() {
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
    let acc = build_create_mutations(namespace, prelock, link_cache, tx, acc)?;
    let acc = build_delete_mutations(namespace, prelock, link_cache, acc)?;
    Ok(acc)
}

/// Append a link `Put` per `Create` op, recording the inserted / moved blob-index
/// entries and the written link metadata.
fn build_create_mutations(
    namespace: &Namespace,
    prelock: &[PrelockOp<'_>],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    tx: &LinksTx<'_>,
    mut acc: LinkMutations,
) -> Result<LinkMutations, TxError> {
    for op in prelock {
        let PrelockOp::Create {
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
            if matches!(tx, LinksTx::RestampLinks) {
                // Guarded restamp: reuse the stored metadata wholesale, upgrade
                // media_type/descriptor only when the op derives one (a stored
                // value is never erased); a moved or vanished link drops the op
                // (the concurrent writer wins).
                let Some(mut metadata) = same_target.then(|| link_cache.remove(*link)).flatten()
                else {
                    continue;
                };
                if media_type.is_some() {
                    metadata.media_type.clone_from(media_type);
                }
                if descriptor.is_some() {
                    metadata.descriptor = descriptor.as_ref().map(|b| b.as_ref().clone());
                }
                acc.put_link(namespace, link, metadata)?;
                continue;
            }
            if !same_target {
                acc.push_blob_op(target, BlobIndexOperation::Insert((*link).clone()));
                if let Some(old) = old_target
                    && *old != **target
                {
                    acc.push_blob_op(old, BlobIndexOperation::Remove((*link).clone()));
                }
            }

            // A same-digest re-push keeps the existing `created_at` (bumping it
            // would let an interleaved peer write lose locally yet win on peers)
            // and `accessed_at` (retention reads it as the last pull time). A
            // stored `created_at` of None (a legacy link) is preserved verbatim
            // outside a real push, so a repair rewrite can never manufacture
            // LWW freshness. A real binding change stamps the new write time
            // and resets the pull history.
            let existing = if same_target {
                link_cache.get(*link)
            } else {
                None
            };
            let created_at = match existing {
                Some(m) => m.created_at.or_else(|| {
                    matches!(tx, LinksTx::StoreManifest { .. })
                        .then(|| tx.created_at().unwrap_or_else(Utc::now))
                }),
                None => Some(tx.created_at().unwrap_or_else(Utc::now)),
            };
            let metadata = LinkMetadata {
                target: (*target).clone(),
                created_at,
                accessed_at: existing.and_then(|m| m.accessed_at),
                referenced_by: HashSet::new(),
                media_type: (*media_type).clone(),
                descriptor: descriptor.as_ref().map(|b| b.as_ref().clone()),
            };
            acc.put_link(namespace, link, metadata)?;
        }
    }
    Ok(acc)
}

/// For each `Delete` op whose live target still matches the pre-lock read, prune
/// one referrer (a tracked link with references left) or remove the link outright.
fn build_delete_mutations(
    namespace: &Namespace,
    prelock: &[PrelockOp<'_>],
    link_cache: &mut HashMap<LinkKind, LinkMetadata>,
    mut acc: LinkMutations,
) -> Result<LinkMutations, TxError> {
    for op in prelock {
        let PrelockOp::Delete {
            link,
            metadata: Some(pre_meta),
            referrer,
            expected_target,
        } = op
        else {
            continue;
        };

        // Only process when the cached target matches the pre-lock read.
        let current_target = link_cache.get(*link).map(|m| &m.target);
        if current_target != Some(&pre_meta.target) {
            continue;
        }

        // A delete conditioned on an expected target keeps a link that was
        // re-pointed since the caller classified it: the concurrent writer
        // wins. The conditioning read joined above aborts a mid-attempt
        // re-point, so this comparison holds through Apply.
        if expected_target
            .as_ref()
            .is_some_and(|expected| pre_meta.target != *expected)
        {
            continue;
        }

        if link.is_tracked() && referrer.is_some() {
            if let Some(mut metadata) = link_cache.remove(*link) {
                if let Some(manifest_digest) = referrer {
                    metadata.remove_referrer(manifest_digest);
                }

                // Keep the link with the referrer pruned if references remain.
                if metadata.has_references() {
                    acc.put_link(namespace, link, metadata)?;
                } else {
                    acc.delete_link(namespace, link, &pre_meta.target);
                }
            }
        } else {
            acc.delete_link(namespace, link, &pre_meta.target);
        }
    }
    Ok(acc)
}

/// Whether this transaction leaves the manifest blob unreferenced (its shard
/// becomes empty and no other namespace references it). The blob's existence is
/// not probed; the caller's reclaim is an idempotent blob-store delete.
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

    // One legacy `index.json` read serves both emptiness checks below.
    let legacy: Option<BlobIndex> = match store.get(&path_builder::blob_index_path(digest)).await {
        Ok(data) => Some(serde_json::from_slice(&data).unwrap_or_default()),
        Err(StorageError::NotFound) => None,
        Err(e) => return Err(TxError::Storage(e)),
    };

    let shard_path_ns = path_builder::blob_index_shard_path(digest, namespace);
    let our_shard_will_be_empty = shard_will_be_empty(
        store,
        namespace,
        ops_for_digest(pending_blob_ops, digest),
        &shard_path_ns,
        legacy.as_ref(),
    )
    .await?;
    if !our_shard_will_be_empty {
        return Ok(false);
    }

    let other_refs_exist =
        any_other_namespace_references_blob(store, namespace, digest, legacy.as_ref())
            .await
            .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
    Ok(!other_refs_exist)
}

/// Prior target per `Create` op, from this attempt's conflict-validated reads.
fn capture_prior_targets(prelock: &[PrelockOp<'_>]) -> Vec<(LinkKind, Option<Digest>)> {
    prelock
        .iter()
        .filter_map(|op| match op {
            PrelockOp::Create {
                link, old_target, ..
            } => Some(((*link).clone(), old_target.clone())),
            PrelockOp::Delete { .. } => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oci::Tag;

    fn digest(seed: &[u8]) -> Digest {
        Digest::sha256_of_bytes(seed)
    }

    fn namespace() -> Namespace {
        Namespace::new("planner/unit").unwrap()
    }

    fn raw_bytes(metadata: &LinkMetadata) -> Bytes {
        Bytes::from(serde_json::to_vec(metadata).unwrap())
    }

    const NONE_REFERRER: Option<Digest> = None;
    const NONE_MEDIA_TYPE: Option<MediaType> = None;
    const NONE_DESCRIPTOR: Option<Box<Descriptor>> = None;
    const NONE_EXPECTED: Option<Digest> = None;

    /// Conditioning reads cover tracked creates (present via bytes, absent via
    /// an absence read) and tag deletes, deduplicated against LWW reads, while
    /// non-tracked self-link writes stay unconditioned.
    #[test]
    fn conditioning_reads_cover_tracked_ops_and_tag_deletes() {
        let namespace = namespace();
        let manifest = digest(b"manifest");
        let layer = digest(b"present layer");
        let config = digest(b"absent config");

        let layer_link = LinkKind::Layer(layer.clone());
        let config_link = LinkKind::Config(config.clone());
        let self_link = LinkKind::Digest(manifest.clone());
        let tag_link = LinkKind::Tag(Tag::new("v1").unwrap());
        let gone_link = LinkKind::Digest(digest(b"deleted digest"));

        let referrer = Some(manifest.clone());
        let layer_meta = LinkMetadata::from_digest(layer.clone());
        let tag_meta = LinkMetadata::from_digest(manifest.clone());
        let gone_meta = LinkMetadata::from_digest(manifest.clone());

        let prelock = vec![
            // Tracked merge over an existing link.
            PrelockOp::Create {
                link: &layer_link,
                target: &layer,
                old_target: Some(layer.clone()),
                referrer: &referrer,
                media_type: &NONE_MEDIA_TYPE,
                descriptor: &NONE_DESCRIPTOR,
            },
            // Tracked insert over an absent link.
            PrelockOp::Create {
                link: &config_link,
                target: &config,
                old_target: None,
                referrer: &referrer,
                media_type: &NONE_MEDIA_TYPE,
                descriptor: &NONE_DESCRIPTOR,
            },
            // Non-tracked self-link create.
            PrelockOp::Create {
                link: &self_link,
                target: &manifest,
                old_target: None,
                referrer: &NONE_REFERRER,
                media_type: &NONE_MEDIA_TYPE,
                descriptor: &NONE_DESCRIPTOR,
            },
            // Tag delete (conditioned) and non-tracked digest delete (not).
            PrelockOp::Delete {
                link: &tag_link,
                metadata: Some(Box::new(tag_meta.clone())),
                referrer: &NONE_REFERRER,
                expected_target: &NONE_EXPECTED,
            },
            PrelockOp::Delete {
                link: &gone_link,
                metadata: Some(Box::new(gone_meta.clone())),
                referrer: &NONE_REFERRER,
                expected_target: &NONE_EXPECTED,
            },
        ];

        let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::from([
            (layer_link.clone(), layer_meta.clone()),
            (tag_link.clone(), tag_meta.clone()),
            (gone_link.clone(), gone_meta.clone()),
        ]);
        let raw_links: HashMap<LinkKind, Bytes> = HashMap::from([
            (layer_link.clone(), raw_bytes(&layer_meta)),
            (tag_link.clone(), raw_bytes(&tag_meta)),
            (gone_link.clone(), raw_bytes(&gone_meta)),
        ]);

        let granted: HashSet<Digest> = [config.clone()].into_iter().collect();
        let tx = LinksTx::StoreManifest {
            created_at: None,
            granted: &granted,
        };

        let acc = build_link_mutations(
            &namespace,
            &prelock,
            &mut link_cache,
            &raw_links,
            &tx,
            Vec::new(),
        )
        .unwrap();
        let built = acc.builder.build();

        let expect_read = |link: &LinkKind, absent: bool| {
            let key = path_builder::link_path(link, &namespace);
            let read = built
                .reads
                .iter()
                .find(|r| r.key == key)
                .unwrap_or_else(|| panic!("{link} must carry a conditioning read"));
            assert_eq!(read.expects_absent(), absent, "absence flag for {link}");
        };
        expect_read(&layer_link, false);
        expect_read(&config_link, true);
        expect_read(&tag_link, false);

        for unconditioned in [&self_link, &gone_link] {
            let key = path_builder::link_path(unconditioned, &namespace);
            assert!(
                !built.reads.iter().any(|r| r.key == key),
                "{unconditioned} must not be conditioned"
            );
        }
    }

    /// A tag key already in the LWW read set is not read twice.
    #[test]
    fn conditioning_reads_deduplicate_against_lww_reads() {
        let namespace = namespace();
        let manifest = digest(b"lww manifest");
        let tag_link = LinkKind::Tag(Tag::new("dedup").unwrap());
        let tag_meta = LinkMetadata::from_digest(manifest.clone());
        let tag_bytes = raw_bytes(&tag_meta);
        let tag_path = path_builder::link_path(&tag_link, &namespace);

        let prelock = vec![PrelockOp::Delete {
            link: &tag_link,
            metadata: Some(Box::new(tag_meta.clone())),
            referrer: &NONE_REFERRER,
            expected_target: &NONE_EXPECTED,
        }];
        let mut link_cache: HashMap<LinkKind, LinkMetadata> =
            HashMap::from([(tag_link.clone(), tag_meta)]);
        let raw_links: HashMap<LinkKind, Bytes> =
            HashMap::from([(tag_link.clone(), tag_bytes.clone())]);

        let acc = build_link_mutations(
            &namespace,
            &prelock,
            &mut link_cache,
            &raw_links,
            &LinksTx::DeleteLinks {
                source_ts: Some(Utc::now()),
            },
            vec![(tag_path.clone(), tag_bytes)],
        )
        .unwrap();
        let built = acc.builder.build();

        assert_eq!(
            built.reads.iter().filter(|r| r.key == tag_path).count(),
            1,
            "the LWW read already covers the tag delete"
        );
    }

    /// Only an ungranted absent tracked create is refused: merges, granted
    /// inserts, and non-tracked creates pass; every other tx kind refuses all
    /// tracked inserts.
    #[test]
    fn refused_tracked_inserts_scopes_to_ungranted_absent_tracked_creates() {
        let manifest = digest(b"scope manifest");
        let layer = digest(b"scope layer");
        let layer_link = LinkKind::Layer(layer.clone());
        let self_link = LinkKind::Digest(manifest.clone());
        let referrer = Some(manifest.clone());

        let insert = PrelockOp::Create {
            link: &layer_link,
            target: &layer,
            old_target: None,
            referrer: &referrer,
            media_type: &NONE_MEDIA_TYPE,
            descriptor: &NONE_DESCRIPTOR,
        };
        let merge = PrelockOp::Create {
            link: &layer_link,
            target: &layer,
            old_target: Some(layer.clone()),
            referrer: &referrer,
            media_type: &NONE_MEDIA_TYPE,
            descriptor: &NONE_DESCRIPTOR,
        };
        let non_tracked = PrelockOp::Create {
            link: &self_link,
            target: &manifest,
            old_target: None,
            referrer: &NONE_REFERRER,
            media_type: &NONE_MEDIA_TYPE,
            descriptor: &NONE_DESCRIPTOR,
        };

        let empty: HashSet<Digest> = HashSet::new();
        let ungranted = LinksTx::StoreManifest {
            created_at: None,
            granted: &empty,
        };
        let granted_set: HashSet<Digest> = [layer.clone()].into_iter().collect();
        let granted = LinksTx::StoreManifest {
            created_at: None,
            granted: &granted_set,
        };

        assert_eq!(
            refused_tracked_inserts(&[insert], &ungranted),
            vec![layer.clone()]
        );
        let insert = PrelockOp::Create {
            link: &layer_link,
            target: &layer,
            old_target: None,
            referrer: &referrer,
            media_type: &NONE_MEDIA_TYPE,
            descriptor: &NONE_DESCRIPTOR,
        };
        assert!(refused_tracked_inserts(&[insert], &granted).is_empty());
        assert!(refused_tracked_inserts(&[merge], &ungranted).is_empty());
        assert!(refused_tracked_inserts(&[non_tracked], &ungranted).is_empty());

        let insert = PrelockOp::Create {
            link: &layer_link,
            target: &layer,
            old_target: None,
            referrer: &referrer,
            media_type: &NONE_MEDIA_TYPE,
            descriptor: &NONE_DESCRIPTOR,
        };
        assert_eq!(
            refused_tracked_inserts(&[insert], &LinksTx::UpdateLinks),
            vec![layer]
        );
    }
}
