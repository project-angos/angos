//! Link metadata storage primitives and the consolidated link-transaction helper.
//!
//! [`MetadataStore::execute_links_tx`] is the single planner shared by the three
//! transactional public methods (`update_links`, `store_manifest`,
//! `delete_manifest`). Each public method prepares its own
//! [`LinksTxExtras`] and delegates here, which builds the
//! [`Transaction`], runs the retry loop, captures post-apply data, and performs
//! cache / directory / namespace cleanup once on success.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use tracing::warn;

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry_payload},
    store::Store,
    transaction::{Mutation, Transaction, TransactionBuilder},
};

use crate::{
    oci::Digest,
    registry::{
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error, LinkMetadata, LinkOperation, MetadataStore,
            blob_data_lock_key, link_kind::LinkKind, sharded::apply_blob_index_operations,
        },
        path_builder,
    },
};

// ── Storage primitives ────────────────────────────────────────────────────────

impl MetadataStore {
    /// Read the stored [`LinkMetadata`] for `link` within `namespace`.
    pub async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store().get(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(StorageError::NotFound) => Err(Error::ReferenceNotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist `metadata` for `link` within `namespace`.
    ///
    /// Used by tests to set up initial link state. Production code uses
    /// the transactional engine via `update_links` instead.
    #[cfg(test)]
    pub async fn write_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        self.store()
            .put(&link_path, serialized)
            .await
            .map_err(Error::from)
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

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

// ── Shard read-modify-write ───────────────────────────────────────────────────

/// Read the current shard state for `digest`/`namespace`, apply `ops`, and
/// append the resulting read + mutation to `builder`.
///
/// Routing: if a legacy `index.json` exists it is the write target; otherwise
/// the per-namespace shard is used.
pub async fn append_shard_for_digest(
    store: &Store,
    namespace: &str,
    digest: &Digest,
    ops: &[BlobIndexOperation],
    mut builder: TransactionBuilder,
) -> Result<TransactionBuilder, Error> {
    let legacy_path = path_builder::blob_index_path(digest);
    let shard_path = path_builder::blob_index_shard_path(digest, namespace);

    // Check for legacy layout first with a cheap HEAD.
    match store.head(&legacy_path).await {
        Ok(_) => {
            match store.get(&legacy_path).await {
                Ok(data) => {
                    let raw = Bytes::from(data.clone());
                    let mut legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
                    {
                        let entry = legacy.namespace.entry(namespace.to_string()).or_default();
                        apply_blob_index_operations(entry, ops);
                        if entry.is_empty() {
                            legacy.namespace.remove(namespace);
                        }
                    }
                    builder = builder.read(legacy_path.clone(), raw);
                    if legacy.namespace.is_empty() {
                        return Ok(builder.mutation(Mutation::Delete {
                            key: legacy_path,
                            expected: None,
                        }));
                    }
                    let body = Bytes::from(serde_json::to_vec(&legacy)?);
                    return Ok(builder.mutation(Mutation::Put {
                        key: legacy_path,
                        body,
                        expected: None,
                    }));
                }
                Err(StorageError::NotFound) => {
                    // Vanished between HEAD and GET — fall through to sharded path.
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(StorageError::NotFound) => {}
        Err(e) => return Err(e.into()),
    }

    // Sharded path.
    match store.get(&shard_path).await {
        Ok(data) => {
            let raw = Bytes::from(data.clone());
            let mut links: HashSet<LinkKind> = serde_json::from_slice(&data).unwrap_or_default();
            apply_blob_index_operations(&mut links, ops);
            builder = builder.read(shard_path.clone(), raw);
            if links.is_empty() {
                Ok(builder.mutation(Mutation::Delete {
                    key: shard_path,
                    expected: None,
                }))
            } else {
                let body = Bytes::from(serde_json::to_vec(&links)?);
                Ok(builder.mutation(Mutation::Put {
                    key: shard_path,
                    body,
                    expected: None,
                }))
            }
        }
        Err(StorageError::NotFound) => {
            let mut links = HashSet::new();
            apply_blob_index_operations(&mut links, ops);
            if links.is_empty() {
                Ok(builder)
            } else {
                let body = Bytes::from(serde_json::to_vec(&links)?);
                Ok(builder.mutation(Mutation::PutIfAbsent {
                    key: shard_path,
                    body,
                }))
            }
        }
        Err(e) => Err(e.into()),
    }
}

// ── Consolidated transaction planner ──────────────────────────────────────────

/// Optional add-on mutations layered on top of a link-update transaction.
///
/// `update_links` uses the default. `store_manifest` carries a manifest-bytes
/// `Put`. `delete_manifest` carries a conditional blob-data `Delete` keyed by
/// digest (the helper resolves the condition during planning).
#[derive(Default)]
pub struct LinksTxExtras<'a> {
    /// Unconditional `Put` of manifest blob-data (content-addressed, so any
    /// concurrent writer storing the same digest stores the same bytes).
    pub blob_data_put: Option<(&'a Digest, Bytes)>,
    /// Conditional `Delete` of `blob-data/<digest>` — included only when the
    /// per-namespace shard becomes empty, no other namespace references the
    /// blob, and the blob currently exists.
    pub blob_data_delete_if_unreferenced: Option<&'a Digest>,
    /// Direct blob-index shard operations for `digest` in the current
    /// namespace, applied alongside (or instead of) the operations derived
    /// from link deletes. `delete_blob` uses this to revoke its ownership
    /// entry (`Remove(Blob(digest))`), which lives purely in the shard and has
    /// no backing link reference, so the conditional blob-data delete above can
    /// fire from the same transaction.
    pub blob_index_ops: Option<(&'a Digest, Vec<BlobIndexOperation>)>,
    /// When `true`, the planner omits the `blob-data:{digest}` coarse lock from
    /// the transaction even though it emits the conditional blob-data delete.
    /// `delete_blob` sets this because it already holds that same coarse lock
    /// for the whole revoke-and-reclaim critical section; the engine lock is
    /// not reentrant, so re-declaring it here would self-deadlock.
    pub caller_holds_blob_data_lock: bool,
    /// When `true`, the planner unconditionally registers the namespace on
    /// success (used by `store_manifest`). Otherwise registration happens only
    /// when at least one `Create` op was processed.
    pub force_register_namespace: bool,
    /// Creation timestamp for newly-written link metadata. `None` (the default,
    /// used by `update_links`/`delete_manifest`) stamps the current time; a
    /// replicated `store_manifest` passes the originating `source_ts` so a tag's
    /// LWW timestamp and retention age track the author's write time, not this
    /// receiver's clock.
    pub created_at: Option<DateTime<Utc>>,
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
    /// Prior target per `Create` op's link (`None` = the link did not exist),
    /// read by the committed attempt (see [`LinksCommit::prior_targets`]).
    prior_targets: Vec<(LinkKind, Option<Digest>)>,
    had_creates: bool,
}

/// Prior link state captured by a committed link transaction.
///
/// The transaction's retry loop re-reads each `Create` op's current target on
/// every attempt and conflicts (retries) when it moved, so the value here is
/// the state the commit was actually validated against. Unlike a separate
/// pre-write read, it cannot be stale by an arbitrary interleaved writer.
/// `accept_put_manifest` derives its no-op-suppression `changed` gate from it.
#[derive(Default)]
pub struct LinksCommit {
    /// Prior target per `Create` op's link; `None` = the link did not exist.
    pub prior_targets: Vec<(LinkKind, Option<Digest>)>,
}

impl LinksCommit {
    /// Whether the committed transaction changed `link`: `true` when the link
    /// was absent or pointed at a different digest than `target` before the
    /// commit. Fails open (`true`) when no `Create` op for `link` was part of
    /// the transaction, so a genuine write is never suppressed by a lookup
    /// miss.
    #[must_use]
    pub fn changed(&self, link: &LinkKind, target: &Digest) -> bool {
        self.prior_targets
            .iter()
            .find(|(l, _)| l == link)
            .is_none_or(|(_, prior)| prior.as_ref() != Some(target))
    }
}

impl MetadataStore {
    /// Engine-backed implementation of `update_links`.
    ///
    /// Thin wrapper over [`Self::execute_links_tx`].
    pub async fn update_links(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        if operations.is_empty() {
            return Ok(());
        }
        self.execute_links_tx(namespace, operations, LinksTxExtras::default())
            .await
            .map(|_| ())
    }

    /// Run the retry loop, build the link-update transaction (plus any extras),
    /// commit it, and perform post-apply cache / directory / namespace cleanup.
    ///
    /// All three transactional methods (`update_links`, `store_manifest`,
    /// `delete_manifest`) share this body — they differ only in the `extras`
    /// they pass.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_links_tx(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
        extras: LinksTxExtras<'_>,
    ) -> Result<LinksCommit, Error> {
        let (_, result) = execute_with_retry_payload(
            self.executor(),
            || async {
                // ── Step 1: pre-lock read of current link state ─────────────────
                let prelock_results = join_all(operations.iter().map(|op| async move {
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
                            (
                                Some((link, target, old_target, referrer, media_type, descriptor)),
                                None,
                            )
                        }
                        LinkOperation::Delete { link, referrer } => {
                            let metadata = self.read_link_reference(namespace, link).await.ok();
                            (None, Some((link, metadata, referrer)))
                        }
                    }
                }))
                .await;

                let had_creates = prelock_results.iter().any(|(c, _)| c.is_some());
                let has_extras = extras.blob_data_put.is_some()
                    || extras.blob_data_delete_if_unreferenced.is_some()
                    || extras.blob_index_ops.is_some();

                // Empty no-op short-circuit: no creates, every delete target
                // already missing, and no extras to apply.
                if !had_creates
                    && !has_extras
                    && prelock_results
                        .iter()
                        .all(|(_, d)| d.as_ref().is_none_or(|(_, meta, _)| meta.is_none()))
                {
                    return Ok((Transaction::builder().build(), LinksTxCaptured::default()));
                }

                // ── Step 2: re-read current link state inside the retry closure
                // for conflict detection (creates) and metadata merging.
                let mut link_cache: HashMap<LinkKind, LinkMetadata> = HashMap::new();
                for (create_data, delete_data) in &prelock_results {
                    if let Some((link, ..)) = create_data {
                        match self.read_link_reference(namespace, link).await {
                            Ok(meta) => {
                                link_cache.insert((*link).clone(), meta);
                            }
                            Err(Error::ReferenceNotFound) => {}
                            Err(e) => {
                                return Err(TxError::Storage(StorageError::Backend(e.to_string())));
                            }
                        }
                    }
                    if let Some((link, _, _)) = delete_data {
                        match self.read_link_reference(namespace, link).await {
                            Ok(meta) => {
                                link_cache.insert((*link).clone(), meta);
                            }
                            Err(Error::ReferenceNotFound) => {}
                            Err(e) => {
                                return Err(TxError::Storage(StorageError::Backend(e.to_string())));
                            }
                        }
                    }
                }

                // ── Step 3: conflict detection for creates ──────────────────────
                for (create_data, _) in &prelock_results {
                    if let Some((link, _, old_target, ..)) = create_data {
                        let current_target = link_cache.get(*link).map(|m| &m.target);
                        if current_target != old_target.as_ref() {
                            return Err(TxError::Conflict);
                        }
                    }
                }

                // ── Step 4: build mutations ─────────────────────────────────────
                let mut builder = Transaction::builder();
                let mut pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>> = HashMap::new();
                let mut written_links: Vec<(LinkKind, LinkMetadata)> = Vec::new();
                let mut deleted_links: Vec<LinkKind> = Vec::new();

                // Seed direct blob-index ops (e.g. `delete_blob`'s ownership
                // revoke) so the conditional blob-data delete and the shard
                // mutations below treat them like link-derived ops.
                if let Some((digest, ops)) = &extras.blob_index_ops {
                    pending_blob_ops
                        .entry((*digest).clone())
                        .or_default()
                        .extend(ops.iter().cloned());
                }

                // Blob-data Put first when present (manifest bytes are
                // content-addressed, so the order doesn't matter — colocating
                // it with the rest of the manifest mutations keeps the txn
                // self-contained).
                if let Some((blob_digest, body)) = &extras.blob_data_put {
                    builder = builder
                        .mutation(Mutation::Put {
                            key: path_builder::blob_path(blob_digest),
                            body: body.clone(),
                            expected: None,
                        })
                        // Serialise against a concurrent manifest-delete on the
                        // same digest under the CAS executor (which takes no
                        // working-set lock).
                        .coarse_lock(blob_data_lock_key(blob_digest));
                }

                // Process creates.
                for (create_data, _) in &prelock_results {
                    let Some((link, target, old_target, referrer, media_type, descriptor)) =
                        create_data
                    else {
                        continue;
                    };

                    if link.is_tracked() && referrer.is_some() {
                        // Tracked link: merge referrer into existing or new metadata.
                        let mut metadata = link_cache.remove(*link).unwrap_or_else(|| {
                            LinkMetadata::from_digest_at(
                                (*target).clone(),
                                extras.created_at.unwrap_or_else(Utc::now),
                            )
                            .with_media_type((*media_type).clone())
                            .with_descriptor(descriptor.as_ref().map(|b| b.as_ref().clone()))
                        });

                        if let Some(manifest_digest) = referrer {
                            metadata.add_referrer((*manifest_digest).clone());
                        }

                        if old_target.is_none() {
                            pending_blob_ops
                                .entry((*target).clone())
                                .or_default()
                                .push(BlobIndexOperation::Insert((*link).clone()));
                        }

                        let key = path_builder::link_path(link, namespace);
                        let body = serde_json::to_vec(&metadata)
                            .map(Bytes::from)
                            .map_err(TxError::Serde)?;
                        builder = builder.mutation(Mutation::Put {
                            key,
                            body,
                            expected: None,
                        });
                        written_links.push(((*link).clone(), metadata));
                    } else {
                        // Non-tracked link.
                        if old_target.as_ref() != Some(*target) {
                            pending_blob_ops
                                .entry((*target).clone())
                                .or_default()
                                .push(BlobIndexOperation::Insert((*link).clone()));
                            if let Some(old) = old_target
                                && *old != **target
                            {
                                pending_blob_ops
                                    .entry(old.clone())
                                    .or_default()
                                    .push(BlobIndexOperation::Remove((*link).clone()));
                            }
                        }

                        let metadata = LinkMetadata::from_digest_at(
                            (*target).clone(),
                            extras.created_at.unwrap_or_else(Utc::now),
                        )
                        .with_media_type((*media_type).clone())
                        .with_descriptor(descriptor.as_ref().map(|b| b.as_ref().clone()));
                        let key = path_builder::link_path(link, namespace);
                        let body = serde_json::to_vec(&metadata)
                            .map(Bytes::from)
                            .map_err(TxError::Serde)?;
                        builder = builder.mutation(Mutation::Put {
                            key,
                            body,
                            expected: None,
                        });
                        written_links.push(((*link).clone(), metadata));
                    }
                }

                // Process deletes.
                for (_, delete_data) in &prelock_results {
                    let Some((link, Some(pre_meta), referrer)) = delete_data else {
                        continue;
                    };

                    // Only process if the cached target matches what was read pre-lock.
                    let current_target = link_cache.get(*link).map(|m| &m.target);
                    if current_target != Some(&pre_meta.target) {
                        continue;
                    }

                    if link.is_tracked() && referrer.is_some() {
                        if let Some(mut metadata) = link_cache.remove(*link) {
                            if let Some(manifest_digest) = referrer {
                                metadata.remove_referrer(manifest_digest);
                            }

                            if metadata.has_references() {
                                let key = path_builder::link_path(link, namespace);
                                let body = serde_json::to_vec(&metadata)
                                    .map(Bytes::from)
                                    .map_err(TxError::Serde)?;
                                builder = builder.mutation(Mutation::Put {
                                    key,
                                    body,
                                    expected: None,
                                });
                                written_links.push(((*link).clone(), metadata));
                            } else {
                                let key = path_builder::link_path(link, namespace);
                                builder = builder.mutation(Mutation::Delete {
                                    key,
                                    expected: None,
                                });
                                pending_blob_ops
                                    .entry(pre_meta.target.clone())
                                    .or_default()
                                    .push(BlobIndexOperation::Remove((*link).clone()));
                                deleted_links.push((*link).clone());
                            }
                        }
                    } else {
                        let key = path_builder::link_path(link, namespace);
                        builder = builder.mutation(Mutation::Delete {
                            key,
                            expected: None,
                        });
                        pending_blob_ops
                            .entry(pre_meta.target.clone())
                            .or_default()
                            .push(BlobIndexOperation::Remove((*link).clone()));
                        deleted_links.push((*link).clone());
                    }
                }

                // Conditional `Delete blob-data/<digest>` — only when the
                // namespace shard becomes empty AND no other namespace
                // references the blob AND the blob currently exists.
                let store = self.store_arc();
                let mut include_blob_delete: Option<&Digest> = None;
                if let Some(digest) = extras.blob_data_delete_if_unreferenced
                    && pending_blob_ops.contains_key(digest)
                {
                    let shard_path_ns = path_builder::blob_index_shard_path(digest, namespace);
                    let legacy_path = path_builder::blob_index_path(digest);
                    let our_shard_will_be_empty = shard_will_be_empty(
                        store.as_ref(),
                        namespace,
                        ops_for_digest(&pending_blob_ops, digest),
                        &shard_path_ns,
                        &legacy_path,
                    )
                    .await?;

                    if our_shard_will_be_empty {
                        let refs_prefix = path_builder::blob_index_refs_dir(digest);
                        let other_refs_exist = any_other_namespace_references_blob(
                            store.as_ref(),
                            namespace,
                            &refs_prefix,
                        )
                        .await
                        .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;

                        if !other_refs_exist {
                            match store.head(&path_builder::blob_path(digest)).await {
                                Ok(_) => include_blob_delete = Some(digest),
                                Err(StorageError::NotFound) => {}
                                Err(e) => return Err(TxError::Storage(e)),
                            }
                        }
                    }
                }

                // Blob-index shard mutations.
                for (digest, ops) in &pending_blob_ops {
                    builder =
                        append_shard_for_digest(store.as_ref(), namespace, digest, ops, builder)
                            .await
                            .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                }

                if let Some(digest) = include_blob_delete {
                    builder = builder.mutation(Mutation::Delete {
                        key: path_builder::blob_path(digest),
                        expected: None,
                    });
                    // Skip the engine coarse lock when the caller already holds
                    // it (`delete_blob`); the lock is not reentrant.
                    if !extras.caller_holds_blob_data_lock {
                        builder = builder.coarse_lock(blob_data_lock_key(digest));
                    }
                }

                // Prior target per `Create` op, from THIS attempt's reads (the
                // ones the conflict check above validated the commit against).
                let prior_targets = prelock_results
                    .iter()
                    .filter_map(|(create_data, _)| {
                        create_data
                            .as_ref()
                            .map(|(link, _, old_target, ..)| ((*link).clone(), old_target.clone()))
                    })
                    .collect();

                Ok((
                    builder.build(),
                    LinksTxCaptured {
                        written_links,
                        deleted_links,
                        prior_targets,
                        had_creates,
                    },
                ))
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map_err(tx_error_to_meta)?;

        // ── Post-apply cleanup (best-effort, outside the engine lock) ───────────
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

        if (extras.force_register_namespace || result.had_creates)
            && let Err(e) = self.register_namespace(namespace).await
        {
            warn!(
                namespace,
                error = %e,
                "Failed to register namespace after engine link transaction"
            );
        }

        Ok(LinksCommit {
            prior_targets: result.prior_targets,
        })
    }
}

// ── Helpers used by the conditional blob-data delete path ─────────────────────

/// Return the ops slice for `digest` from the map, or an empty slice.
fn ops_for_digest<'a>(
    map: &'a HashMap<Digest, Vec<BlobIndexOperation>>,
    digest: &Digest,
) -> &'a [BlobIndexOperation] {
    map.get(digest).map_or(&[] as &[_], Vec::as_slice)
}

/// Check whether the shard for `namespace` will be empty after applying `ops`.
async fn shard_will_be_empty(
    store: &Store,
    namespace: &str,
    ops: &[BlobIndexOperation],
    shard_path: &str,
    legacy_path: &str,
) -> Result<bool, TxError> {
    // Try legacy path first.
    match store.get(legacy_path).await {
        Ok(data) => {
            let mut legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
            if let Some(entry) = legacy.namespace.get_mut(namespace) {
                apply_blob_index_operations(entry, ops);
                return Ok(entry.is_empty());
            }
            return Ok(false);
        }
        Err(StorageError::NotFound) => {}
        Err(e) => return Err(TxError::Storage(e)),
    }

    // Sharded path.
    match store.get(shard_path).await {
        Ok(data) => {
            let mut links: HashSet<LinkKind> = serde_json::from_slice(&data).unwrap_or_default();
            apply_blob_index_operations(&mut links, ops);
            Ok(links.is_empty())
        }
        Err(StorageError::NotFound) => Ok(true),
        Err(e) => Err(TxError::Storage(e)),
    }
}

/// Return `true` when any namespace other than `our_namespace` has a live
/// shard entry in the refs directory.
async fn any_other_namespace_references_blob(
    store: &Store,
    our_namespace: &str,
    refs_prefix: &str,
) -> Result<bool, Error> {
    let mut continuation = None;
    loop {
        let page = store
            .list(refs_prefix, 100, continuation)
            .await
            .map_err(Error::from)?;
        for key in &page.items {
            let encoded_ns = key.strip_suffix(".json").unwrap_or("");
            let ns = encoded_ns.replace("%2F", "/").replace("%25", "%");
            if ns != our_namespace {
                return Ok(true);
            }
        }
        continuation = page.next_token;
        if continuation.is_none() {
            break;
        }
    }
    Ok(false)
}
