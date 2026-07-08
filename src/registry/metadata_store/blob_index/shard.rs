//! Blob-index shard operations in two layers: a pure in-memory layer
//! (`apply_blob_index_operations` and friends, no I/O) and a store
//! read-modify-write layer that turns applied [`BlobIndexOperation`]s into
//! transaction reads + mutations. Both the link-transaction planner and the
//! standalone shard writers in [`super`] build on these.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    store::Store,
    transaction::{Mutation, TransactionBuilder},
};

use crate::{
    oci::{Digest, Namespace},
    registry::{
        Error,
        metadata_store::{BlobIndex, BlobIndexOperation, LinkKind},
        path_builder,
    },
};

// Pure shard operations (in-memory; no I/O)

pub const SHARD_READ_CONCURRENCY: usize = 10;

pub fn decode_blob_index_shard_namespace(file_name: &str) -> String {
    file_name
        .strip_suffix(".json")
        .unwrap_or(file_name)
        .replace("%2F", "/")
        .replace("%25", "%")
}

pub fn collect_blob_index_shards(
    shards: impl IntoIterator<Item = (Namespace, HashSet<LinkKind>)>,
) -> BlobIndex {
    let mut index = BlobIndex::default();
    for (namespace, links) in shards {
        if !links.is_empty() {
            index.namespace.insert(namespace, links);
        }
    }
    index
}

pub fn apply_blob_index_operations(
    links: &mut HashSet<LinkKind>,
    operations: &[BlobIndexOperation],
) {
    for operation in operations {
        match operation {
            BlobIndexOperation::Insert(link) => {
                links.insert(link.clone());
            }
            BlobIndexOperation::Remove(link) => {
                links.remove(link);
            }
        }
    }
}

pub fn non_empty_links_or_not_found(links: HashSet<LinkKind>) -> Result<HashSet<LinkKind>, Error> {
    if links.is_empty() {
        Err(Error::NotFound)
    } else {
        Ok(links)
    }
}

pub fn namespace_links_from_index(
    index: &BlobIndex,
    namespace: &Namespace,
) -> Result<HashSet<LinkKind>, Error> {
    index
        .namespace
        .get(namespace)
        .cloned()
        .filter(|links| !links.is_empty())
        .ok_or(Error::NotFound)
}

// Store read-modify-write

/// The write target the legacy-vs-shard routing resolved: a whole-index
/// legacy `index.json` when one exists, else the per-namespace shard.
pub enum ShardTarget {
    /// Legacy whole-index file: raw bytes for the transaction read set plus
    /// the parsed index.
    Legacy { raw: Bytes, index: BlobIndex },
    /// Per-namespace shard state; `None` when the shard is absent.
    Shard(Option<(Bytes, HashSet<LinkKind>)>),
}

/// Resolve the legacy-vs-shard routing with a single GET per layer. The one
/// home of the routing rule shared by the transaction planner and the
/// emptiness probe.
pub async fn read_shard_target(
    store: &Store,
    legacy_path: &str,
    shard_path: &str,
) -> Result<ShardTarget, StorageError> {
    match store.object_store().get(legacy_path).await {
        Ok(data) => {
            let raw = Bytes::from(data.clone());
            let index: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
            return Ok(ShardTarget::Legacy { raw, index });
        }
        Err(StorageError::NotFound) => {}
        Err(e) => return Err(e),
    }
    Ok(ShardTarget::Shard(read_shard(store, shard_path).await?))
}

/// Read one per-namespace shard: its raw bytes plus parsed links, or `None`
/// when absent.
pub async fn read_shard(
    store: &Store,
    shard_path: &str,
) -> Result<Option<(Bytes, HashSet<LinkKind>)>, StorageError> {
    match store.object_store().get(shard_path).await {
        Ok(data) => {
            let raw = Bytes::from(data.clone());
            let links: HashSet<LinkKind> = serde_json::from_slice(&data).unwrap_or_default();
            Ok(Some((raw, links)))
        }
        Err(StorageError::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Apply `ops` over the shard's current `existing` state and append the
/// fingerprint read plus the resulting mutation to `builder`: `Put`/`Delete`
/// over a present shard, create-only `PutIfAbsent` over an absent one.
pub fn append_shard_ops(
    shard_path: String,
    existing: Option<(Bytes, HashSet<LinkKind>)>,
    ops: &[BlobIndexOperation],
    mut builder: TransactionBuilder,
) -> Result<TransactionBuilder, serde_json::Error> {
    let Some((raw, mut links)) = existing else {
        let mut links = HashSet::new();
        apply_blob_index_operations(&mut links, ops);
        if links.is_empty() {
            return Ok(builder);
        }
        let body = Bytes::from(serde_json::to_vec(&links)?);
        return Ok(builder.mutation(Mutation::PutIfAbsent {
            key: shard_path,
            body,
        }));
    };

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

/// Read the current shard state for `digest`/`namespace`, apply `ops`, and
/// append the resulting read + mutation to `builder`.
///
/// Routing: if a legacy `index.json` exists it is the write target; otherwise
/// the per-namespace shard is used.
pub async fn append_shard_for_digest(
    store: &Store,
    namespace: &Namespace,
    digest: &Digest,
    ops: &[BlobIndexOperation],
    mut builder: TransactionBuilder,
) -> Result<TransactionBuilder, Error> {
    let legacy_path = path_builder::blob_index_path(digest);
    let shard_path = path_builder::blob_index_shard_path(digest, namespace);

    match read_shard_target(store, &legacy_path, &shard_path).await? {
        ShardTarget::Legacy { raw, mut index } => {
            {
                let entry = index.namespace.entry(namespace.clone()).or_default();
                apply_blob_index_operations(entry, ops);
                if entry.is_empty() {
                    index.namespace.remove(namespace);
                }
            }
            builder = builder.read(legacy_path.clone(), raw);
            if index.namespace.is_empty() {
                return Ok(builder.mutation(Mutation::Delete {
                    key: legacy_path,
                    expected: None,
                }));
            }
            let body = Bytes::from(serde_json::to_vec(&index)?);
            Ok(builder.mutation(Mutation::Put {
                key: legacy_path,
                body,
                expected: None,
            }))
        }
        ShardTarget::Shard(existing) => {
            append_shard_ops(shard_path, existing, ops, builder).map_err(Error::from)
        }
    }
}

/// Return the ops slice for `digest` from the map, or an empty slice.
pub fn ops_for_digest<'a>(
    map: &'a HashMap<Digest, Vec<BlobIndexOperation>>,
    digest: &Digest,
) -> &'a [BlobIndexOperation] {
    map.get(digest).map_or(&[] as &[_], Vec::as_slice)
}

/// Check whether the shard for `namespace` will be empty after applying `ops`.
pub async fn shard_will_be_empty(
    store: &Store,
    namespace: &Namespace,
    ops: &[BlobIndexOperation],
    shard_path: &str,
    legacy_path: &str,
) -> Result<bool, TxError> {
    match read_shard_target(store, legacy_path, shard_path)
        .await
        .map_err(TxError::Storage)?
    {
        ShardTarget::Legacy { mut index, .. } => match index.namespace.get_mut(namespace) {
            Some(entry) => {
                apply_blob_index_operations(entry, ops);
                Ok(entry.is_empty())
            }
            None => Ok(false),
        },
        ShardTarget::Shard(Some((_, mut links))) => {
            apply_blob_index_operations(&mut links, ops);
            Ok(links.is_empty())
        }
        ShardTarget::Shard(None) => Ok(true),
    }
}

/// Return `true` when any namespace other than `our_namespace` has a live
/// shard entry in the refs directory.
pub async fn any_other_namespace_references_blob(
    store: &Store,
    our_namespace: &Namespace,
    refs_prefix: &str,
) -> Result<bool, Error> {
    let mut continuation = None;
    loop {
        let page = store
            .object_store()
            .list(refs_prefix, 100, continuation)
            .await
            .map_err(Error::from)?;
        for key in &page.items {
            let ns = decode_blob_index_shard_namespace(key);
            if ns != our_namespace.as_ref() {
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
