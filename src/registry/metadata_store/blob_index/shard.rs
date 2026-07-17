//! Blob-index shard operations in two layers: a pure in-memory layer
//! (`apply_blob_index_operations` and friends, no I/O) and a store
//! read-modify-write layer that turns applied [`BlobIndexOperation`]s into
//! transaction reads + mutations. Both the link-transaction planner and the
//! standalone shard writers in [`super`] build on these.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use serde_json::Value;

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

// Store read-modify-write

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

/// Fold `ops` into the disjoint `(add, remove)` link sets a [`Mutation::MergeSet`]
/// carries, applying last-op-wins per link so the result matches an ordered
/// [`apply_blob_index_operations`] over the same ops.
fn ops_to_merge_delta(
    ops: &[BlobIndexOperation],
) -> Result<(Vec<Value>, Vec<Value>), serde_json::Error> {
    let mut add: Vec<LinkKind> = Vec::new();
    let mut remove: Vec<LinkKind> = Vec::new();
    for op in ops {
        match op {
            BlobIndexOperation::Insert(link) => {
                remove.retain(|existing| existing != link);
                if !add.contains(link) {
                    add.push(link.clone());
                }
            }
            BlobIndexOperation::Remove(link) => {
                add.retain(|existing| existing != link);
                if !remove.contains(link) {
                    remove.push(link.clone());
                }
            }
        }
    }
    let add = add
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()?;
    let remove = remove
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()?;
    Ok((add, remove))
}

/// Append the shard mutation for `ops` to `builder` as an idempotent
/// [`Mutation::MergeSet`], or leave `builder` untouched when `ops` net to
/// nothing.
///
/// A present shard is joined to the read set: its fingerprint lets the executor
/// catch a concurrent shard write at prepare and retry the whole transaction
/// cleanly, so the healthy path settles fast. If a write still slips in after
/// the commit point, the carried delta lets the merge reconcile against live
/// state on replay rather than stranding a partial commit.
pub fn append_shard_ops(
    shard_path: String,
    existing: Option<Bytes>,
    ops: &[BlobIndexOperation],
    builder: TransactionBuilder,
) -> Result<TransactionBuilder, serde_json::Error> {
    let (add, remove) = ops_to_merge_delta(ops)?;
    if add.is_empty() && remove.is_empty() {
        return Ok(builder);
    }
    let builder = match existing {
        Some(raw) => builder.read(shard_path.clone(), raw),
        None => builder,
    };
    Ok(builder.mutation(Mutation::MergeSet {
        key: shard_path,
        add,
        remove,
    }))
}

/// Read the current shard for `digest`/`namespace` and append its merge mutation
/// to `builder`.
pub async fn append_shard_for_digest(
    store: &Store,
    namespace: &Namespace,
    digest: &Digest,
    ops: &[BlobIndexOperation],
    builder: TransactionBuilder,
) -> Result<TransactionBuilder, Error> {
    let shard_path = path_builder::blob_index_shard_path(digest, namespace);
    let existing = match store.object_store().get(&shard_path).await {
        Ok(data) => Some(Bytes::from(data)),
        Err(StorageError::NotFound) => None,
        Err(e) => return Err(Error::from(e)),
    };
    append_shard_ops(shard_path, existing, ops, builder).map_err(Error::from)
}

/// Return the ops slice for `digest` from the map, or an empty slice.
pub fn ops_for_digest<'a>(
    map: &'a HashMap<Digest, Vec<BlobIndexOperation>>,
    digest: &Digest,
) -> &'a [BlobIndexOperation] {
    map.get(digest).map_or(&[] as &[_], Vec::as_slice)
}

/// Check whether the shard at `shard_path` will be empty after applying `ops`.
pub async fn shard_will_be_empty(
    store: &Store,
    ops: &[BlobIndexOperation],
    shard_path: &str,
) -> Result<bool, TxError> {
    match read_shard(store, shard_path)
        .await
        .map_err(TxError::Storage)?
    {
        Some((_, mut links)) => {
            apply_blob_index_operations(&mut links, ops);
            Ok(links.is_empty())
        }
        None => Ok(true),
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
