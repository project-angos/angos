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
        metadata_store::{BlobIndex, BlobIndexOperation, Error, LinkKind},
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
        Err(Error::ReferenceNotFound)
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
        .ok_or(Error::ReferenceNotFound)
}

// Store read-modify-write

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

    // Check for legacy layout first with a cheap HEAD.
    match store.object_store().head(&legacy_path).await {
        Ok(_) => {
            match store.object_store().get(&legacy_path).await {
                Ok(data) => {
                    let raw = Bytes::from(data.clone());
                    let mut legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
                    {
                        let entry = legacy.namespace.entry(namespace.clone()).or_default();
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
                    // Vanished between HEAD and GET: fall through to sharded path.
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(StorageError::NotFound) => {}
        Err(e) => return Err(e.into()),
    }

    // Sharded path.
    match store.object_store().get(&shard_path).await {
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
    // Try legacy path first.
    match store.object_store().get(legacy_path).await {
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
    match store.object_store().get(shard_path).await {
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
