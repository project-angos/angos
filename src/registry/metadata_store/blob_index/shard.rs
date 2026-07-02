//! Blob-index shard operations in two layers: a pure in-memory layer
//! (`apply_blob_index_operations` and friends) and a store read-modify-write
//! layer turning [`BlobIndexOperation`]s into transaction reads + mutations.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};

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

/// Read a shard's raw bytes, mapping [`StorageError::NotFound`] to `None`.
async fn read_shard(store: &Store, shard_path: &str) -> Result<Option<Vec<u8>>, Error> {
    match store.get(shard_path).await {
        Ok(data) => Ok(Some(data)),
        Err(StorageError::NotFound) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Fold `existing` (a shard's current raw bytes, or `None` when absent) plus
/// `ops` into `builder` as a read + Put/Delete/PutIfAbsent. Assumes no legacy
/// `index.json` remains ([`append_shard_for_digest`] drains it first).
fn fold_shard_into_builder(
    builder: TransactionBuilder,
    shard_path: String,
    existing: Option<Vec<u8>>,
    ops: &[BlobIndexOperation],
) -> Result<TransactionBuilder, Error> {
    if let Some(data) = existing {
        let raw = Bytes::from(data.clone());
        let mut links: HashSet<LinkKind> = serde_json::from_slice(&data).unwrap_or_default();
        apply_blob_index_operations(&mut links, ops);
        let builder = builder.read(shard_path.clone(), raw);
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
    } else {
        let mut links = HashSet::new();
        apply_blob_index_operations(&mut links, ops);
        if links.is_empty() {
            Ok(builder)
        } else {
            let body = Bytes::from(serde_json::to_vec(&links)?);
            // Absence read: keys the shard birth so it is verified at
            // Prepare and ordered ahead of the legacy-drain Delete.
            let builder = builder.read(shard_path.clone(), Bytes::new());
            Ok(builder.mutation(Mutation::PutIfAbsent {
                key: shard_path,
                body,
            }))
        }
    }
}

/// Apply `ops` to `digest`/`namespace`'s shard and append the read + mutation to
/// `builder`. Assumes no legacy `index.json` remains ([`append_shard_for_digest`]
/// drains it first).
async fn append_to_shard(
    store: &Store,
    namespace: &Namespace,
    digest: &Digest,
    ops: &[BlobIndexOperation],
    builder: TransactionBuilder,
) -> Result<TransactionBuilder, Error> {
    let shard_path = path_builder::blob_index_shard_path(digest, namespace);
    let existing = read_shard(store, &shard_path).await?;
    fold_shard_into_builder(builder, shard_path, existing, ops)
}

/// Read `digest`/`namespace`'s shard state, apply `ops`, and append the reads +
/// mutations to `builder`.
///
/// Always writes the per-namespace shard (`refs/<ns>.json`), never the legacy
/// `index.json`. A still-present legacy file is drained in the same transaction:
/// every namespace it holds is folded into its own shard and the file is
/// deleted, so a digest never carries both at once.
pub async fn append_shard_for_digest(
    store: &Store,
    namespace: &Namespace,
    digest: &Digest,
    ops: &[BlobIndexOperation],
    mut builder: TransactionBuilder,
) -> Result<TransactionBuilder, Error> {
    let legacy_path = path_builder::blob_index_path(digest);

    let legacy_data = match store.get(&legacy_path).await {
        Ok(data) => data,
        Err(StorageError::NotFound) => {
            return append_to_shard(store, namespace, digest, ops, builder).await;
        }
        Err(e) => return Err(e.into()),
    };

    // Fold every namespace the legacy file holds into its own shard (applying
    // `ops` to the target namespace), then delete the legacy file.
    let legacy: BlobIndex = serde_json::from_slice(&legacy_data).unwrap_or_default();
    builder = builder.read(legacy_path.clone(), Bytes::from(legacy_data));

    let mut namespaces: Vec<Namespace> = legacy.namespace.keys().cloned().collect();
    if !legacy.namespace.contains_key(namespace) {
        namespaces.push(namespace.clone());
    }

    // Each namespace's shard path plus the ops reconstructing it from the legacy
    // entry (with `ops` applied to the target namespace).
    let plans: Vec<(String, Vec<BlobIndexOperation>)> = namespaces
        .iter()
        .map(|ns| {
            let mut ns_ops: Vec<BlobIndexOperation> = legacy
                .namespace
                .get(ns)
                .into_iter()
                .flatten()
                .map(|link| BlobIndexOperation::Insert(link.clone()))
                .collect();
            if ns == namespace {
                ns_ops.extend(ops.iter().cloned());
            }
            (path_builder::blob_index_shard_path(digest, ns), ns_ops)
        })
        .collect();

    // Read every shard concurrently (bounded); `buffered` preserves input order
    // so the fold into the single transaction stays deterministic. The stream is
    // driven from owned paths so each future takes its path by value rather than
    // borrowing `plans`, which keeps the map closure free of the higher-ranked
    // lifetime the retry harness cannot infer.
    let shard_paths: Vec<String> = plans.iter().map(|(path, _)| path.clone()).collect();
    let reads = stream::iter(
        shard_paths
            .into_iter()
            .map(|shard_path| async move { read_shard(store, &shard_path).await }),
    )
    .buffered(SHARD_READ_CONCURRENCY)
    .collect::<Vec<Result<Option<Vec<u8>>, Error>>>()
    .await;

    for ((shard_path, ns_ops), existing) in plans.into_iter().zip(reads) {
        builder = fold_shard_into_builder(builder, shard_path, existing?, &ns_ops)?;
    }

    Ok(builder.mutation(Mutation::Delete {
        key: legacy_path,
        expected: None,
    }))
}

/// Return the ops slice for `digest` from the map, or an empty slice.
pub fn ops_for_digest<'a>(
    map: &'a HashMap<Digest, Vec<BlobIndexOperation>>,
    digest: &Digest,
) -> &'a [BlobIndexOperation] {
    map.get(digest).map_or(&[] as &[_], Vec::as_slice)
}

/// Whether `namespace`'s references vanish after applying `ops`: the union of
/// the pre-fetched legacy `index.json` entry and the shard's links, with `ops`
/// applied, is empty. Both files can coexist (a partially-committed drain
/// awaiting recovery, or a 1.3 replica writing legacy beside 1.4 shards), so
/// neither may be judged alone.
pub async fn shard_will_be_empty(
    store: &Store,
    namespace: &Namespace,
    ops: &[BlobIndexOperation],
    shard_path: &str,
    legacy: Option<&BlobIndex>,
) -> Result<bool, TxError> {
    let mut links: HashSet<LinkKind> = legacy
        .and_then(|index| index.namespace.get(namespace))
        .cloned()
        .unwrap_or_default();

    match store.get(shard_path).await {
        Ok(data) => {
            let shard: HashSet<LinkKind> = serde_json::from_slice(&data).unwrap_or_default();
            links.extend(shard);
        }
        Err(StorageError::NotFound) => {}
        Err(e) => return Err(TxError::Storage(e)),
    }

    apply_blob_index_operations(&mut links, ops);
    Ok(links.is_empty())
}

/// `true` when any namespace other than `our_namespace` still references
/// `digest`, considering both the sharded `refs/` layout and the pre-fetched
/// legacy `index.json`.
pub async fn any_other_namespace_references_blob(
    store: &Store,
    our_namespace: &Namespace,
    digest: &Digest,
    legacy: Option<&BlobIndex>,
) -> Result<bool, Error> {
    let refs_prefix = path_builder::blob_index_refs_dir(digest);
    let mut continuation = None;
    loop {
        let page = store
            .list(&refs_prefix, 100, continuation)
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

    // A not-yet-drained legacy file may still hold another namespace's reference.
    Ok(legacy.is_some_and(|index| {
        index
            .namespace
            .iter()
            .any(|(ns, links)| ns != our_namespace && !links.is_empty())
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::transaction::Transaction;

    use super::*;
    use crate::registry::test_utils::{build_store, build_test_fs_executor};

    fn fs_store(dir: &tempfile::TempDir) -> Arc<Store> {
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
        build_store(object, build_test_fs_executor(root, false))
    }

    fn digest() -> Digest {
        Digest::sha256_of_bytes(b"shard test blob")
    }

    /// A legacy `index.json` entry and a shard can coexist; the emptiness
    /// verdict must union them, or removing the legacy-held link would report
    /// empty while the shard still references the blob.
    #[tokio::test]
    async fn shard_will_be_empty_unions_legacy_and_shard() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = fs_store(&dir);
        let namespace = Namespace::new("union/ns").unwrap();
        let digest = digest();

        let mut legacy = BlobIndex::default();
        legacy.namespace.insert(
            namespace.clone(),
            HashSet::from([LinkKind::Layer(digest.clone())]),
        );
        let shard: HashSet<LinkKind> = HashSet::from([LinkKind::Blob(digest.clone())]);
        let shard_path = path_builder::blob_index_shard_path(&digest, &namespace);
        store
            .put(
                &shard_path,
                Bytes::from(serde_json::to_vec(&shard).unwrap()),
            )
            .await
            .unwrap();

        let remove_layer = [BlobIndexOperation::Remove(LinkKind::Layer(digest.clone()))];
        assert!(
            !shard_will_be_empty(
                &store,
                &namespace,
                &remove_layer,
                &shard_path,
                Some(&legacy)
            )
            .await
            .unwrap(),
            "the shard's Blob entry still references the blob"
        );

        let remove_both = [
            BlobIndexOperation::Remove(LinkKind::Layer(digest.clone())),
            BlobIndexOperation::Remove(LinkKind::Blob(digest.clone())),
        ];
        assert!(
            shard_will_be_empty(&store, &namespace, &remove_both, &shard_path, Some(&legacy))
                .await
                .unwrap(),
            "removing both files' entries empties the union"
        );

        assert!(
            shard_will_be_empty(&store, &namespace, &[], "union/absent-shard", None)
                .await
                .unwrap(),
            "no legacy entry and no shard is empty"
        );
    }

    /// A shard birth is read-keyed: the transaction records the absence so the
    /// `PutIfAbsent` is verified at Prepare and ordered ahead of the
    /// legacy-drain Delete.
    #[tokio::test]
    async fn shard_birth_records_absence_read() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = fs_store(&dir);
        let namespace = Namespace::new("birth/ns").unwrap();
        let digest = digest();
        let shard_path = path_builder::blob_index_shard_path(&digest, &namespace);

        let ops = [BlobIndexOperation::Insert(LinkKind::Layer(digest.clone()))];
        let builder =
            append_shard_for_digest(&store, &namespace, &digest, &ops, Transaction::builder())
                .await
                .unwrap();
        let tx = builder.build();

        let read = tx
            .reads
            .iter()
            .find(|r| r.key == shard_path)
            .expect("the shard birth must join an absence read");
        assert!(read.expects_absent());
        assert!(
            tx.mutations
                .iter()
                .any(|m| matches!(m, Mutation::PutIfAbsent { key, .. } if *key == shard_path)),
            "the birth itself stays a PutIfAbsent"
        );
    }

    /// Draining a legacy `index.json` holding several namespaces folds each into
    /// its own shard, applying `ops` only to the target namespace and deleting
    /// the legacy file in one transaction. A misaligned concurrent shard read
    /// would fold the wrong links into a shard.
    #[tokio::test]
    async fn drain_folds_each_namespace_into_its_shard() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = fs_store(&dir);
        let digest = digest();
        let ns_a = Namespace::new("drain/a").unwrap();
        let ns_b = Namespace::new("drain/b").unwrap();

        let layer = LinkKind::Layer(digest.clone());
        let blob = LinkKind::Blob(digest.clone());
        let config = LinkKind::Config(digest.clone());

        let mut legacy = BlobIndex::default();
        legacy
            .namespace
            .insert(ns_a.clone(), HashSet::from([layer.clone()]));
        legacy
            .namespace
            .insert(ns_b.clone(), HashSet::from([blob.clone()]));
        store
            .put(
                &path_builder::blob_index_path(&digest),
                Bytes::from(serde_json::to_vec(&legacy).unwrap()),
            )
            .await
            .unwrap();

        // Insert an extra link into ns_a while draining; ns_b must be untouched.
        let ops = [BlobIndexOperation::Insert(config.clone())];
        let tx = append_shard_for_digest(&store, &ns_a, &digest, &ops, Transaction::builder())
            .await
            .unwrap()
            .build();

        let legacy_path = path_builder::blob_index_path(&digest);
        let shard_a = path_builder::blob_index_shard_path(&digest, &ns_a);
        let shard_b = path_builder::blob_index_shard_path(&digest, &ns_b);

        let shard_body = |key: &str| -> HashSet<LinkKind> {
            tx.mutations
                .iter()
                .find_map(|m| match m {
                    Mutation::PutIfAbsent { key: k, body } if k == key => {
                        Some(serde_json::from_slice(body).unwrap())
                    }
                    _ => None,
                })
                .expect("shard must be born as a PutIfAbsent")
        };

        assert_eq!(shard_body(&shard_a), HashSet::from([layer, config]));
        assert_eq!(shard_body(&shard_b), HashSet::from([blob]));
        assert!(
            tx.mutations
                .iter()
                .any(|m| matches!(m, Mutation::Delete { key, .. } if *key == legacy_path)),
            "the legacy file is deleted in the same transaction"
        );
    }
}
