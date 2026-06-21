//! The blob-index domain: cross-namespace blob reference tracking.
//!
//! Holds the [`BlobIndex`] / [`BlobIndexOperation`] value types, the read/write
//! methods over the per-namespace shards, and the shard operations ([`shard`]):
//! both the pure in-memory layer and the store read-modify-write.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry},
    transaction::{Mutation, Read, Transaction},
};

use crate::{
    oci::{Digest, Namespace},
    registry::{
        metadata_store::{Error, LinkKind, LinksTx, MetadataStore, tx_error_to_meta},
        path_builder,
    },
};

pub(crate) mod shard;

use self::shard::{
    SHARD_READ_CONCURRENCY, append_shard_for_digest, apply_blob_index_operations,
    collect_blob_index_shards, decode_blob_index_shard_namespace, namespace_links_from_index,
    non_empty_links_or_not_found,
};

// Domain types

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlobIndex {
    pub namespace: HashMap<Namespace, HashSet<LinkKind>>,
}

#[derive(Debug, Clone)]
pub enum BlobIndexOperation {
    Insert(LinkKind),
    Remove(LinkKind),
}

// Engine-backed write methods

impl MetadataStore {
    /// Update the blob-index shard for `digest` in `namespace`.
    ///
    /// Reads the current shard, applies `operation`, and submits a `Transaction`,
    /// retrying on `Conflict`/`Precondition` up to [`DEFAULT_RETRY_BUDGET`] times.
    #[instrument(skip(self))]
    pub async fn update_blob_index(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error> {
        let operations = [operation];
        execute_with_retry(
            self.executor(),
            || async {
                let store = self.store_arc();
                let builder = Transaction::builder();
                let builder = append_shard_for_digest(
                    store.as_ref(),
                    namespace,
                    digest,
                    &operations,
                    builder,
                )
                .await
                .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                Ok(builder.build())
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map(|_| ())
        .map_err(tx_error_to_meta)
    }

    /// Build the engine reads + mutations that insert `LinkKind::Blob(digest)`
    /// into the per-namespace shard for `digest`, without executing them.
    ///
    /// The caller embeds these in a larger transaction and propagates any
    /// `Conflict`; in-place CAS retry is [`Self::update_blob_index`]'s job.
    pub async fn build_grant_mutations(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(Vec<Read>, Vec<Mutation>), Error> {
        let store = self.store_arc();
        let ops = [BlobIndexOperation::Insert(LinkKind::Blob(digest.clone()))];
        let builder = append_shard_for_digest(
            store.as_ref(),
            namespace,
            digest,
            &ops,
            Transaction::builder(),
        )
        .await?;
        let tx = builder.build();
        Ok((tx.reads, tx.mutations))
    }

    /// Revoke `namespace`'s ownership of `digest` in an atomic transaction and
    /// return whether the blob became unreferenced, so the caller reclaims its
    /// blob-data from the blob store under the `blob-data:{digest}` lock it must
    /// hold across this call (serialising against concurrent pushes/deletes of
    /// the same digest).
    pub async fn revoke_blob_ownership(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<bool, Error> {
        let tx = LinksTx::RevokeBlobOwnership {
            blob: digest,
            ops: vec![BlobIndexOperation::Remove(LinkKind::Blob(digest.clone()))],
        };
        self.execute_links_tx(namespace, &[], tx)
            .await
            .map(|c| c.reclaim_blob)
    }

    #[instrument(skip(self))]
    pub async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut index = BlobIndex::default();
        let mut found_shards = false;
        let mut token = None;

        loop {
            let page = self
                .store()
                .list_children(&refs_dir, 1000, token, None)
                .await?;

            if !page.objects.is_empty() {
                found_shards = true;
            }

            let shard_results = stream::iter(page.objects.into_iter().map(|obj| {
                let shard_path = format!("{refs_dir}/{obj}");
                async move {
                    match self.store().get(&shard_path).await {
                        Ok(data) => {
                            // The shard filename was written from a validated
                            // `Namespace`; an undecodable name is skipped.
                            match (
                                serde_json::from_slice::<HashSet<LinkKind>>(&data),
                                Namespace::new(&decode_blob_index_shard_namespace(&obj)),
                            ) {
                                (Ok(links), Ok(namespace)) if !links.is_empty() => {
                                    Ok(Some((namespace, links)))
                                }
                                _ => Ok(None),
                            }
                        }
                        Err(StorageError::NotFound) => Ok(None),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            }))
            .buffer_unordered(SHARD_READ_CONCURRENCY)
            .collect::<Vec<Result<Option<(Namespace, HashSet<LinkKind>)>, Error>>>()
            .await;

            let shards = shard_results
                .into_iter()
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>, _>>()?;
            index
                .namespace
                .extend(collect_blob_index_shards(shards).namespace);

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        if !found_shards || index.namespace.is_empty() {
            return match self.read_legacy_blob_index(digest).await? {
                Some(legacy) if !legacy.namespace.is_empty() => Ok(legacy),
                _ => Err(Error::ReferenceNotFound),
            };
        }
        Ok(index)
    }

    #[instrument(skip(self))]
    pub async fn has_blob_references(&self, digest: &Digest) -> Result<bool, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut token = None;

        loop {
            let page = self
                .store()
                .list_children(&refs_dir, 1, token, None)
                .await?;

            for obj in page.objects {
                let shard_path = format!("{refs_dir}/{obj}");
                match self.store().get(&shard_path).await {
                    Ok(data) => {
                        let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                        if !links.is_empty() {
                            return Ok(true);
                        }
                    }
                    Err(StorageError::NotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        Ok(self
            .read_legacy_blob_index(digest)
            .await?
            .is_some_and(|legacy| legacy.namespace.values().any(|links| !links.is_empty())))
    }

    #[instrument(skip(self))]
    pub async fn read_blob_index_namespace(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        match self.store().get(&shard_path).await {
            Ok(data) => {
                let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                non_empty_links_or_not_found(links)
            }
            Err(StorageError::NotFound) => match self.read_legacy_blob_index(digest).await? {
                Some(legacy) => namespace_links_from_index(&legacy, namespace),
                None => Err(Error::ReferenceNotFound),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Read the legacy single-file blob index for `digest`, or `None` when no
    /// legacy object exists. A malformed body deserializes to an empty
    /// [`BlobIndex`] (`unwrap_or_default`); callers apply their own non-empty /
    /// not-found projection over the result.
    async fn read_legacy_blob_index(&self, digest: &Digest) -> Result<Option<BlobIndex>, Error> {
        match self
            .store()
            .get(&path_builder::blob_index_path(digest))
            .await
        {
            Ok(data) => Ok(Some(serde_json::from_slice(&data).unwrap_or_default())),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn migrate_blob_index(&self, digest: &Digest) -> Result<(), Error> {
        let legacy_path = path_builder::blob_index_path(digest);

        // Read the legacy file once outside the retry loop; if absent, nothing
        // to migrate. On conflict the engine re-reads under the lock, so a
        // concurrent migrator wins and we bail cleanly.
        let data = match self.store().get(&legacy_path).await {
            Ok(d) => d,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        let blob_index: BlobIndex = serde_json::from_slice(&data).map_err(Error::from)?;
        let namespace_count = blob_index.namespace.len();
        let raw_legacy = Bytes::from(data);

        execute_with_retry(
            self.executor(),
            || async {
                let mut builder =
                    Transaction::builder().read(legacy_path.clone(), raw_legacy.clone());

                for (namespace, links) in &blob_index.namespace {
                    let shard_path = path_builder::blob_index_shard_path(digest, namespace);
                    let operations: Vec<BlobIndexOperation> = links
                        .iter()
                        .map(|l| BlobIndexOperation::Insert(l.clone()))
                        .collect();

                    // Re-read each shard inside the closure so the
                    // fingerprint is fresh on every retry attempt.
                    match self.store().get(&shard_path).await {
                        Ok(shard_data) => {
                            let mut existing: HashSet<LinkKind> =
                                serde_json::from_slice(&shard_data).unwrap_or_default();
                            apply_blob_index_operations(&mut existing, &operations);
                            builder = builder.read(shard_path.clone(), Bytes::from(shard_data));
                            if existing.is_empty() {
                                builder = builder.mutation(Mutation::Delete {
                                    key: shard_path,
                                    expected: None,
                                });
                            } else {
                                let body = Bytes::from(
                                    serde_json::to_vec(&existing).map_err(TxError::Serde)?,
                                );
                                builder = builder.mutation(Mutation::Put {
                                    key: shard_path,
                                    body,
                                    expected: None,
                                });
                            }
                        }
                        Err(StorageError::NotFound) => {
                            let mut new_links = HashSet::new();
                            apply_blob_index_operations(&mut new_links, &operations);
                            if !new_links.is_empty() {
                                let body = Bytes::from(
                                    serde_json::to_vec(&new_links).map_err(TxError::Serde)?,
                                );
                                builder = builder.mutation(Mutation::PutIfAbsent {
                                    key: shard_path,
                                    body,
                                });
                            }
                        }
                        Err(e) => return Err(TxError::Storage(e)),
                    }
                }

                builder = builder.mutation(Mutation::Delete {
                    key: legacy_path.clone(),
                    expected: None,
                });

                Ok(builder.build())
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map_err(tx_error_to_meta)?;

        info!("Migrated legacy blob index for '{digest}' ({namespace_count} namespaces)",);
        Ok(())
    }
}
