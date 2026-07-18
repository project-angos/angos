//! The blob-index domain: cross-namespace blob reference tracking.
//!
//! Holds the [`BlobIndex`] / [`BlobIndexOperation`] value types, the read/write
//! methods over the per-namespace shards, and the shard operations ([`shard`]):
//! both the pure in-memory layer and the store read-modify-write.

use std::collections::{HashMap, HashSet};
use std::pin::pin;

use futures_util::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use angos_storage::paginated;
use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry},
    transaction::Transaction,
};

use crate::{
    oci::{Digest, Namespace},
    registry::{
        Error,
        metadata_store::{LinkKind, LinksTx, MetadataStore},
        path_builder,
    },
};

pub mod shard;

use self::shard::{
    SHARD_READ_CONCURRENCY, append_shard_for_digest, decode_blob_index_shard_namespace,
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
                let builder = append_shard_for_digest(
                    self.store_arc().as_ref(),
                    namespace,
                    digest,
                    &operations,
                    Transaction::builder(),
                )
                .await
                .map_err(|e| TxError::Storage(StorageError::Backend(e.to_string())))?;
                Ok(builder.build())
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await
        .map(|_| ())
        .map_err(Error::from)
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

    /// Stream each present shard under `refs_dir` as its relative filename plus
    /// raw body: shard names page off the listing while up to
    /// [`SHARD_READ_CONCURRENCY`] bodies read concurrently. A shard deleted
    /// between listing and read is dropped; parsing is left to the caller so
    /// each derives its own decode and empty-shard policy.
    fn stream_shards<'a>(
        &'a self,
        refs_dir: &'a str,
    ) -> impl Stream<Item = Result<(String, Vec<u8>), Error>> + 'a {
        paginated(move |token| async move {
            let page = self
                .store()
                .object_store()
                .list_children(refs_dir, 1000, token, None)
                .await?;
            Ok((page.objects, page.next_token))
        })
        .map_ok(move |obj| async move {
            let shard_path = format!("{refs_dir}/{obj}");
            match self.store().object_store().get(&shard_path).await {
                Ok(data) => Ok(Some((obj, data))),
                Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(Error::from(e)),
            }
        })
        .try_buffer_unordered(SHARD_READ_CONCURRENCY)
        .try_filter_map(|shard| async move { Ok(shard) })
    }

    #[instrument(skip(self))]
    pub async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut index = BlobIndex::default();
        let mut shards = pin!(self.stream_shards(&refs_dir));
        while let Some((obj, data)) = shards.try_next().await? {
            // The shard filename was written from a validated `Namespace`; an
            // undecodable name or unparseable body is skipped.
            let (Ok(links), Ok(namespace)) = (
                serde_json::from_slice::<HashSet<LinkKind>>(&data),
                Namespace::new(&decode_blob_index_shard_namespace(&obj)),
            ) else {
                continue;
            };
            if !links.is_empty() {
                index.namespace.insert(namespace, links);
            }
        }

        if index.namespace.is_empty() {
            return Err(Error::NotFound);
        }
        Ok(index)
    }

    #[instrument(skip(self))]
    pub async fn has_blob_references(&self, digest: &Digest) -> Result<bool, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);

        // Short-circuit on the first non-empty shard; the single-shard common
        // case stays one list plus one get.
        let mut shards = pin!(self.stream_shards(&refs_dir));
        while let Some((_, data)) = shards.try_next().await? {
            let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
            if !links.is_empty() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    #[instrument(skip(self))]
    pub async fn read_blob_index_namespace(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        match self.store().object_store().get(&shard_path).await {
            Ok(data) => {
                let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                non_empty_links_or_not_found(links)
            }
            Err(StorageError::NotFound) => Err(Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }
}
