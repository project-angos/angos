//! Blob-index types for tracking cross-namespace blob references.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use angos_storage::Error as StorageError;
use angos_tx_engine::{
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry},
    transaction::{Mutation, Read, Transaction},
};

use crate::{
    oci::Digest,
    registry::metadata_store::{
        Error, MetadataStore,
        link_kind::LinkKind,
        link_ops::{append_shard_for_digest, tx_error_to_meta},
    },
};

// ── Domain types ──────────────────────────────────────────────────────────────

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlobIndex {
    pub namespace: HashMap<String, HashSet<LinkKind>>,
}

#[derive(Debug, Clone)]
pub enum BlobIndexOperation {
    Insert(LinkKind),
    Remove(LinkKind),
}

// ── Engine-backed write methods ───────────────────────────────────────────────

impl MetadataStore {
    /// Update the blob-index shard for `digest` in `namespace`.
    ///
    /// Reads the current shard state, applies `operations`, and submits a
    /// `Transaction` through the stored executor. Retries on `Conflict` or
    /// `Precondition` up to [`DEFAULT_RETRY_BUDGET`] times.
    pub async fn update_blob_index_batch(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        execute_with_retry(
            self.executor(),
            || async {
                let store = self.store_arc();
                let builder = Transaction::builder();
                let builder =
                    append_shard_for_digest(store.as_ref(), namespace, digest, operations, builder)
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
    /// The caller embeds these in a larger transaction (typically the cache
    /// job handler merging the grant with the upload promotion and queue
    /// cleanup). On `Conflict` the caller propagates the error; in-place CAS
    /// retry is the responsibility of [`Self::update_blob_index_batch`].
    pub async fn build_grant_mutations(
        &self,
        namespace: &str,
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
}
