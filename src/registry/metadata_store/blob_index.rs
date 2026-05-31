//! Blob-index types for tracking cross-namespace blob references.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tracing::instrument;

use angos_tx_engine::{
    StorageError,
    error::Error as TxError,
    executor::{DEFAULT_RETRY_BUDGET, execute_with_retry},
    transaction::{Mutation, Read, Transaction},
};

use crate::{
    oci::Digest,
    registry::metadata_store::{
        Error, MetadataStore,
        link_kind::LinkKind,
        link_ops::{LinksTxExtras, append_shard_for_digest, tx_error_to_meta},
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
    /// Reads the current shard state, applies `operation`, and submits a
    /// `Transaction` through the stored executor. Retries on `Conflict` or
    /// `Precondition` up to [`DEFAULT_RETRY_BUDGET`] times.
    #[instrument(skip(self))]
    pub async fn update_blob_index(
        &self,
        namespace: &str,
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
    /// The caller embeds these in a larger transaction (typically the cache
    /// job handler merging the grant with the upload promotion and queue
    /// cleanup). On `Conflict` the caller propagates the error; in-place CAS
    /// retry is the responsibility of [`Self::update_blob_index`].
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

    /// Revoke `namespace`'s ownership of `digest` and, in the same atomic
    /// transaction, delete the blob-data when no remaining namespace holds a
    /// reference. Mirrors `delete_manifest`: both express the shard-entry
    /// removal and the conditional blob-data reclaim as a single link
    /// transaction, so a crash cannot orphan the blob bytes. The
    /// `blob-data:{digest}` coarse lock declared by the planner serialises this
    /// against concurrent manifest pushes referencing the same
    /// content-addressed digest.
    pub async fn revoke_blob_ownership(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<(), Error> {
        let extras = LinksTxExtras {
            blob_index_ops: Some((
                digest,
                vec![BlobIndexOperation::Remove(LinkKind::Blob(digest.clone()))],
            )),
            blob_data_delete_if_unreferenced: Some(digest),
            caller_holds_blob_data_lock: true,
            ..LinksTxExtras::default()
        };
        self.execute_links_tx(namespace, &[], extras).await
    }
}
