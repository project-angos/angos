//! `store_manifest` and `delete_manifest`: thin wrappers over the shared
//! link-transaction planner in [`crate::registry::metadata_store::link_ops`].

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{
    oci::Digest,
    registry::metadata_store::{
        Error, LinkOperation, MetadataStore,
        link_ops::{LinksCommit, LinksTxExtras},
    },
};

impl MetadataStore {
    /// Persist a manifest: writes blob-data, all link metadata, and blob-index
    /// shard updates as a single atomic transaction. Returns the
    /// [`LinksCommit`] carrying each created link's commit-validated prior
    /// target.
    pub async fn store_manifest(
        &self,
        namespace: &str,
        digest: &Digest,
        manifest_bytes: &[u8],
        operations: &[LinkOperation],
        created_at: Option<DateTime<Utc>>,
    ) -> Result<LinksCommit, Error> {
        let extras = LinksTxExtras {
            blob_data_put: Some((digest, Bytes::from(manifest_bytes.to_vec()))),
            blob_data_delete_if_unreferenced: None,
            blob_index_ops: None,
            caller_holds_blob_data_lock: false,
            force_register_namespace: true,
            created_at,
            delete_source_ts: None,
        };
        self.execute_links_tx(namespace, operations, extras).await
    }

    /// Delete a manifest: removes link metadata, cleans up blob-index shards,
    /// and conditionally deletes the manifest blob-data when no remaining
    /// namespace holds a reference, all as a single atomic transaction.
    pub async fn delete_manifest(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[LinkOperation],
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let extras = LinksTxExtras {
            blob_data_put: None,
            blob_data_delete_if_unreferenced: Some(digest),
            blob_index_ops: None,
            caller_holds_blob_data_lock: false,
            force_register_namespace: false,
            created_at: None,
            delete_source_ts: source_ts,
        };
        self.execute_links_tx(namespace, operations, extras)
            .await
            .map(|_| ())
    }
}
