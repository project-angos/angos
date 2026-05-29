//! `store_manifest` and `delete_manifest` — thin wrappers over the shared
//! link-transaction planner in [`crate::registry::metadata_store::link_ops`].

use bytes::Bytes;

use crate::{
    oci::Digest,
    registry::metadata_store::{Error, LinkOperation, MetadataStore, link_ops::LinksTxExtras},
};

impl MetadataStore {
    /// Persist a manifest: writes blob-data, all link metadata, and blob-index
    /// shard updates as a single atomic transaction.
    pub async fn store_manifest(
        &self,
        namespace: &str,
        digest: &Digest,
        manifest_bytes: &[u8],
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        let extras = LinksTxExtras {
            blob_data_put: Some((digest, Bytes::from(manifest_bytes.to_vec()))),
            blob_data_delete_if_unreferenced: None,
            force_register_namespace: true,
        };
        self.execute_links_tx(namespace, operations, extras).await
    }

    /// Delete a manifest: removes link metadata, cleans up blob-index shards,
    /// and conditionally deletes the manifest blob-data when no remaining
    /// namespace holds a reference — all as a single atomic transaction.
    pub async fn delete_manifest(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        let extras = LinksTxExtras {
            blob_data_put: None,
            blob_data_delete_if_unreferenced: Some(digest),
            force_register_namespace: false,
        };
        self.execute_links_tx(namespace, operations, extras).await
    }
}
