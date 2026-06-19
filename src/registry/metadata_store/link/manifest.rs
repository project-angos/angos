//! `store_manifest` and `delete_manifest`: thin wrappers over the shared
//! link-transaction planner in [`super::ops`].

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{
    oci::Digest,
    registry::metadata_store::{Error, LinkOperation, LinksCommit, LinksTx, MetadataStore},
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
        let tx = LinksTx::StoreManifest {
            blob: (digest, Bytes::from(manifest_bytes.to_vec())),
            created_at,
        };
        self.execute_links_tx(namespace, operations, tx).await
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
        // Hold the blob-data lock across the unreferenced-check + reclaim so a
        // concurrent reference grant isn't missed (the `ManifestBlobUnknown`
        // race; see `acquire_blob_data_lock`).
        let session = self.acquire_blob_data_lock(digest).await?;
        let tx = LinksTx::DeleteManifest {
            blob: digest,
            source_ts,
        };
        let result = self.execute_links_tx(namespace, operations, tx).await;
        session.release().await;
        result.map(|_| ())
    }
}
