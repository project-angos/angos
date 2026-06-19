//! `store_manifest` and `delete_manifest`: thin wrappers over the shared
//! link-transaction planner in [`super::ops`].

use chrono::{DateTime, Utc};

use crate::{
    oci::Digest,
    registry::metadata_store::{Error, LinkOperation, LinksCommit, LinksTx, MetadataStore},
};

impl MetadataStore {
    /// Persist a manifest's link metadata and blob-index shard updates as a
    /// single atomic transaction. The manifest blob-data itself is content and
    /// is written separately to the blob store by the caller. Returns the
    /// [`LinksCommit`] carrying each created link's commit-validated prior
    /// target.
    pub async fn store_manifest(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
        created_at: Option<DateTime<Utc>>,
    ) -> Result<LinksCommit, Error> {
        let tx = LinksTx::StoreManifest { created_at };
        self.execute_links_tx(namespace, operations, tx).await
    }

    /// Delete a manifest: removes link metadata and cleans up blob-index shards
    /// as a single atomic transaction. Returns whether the manifest blob became
    /// unreferenced, so the caller reclaims its blob-data from the blob store
    /// under the blob-data lock it must hold across this call (so a concurrent
    /// reference grant isn't missed; the `ManifestBlobUnknown` race).
    pub async fn delete_manifest(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[LinkOperation],
        source_ts: Option<DateTime<Utc>>,
    ) -> Result<bool, Error> {
        let tx = LinksTx::DeleteManifest {
            blob: digest,
            source_ts,
        };
        self.execute_links_tx(namespace, operations, tx)
            .await
            .map(|c| c.reclaim_blob)
    }
}
