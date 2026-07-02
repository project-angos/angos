//! `store_manifest` and `delete_manifest`: thin wrappers over the shared
//! link-transaction planner in [`super::ops`].

use std::collections::HashSet;

use chrono::{DateTime, Utc};

use crate::{
    oci::{Digest, Namespace},
    registry::metadata_store::{Error, LinkOperation, LinksCommit, LinksTx, MetadataStore},
};

impl MetadataStore {
    /// Persist a manifest's link metadata and blob-index shard updates as a
    /// single atomic transaction. The manifest blob-data itself is content and
    /// is written separately to the blob store by the caller.
    ///
    /// The caller must hold the `blob-data` lock for the manifest digest and
    /// every digest in `granted`; a tracked grant insert for a target outside
    /// `granted` commits nothing and returns it in
    /// [`LinksCommit::needs_locks`], so the caller can enlarge its lock set
    /// and retry. On success the [`LinksCommit`] carries each created link's
    /// commit-validated prior target.
    pub async fn store_manifest(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
        created_at: Option<DateTime<Utc>>,
        granted: &HashSet<Digest>,
    ) -> Result<LinksCommit, Error> {
        let tx = LinksTx::StoreManifest {
            created_at,
            granted,
        };
        self.execute_links_tx(namespace, operations, tx).await
    }

    /// Delete a manifest: removes link metadata and cleans up blob-index shards
    /// as a single atomic transaction. Returns whether the manifest blob became
    /// unreferenced, so the caller reclaims its blob-data from the blob store
    /// under the blob-data lock it must hold across this call (so a concurrent
    /// reference grant isn't missed; the `ManifestBlobUnknown` race).
    pub async fn delete_manifest(
        &self,
        namespace: &Namespace,
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

    /// Test seeding: `store_manifest` with every create target pre-granted, so
    /// tracked links can be planted without holding blob-data locks.
    #[cfg(test)]
    pub async fn seed_links(
        &self,
        namespace: &Namespace,
        operations: &[LinkOperation],
    ) -> Result<(), Error> {
        let granted: HashSet<Digest> = operations
            .iter()
            .filter_map(|op| match op {
                LinkOperation::Create { target, .. } => Some(target.clone()),
                LinkOperation::Delete { .. } => None,
            })
            .collect();
        self.store_manifest(namespace, operations, None, &granted)
            .await
            .map(|_| ())
    }
}
