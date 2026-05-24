use std::collections::HashSet;

use bytes::Bytes;
use tracing::{debug, warn};

use angos_storage::Error as StorageError;

use crate::{
    oci::Digest,
    registry::{
        metadata_store::{
            BlobIndex, BlobIndexOperation, Error,
            backend::{
                Backend,
                coordinator::{MAX_CAS_RETRIES, sleep_cas_retry},
            },
            link_kind::LinkKind,
            lock_ops::{blob_index_lock_key, with_validated_lock},
            sharded,
        },
        path_builder,
    },
};

impl Backend {
    /// CAS-based blob-index shard update (retries on `PreconditionFailed`).
    ///
    /// Writes to the sharded layout exclusively. Use [`Self::update_blob_index_cas`]
    /// for the dispatcher that prefers an existing legacy `index.json` when
    /// present.
    pub async fn update_blob_index_cas_sharded(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let Some(cs) = self.coordinator.conditional_store() else {
            return Err(Error::Lock(
                "update_blob_index_cas_sharded called on non-CAS backend".to_string(),
            ));
        };

        let shard_path = path_builder::blob_index_shard_path(digest, namespace);

        for attempt in 0..MAX_CAS_RETRIES {
            let (mut links, etag) = match cs.get_with_etag(&shard_path).await {
                Ok((data, etag)) => (
                    serde_json::from_slice::<HashSet<LinkKind>>(&data).unwrap_or_default(),
                    etag,
                ),
                Err(StorageError::NotFound) => (HashSet::new(), None),
                Err(e) => return Err(e.into()),
            };

            sharded::apply_blob_index_operations(&mut links, operations);

            let content = Bytes::from(serde_json::to_vec(&links)?);

            let write_result = if let Some(ref etag) = etag {
                cs.put_if_match(&shard_path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                cs.put_if_absent(&shard_path, content).await.map(|_| ())
            };

            match write_result {
                Ok(()) => return Ok(()),
                Err(StorageError::PreconditionFailed) => {
                    debug!(
                        digest = %digest,
                        namespace,
                        attempt,
                        "Blob index shard CAS conflict, retrying"
                    );
                    sleep_cas_retry(attempt).await;
                }
                Err(e) => return Err(e.into()),
            }
        }

        warn!(
            %digest,
            namespace,
            attempts = MAX_CAS_RETRIES,
            "Blob index shard CAS retries exhausted"
        );
        Err(Error::Lock(format!(
            "blob index CAS retries exhausted for digest {digest} after {MAX_CAS_RETRIES} attempts"
        )))
    }

    /// Lock-guarded unconditional shard update (caller must hold lock).
    ///
    /// Writes to the sharded layout exclusively. Use [`Self::update_blob_index_locked`]
    /// for the dispatcher that prefers an existing legacy `index.json` when
    /// present.
    pub async fn update_blob_index_locked_sharded(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        let mut links: HashSet<LinkKind> = match self.store().get(&shard_path).await {
            Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
            Err(StorageError::NotFound) => HashSet::new(),
            Err(e) => return Err(e.into()),
        };

        sharded::apply_blob_index_operations(&mut links, operations);

        if links.is_empty() {
            self.store().delete(&shard_path).await?;
        } else {
            let content = Bytes::from(serde_json::to_vec(&links)?);
            self.store().put(&shard_path, content).await?;
        }

        Ok(())
    }

    /// GET → apply ops → PUT (or DELETE when empty) on the legacy `index.json`
    /// for `digest`. Caller MUST hold the per-digest blob-index lock.
    ///
    /// Returns `Ok(true)` when the legacy file was updated (or deleted because
    /// it became empty). Returns `Ok(false)` when no legacy file exists; the
    /// caller must then route the update through the sharded layout.
    async fn apply_legacy_blob_index_update(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<bool, Error> {
        let legacy_path = path_builder::blob_index_path(digest);
        let data = match self.store().get(&legacy_path).await {
            Ok(data) => data,
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let mut legacy: BlobIndex = serde_json::from_slice(&data).unwrap_or_default();
        {
            let entry = legacy.namespace.entry(namespace.to_string()).or_default();
            sharded::apply_blob_index_operations(entry, operations);
            if entry.is_empty() {
                legacy.namespace.remove(namespace);
            }
        }
        if legacy.namespace.is_empty() {
            self.store().delete(&legacy_path).await?;
        } else {
            let body = Bytes::from(serde_json::to_vec(&legacy)?);
            self.store().put(&legacy_path, body).await?;
        }
        Ok(true)
    }

    /// CAS-mode update applied to the legacy `index.json` when it exists.
    ///
    /// Cheap `head` short-circuits when no legacy file is present so
    /// post-migration deployments pay only one metadata request per write and
    /// never touch the lock. When the legacy file is present, the per-digest
    /// blob-index lock is acquired so the read-modify-write is serialised
    /// against [`crate::registry::metadata_store::MetadataStore::migrate_blob_index`] and against concurrent
    /// locked-mode writers.
    ///
    /// Returns `Ok(true)` when the legacy file was updated (or deleted because
    /// it became empty). Returns `Ok(false)` when there is no legacy file or
    /// when a concurrent migration removed it between the `head` and the
    /// in-lock GET — the caller must then route the update through the sharded
    /// layout.
    async fn update_blob_index_cas_legacy(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<bool, Error> {
        // Cheap existence check first so post-migration deployments (no legacy
        // files left on disk) pay only one metadata request per write and never
        // touch the lock.
        let legacy_path = path_builder::blob_index_path(digest);
        match self.store().head(&legacy_path).await {
            Ok(_) => {}
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(e.into()),
        }

        let lock_keys = [blob_index_lock_key(digest)];
        with_validated_lock(
            self.lock(),
            &lock_keys,
            "lock invalidated during legacy blob index update",
            || async {
                // Re-check inside the lock: a concurrent migration may have just
                // deleted the legacy file. `apply_legacy_blob_index_update`
                // returns Ok(false) in that case and the dispatcher falls
                // through to the sharded write path.
                self.apply_legacy_blob_index_update(namespace, digest, operations)
                    .await
            },
        )
        .await
    }

    /// Lock-guarded unconditional update applied to the legacy `index.json`
    /// when it exists. Caller must hold the per-digest blob-index lock.
    ///
    /// Returns `Ok(true)` when the legacy file was updated (or deleted because
    /// it became empty). Returns `Ok(false)` when no legacy file exists; the
    /// caller must then route the update through the sharded layout.
    async fn update_blob_index_locked_legacy(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<bool, Error> {
        // Caller already holds the per-digest blob-index lock.
        self.apply_legacy_blob_index_update(namespace, digest, operations)
            .await
    }

    /// CAS-based blob-index update dispatcher.
    ///
    /// If a legacy `index.json` exists for this digest, the update is applied
    /// in-place (legacy file stays the source of truth for that blob until a
    /// scrub-driven migration moves it to shards). Otherwise the update is
    /// written into the sharded layout.
    pub async fn update_blob_index_cas(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        if self
            .update_blob_index_cas_legacy(namespace, digest, operations)
            .await?
        {
            return Ok(());
        }
        self.update_blob_index_cas_sharded(namespace, digest, operations)
            .await
    }

    /// Lock-guarded blob-index update dispatcher.
    ///
    /// See [`Self::update_blob_index_cas`] for the legacy-vs-sharded routing.
    pub async fn update_blob_index_locked(
        &self,
        namespace: &str,
        digest: &Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        if self
            .update_blob_index_locked_legacy(namespace, digest, operations)
            .await?
        {
            return Ok(());
        }
        self.update_blob_index_locked_sharded(namespace, digest, operations)
            .await
    }

    /// Convert a legacy `index.json` payload into per-namespace shards.
    ///
    /// This calls the `_sharded` variants directly so that the migration always
    /// writes shards regardless of whether the legacy file still exists — if it
    /// went through [`Self::update_blob_index_cas`] / [`Self::update_blob_index_locked`]
    /// the dispatcher would detect the not-yet-deleted legacy file and write
    /// back into it, defeating the migration.
    pub async fn migrate_legacy_blob_index_data(
        &self,
        digest: &Digest,
        blob_index: &BlobIndex,
    ) -> Result<(), Error> {
        for (namespace, links) in &blob_index.namespace {
            let operations: Vec<BlobIndexOperation> = links
                .iter()
                .map(|link| BlobIndexOperation::Insert(link.clone()))
                .collect();
            if self.coordinator.is_cas() {
                self.update_blob_index_cas_sharded(namespace, digest, &operations)
                    .await?;
            } else {
                self.update_blob_index_locked_sharded(namespace, digest, &operations)
                    .await?;
            }
        }

        let legacy_path = path_builder::blob_index_path(digest);
        self.store().delete(&legacy_path).await?;
        Ok(())
    }
}
