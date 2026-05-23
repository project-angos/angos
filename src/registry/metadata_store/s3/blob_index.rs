use std::{collections::HashSet, io::ErrorKind};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, instrument, warn};

use super::{Backend, sleep_cas_retry};
use crate::{
    oci::Digest,
    registry::{
        metadata_store::{BlobIndex, BlobIndexOperation, Error, link_kind::LinkKind, sharded},
        path_builder,
    },
};
use angos_s3_client as s3_client;

impl Backend {
    /// Applies a batch of blob index operations for a single digest using optimistic
    /// concurrency (CAS). Reads the current blob index with its `ETag`, applies all
    /// operations, and writes back with `If-Match`. Retries on `ETag` conflict.
    ///
    /// For new blob indexes (not-found), uses `If-None-Match: *` to create atomically,
    /// falling back to CAS if another writer created the index concurrently.
    pub async fn update_blob_index_cas(
        &self,
        namespace: &str,
        digest: &crate::oci::Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);

        for attempt in 0..super::MAX_BLOB_INDEX_CAS_RETRIES {
            let (mut links, etag) = match self.store.read_with_etag(&shard_path).await {
                Ok((data, etag)) => (
                    serde_json::from_slice::<HashSet<LinkKind>>(&data).unwrap_or_default(),
                    etag,
                ),
                Err(e) if e.kind() == ErrorKind::NotFound => (HashSet::new(), None),
                Err(e) => return Err(Error::from(e)),
            };

            sharded::apply_blob_index_operations(&mut links, operations);

            // Write the shard back with CAS. Empty shards are written rather than
            // deleted to avoid a race where a concurrent writer creates a new entry
            // between our read and a non-conditional delete. Scrub handles cleanup.
            let content = Bytes::from(serde_json::to_vec(&links)?);

            let write_result = if let Some(ref etag) = etag {
                self.store
                    .put_object_if_match(&shard_path, etag, content)
                    .await
                    .map(|_| ())
            } else {
                self.store
                    .put_object_if_not_exists(&shard_path, content)
                    .await
                    .map(|_| ())
            };

            match write_result {
                Ok(()) => return Ok(()),
                Err(s3_client::Error::PreconditionFailed) => {
                    debug!(
                        digest = %digest,
                        namespace,
                        attempt,
                        "Blob index shard CAS conflict, retrying"
                    );
                    sleep_cas_retry(attempt).await;
                }
                Err(e) => return Err(Error::StorageBackend(e.to_string())),
            }
        }

        warn!(
            %digest,
            namespace,
            attempts = super::MAX_BLOB_INDEX_CAS_RETRIES,
            "Blob index shard CAS retries exhausted"
        );
        Err(Error::Lock(format!(
            "blob index CAS retries exhausted for digest {digest} after {} attempts",
            super::MAX_BLOB_INDEX_CAS_RETRIES
        )))
    }

    #[instrument(skip(self))]
    pub async fn read_blob_index_impl(&self, digest: &Digest) -> Result<BlobIndex, Error> {
        // Try sharded format first (refs/{namespace}.json per namespace)
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut index = BlobIndex::default();
        let mut found_shards = false;
        let mut continuation_token = None;

        loop {
            let (_, objects, next_token) = self
                .store
                .list_prefixes(&refs_dir, "/", 1000, continuation_token, None)
                .await?;

            if !objects.is_empty() {
                found_shards = true;
            }

            let shard_results = stream::iter(objects.into_iter().map(|obj| {
                let shard_path = format!("{refs_dir}/{obj}");
                async move {
                    match self.store.read(&shard_path).await {
                        Ok(data) => {
                            if let Ok(links) = serde_json::from_slice::<HashSet<LinkKind>>(&data) {
                                let namespace = sharded::decode_blob_index_shard_namespace(&obj);
                                if !links.is_empty() {
                                    return Ok(Some((namespace, links)));
                                }
                            }
                            Ok(None)
                        }
                        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                        Err(e) => Err(Error::from(e)),
                    }
                }
            }))
            .buffer_unordered(sharded::SHARD_READ_CONCURRENCY)
            .collect::<Vec<Result<Option<(String, HashSet<LinkKind>)>, Error>>>()
            .await;

            let shards = shard_results
                .into_iter()
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>, _>>()?;
            index
                .namespace
                .extend(sharded::collect_blob_index_shards(shards).namespace);

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        if found_shards {
            if index.namespace.is_empty() {
                return Err(Error::ReferenceNotFound);
            }
            return Ok(index);
        }

        // Legacy index.json fallback — remove after v2.0.0 migration
        let legacy_path = path_builder::blob_index_path(digest);
        let data = match self.store.read(&legacy_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(Error::ReferenceNotFound);
            }
            Err(e) => return Err(e.into()),
        };
        let blob_index: BlobIndex = serde_json::from_slice(&data).map_err(Error::from)?;

        self.migrate_legacy_blob_index_data(digest, &blob_index)
            .await?;
        info!(
            "Migrated legacy blob index for '{digest}' ({} namespaces)",
            blob_index.namespace.len()
        );

        Ok(blob_index)
    }

    pub async fn read_blob_index_namespace_impl(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        match self.store.read(&shard_path).await {
            Ok(data) => {
                let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                sharded::non_empty_links_or_not_found(links)
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let blob_index = self.read_blob_index_impl(digest).await?;
                sharded::namespace_links_from_index(&blob_index, namespace)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn has_blob_references_impl(&self, digest: &Digest) -> Result<bool, Error> {
        let refs_dir = path_builder::blob_index_refs_dir(digest);
        let mut found_shards = false;
        let mut continuation_token = None;

        loop {
            let (_, objects, next_token) = self
                .store
                .list_prefixes(&refs_dir, "/", 1, continuation_token, None)
                .await?;

            if !objects.is_empty() {
                found_shards = true;
            }

            for object in objects {
                let shard_path = format!("{refs_dir}/{object}");
                match self.store.read(&shard_path).await {
                    Ok(data) => {
                        let links = serde_json::from_slice::<HashSet<LinkKind>>(&data)?;
                        if !links.is_empty() {
                            return Ok(true);
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }

            continuation_token = next_token;
            if continuation_token.is_none() {
                break;
            }
        }

        if found_shards {
            return Ok(false);
        }

        let legacy_path = path_builder::blob_index_path(digest);
        match self.store.read(&legacy_path).await {
            Ok(data) => {
                let blob_index: BlobIndex = serde_json::from_slice(&data)?;
                Ok(sharded::blob_index_has_references(&blob_index))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Unconditional read-modify-write on a namespace shard (caller must hold lock).
    pub async fn update_blob_index_shard(
        &self,
        namespace: &str,
        digest: &crate::oci::Digest,
        operations: &[BlobIndexOperation],
    ) -> Result<(), Error> {
        let shard_path = path_builder::blob_index_shard_path(digest, namespace);
        let mut links: HashSet<LinkKind> = match self.store.read(&shard_path).await {
            Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
            Err(e) if e.kind() == ErrorKind::NotFound => HashSet::new(),
            Err(e) => return Err(e.into()),
        };

        sharded::apply_blob_index_operations(&mut links, operations);

        if links.is_empty() {
            self.store.delete(&shard_path).await?;
        } else {
            let content = Bytes::from(serde_json::to_vec(&links)?);
            self.store.put_object(&shard_path, content).await?;
        }

        Ok(())
    }

    async fn migrate_legacy_blob_index_data(
        &self,
        digest: &Digest,
        blob_index: &BlobIndex,
    ) -> Result<(), Error> {
        for (namespace, links) in &blob_index.namespace {
            let operations: Vec<BlobIndexOperation> = links
                .iter()
                .map(|link| BlobIndexOperation::Insert(link.clone()))
                .collect();

            if self.conditional_capabilities.supports_cas() {
                self.update_blob_index_cas(namespace, digest, &operations)
                    .await?;
            } else {
                self.update_blob_index_shard(namespace, digest, &operations)
                    .await?;
            }
        }

        let legacy_path = path_builder::blob_index_path(digest);
        self.store.delete(&legacy_path).await?;
        Ok(())
    }

    pub async fn migrate_blob_index_layout(&self, digest: &Digest) -> Result<(), Error> {
        let legacy_path = path_builder::blob_index_path(digest);
        let data = match self.store.read(&legacy_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let blob_index: BlobIndex = serde_json::from_slice(&data).map_err(Error::from)?;

        self.migrate_legacy_blob_index_data(digest, &blob_index)
            .await?;
        info!(
            "Migrated legacy blob index for '{digest}' ({} namespaces)",
            blob_index.namespace.len()
        );
        Ok(())
    }
}
