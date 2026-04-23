use std::{collections::HashSet, io::ErrorKind, time::Duration};

use bytes::Bytes;
use tracing::{debug, warn};

use super::Backend;
use crate::registry::{
    data_store,
    metadata_store::{BlobIndexOperation, Error, link_kind::LinkKind, simple_jitter},
    path_builder,
};

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

            for op in operations {
                match op {
                    BlobIndexOperation::Insert(link) => {
                        links.insert(link.clone());
                    }
                    BlobIndexOperation::Remove(link) => {
                        links.remove(link);
                    }
                }
            }

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
                Err(data_store::Error::PreconditionFailed) => {
                    debug!(
                        digest = %digest,
                        namespace,
                        attempt,
                        "Blob index shard CAS conflict, retrying"
                    );
                    let max_ms = 50u64.saturating_mul(1u64 << attempt.min(4));
                    tokio::time::sleep(Duration::from_millis(simple_jitter(max_ms))).await;
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

        for operation in operations {
            match operation {
                BlobIndexOperation::Insert(link) => {
                    links.insert(link.clone());
                }
                BlobIndexOperation::Remove(link) => {
                    links.remove(link);
                }
            }
        }

        if links.is_empty() {
            self.store.delete(&shard_path).await?;
        } else {
            let content = Bytes::from(serde_json::to_vec(&links)?);
            self.store.put_object(&shard_path, content).await?;
        }

        Ok(())
    }
}
