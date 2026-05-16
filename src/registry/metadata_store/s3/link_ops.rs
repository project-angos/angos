use std::{collections::HashMap, io::ErrorKind};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use tracing::warn;

use super::Backend;
use crate::{
    oci::Digest,
    registry::{
        data_store,
        metadata_store::{
            BlobIndexOperation, Error, LinkMetadata, link_kind::LinkKind, lock_ops::LockOps,
        },
        path_builder,
    },
};

impl Backend {
    pub fn cache_key(namespace: &str, link: &LinkKind) -> String {
        format!("link:{namespace}:{link}")
    }

    pub async fn cache_get(&self, namespace: &str, link: &LinkKind) -> Option<LinkMetadata> {
        if self.link_cache_ttl == 0 {
            return None;
        }
        let cache = self.cache.as_ref()?;
        cache
            .retrieve::<LinkMetadata>(&Self::cache_key(namespace, link))
            .await
            .ok()
            .flatten()
    }

    /// Reads the link with its `ETag`, then spawns the access time write as a
    /// background task so the caller doesn't block on the CAS round-trip.
    ///
    /// The spawned write is best-effort: under concurrent pulls the `ETag` may
    /// become stale and the CAS will silently fail. This is acceptable because
    /// access times are advisory — a missed update simply means the timestamp
    /// lags by one pull interval. Requires a CAS-capable backend (`put_if_match`).
    pub async fn read_and_spawn_access_time_update(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);

        let (data, etag) = match self.store.read_with_etag(&link_path).await {
            Ok(result) => result,
            Err(e) if e.kind() == ErrorKind::NotFound => return Err(Error::ReferenceNotFound),
            Err(e) => return Err(e.into()),
        };
        let link_data = LinkMetadata::from_bytes(data)?;

        if let Some(etag) = etag {
            let store = self.store.clone();
            let updated = link_data.clone().accessed();
            let path = link_path;
            tokio::spawn(async move {
                let serialized = match serde_json::to_vec(&updated) {
                    Ok(v) => Bytes::from(v),
                    Err(_) => return,
                };
                let _ = store.put_object_if_match(&path, &etag, serialized).await;
            });
        }

        Ok(link_data)
    }

    /// Atomically creates a link only if it does not already exist, using
    /// S3 conditional `If-None-Match: *`. Returns `true` if the link was
    /// created, `false` if it already existed (precondition failed).
    pub async fn write_link_if_not_exists(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<bool, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        match self
            .store
            .put_object_if_not_exists(&link_path, serialized)
            .await
        {
            Ok(_) => Ok(true),
            Err(data_store::Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(Error::StorageBackend(e.to_string())),
        }
    }
}

#[async_trait]
impl LockOps for Backend {
    async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store.read(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(Error::ReferenceNotFound),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized_link_data = Bytes::from(serde_json::to_vec(metadata)?);
        self.store
            .put_object(&link_path, serialized_link_data)
            .await?;
        Ok(())
    }

    async fn delete_link_reference(&self, namespace: &str, link: &LinkKind) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        self.store.delete(&link_path).await?;
        Ok(())
    }

    fn lock_key_for_link(namespace: &str, link: &LinkKind) -> String {
        format!("{namespace}:{link}")
    }

    fn lock_key_for_blob_index(namespace: &str, digest: &Digest) -> String {
        format!("{namespace}:blob:{digest}")
    }

    async fn cache_put(&self, namespace: &str, link: &LinkKind, metadata: &LinkMetadata) {
        if self.link_cache_ttl == 0 {
            return;
        }
        if let Some(cache) = &self.cache {
            let key = Self::cache_key(namespace, link);
            if let Err(err) = cache.store(&key, metadata, self.link_cache_ttl).await {
                warn!("Failed to store link metadata in cache for {namespace}/{link}: {err}");
            }
        }
    }

    async fn cache_invalidate(&self, namespace: &str, link: &LinkKind) {
        if let Some(cache) = &self.cache {
            let _ = cache.delete_value(&Self::cache_key(namespace, link)).await;
        }
    }

    /// Applies blob-index operations concurrently, one task per digest.
    ///
    /// CAS-capable providers use optimistic shard updates so concurrent
    /// ownership grants and manifest link writes cannot overwrite each other.
    /// Providers without conditional writes fall back to the lock-protected
    /// read-modify-write path.
    async fn apply_pending_blob_index_ops(
        &self,
        namespace: &str,
        pending_blob_ops: HashMap<Digest, Vec<BlobIndexOperation>>,
    ) -> Result<(), Error> {
        if self.conditional_capabilities.supports_cas() {
            join_all(
                pending_blob_ops
                    .iter()
                    .map(|(digest, ops)| self.update_blob_index_cas(namespace, digest, ops)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        } else {
            join_all(
                pending_blob_ops
                    .iter()
                    .map(|(digest, ops)| self.update_blob_index_shard(namespace, digest, ops)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        }
        Ok(())
    }

    /// Registers the namespace in the namespace registry after a successful
    /// transaction that included create operations.
    ///
    /// Failure to register is non-fatal: the error is logged at `warn` level
    /// and `Ok(())` is returned so the transaction result is not affected.
    async fn after_update(&self, namespace: &str, had_creates: bool) -> Result<(), Error> {
        if had_creates && let Err(e) = self.register_namespace(namespace).await {
            warn!(namespace, error = %e, "Failed to register namespace");
        }
        Ok(())
    }
}
