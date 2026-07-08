//! Link reference storage primitives: read/write a single link's
//! [`LinkMetadata`], the cache-aware `read_link`, and the link-metadata
//! cache helpers behind it (gated on `link_cache_ttl`).

#[cfg(test)]
use bytes::Bytes;
use tracing::{instrument, warn};

use angos_tx_engine::StorageError;

use crate::{
    oci::Namespace,
    registry::{
        Error,
        metadata_store::{LinkKind, LinkMetadata, MetadataStore},
        path_builder,
    },
};

impl MetadataStore {
    /// Read the stored [`LinkMetadata`] for `link` within `namespace`.
    pub async fn read_link_reference(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store().object_store().get(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(StorageError::NotFound) => Err(Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Cache-aware link read with no access-time side effect: serve from the
    /// link cache, else read through and populate it.
    #[instrument(skip(self))]
    pub async fn read_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        if let Some(cached) = self.cache_get(namespace, link).await {
            return Ok(cached);
        }
        let data = self.read_link_reference(namespace, link).await?;
        self.cache_put(namespace, link, &data).await;
        Ok(data)
    }

    /// Persist `metadata` for `link` within `namespace`. Used by tests to set up
    /// initial state; production code goes through `update_links`.
    #[cfg(test)]
    pub async fn write_link_reference(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        self.store()
            .object_store()
            .put(&link_path, serialized)
            .await
            .map_err(Error::from)
    }

    fn cache_key(namespace: &Namespace, link: &LinkKind) -> String {
        format!("link:{namespace}:{link}")
    }

    pub async fn cache_get(&self, namespace: &Namespace, link: &LinkKind) -> Option<LinkMetadata> {
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

    pub async fn cache_put(&self, namespace: &Namespace, link: &LinkKind, metadata: &LinkMetadata) {
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

    pub async fn cache_invalidate(&self, namespace: &Namespace, link: &LinkKind) {
        if let Some(cache) = &self.cache {
            let _ = cache.delete_value(&Self::cache_key(namespace, link)).await;
        }
    }
}
