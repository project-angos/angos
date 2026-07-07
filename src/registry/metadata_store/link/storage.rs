//! Link reference storage primitives: read/write a single link's
//! [`LinkMetadata`], plus the cache-aware `read_link`.

#[cfg(test)]
use bytes::Bytes;
use tracing::instrument;

use angos_tx_engine::StorageError;

use crate::{
    oci::Namespace,
    registry::{
        metadata_store::{Error, LinkKind, LinkMetadata, MetadataStore},
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
            Err(StorageError::NotFound) => Err(Error::ReferenceNotFound),
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
}
