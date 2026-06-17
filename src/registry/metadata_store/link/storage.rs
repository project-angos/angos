//! Link reference storage primitives: read/write a single link's
//! [`LinkMetadata`], plus the cache- and access-time-aware `read_link`.

use bytes::Bytes;
use tracing::instrument;

use angos_tx_engine::{
    StorageError, error::Error as TxError, executor::DEFAULT_RETRY_BUDGET, transaction::Mutation,
};

use crate::registry::{
    metadata_store::{Error, LinkKind, LinkMetadata, MetadataStore, tx_error_to_meta},
    path_builder,
};

impl MetadataStore {
    /// Read the stored [`LinkMetadata`] for `link` within `namespace`.
    pub async fn read_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        match self.store().get(&link_path).await {
            Ok(data) => LinkMetadata::from_bytes(data),
            Err(StorageError::NotFound) => Err(Error::ReferenceNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn read_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        update_access_time: bool,
    ) -> Result<LinkMetadata, Error> {
        if update_access_time {
            if let Some(writer) = &self.access_time_writer {
                let link_data = if let Some(cached) = self.cache_get(namespace, link).await {
                    cached
                } else {
                    let data = self.read_link_reference(namespace, link).await?;
                    self.cache_put(namespace, link, &data).await;
                    data
                };
                writer.record(namespace, link).await;
                Ok(link_data)
            } else {
                let link_data = self.update_link_access_time(namespace, link).await?;
                self.cache_put(namespace, link, &link_data).await;
                Ok(link_data)
            }
        } else if let Some(cached) = self.cache_get(namespace, link).await {
            Ok(cached)
        } else {
            let link_data = self.read_link_reference(namespace, link).await?;
            self.cache_put(namespace, link, &link_data).await;
            Ok(link_data)
        }
    }

    /// Mark the access time for `link` in `namespace` using a read-modify-write
    /// transaction, returning the updated [`LinkMetadata`]. Concurrent updaters
    /// resolve via content-hash conflict detection — last writer wins, which is
    /// fine for advisory access-time stamps.
    async fn update_link_access_time(
        &self,
        namespace: &str,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        let link_path = path_builder::link_path(link, namespace);
        let keys = [link_path.clone()];

        let (_, link_data) = self
            .store()
            .update_with_payload(
                &keys,
                |snaps| {
                    let link_path = link_path.clone();
                    async move {
                        let snap = &snaps[0];
                        if !snap.present {
                            return Err(TxError::Storage(StorageError::NotFound));
                        }
                        let link_data = LinkMetadata::from_bytes(snap.body.to_vec())
                            .map_err(|e| TxError::Build(e.to_string()))?
                            .accessed();
                        let serialized =
                            Bytes::from(serde_json::to_vec(&link_data).map_err(TxError::Serde)?);
                        Ok((
                            vec![Mutation::Put {
                                key: link_path,
                                body: serialized,
                                expected: None,
                            }],
                            link_data,
                        ))
                    }
                },
                DEFAULT_RETRY_BUDGET,
            )
            .await
            .map_err(tx_error_to_meta)?;

        Ok(link_data)
    }

    /// Persist `metadata` for `link` within `namespace`. Used by tests to set up
    /// initial state; production code goes through `update_links`.
    #[cfg(test)]
    pub async fn write_link_reference(
        &self,
        namespace: &str,
        link: &LinkKind,
        metadata: &LinkMetadata,
    ) -> Result<(), Error> {
        let link_path = path_builder::link_path(link, namespace);
        let serialized = Bytes::from(serde_json::to_vec(metadata)?);
        self.store()
            .put(&link_path, serialized)
            .await
            .map_err(Error::from)
    }
}
