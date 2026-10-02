//! A layer's stored listing: its entries in chunks sorted by path, its inflate
//! checkpoints in chunks, and the `listing` object whose presence marks the
//! layer indexed.

use bytes::Bytes;

use angos_extension_service::LayerEntry;
use angos_oci::Digest;
use angos_storage::Error as StorageError;

use crate::{
    layer::{Checkpoints, Listing},
    registry::{Error, keys::DigestKeys, metadata_store::MetadataStore},
};

impl MetadataStore {
    /// A layer's listing; `None` when it was never indexed.
    pub async fn read_listing(&self, digest: &Digest) -> Result<Option<Listing>, Error> {
        match self.object_store().get(&digest.layer_listing_path()).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Chunk `chunk` of a layer's entries.
    pub async fn read_entries(
        &self,
        digest: &Digest,
        chunk: usize,
    ) -> Result<Vec<LayerEntry>, Error> {
        let bytes = self
            .object_store()
            .get(&digest.layer_entries_chunk_path(chunk))
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Chunk `chunk` of a layer's checkpoints, empty when none was stored.
    pub async fn read_checkpoints(
        &self,
        digest: &Digest,
        chunk: usize,
    ) -> Result<Checkpoints, Error> {
        match self
            .object_store()
            .get(&digest.layer_checkpoints_path(chunk))
            .await
        {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(StorageError::NotFound) => Ok(Checkpoints::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn put_entries(
        &self,
        digest: &Digest,
        chunk: usize,
        entries: &[LayerEntry],
    ) -> Result<(), Error> {
        let body = Bytes::from(serde_json::to_vec(entries)?);
        self.object_store()
            .put(&digest.layer_entries_chunk_path(chunk), body)
            .await?;
        Ok(())
    }

    pub async fn put_checkpoints(
        &self,
        digest: &Digest,
        chunk: usize,
        checkpoints: &Checkpoints,
    ) -> Result<(), Error> {
        let body = Bytes::from(serde_json::to_vec(checkpoints)?);
        self.object_store()
            .put(&digest.layer_checkpoints_path(chunk), body)
            .await?;
        Ok(())
    }

    /// Written after the chunks it describes: its presence is what marks the
    /// layer indexed.
    pub async fn put_listing(&self, digest: &Digest, listing: &Listing) -> Result<(), Error> {
        let body = Bytes::from(serde_json::to_vec(listing)?);
        self.object_store()
            .put(&digest.layer_listing_path(), body)
            .await?;
        Ok(())
    }

    /// Delete a layer's listing with all its chunks.
    pub async fn delete_listing(&self, digest: &Digest) -> Result<(), Error> {
        self.object_store()
            .delete_prefix(&digest.layer_dir())
            .await?;
        Ok(())
    }
}
