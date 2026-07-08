use std::collections::HashSet;

use crate::{
    oci::{Digest, Namespace},
    registry::{
        Error,
        metadata_store::{BlobIndexOperation, LinkKind, MetadataStore},
    },
};

pub struct BlobOwnership<'a> {
    metadata_store: &'a MetadataStore,
}

impl<'a> BlobOwnership<'a> {
    pub fn new(metadata_store: &'a MetadataStore) -> Self {
        Self { metadata_store }
    }

    /// Insert `namespace`'s blob ownership reference into the blob index.
    /// Committed on the metadata store's executor with the engine's conflict
    /// retry, and idempotent, so a retry re-grants harmlessly. A caller that
    /// must serialize against a concurrent reclaim runs this inside
    /// `MetadataStore::with_blob_data_lock`.
    pub async fn grant(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        self.metadata_store
            .update_blob_index(
                namespace,
                digest,
                BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
            )
            .await
    }

    pub async fn can_read(&self, namespace: &Namespace, digest: &Digest) -> Result<bool, Error> {
        match self
            .metadata_store
            .read_blob_index_namespace(namespace, digest)
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn references(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        match self
            .metadata_store
            .read_blob_index_namespace(namespace, digest)
            .await
        {
            Ok(links) => Ok(links),
            Err(Error::NotFound) => Ok(HashSet::new()),
            Err(error) => Err(error),
        }
    }

    /// Every local namespace referencing `digest`, per the blob index; empty
    /// when none do (a missing index entry is not an error). Malformed index
    /// keys are skipped rather than failing the enumeration.
    pub async fn referencing_namespaces(&self, digest: &Digest) -> Result<Vec<Namespace>, Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(Error::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };

        Ok(index.namespace.into_keys().collect())
    }

    /// The lexicographically-smallest namespace referencing `digest`, excluding
    /// `exclude`; `None` when no other namespace references it. Takes the
    /// minimum without materialising the full referencing-namespace set.
    pub async fn smallest_referencing_namespace(
        &self,
        digest: &Digest,
        exclude: &str,
    ) -> Result<Option<Namespace>, Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(Error::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };

        Ok(index
            .namespace
            .into_keys()
            .filter(|key| key != exclude)
            .filter_map(|key| Namespace::new(&key).ok())
            .min())
    }
}
