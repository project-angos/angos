use std::collections::HashSet;

use crate::{
    oci::{Digest, Namespace},
    registry::{
        Error,
        metadata_store::{BlobIndexOperation, Error as MetadataError, LinkKind, MetadataStore},
    },
};

pub struct BlobOwnership<'a> {
    metadata_store: &'a MetadataStore,
}

impl<'a> BlobOwnership<'a> {
    pub fn new(metadata_store: &'a MetadataStore) -> Self {
        Self { metadata_store }
    }

    pub async fn grant(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        self.metadata_store
            .update_blob_index(
                namespace,
                digest,
                BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
            )
            .await
            .map_err(Error::from)
    }

    pub async fn can_read(&self, namespace: &Namespace, digest: &Digest) -> Result<bool, Error> {
        match self
            .metadata_store
            .read_blob_index_namespace(namespace, digest)
            .await
        {
            Ok(_) => Ok(true),
            Err(MetadataError::ReferenceNotFound) => Ok(false),
            Err(error) => Err(error.into()),
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
            Err(MetadataError::ReferenceNotFound) => Ok(HashSet::new()),
            Err(error) => Err(error.into()),
        }
    }

    /// Every local namespace referencing `digest`, per the blob index; empty when
    /// none do (a missing entry is not an error).
    pub async fn referencing_namespaces(&self, digest: &Digest) -> Result<Vec<Namespace>, Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(MetadataError::ReferenceNotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };

        Ok(index.namespace.into_keys().collect())
    }

    /// The lexicographically-smallest namespace referencing `digest`, excluding
    /// `exclude`; `None` when no other references it.
    pub async fn smallest_referencing_namespace(
        &self,
        digest: &Digest,
        exclude: &str,
    ) -> Result<Option<Namespace>, Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(MetadataError::ReferenceNotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        Ok(index
            .namespace
            .into_keys()
            .filter(|key| key != exclude)
            .filter_map(|key| Namespace::new(&key).ok())
            .min())
    }
}
