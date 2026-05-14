use std::collections::HashSet;

use crate::{
    oci::{Digest, Namespace},
    registry::{
        Error,
        metadata_store::{
            BlobIndexOperation, Error as MetadataError, MetadataStore, link_kind::LinkKind,
        },
    },
};

pub struct BlobOwnership<'a> {
    metadata_store: &'a (dyn MetadataStore + Send + Sync),
}

impl<'a> BlobOwnership<'a> {
    pub fn new(metadata_store: &'a (dyn MetadataStore + Send + Sync)) -> Self {
        Self { metadata_store }
    }

    pub async fn grant(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        self.metadata_store
            .update_blob_index(
                namespace.as_ref(),
                digest,
                BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
            )
            .await
            .map_err(Error::from)
    }

    pub async fn can_read(&self, namespace: &Namespace, digest: &Digest) -> Result<bool, Error> {
        match self.metadata_store.read_blob_index(digest).await {
            Ok(blob_index) => Ok(blob_index.namespace.contains_key(namespace.as_ref())),
            Err(MetadataError::ReferenceNotFound) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn revoke(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        link: LinkKind,
    ) -> Result<(), Error> {
        self.metadata_store
            .update_blob_index(namespace.as_ref(), digest, BlobIndexOperation::Remove(link))
            .await
            .map_err(Error::from)
    }

    pub async fn references(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        match self.metadata_store.read_blob_index(digest).await {
            Ok(blob_index) => Ok(blob_index
                .namespace
                .get(namespace.as_ref())
                .cloned()
                .unwrap_or_default()),
            Err(MetadataError::ReferenceNotFound) => Ok(HashSet::new()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn has_any_reference(&self, digest: &Digest) -> Result<bool, Error> {
        match self.metadata_store.read_blob_index(digest).await {
            Ok(blob_index) => Ok(!blob_index.namespace.is_empty()),
            Err(MetadataError::ReferenceNotFound) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}
