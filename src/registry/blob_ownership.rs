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
    metadata_store: &'a MetadataStore,
}

impl<'a> BlobOwnership<'a> {
    pub fn new(metadata_store: &'a MetadataStore) -> Self {
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
        match self
            .metadata_store
            .read_blob_index_namespace(namespace.as_ref(), digest)
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
            .read_blob_index_namespace(namespace.as_ref(), digest)
            .await
        {
            Ok(links) => Ok(links),
            Err(MetadataError::ReferenceNotFound) => Ok(HashSet::new()),
            Err(error) => Err(error.into()),
        }
    }

    /// Every local namespace that holds a reference to `digest`, per the blob
    /// index. Empty when no namespace references the blob (including a missing
    /// index entry, `ReferenceNotFound`); other backend errors propagate. Index
    /// keys that are not valid namespaces are skipped — a malformed key must not
    /// fail an otherwise valid enumeration.
    pub async fn referencing_namespaces(&self, digest: &Digest) -> Result<Vec<Namespace>, Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(MetadataError::ReferenceNotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };

        let mut namespaces = Vec::with_capacity(index.namespace.len());
        for key in index.namespace.into_keys() {
            if let Ok(namespace) = Namespace::new(&key) {
                namespaces.push(namespace);
            }
        }
        Ok(namespaces)
    }

    /// The lexicographically-smallest namespace that references `digest`,
    /// excluding `exclude` — the cross-repo mount source the replication pipeline
    /// offers. Takes the minimum by a streaming scan of the blob-index keys, so
    /// the full referencing-namespace set is never materialised (unlike
    /// [`Self::referencing_namespaces`]). Returns `None` when no other namespace
    /// references the blob.
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
