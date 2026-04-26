mod error;

use std::{
    collections::{HashMap, HashSet, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
pub use error::Error;
use serde::{Deserialize, Serialize};

use crate::oci::{Descriptor, Digest};

mod config;
pub mod fs;
pub mod link_kind;
mod link_metadata;
mod lock;
pub mod lock_ops;
pub mod s3;

#[cfg(test)]
mod tests;

pub use config::MetadataStoreConfig;
pub use link_metadata::LinkMetadata;
pub use lock::{LockStrategy, redis::LockConfig};

use crate::registry::metadata_store::link_kind::LinkKind;

pub fn simple_jitter(max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }
    RandomState::new().build_hasher().finish() % max_ms
}

/// Granular S3 conditional operation capabilities.
///
/// Each field corresponds to a distinct HTTP conditional header that the S3-compatible
/// provider may or may not support. Configure these explicitly in `[metadata_store.s3.capabilities]`
/// to skip the startup probe, or omit to auto-detect when using `lock_strategy = "s3"`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConditionalCapabilities {
    /// `PutObject` with `If-None-Match: *` — create-only, reject if object exists.
    pub put_if_none_match: bool,
    /// `PutObject` with `If-Match: <etag>` — update-only, reject if `ETag` mismatch.
    pub put_if_match: bool,
    /// `DeleteObject` with `If-Match: <etag>` — conditional delete.
    pub delete_if_match: bool,
}

impl ConditionalCapabilities {
    /// Both conditional put operations are needed for CAS read-modify-write loops.
    pub fn supports_cas(&self) -> bool {
        self.put_if_none_match && self.put_if_match
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlobIndex {
    pub namespace: HashMap<String, HashSet<LinkKind>>,
}

#[derive(Debug, Clone)]
pub enum BlobIndexOperation {
    Insert(LinkKind),
    Remove(LinkKind),
}

pub struct ResolvedCreate {
    pub link: LinkKind,
    pub target: Digest,
    pub old_target: Option<Digest>,
    pub referrer: Option<Digest>,
    pub media_type: Option<String>,
    pub descriptor: Option<Descriptor>,
}

pub struct ResolvedDelete {
    pub link: LinkKind,
    pub target: Digest,
    pub referrer: Option<Digest>,
}

#[derive(Debug, Clone)]
pub enum LinkOperation {
    Create {
        link: LinkKind,
        target: Digest,
        referrer: Option<Digest>,
        media_type: Option<String>,
        descriptor: Option<Box<Descriptor>>,
    },
    Delete {
        link: LinkKind,
        referrer: Option<Digest>,
    },
}

pub struct Transaction {
    store: Arc<dyn MetadataStore + Send + Sync>,
    namespace: String,
    operations: Vec<LinkOperation>,
}

/// Builder for a single `Create` link operation within a [`Transaction`].
///
/// Obtain one via [`Transaction::create_link`] and finalize it by calling [`add`](Self::add).
pub struct CreateLinkBuilder<'a> {
    tx: &'a mut Transaction,
    link: LinkKind,
    target: Digest,
    referrer: Option<Digest>,
    media_type: Option<String>,
    descriptor: Option<Descriptor>,
}

impl CreateLinkBuilder<'_> {
    /// Associates a referrer digest with this link.
    pub fn with_referrer(mut self, referrer: &Digest) -> Self {
        self.referrer = Some(referrer.clone());
        self
    }

    /// Associates a media type with this link.
    pub fn with_media_type(mut self, media_type: &str) -> Self {
        self.media_type = Some(media_type.to_string());
        self
    }

    /// Associates an OCI descriptor with this link.
    pub fn with_descriptor(mut self, descriptor: Descriptor) -> Self {
        self.descriptor = Some(descriptor);
        self
    }

    /// Appends the operation to the enclosing transaction.
    pub fn add(self) {
        self.tx.operations.push(LinkOperation::Create {
            link: self.link,
            target: self.target,
            referrer: self.referrer,
            media_type: self.media_type,
            descriptor: self.descriptor.map(Box::new),
        });
    }
}

impl Transaction {
    pub fn create_link(&mut self, link: &LinkKind, target: &Digest) -> CreateLinkBuilder<'_> {
        CreateLinkBuilder {
            tx: self,
            link: link.clone(),
            target: target.clone(),
            referrer: None,
            media_type: None,
            descriptor: None,
        }
    }

    pub fn delete_link(&mut self, link: &LinkKind) {
        self.operations.push(LinkOperation::Delete {
            link: link.clone(),
            referrer: None,
        });
    }

    pub fn delete_link_with_referrer(&mut self, link: &LinkKind, referrer: &Digest) {
        self.operations.push(LinkOperation::Delete {
            link: link.clone(),
            referrer: Some(referrer.clone()),
        });
    }

    pub async fn commit(self) -> Result<(), Error> {
        if self.operations.is_empty() {
            return Ok(());
        }
        self.store
            .update_links(&self.namespace, &self.operations)
            .await
    }
}

pub trait MetadataStoreExt {
    fn begin_transaction(&self, namespace: &str) -> Transaction;
}

impl MetadataStoreExt for Arc<dyn MetadataStore + Send + Sync> {
    fn begin_transaction(&self, namespace: &str) -> Transaction {
        Transaction {
            store: self.clone(),
            namespace: namespace.to_string(),
            operations: Vec::new(),
        }
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn list_namespaces(
        &self,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error>;

    async fn list_tags(
        &self,
        namespace: &str,
        n: u16,
        last: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error>;

    async fn list_referrers(
        &self,
        namespace: &str,
        digest: &Digest,
        artifact_type: Option<String>,
    ) -> Result<Vec<Descriptor>, Error>;

    async fn has_referrers(&self, namespace: &str, subject: &Digest) -> Result<bool, Error>;

    async fn list_revisions(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error>;

    async fn count_manifests(&self, namespace: &str) -> Result<usize, Error>;

    async fn read_blob_index(&self, digest: &Digest) -> Result<BlobIndex, Error>;

    async fn update_blob_index(
        &self,
        namespace: &str,
        digest: &Digest,
        operation: BlobIndexOperation,
    ) -> Result<(), Error>;

    async fn read_link(
        &self,
        namespace: &str,
        link: &LinkKind,
        update_access_time: bool,
    ) -> Result<LinkMetadata, Error>;

    async fn update_links(
        &self,
        namespace: &str,
        operations: &[LinkOperation],
    ) -> Result<(), Error>;

    async fn flush_access_times(&self) {}
}
