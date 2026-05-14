mod blob_index;
mod capabilities;
mod error;

use std::{
    collections::{HashSet, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
};

use async_trait::async_trait;
pub use error::Error;

use crate::oci::{Descriptor, Digest};

mod config;
pub mod fs;
pub mod link_kind;
mod link_metadata;
pub mod lock;
pub mod lock_ops;
pub mod referrer_resolver;
pub mod s3;
pub mod transaction;

#[cfg(test)]
mod tests;

pub use blob_index::{BlobIndex, BlobIndexOperation};
pub use capabilities::ConditionalCapabilities;
pub use config::MetadataStoreConfig;
pub use link_metadata::LinkMetadata;
pub use lock::{LockStrategy, redis::LockConfig};
pub use transaction::{LinkOperation, ResolvedCreate, ResolvedDelete, Transaction};

use crate::registry::metadata_store::link_kind::LinkKind;

/// Returns a random number in `[0, max_ms)` for use as retry-loop jitter.
///
/// Entropy comes from `std::collections::hash_map::RandomState`, which the
/// stdlib seeds from the OS at process start. This is sufficient for jitter
/// because the only goal is to spread retry attempts across competing tasks —
/// adversaries learning the value gain nothing. **This is not a
/// security-critical RNG**: do not use it for tokens, nonces, or anything an
/// attacker could exploit if predicted. For those uses, draw from
/// `rand::rng()` or a CSPRNG instead.
pub fn simple_jitter(max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }
    RandomState::new().build_hasher().finish() % max_ms
}

pub trait MetadataStoreExt {
    fn begin_transaction(&self, namespace: &str) -> Transaction<'_>;
}

impl MetadataStoreExt for dyn MetadataStore + Send + Sync {
    fn begin_transaction(&self, namespace: &str) -> Transaction<'_> {
        Transaction::new(self, namespace.to_string())
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

    async fn has_blob_references(&self, digest: &Digest) -> Result<bool, Error> {
        match self.read_blob_index(digest).await {
            Ok(blob_index) => Ok(!blob_index.namespace.is_empty()),
            Err(Error::ReferenceNotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn read_blob_index_namespace(
        &self,
        namespace: &str,
        digest: &Digest,
    ) -> Result<HashSet<LinkKind>, Error> {
        let blob_index = self.read_blob_index(digest).await?;
        blob_index
            .namespace
            .get(namespace)
            .cloned()
            .filter(|links| !links.is_empty())
            .ok_or(Error::ReferenceNotFound)
    }

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
