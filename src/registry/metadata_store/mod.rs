use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use angos_oci::Namespace;
use angos_storage::ObjectStore;

use crate::registry::{Error, keys::REPOS_ROOT, pagination};
use angos_cache::Cache;

pub mod access_time;
mod blob_index;
mod catalog;
mod gc;
pub mod link;
mod mutation;

#[cfg(test)]
mod tests;

pub use access_time::{AccessEntry, parse_atime_entry};
pub use blob_index::{BlobIndex, BlobIndexOperation};
pub use link::tag::{parse_tag_entry, tag_ord, tag_ord_ts};
pub use link::{LinkKind, LinkMetadata, LinkOperation, LinksCommit, LinksTx, ReferencePolicy};

#[derive(Clone)]
pub struct MetadataStore {
    object: Arc<dyn ObjectStore>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    namespace_walk_concurrency: usize,
    /// How long fresh blob data and fresh reference keys are unconditionally
    /// live, and how long a collector's range marker outlives its last
    /// refresh. Writers and collectors over the same store share the value.
    gc_grace_secs: u64,
    /// How long superseded access entries are kept as pull history before
    /// scrub collects them.
    atime_audit_window_secs: u64,
    /// Namespaces whose catalog index key this process already ensured; being
    /// wrong only costs one redundant put.
    catalog_indexed: Arc<Mutex<HashSet<Namespace>>>,
}

/// Default reclamation grace period, which only has to exceed the widest
/// adjacent-request gap on a write path plus clock skew.
pub const DEFAULT_GC_GRACE_SECS: u64 = 300;

/// Default retention for superseded access entries.
pub const DEFAULT_ATIME_AUDIT_WINDOW_SECS: u64 = 3600;

pub struct Builder {
    object: Arc<dyn ObjectStore>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    namespace_walk_concurrency: usize,
    gc_grace_secs: u64,
    atime_audit_window_secs: u64,
}

impl Builder {
    fn new(object: Arc<dyn ObjectStore>) -> Self {
        Self {
            object,
            cache: None,
            link_cache_ttl: 30,
            namespace_walk_concurrency: pagination::NAMESPACE_WALK_CONCURRENCY,
            gc_grace_secs: DEFAULT_GC_GRACE_SECS,
            atime_audit_window_secs: DEFAULT_ATIME_AUDIT_WINDOW_SECS,
        }
    }

    /// The reclamation grace period, in seconds; tests and offline
    /// maintenance runs shrink it to exercise reclamation immediately.
    #[must_use]
    pub fn gc_grace_secs(mut self, secs: u64) -> Self {
        self.gc_grace_secs = secs;
        self
    }

    /// How long superseded access entries are retained as pull history.
    #[must_use]
    pub fn atime_audit_window_secs(mut self, secs: u64) -> Self {
        self.atime_audit_window_secs = secs;
        self
    }

    pub fn cache(mut self, cache: Arc<Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn link_cache_ttl(mut self, ttl: u64) -> Self {
        self.link_cache_ttl = ttl;
        self
    }

    /// Concurrent directory-scan fan-out for catalog namespace walks.
    #[must_use]
    pub fn namespace_walk_concurrency(mut self, concurrency: usize) -> Self {
        self.namespace_walk_concurrency = concurrency.max(1);
        self
    }

    #[must_use]
    pub fn build(self) -> MetadataStore {
        MetadataStore {
            object: self.object,
            cache: self.cache,
            link_cache_ttl: self.link_cache_ttl,
            namespace_walk_concurrency: self.namespace_walk_concurrency,
            gc_grace_secs: self.gc_grace_secs,
            atime_audit_window_secs: self.atime_audit_window_secs,
            catalog_indexed: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl MetadataStore {
    /// Return a builder over the object store all reads and writes flow
    /// through.
    pub fn builder(object: Arc<dyn ObjectStore>) -> Builder {
        Builder::new(object)
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    pub fn gc_grace_secs(&self) -> u64 {
        self.gc_grace_secs
    }

    pub fn atime_audit_window_secs(&self) -> u64 {
        self.atime_audit_window_secs
    }

    /// One bounded listing probes the backend; readiness must not walk the
    /// namespace tree.
    pub async fn check_ready(&self) -> Result<(), Error> {
        self.object
            .list_children(REPOS_ROOT, 1, None, None)
            .await
            .map_err(|e| Error::Internal(format!("storage backend not ready: {e}")))?;
        Ok(())
    }
}
