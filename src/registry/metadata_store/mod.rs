use std::{future::Future, sync::Arc};

use angos_tx_engine::{executor::TransactionExecutor, lock::LockSession, store::Store};

use crate::{
    cache::Cache,
    oci::Digest,
    registry::{Error, pagination},
};

mod access_time;
mod blob_index;
mod catalog;
mod link;

#[cfg(test)]
mod tests;

pub use blob_index::{BlobIndex, BlobIndexOperation, shard::decode_blob_index_shard_namespace};
pub use link::{LinkKind, LinkMetadata, LinkOperation, LinksCommit, LinksTx, ReferencePolicy};

use access_time::{AccessTimeWriter, FlushHandle};

/// Canonical key for the coarse `blob-data:{digest}` lock, held via
/// [`MetadataStore::acquire_blob_data_lock`] / `with_blob_data_lock` by every
/// path that mutates a blob's bytes or ownership grants.
pub fn blob_data_lock_key(digest: &Digest) -> String {
    format!("blob-data:{digest}")
}

// MetadataStore (concrete implementation)

#[derive(Clone)]
pub struct MetadataStore {
    /// Storage façade owning the object store and transaction executor; all
    /// reads, reads-for-update, and coordinated writes flow through it.
    store: Arc<Store>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    /// Concurrent directory scans a catalog namespace walk keeps in flight.
    namespace_walk_concurrency: usize,
    access_time_writer: Option<AccessTimeWriter>,
    // Held for Drop side-effect: signals the flush task to exit when the last clone is dropped.
    _flush_handle: Option<Arc<FlushHandle>>,
}

pub struct Builder {
    store: Arc<Store>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_debounce_secs: u64,
    namespace_walk_concurrency: usize,
}

impl Builder {
    fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            cache: None,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            namespace_walk_concurrency: pagination::NAMESPACE_WALK_CONCURRENCY,
        }
    }

    pub fn cache(mut self, cache: Arc<Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn link_cache_ttl(mut self, ttl: u64) -> Self {
        self.link_cache_ttl = ttl;
        self
    }

    pub fn access_time_debounce_secs(mut self, secs: u64) -> Self {
        self.access_time_debounce_secs = secs;
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
        let (access_time_writer, flush_handle) =
            access_time::build_writer(&self.store, self.access_time_debounce_secs);

        MetadataStore {
            store: self.store,
            cache: self.cache,
            link_cache_ttl: self.link_cache_ttl,
            namespace_walk_concurrency: self.namespace_walk_concurrency,
            access_time_writer,
            _flush_handle: flush_handle,
        }
    }
}

impl MetadataStore {
    /// Return a builder over the storage façade `store` (object store for reads
    /// plus the transaction executor). `cache`, `link_cache_ttl` and
    /// `access_time_debounce_secs` are optional fluent setters.
    pub fn builder(store: Arc<Store>) -> Builder {
        Builder::new(store)
    }

    /// Returns the storage façade used for all reads and coordinated writes.
    pub fn store(&self) -> &Store {
        self.store.as_ref()
    }

    /// Returns an owned handle to the storage façade, for closures and helpers
    /// that need to capture it across `await` points.
    pub fn store_arc(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn executor(&self) -> &dyn TransactionExecutor {
        self.store.executor().as_ref()
    }

    /// Acquire the coarse [`blob_data_lock_key`] lock for `digest`, which
    /// serialises blob-data creation (upload completion) against reclamation
    /// (unreferenced delete) and against concurrent manifest pushes, which
    /// declare the same coarse lock on their link transactions.
    ///
    /// Lives on the METADATA engine, the one domain every blob-data
    /// participant (manifest push, upload, scrub) agrees on, even though the
    /// bytes may be mutated on the separate BLOB engine, so the pairing can't
    /// drift.
    pub async fn acquire_blob_data_lock(&self, digest: &Digest) -> Result<LockSession, Error> {
        let keys = [blob_data_lock_key(digest)];
        self.store
            .acquire(&keys)
            .await
            .map_err(|e| Error::Internal(format!("blob-data lock acquire failed: {e}")))
    }

    /// Run `op` while holding the coarse blob-data lock for `digest`,
    /// releasing the lock whatever the outcome. `op` is not polled before the
    /// lock is acquired.
    pub async fn with_blob_data_lock<T, E>(
        &self,
        digest: &Digest,
        op: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E>
    where
        E: From<Error>,
    {
        let session = self.acquire_blob_data_lock(digest).await?;
        let result = op.await;
        session.release().await;
        result
    }
}
