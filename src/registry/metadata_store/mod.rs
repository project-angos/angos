use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use angos_tx_engine::{executor::TransactionExecutor, lock::LockSession, store::Store};

use crate::{cache::Cache, oci::Digest};

mod access_time;
mod blob_index;
mod catalog;
mod error;
mod link;

#[cfg(test)]
mod tests;

pub use blob_index::{BlobIndex, BlobIndexOperation};
pub use error::Error;
pub use link::{LinkKind, LinkMetadata, LinkOperation, LinksCommit, LinksTx, tx_error_to_meta};

use access_time::{AccessTimeWriter, FlushHandle};

/// Canonical key for the coarse `blob-data:{digest}` lock — the single source of
/// truth shared by [`MetadataStore::acquire_blob_data_lock`] and the
/// `coarse_lock(...)` calls in `link::ops`, so the two never drift.
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
    access_time_writer: Option<AccessTimeWriter>,
    // Held for Drop side-effect: signals the flush task to exit when the last clone is dropped.
    _flush_handle: Option<Arc<FlushHandle>>,
}

pub struct Builder {
    store: Arc<Store>,
    cache: Option<Arc<Cache>>,
    link_cache_ttl: u64,
    access_time_debounce_secs: u64,
}

impl Builder {
    fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            cache: None,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
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

    #[must_use]
    pub fn build(self) -> MetadataStore {
        let store = self.store;

        let (access_time_writer, flush_handle) = if self.access_time_debounce_secs > 0 {
            let writer = AccessTimeWriter::new();
            let shutdown = Arc::new(AtomicBool::new(false));
            let interval = Duration::from_secs(self.access_time_debounce_secs);

            let flush_backend = MetadataStore {
                store: store.clone(),
                cache: None,
                link_cache_ttl: self.link_cache_ttl,
                access_time_writer: Some(writer.clone()),
                _flush_handle: None,
            };

            MetadataStore::spawn_flush_task(flush_backend, shutdown.clone(), interval);

            (Some(writer), Some(Arc::new(FlushHandle::new(shutdown))))
        } else {
            (None, None)
        };

        MetadataStore {
            store,
            cache: self.cache,
            link_cache_ttl: self.link_cache_ttl,
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

    /// Acquire the coarse [`blob_data_lock_key`] lock for `digest`.
    ///
    /// Lives on the METADATA executor — the one domain every blob-data
    /// participant (manifest push, upload, scrub) agrees on — even though the
    /// bytes may be mutated on the separate BLOB engine, so the pairing can't
    /// drift.
    pub async fn acquire_blob_data_lock(&self, digest: &Digest) -> Result<LockSession, Error> {
        let keys = [blob_data_lock_key(digest)];
        self.executor()
            .acquire(&keys)
            .await
            .map_err(|e| Error::Coordination(format!("blob-data lock acquire failed: {e}")))
    }

    pub async fn flush_access_times(&self) {
        if let Some(writer) = &self.access_time_writer {
            writer.flush(self).await;
        }
    }
}
