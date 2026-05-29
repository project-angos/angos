//! Blob storage subsystem.
//!
//! Exposes a single unified [`BlobStore`] struct that carries an
//! `Arc<dyn Storage>` (any [`ObjectStore`] + [`UploadSessionStore`]) plus
//! an optional `Arc<dyn PresignedStore>` for presigning, an optional typed
//! `Arc<StorageFsBackend>` for FS-specific tidy-up (empty-ancestor pruning),
//! and the transaction executor used by the upload-session commit step. FS
//! and S3 are wired through the same code path; the [`BlobStoreConfig`] enum
//! only picks the underlying storage handles. All public methods live as
//! inherent methods on `BlobStore` — no caller-facing trait split.

mod config;
mod error;
pub mod hashing_reader;
pub mod sha256_ext;
pub mod upload_session;

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::io::AsyncRead;
use tracing::{debug, instrument};

use angos_storage::{
    Error as StorageError, ObjectStore, PresignedStore, fs::Backend as StorageFsBackend,
};
use angos_tx_engine::executor::TransactionExecutor;

pub use angos_storage::UploadSessionStore;
pub use config::BlobStoreConfig;
// Inner config structs are only constructed by tests; production code builds
// backends through `BlobStoreConfig`. Re-export them for test builds only.
#[cfg(test)]
pub use config::{FsBackendConfig, S3BackendConfig, TransportFields};
pub use error::Error;

use crate::{
    oci::Digest,
    registry::{pagination, path_builder},
};

pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;

/// Summary of an in-progress or completed upload session.
#[derive(Debug, Clone)]
pub struct UploadSummary {
    pub size: u64,
    pub started_at: DateTime<Utc>,
}

/// Combined supertrait every storage backend handed to [`BlobStore`] must
/// satisfy: `ObjectStore` for the floor surface, `UploadSessionStore` for
/// resumable streaming uploads.
pub trait Storage: ObjectStore + UploadSessionStore + Send + Sync {}
impl<T: ObjectStore + UploadSessionStore + Send + Sync + ?Sized> Storage for T {}

#[derive(Clone)]
pub struct BlobStore {
    pub store: Arc<dyn Storage>,
    pub presign: Option<Arc<dyn PresignedStore>>,
    /// Typed FS handle used solely for `prune_empty_ancestors` on
    /// upload-session complete/abort. `None` when the underlying storage
    /// is not FS — pruning is meaningless on S3.
    pub fs_prune: Option<Arc<StorageFsBackend>>,
    executor: Arc<dyn TransactionExecutor>,
}

impl Debug for BlobStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore").finish_non_exhaustive()
    }
}

impl BlobStore {
    pub fn builder() -> BlobStoreBuilder {
        BlobStoreBuilder::default()
    }

    pub fn executor(&self) -> &Arc<dyn TransactionExecutor> {
        &self.executor
    }
}

#[derive(Default)]
pub struct BlobStoreBuilder {
    store: Option<Arc<dyn Storage>>,
    presign: Option<Arc<dyn PresignedStore>>,
    fs_prune: Option<Arc<StorageFsBackend>>,
    executor: Option<Arc<dyn TransactionExecutor>>,
}

impl BlobStoreBuilder {
    #[must_use]
    pub fn store(mut self, store: Arc<dyn Storage>) -> Self {
        self.store = Some(store);
        self
    }

    #[must_use]
    pub fn presign(mut self, presign: Arc<dyn PresignedStore>) -> Self {
        self.presign = Some(presign);
        self
    }

    /// Typed FS storage backend. Set this when the storage is FS so the
    /// blob-store can call `prune_empty_ancestors` after upload-session
    /// complete/abort. Leave unset for S3 or any other backend that
    /// has no directory concept.
    #[must_use]
    pub fn fs_prune(mut self, fs: Arc<StorageFsBackend>) -> Self {
        self.fs_prune = Some(fs);
        self
    }

    #[must_use]
    pub fn executor(mut self, executor: Arc<dyn TransactionExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn build(self) -> Result<BlobStore, Error> {
        let store = self
            .store
            .ok_or_else(|| Error::StorageBackend("blob_store builder requires a store".into()))?;
        let executor = self.executor.ok_or_else(|| {
            Error::StorageBackend("blob_store builder requires an executor".into())
        })?;
        Ok(BlobStore {
            store,
            presign: self.presign,
            fs_prune: self.fs_prune,
            executor,
        })
    }
}

// ─── blob CRUD (formerly `impl BlobStore`) ────────────────────────────────

impl BlobStore {
    #[instrument(skip(self))]
    pub async fn list_blobs(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        debug!("Fetching {n} blob(s) with continuation token: {continuation_token:?}");
        let algorithm = "sha256";
        let blob_prefix = format!("{}/{algorithm}/", path_builder::blobs_root_dir());

        let mut all_blobs = Vec::new();
        let mut list_continuation_token = None;
        loop {
            let page = self
                .store
                .list(&blob_prefix, 1000, list_continuation_token)
                .await?;
            for key in page.items {
                let Some(key_without_data) = key.strip_suffix("/data") else {
                    continue;
                };
                if let Some(slash_pos) = key_without_data.rfind('/') {
                    let digest = &key_without_data[slash_pos + 1..];
                    all_blobs.push(Digest::Sha256(digest.into()));
                }
            }
            list_continuation_token = page.next_token;
            if list_continuation_token.is_none() {
                break;
            }
        }

        Ok(pagination::paginate_sorted(
            &all_blobs,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self))]
    pub async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        let path = path_builder::blob_path(digest);
        match self.store.get(&path).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        let path = path_builder::blob_path(digest);
        match self.store.head(&path).await {
            Ok(meta) => Ok(meta.size),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let path = path_builder::blob_path(digest);
        match self.store.get_stream(&path, start_offset).await {
            Ok((reader, total)) => Ok((reader, total)),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn delete_blob(&self, digest: &Digest) -> Result<(), Error> {
        let container = path_builder::blob_container_dir(digest);
        self.store.delete_prefix(&container).await?;
        if let Some(fs) = &self.fs_prune {
            fs.prune_empty_ancestors(&container, 2).await;
        }
        Ok(())
    }
}

// ─── presigning (formerly `impl PresignedBlobStore`) ──────────────────────

impl BlobStore {
    /// Generate a presigned download URL for `digest` when the underlying
    /// storage supports presigning. Returns `Ok(None)` when no presigning
    /// backend was provided to the builder.
    #[instrument(skip(self))]
    pub async fn presigned_url(
        &self,
        digest: &Digest,
        content_type: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let Some(presign) = &self.presign else {
            return Ok(None);
        };
        let path = path_builder::blob_path(digest);
        let url = presign
            .presign_get(&path, Duration::from_mins(30), content_type)
            .await?;
        Ok(Some(url))
    }
}

#[cfg(test)]
mod tests;
