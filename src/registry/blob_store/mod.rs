//! Blob storage subsystem.
//!
//! Exposes a single [`BlobStore`] over an `Arc<Store>` façade that bundles the
//! object store, upload-session store, optional presign backend, and the
//! transaction executor for upload promotion. FS and S3 share one code path
//! (the [`BlobStoreConfig`] enum only picks the storage handles); all public
//! methods are inherent on `BlobStore`, with no caller-facing trait.

mod config;
mod error;
pub mod hashing_reader;
mod multipart_cleanup;
pub mod resumable_hasher;
pub mod upload_session;

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::io::AsyncRead;
use tracing::{debug, instrument};

use angos_tx_engine::{StorageError, store::Store};

pub use config::BlobStoreConfig;
// Inner config structs are only constructed by tests; production code builds
// backends through `BlobStoreConfig`. Re-export them for test builds only.
#[cfg(test)]
pub use config::{FsBackendConfig, S3BackendConfig, TransportFields};
pub use error::Error;
pub use multipart_cleanup::{MultipartCleanup, OrphanMultipartUpload};

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

#[derive(Clone)]
pub struct BlobStore {
    /// Storage façade: object reads/writes, the upload lifecycle (including
    /// finalization), and presigning all flow through here. The façade owns the
    /// executor used by the upload-promotion transaction. (On FS, the backend
    /// prunes its own empty ancestor directories on delete, callers don't.)
    pub store: Arc<Store>,
}

impl Debug for BlobStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore").finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Construct a blob store over the storage façade `store`. Build the façade
    /// with the upload-session store enabled (and, where applicable, the
    /// presign backend).
    #[must_use]
    pub fn new(store: Arc<Store>) -> Self {
        BlobStore { store }
    }
}

// blob CRUD (formerly `impl BlobStore`)

impl BlobStore {
    #[instrument(skip(self))]
    pub async fn list_blobs(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        debug!("Fetching {n} blob(s) with continuation token: {continuation_token:?}");

        // Blobs are sharded by algorithm (`blobs/<algo>/<shard>/<hash>/data`); each
        // blob is the `/data` key under its container.
        pagination::paginate_by_algorithm(
            n,
            continuation_token,
            |algorithm, limit, cursor| async move {
                let blob_prefix = format!("{}/{algorithm}/", path_builder::blobs_root_dir());
                let page = self.store.list(&blob_prefix, limit, cursor).await?;
                let blobs = page
                    .items
                    .into_iter()
                    .filter_map(|key| {
                        let hash = key.strip_suffix("/data")?.rsplit_once('/')?.1;
                        Digest::with_algorithm(algorithm, hash).ok()
                    })
                    .collect();
                Ok((blobs, page.next_token))
            },
        )
        .await
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

    /// The blob bytes' last-modified time, or `None` when the backend records
    /// none. Used to age-gate orphan-grant cleanup so an in-flight push (which
    /// grants ownership before linking the manifest) is never reaped.
    #[instrument(skip(self))]
    pub async fn last_modified(&self, digest: &Digest) -> Result<Option<DateTime<Utc>>, Error> {
        let path = path_builder::blob_path(digest);
        match self.store.head(&path).await {
            Ok(meta) => Ok(meta.last_modified),
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
        Ok(())
    }

    /// Delete a namespace's repository subtree (its in-flight uploads) by raw
    /// on-disk name, so scrub can reclaim an upload directory whose name fails
    /// `Namespace` validation.
    #[instrument(skip(self))]
    pub async fn delete_namespace_directory(&self, name: &str) -> Result<(), Error> {
        let prefix = path_builder::namespace_dir(name).ok_or_else(|| {
            Error::InvalidFormat(format!("unsafe namespace directory name: '{name}'"))
        })?;
        self.store.delete_prefix(&prefix).await?;
        Ok(())
    }

    /// Write `body` directly at the content-addressed blob path, for small
    /// in-memory content (manifest bodies); layer blobs use the streaming upload
    /// lifecycle instead. Idempotent (the digest fixes path and bytes); callers
    /// serialise against concurrent reclaim with the blob-data lock.
    #[instrument(skip(self, body))]
    pub async fn put_blob(&self, digest: &Digest, body: Bytes) -> Result<(), Error> {
        self.store
            .put(&path_builder::blob_path(digest), body)
            .await?;
        Ok(())
    }
}

// presigning (formerly `impl PresignedBlobStore`)

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
        let path = path_builder::blob_path(digest);
        let url = self
            .store
            .presigned_get(&path, Duration::from_mins(30), content_type)
            .await?;
        Ok(url)
    }
}

#[cfg(test)]
mod tests;
