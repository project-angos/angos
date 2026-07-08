//! Blob storage subsystem.
//!
//! Exposes a single [`BlobStore`] over a plain [`ObjectStore`] plus an optional
//! [`PresignedStore`]. The blob store is pure storage: it performs no
//! coordination and holds no transaction executor. Blob-lifecycle serialisation
//! lives entirely on the metadata store's `blob-data:{digest}` lock, which every
//! caller (push, upload, delete, scrub) acquires before mutating blob bytes. FS
//! and S3 share one code path (the [`BlobStoreConfig`] enum only picks the
//! storage handles); all public methods are inherent on `BlobStore`, with no
//! caller-facing trait.

mod config;
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

use angos_storage::{ObjectStore, PresignedStore};
use angos_tx_engine::StorageError;

pub use config::BlobStoreConfig;
// Inner config structs are only constructed by tests; production code builds
// backends through `BlobStoreConfig`. Re-export them for test builds only.
#[cfg(test)]
pub use config::{FsBackendConfig, S3BackendConfig, TransportFields};
pub use multipart_cleanup::{MultipartCleanup, OrphanMultipartUpload};

use crate::{
    oci::Digest,
    registry::{Error, pagination, path_builder},
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
    /// Object reads/writes and the upload lifecycle. On FS the backend prunes
    /// its own empty ancestor directories on delete, so callers don't.
    object: Arc<dyn ObjectStore>,
    /// Presign backend, when the storage supports it (S3 only). Absent for FS,
    /// where reads stream instead.
    presign: Option<Arc<dyn PresignedStore>>,
}

impl Debug for BlobStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore").finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Construct a blob store over `object`, optionally with a `presign`
    /// backend for signed download URLs (S3; `None` on FS).
    #[must_use]
    pub fn new(object: Arc<dyn ObjectStore>, presign: Option<Arc<dyn PresignedStore>>) -> Self {
        BlobStore { object, presign }
    }

    /// The underlying object store. Test escape hatch for raw prefix access
    /// (fixture cleanup, storage-seam fault injection) outside the blob API.
    #[cfg(test)]
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    /// Whether a presign backend is wired (S3 has one; FS does not).
    #[cfg(test)]
    #[must_use]
    pub fn supports_presign(&self) -> bool {
        self.presign.is_some()
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
                let page = self.object.list(&blob_prefix, limit, cursor).await?;
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
        match self.object.get(&path).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => Err(Error::BlobUnknown),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        let path = path_builder::blob_path(digest);
        match self.object.head(&path).await {
            Ok(meta) => Ok(meta.size),
            Err(StorageError::NotFound) => Err(Error::BlobUnknown),
            Err(e) => Err(e.into()),
        }
    }

    /// The blob bytes' last-modified time, or `None` when the backend records
    /// none. Used to age-gate orphan-grant cleanup so an in-flight push (which
    /// grants ownership before linking the manifest) is never reaped.
    #[instrument(skip(self))]
    pub async fn last_modified(&self, digest: &Digest) -> Result<Option<DateTime<Utc>>, Error> {
        let path = path_builder::blob_path(digest);
        match self.object.head(&path).await {
            Ok(meta) => Ok(meta.last_modified),
            Err(StorageError::NotFound) => Err(Error::BlobUnknown),
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
        match self.object.get_stream(&path, start_offset).await {
            Ok((reader, total)) => Ok((reader, total)),
            Err(StorageError::NotFound) => Err(Error::BlobUnknown),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    pub async fn delete_blob(&self, digest: &Digest) -> Result<(), Error> {
        let container = path_builder::blob_container_dir(digest);
        self.object.delete_prefix(&container).await?;
        Ok(())
    }

    /// Delete a namespace's repository subtree (its in-flight uploads) by raw
    /// on-disk name, so scrub can reclaim an upload directory whose name fails
    /// `Namespace` validation.
    #[instrument(skip(self))]
    pub async fn delete_namespace_directory(&self, name: &str) -> Result<(), Error> {
        let prefix = path_builder::namespace_dir(name)
            .ok_or_else(|| Error::Internal(format!("unsafe namespace directory name: '{name}'")))?;
        self.object.delete_prefix(&prefix).await?;
        Ok(())
    }

    /// Write `body` directly at the content-addressed blob path, for small
    /// in-memory content (manifest bodies); layer blobs use the streaming upload
    /// lifecycle instead. Idempotent (the digest fixes path and bytes); callers
    /// serialise against concurrent reclaim with the blob-data lock.
    #[instrument(skip(self, body))]
    pub async fn put_blob(&self, digest: &Digest, body: Bytes) -> Result<(), Error> {
        self.object
            .put(&path_builder::blob_path(digest), body)
            .await?;
        Ok(())
    }
}

// presigning (formerly `impl PresignedBlobStore`)

impl BlobStore {
    /// Generate a presigned download URL for `digest` when the underlying
    /// storage supports presigning. Returns `Ok(None)` when no presigning
    /// backend was wired (FS).
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
