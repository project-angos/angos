//! Blob storage subsystem.
//!
//! Exposes a single [`BlobStore`] over a plain [`ObjectStore`] plus an optional
//! [`PresignedStore`]. The blob store is pure storage: it performs no
//! coordination and holds no transaction executor. Blob-lifecycle serialisation
//! lives entirely on the metadata store's `blob-data:{digest}` lock, which every
//! caller (push, upload, delete, scrub) acquires before mutating blob bytes. FS
//! and S3 share one code path (the [`BlobStoreConfig`] enum only picks the
//! storage handles); all public methods are inherent on `BlobStore`, with no
//! caller-facing trait. The one deliberate exception is
//! [`multipart_cleanup::MultipartCleanup`], kept as a trait so prune can
//! inject a test double.

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
use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use tokio::io::AsyncRead;
use tracing::instrument;

use angos_storage::{ObjectStore, PresignedStore, paginated};
use angos_tx_engine::StorageError;

pub use config::BlobStoreConfig;
// Inner config structs are only constructed by tests; production code builds
// backends through `BlobStoreConfig`. Re-export them for test builds only.
#[cfg(test)]
pub use config::{FsBackendConfig, S3BackendConfig, TransportFields};
pub use multipart_cleanup::{MultipartCleanup, OrphanMultipartUpload};

use crate::{
    oci::{Algorithm, Digest},
    registry::{Error, path_builder},
};

pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;

/// Fan-out for the per-shard page chains behind [`BlobStore::stream_blobs`].
const BLOB_LIST_CONCURRENCY: usize = 32;

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

    /// The underlying object store, for raw key access outside the blob API:
    /// the scrub walk (quarantine/corrupt-object deletion on exact listed
    /// keys) and test fixtures.
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
    /// Streams every stored blob digest, unordered. Blobs live at
    /// `blobs/<algo>/<shard>/<hash>/data` with the shard the hash's first two
    /// hex digits, so hashes distribute uniformly over the shard directories:
    /// each algorithm's existing shards are discovered with one children
    /// listing, then walked as up to [`BLOB_LIST_CONCURRENCY`] concurrent
    /// page chains instead of one serial continuation-token chain.
    pub fn stream_blobs(&self) -> impl Stream<Item = Result<Digest, Error>> + Send + '_ {
        stream::once(async move {
            let shards = self.collect_blob_shards().await?;
            Ok::<_, Error>(
                stream::iter(shards)
                    .map(move |(algorithm, shard)| Box::pin(self.shard_blobs(algorithm, &shard)))
                    .flatten_unordered(BLOB_LIST_CONCURRENCY),
            )
        })
        .try_flatten()
    }

    /// The existing `(algorithm, shard)` directories across every supported
    /// algorithm, each algorithm discovered with one children listing. Shard
    /// names are a small bounded set (two-hex-digit prefixes), so collecting
    /// them up front lets [`Self::stream_blobs`] walk every shard as one
    /// concurrent fan-out rather than a stream of streams of streams.
    async fn collect_blob_shards(&self) -> Result<Vec<(Algorithm, String)>, Error> {
        let mut shards = Vec::new();
        for algorithm in Algorithm::supported_algorithms() {
            let root = format!("{}/{algorithm}/", path_builder::blobs_root_dir());
            let names = paginated(move |token| {
                let root = root.clone();
                async move {
                    let page = self.object.list_children(&root, 1000, token, None).await?;
                    Ok::<_, Error>((page.sub_prefixes, page.next_token))
                }
            })
            .try_collect::<Vec<String>>()
            .await?;
            shards.extend(names.into_iter().map(|shard| (*algorithm, shard)));
        }
        Ok(shards)
    }

    /// One shard directory's blobs (each the `<hash>/data` key under the
    /// shard prefix), one listing page at a time. The returned stream borrows
    /// only `self`: `shard` is consumed into the owned prefix up front.
    fn shard_blobs<'a>(
        &'a self,
        algorithm: Algorithm,
        shard: &str,
    ) -> impl Stream<Item = Result<Digest, Error>> + Send + use<'a> {
        let prefix = format!("{}/{algorithm}/{shard}/", path_builder::blobs_root_dir());
        paginated(move |token| {
            let prefix = prefix.clone();
            async move {
                let page = self.object.list(&prefix, 1000, token).await?;
                let blobs = page
                    .items
                    .into_iter()
                    .filter_map(|key| {
                        let hash = key.strip_suffix("/data")?;
                        Digest::with_algorithm(algorithm, hash).ok()
                    })
                    .collect();
                Ok((blobs, page.next_token))
            }
        })
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
