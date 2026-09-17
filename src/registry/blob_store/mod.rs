//! Blob storage: a single [`BlobStore`] over an [`ObjectStore`] plus an
//! optional [`PresignedStore`], shared by the FS and S3 backends.
//!
//! It is pure storage with no coordination of its own: writers rely on the
//! collector's grace period and the `v2/gc` marker protocol in the metadata
//! store.

mod config;
pub mod hashing;
mod multipart_cleanup;
pub mod upload_session;

use std::{
    fmt::{self, Debug, Formatter},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use tracing::instrument;

use angos_oci::{Algorithm, Digest};
use angos_storage::Error as StorageError;
use angos_storage::{ObjectStore, PresignedStore, paginated};

use crate::registry::{
    Error,
    keys::{BLOBS_ROOT, DigestKeys, namespace_dir},
    pagination,
};
pub use config::BlobStoreConfig;
// Production code builds backends through `BlobStoreConfig`; only tests
// construct the inner structs.
#[cfg(test)]
pub use config::{FsBackendConfig, S3BackendConfig, TransportFields};
pub use multipart_cleanup::OrphanMultipartUpload;

pub use angos_storage::BoxedReader;

/// A missing object is an unknown blob to every reader below; any other
/// storage failure surfaces as itself.
fn blob_error(error: StorageError) -> Error {
    match error {
        StorageError::NotFound => Error::BlobUnknown,
        backend @ StorageError::Backend(_) => backend.into(),
    }
}

/// Fan-out for the per-shard page chains behind [`BlobStore::stream_blobs`].
const BLOB_LIST_CONCURRENCY: usize = 32;

/// Default lifetime of a presigned blob download URL.
pub const DEFAULT_PRESIGN_TTL_SECS: u64 = 1800;

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
    /// Presign backend and the lifetime of the URLs it signs; absent on FS,
    /// which streams instead.
    presign: Option<(Arc<dyn PresignedStore>, Duration)>,
    /// Concurrent directory scans an upload-namespace walk keeps in flight.
    namespace_walk_concurrency: NonZeroUsize,
}

impl Debug for BlobStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStore").finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Construct a blob store over `object`, optionally with a `presign`
    /// backend for signed download URLs and the lifetime those URLs carry.
    #[must_use]
    pub fn new(
        object: Arc<dyn ObjectStore>,
        presign: Option<(Arc<dyn PresignedStore>, Duration)>,
    ) -> Self {
        BlobStore {
            object,
            presign,
            namespace_walk_concurrency: pagination::NAMESPACE_WALK_CONCURRENCY,
        }
    }

    /// Set the concurrent directory-scan fan-out for upload-namespace walks.
    #[must_use]
    pub fn with_namespace_walk_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.namespace_walk_concurrency = concurrency;
        self
    }

    /// The underlying object store, for raw key access outside the blob API
    /// (the scrub walk and test fixtures).
    #[must_use]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    /// Streams every stored blob digest, unordered. Blobs are sharded by the
    /// hash's first two hex digits, so the shards are discovered with one
    /// children listing per algorithm and then walked as up to
    /// [`BLOB_LIST_CONCURRENCY`] concurrent page chains.
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

    /// The existing `(algorithm, shard)` directories, one children listing per
    /// algorithm. Shard names are a small bounded set, so collecting them up
    /// front lets [`Self::stream_blobs`] be one flat concurrent fan-out.
    async fn collect_blob_shards(&self) -> Result<Vec<(Algorithm, String)>, Error> {
        let mut shards = Vec::new();
        for algorithm in Algorithm::supported_algorithms() {
            let root = format!("{BLOBS_ROOT}/{algorithm}/");
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

    /// One shard directory's blobs, a listing page at a time. The returned
    /// stream borrows only `self`, `shard` being consumed into the owned
    /// prefix up front.
    fn shard_blobs<'a>(
        &'a self,
        algorithm: Algorithm,
        shard: &str,
    ) -> impl Stream<Item = Result<Digest, Error>> + Send + use<'a> {
        let prefix = format!("{BLOBS_ROOT}/{algorithm}/{shard}/");
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
        self.object
            .get(&digest.blob_path())
            .await
            .map_err(blob_error)
    }

    #[instrument(skip(self))]
    pub async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        self.object
            .head(&digest.blob_path())
            .await
            .map(|meta| meta.size)
            .map_err(blob_error)
    }

    /// The blob bytes' last-modified time, or `None` when the backend records
    /// none. Age-gates orphan-grant cleanup so an in-flight push, which grants
    /// ownership before linking the manifest, is never reaped.
    #[instrument(skip(self))]
    pub async fn last_modified(&self, digest: &Digest) -> Result<Option<DateTime<Utc>>, Error> {
        self.object
            .head(&digest.blob_path())
            .await
            .map(|meta| meta.last_modified)
            .map_err(blob_error)
    }

    #[instrument(skip(self))]
    pub async fn reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        self.object
            .get_stream(&digest.blob_path(), start_offset)
            .await
            .map_err(blob_error)
    }

    #[instrument(skip(self))]
    pub async fn delete_blob(&self, digest: &Digest) -> Result<(), Error> {
        let container = digest.blob_dir();
        self.object.delete_prefix(&container).await?;
        Ok(())
    }

    /// Delete a namespace's repository subtree (its in-flight uploads) by raw
    /// on-disk name, so scrub can reclaim an upload directory whose name fails
    /// `Namespace` validation.
    #[instrument(skip(self))]
    pub async fn delete_namespace_directory(&self, name: &str) -> Result<(), Error> {
        let prefix = namespace_dir(name)
            .ok_or_else(|| Error::Internal(format!("unsafe namespace directory name: '{name}'")))?;
        self.object.delete_prefix(&prefix).await?;
        Ok(())
    }

    /// Write `body` directly at the content-addressed blob path, for small
    /// in-memory content such as manifest bodies. The digest fixes both path
    /// and bytes, and fresh bytes sit inside the collector's grace period, so
    /// no reclaim can race this.
    #[instrument(skip(self, body))]
    pub async fn put_blob(&self, digest: &Digest, body: Bytes) -> Result<(), Error> {
        self.object.put(&digest.blob_path(), body).await?;
        Ok(())
    }

    /// Generate a presigned download URL for `digest`, or `Ok(None)` when no
    /// presign backend was wired.
    #[instrument(skip(self))]
    pub async fn presigned_url(
        &self,
        digest: &Digest,
        content_type: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let Some((presign, ttl)) = &self.presign else {
            return Ok(None);
        };
        let path = digest.blob_path();
        let url = presign.presign_get(&path, *ttl, content_type).await?;
        Ok(Some(url))
    }
}

#[cfg(test)]
mod tests;
