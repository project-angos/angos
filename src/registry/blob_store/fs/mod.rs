#[cfg(test)]
pub mod tests;

use std::{
    fmt::{self, Debug, Formatter},
    io::SeekFrom,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sha2::{Digest as Sha256Digest, Sha256};
use tokio::io::{AsyncRead, AsyncSeekExt, copy};
use tracing::{debug, error, info, instrument};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            BlobStore, BoxedReader, Error, MultipartCleanup, OrphanMultipartUpload, UploadStore,
            UploadSummary, hashing_reader::HashingReader, sha256_ext::Sha256Ext,
        },
        pagination, path_builder,
    },
};
use angos_storage::{Error as StorageError, ObjectStore, fs::Backend as StorageFsBackend};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    #[serde(default)]
    pub sync_to_disk: bool,
}

#[derive(Clone)]
pub struct Backend {
    store: StorageFsBackend,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using filesystem blob-store backend");
        let store = StorageFsBackend::builder()
            .root_dir(&config.root_dir)
            .sync_to_disk(config.sync_to_disk)
            .build()?;
        Ok(Self { store })
    }

    async fn save_hasher(
        &self,
        name: &str,
        uuid: &str,
        offset: u64,
        state: &[u8],
    ) -> Result<(), Error> {
        let state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        self.store
            .put(&state_path, Bytes::copy_from_slice(state))
            .await?;
        Ok(())
    }

    async fn load_hasher(&self, name: &str, uuid: &str, offset: u64) -> Result<Sha256, Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        let state = self.store.get(&hash_state_path).await?;
        Sha256::from_state(&state)
    }

    async fn file_size_or_err(&self, path: &str, not_found: Error) -> Result<u64, Error> {
        match self.store.head(path).await {
            Ok(meta) => Ok(meta.size),
            Err(StorageError::NotFound) => Err(not_found),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl BlobStore for Backend {
    #[instrument(skip(self))]
    async fn list(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        let sha256_prefix = format!("{}/sha256", path_builder::blobs_root_dir());

        let (shards, _) = self.store.list_all_children(&sha256_prefix).await?;

        let mut digests = Vec::new();
        for shard in shards {
            let shard_path = format!("{sha256_prefix}/{shard}");
            let (digest_names, _) = self.store.list_all_children(&shard_path).await?;
            for digest_name in digest_names {
                digests.push(Digest::Sha256(digest_name.into()));
            }
        }

        Ok(pagination::paginate(
            &digests,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self, content))]
    async fn create(&self, content: &[u8]) -> Result<Digest, Error> {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let digest = hasher.digest();
        self.store
            .put(
                &path_builder::blob_path(&digest),
                Bytes::copy_from_slice(content),
            )
            .await?;
        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        match self.store.get(&path_builder::blob_path(digest)).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        self.file_size_or_err(&path_builder::blob_path(digest), Error::BlobNotFound)
            .await
    }

    #[instrument(skip(self))]
    async fn reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        match self
            .store
            .get_stream(&path_builder::blob_path(digest), start_offset)
            .await
        {
            Ok((reader, total)) => Ok((reader, total)),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn delete(&self, digest: &Digest) -> Result<(), Error> {
        let container = path_builder::blob_container_dir(digest);
        self.store.delete_prefix(&container).await?;
        // Tidy up the two-character shard parent (`<algo>/<prefix>`); stop at
        // the `blobs/<algo>` level which is shared across all blobs.
        self.store.prune_empty_ancestors(&container, 2).await;
        Ok(())
    }
}

#[async_trait]
impl UploadStore for Backend {
    #[instrument(skip(self))]
    async fn list(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let uploads_root = path_builder::uploads_root_dir(namespace);
        let page = self
            .store
            .list_children(&uploads_root, n, continuation_token, None)
            .await?;
        Ok((page.sub_prefixes, page.next_token))
    }

    #[instrument(skip(self))]
    async fn create(&self, name: &str, uuid: &str) -> Result<String, Error> {
        let content_path = path_builder::upload_path(name, uuid);
        self.store.put(&content_path, Bytes::new()).await?;

        let date_path = path_builder::upload_start_date_path(name, uuid);
        self.store
            .put(&date_path, Bytes::from(Utc::now().to_rfc3339()))
            .await?;

        let state = Sha256::new().serialized_state();
        self.save_hasher(name, uuid, 0, &state).await?;

        Ok(uuid.to_string())
    }

    #[instrument(skip(self, stream))]
    async fn write(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        _content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        // ObjectStore::put is always a full atomic replace; append-mode needs
        // a direct file open. open_for_write returns the pre-open file size so
        // we know where to seek the hasher without an extra head call.
        let upload_key = path_builder::upload_path(name, uuid);
        let (mut file, upload_size) = self
            .store
            .open_for_write(&upload_key, append)
            .await
            .map_err(|e| classify_open_error_from_storage(e, name, uuid))?;

        file.seek(SeekFrom::Start(upload_size)).await?;

        let hasher = self.load_hasher(name, uuid, upload_size).await?;
        let mut reader = HashingReader::with_hasher(stream, hasher);

        let written = copy(&mut reader, &mut file).await?;
        let total_size = upload_size + written;
        let digest = reader.digest();

        self.save_hasher(name, uuid, total_size, &reader.serialized_state())
            .await?;

        Ok((digest, total_size))
    }

    #[instrument(skip(self))]
    async fn summary(&self, name: &str, uuid: &str) -> Result<UploadSummary, Error> {
        let size = self
            .file_size_or_err(
                &path_builder::upload_path(name, uuid),
                Error::UploadNotFound,
            )
            .await?;

        let date_path = path_builder::upload_start_date_path(name, uuid);
        let date_bytes = self.store.get(&date_path).await.unwrap_or_default();
        let date_str = String::from_utf8_lossy(&date_bytes);
        let started_at = DateTime::parse_from_rfc3339(&date_str)
            .ok()
            .unwrap_or_default()
            .with_timezone(&Utc);

        Ok(UploadSummary { size, started_at })
    }

    #[instrument(skip(self))]
    async fn complete(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let size = self
            .file_size_or_err(
                &path_builder::upload_path(name, uuid),
                Error::UploadNotFound,
            )
            .await?;

        let digest = if let Some(digest) = digest {
            digest.clone()
        } else {
            self.load_hasher(name, uuid, size).await?.digest()
        };

        let upload_key = path_builder::upload_path(name, uuid);
        let blob_key = path_builder::blob_path(&digest);
        // rename is atomic and zero-copy; ObjectStore::copy would read + rewrite.
        self.store.rename(&upload_key, &blob_key).await?;

        let container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&container).await?;
        self.store.prune_empty_ancestors(&container, 2).await;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn delete(&self, name: &str, uuid: &str) -> Result<(), Error> {
        let container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&container).await?;
        self.store.prune_empty_ancestors(&container, 2).await;
        Ok(())
    }
}

#[async_trait]
impl MultipartCleanup for Backend {
    // FS uploads are plain files; there are no S3 multipart uploads to clean up.
    async fn list_orphan_multipart_uploads(
        &self,
        _timeout: Duration,
    ) -> Result<Vec<OrphanMultipartUpload>, Error> {
        Ok(Vec::new())
    }

    async fn abort_orphan_multipart_upload(
        &self,
        _upload: &OrphanMultipartUpload,
    ) -> Result<(), Error> {
        Ok(())
    }
}

fn classify_open_error_from_storage(error: StorageError, name: &str, uuid: &str) -> Error {
    match error {
        StorageError::NotFound => {
            debug!("Upload file not found for name={name}, uuid={uuid}");
            Error::UploadNotFound
        }
        other => {
            error!("Error opening upload file for name={name}, uuid={uuid}: {other}");
            other.into()
        }
    }
}
