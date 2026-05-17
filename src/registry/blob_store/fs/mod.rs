#[cfg(test)]
pub mod tests;

use std::{
    fmt::{self, Debug, Formatter},
    io::{self, ErrorKind, SeekFrom},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sha2::{Digest as Sha256Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncRead, AsyncSeekExt, copy},
};
use tracing::{error, info, instrument};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            BlobStore, BoxedReader, Error, MultipartCleanup, OrphanMultipartUpload, UploadStore,
            UploadSummary, hashing_reader::HashingReader, sha256_ext::Sha256Ext,
        },
        fs_ops::{
            atomic_write, ensure_parent_dir, list_dir_or_empty, prune_empty_ancestors,
            remove_dir_all_if_exists,
        },
        pagination, path_builder,
    },
};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    #[serde(default)]
    pub sync_to_disk: bool,
}

#[derive(Clone)]
pub struct Backend {
    root: PathBuf,
    sync_to_disk: bool,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Self {
        info!("Using filesystem blob-store backend");
        Self {
            root: PathBuf::from(&config.root_dir),
            sync_to_disk: config.sync_to_disk,
        }
    }

    fn full_path(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }

    async fn save_hasher(
        &self,
        name: &str,
        uuid: &str,
        offset: u64,
        state: &[u8],
    ) -> Result<(), Error> {
        let state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        atomic_write(&self.full_path(&state_path), state, self.sync_to_disk).await?;
        Ok(())
    }

    async fn load_hasher(&self, name: &str, uuid: &str, offset: u64) -> Result<Sha256, Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        let state = fs::read(self.full_path(&hash_state_path)).await?;
        Sha256::from_state(&state)
    }

    async fn file_size_or_err(&self, path: &str, not_found: Error) -> Result<u64, Error> {
        match fs::metadata(self.full_path(path)).await {
            Ok(meta) => Ok(meta.len()),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(not_found),
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
        let blobs_dir = self
            .full_path(path_builder::blobs_root_dir())
            .join("sha256");
        let all_prefixes = list_dir_or_empty(&blobs_dir).await?;
        let mut digests = Vec::new();
        for prefix in all_prefixes {
            let prefix_dir = blobs_dir.join(&prefix);
            for digest in list_dir_or_empty(&prefix_dir).await? {
                digests.push(Digest::Sha256(digest.into()));
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
        atomic_write(
            &self.full_path(&path_builder::blob_path(&digest)),
            content,
            self.sync_to_disk,
        )
        .await?;
        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        Ok(fs::read(self.full_path(&path_builder::blob_path(digest))).await?)
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
        let path = self.full_path(&path_builder::blob_path(digest));
        let mut file = match fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Err(Error::BlobNotFound),
            Err(e) => return Err(e.into()),
        };
        let size = file.metadata().await?.len();
        if let Some(offset) = start_offset {
            file.seek(SeekFrom::Start(offset)).await?;
        }
        Ok((Box::new(file), size))
    }

    #[instrument(skip(self))]
    async fn delete(&self, digest: &Digest) -> Result<(), Error> {
        let path = self.full_path(&path_builder::blob_container_dir(digest));
        remove_dir_all_if_exists(&path).await?;
        // Tidy up the two-character shard parent (`<algo>/<prefix>`); stop at
        // the `blobs/<algo>` level which is shared across all blobs.
        let _ = prune_empty_ancestors(&path, &self.root, 2).await;
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
        let path = self.full_path(&path_builder::uploads_root_dir(namespace));
        let uploads = list_dir_or_empty(&path).await?;
        Ok(pagination::paginate(
            &uploads,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self))]
    async fn create(&self, name: &str, uuid: &str) -> Result<String, Error> {
        let content_path = self.full_path(&path_builder::upload_path(name, uuid));
        atomic_write(&content_path, &[], self.sync_to_disk).await?;

        let date_path = self.full_path(&path_builder::upload_start_date_path(name, uuid));
        atomic_write(
            &date_path,
            Utc::now().to_rfc3339().as_bytes(),
            self.sync_to_disk,
        )
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
        let upload_size = if append {
            self.file_size_or_err(
                &path_builder::upload_path(name, uuid),
                Error::UploadNotFound,
            )
            .await?
        } else {
            0
        };

        let file_path = self.full_path(&path_builder::upload_path(name, uuid));
        ensure_parent_dir(&file_path).await?;
        let mut file = if append {
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await
        } else {
            fs::File::create(&file_path).await
        }
        .map_err(|e| classify_open_error(e, &file_path))?;
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

        let date_path = self.full_path(&path_builder::upload_start_date_path(name, uuid));
        let date_str = fs::read_to_string(&date_path).await.unwrap_or_default();
        let started_at = DateTime::parse_from_rfc3339(&date_str)
            .ok()
            .unwrap_or_default() // Fallbacks to epoch
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

        let upload_path = self.full_path(&path_builder::upload_path(name, uuid));
        let blob_path = self.full_path(&path_builder::blob_path(&digest));
        ensure_parent_dir(&blob_path).await?;
        fs::rename(upload_path, blob_path).await?;

        let container = self.full_path(&path_builder::upload_container_path(name, uuid));
        remove_dir_all_if_exists(&container).await?;
        let _ = prune_empty_ancestors(&container, &self.root, 2).await;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn delete(&self, name: &str, uuid: &str) -> Result<(), Error> {
        let container = self.full_path(&path_builder::upload_container_path(name, uuid));
        remove_dir_all_if_exists(&container).await?;
        let _ = prune_empty_ancestors(&container, &self.root, 2).await;
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

fn classify_open_error(error: io::Error, path: &Path) -> Error {
    error!("Error opening upload file {}: {error}", path.display());
    match error.kind() {
        ErrorKind::NotFound => Error::UploadNotFound,
        _ => error.into(),
    }
}
