#[cfg(test)]
pub mod tests;

use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::{ErrorKind, SeekFrom};
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sha2::{Digest as Sha256Digest, Sha256};
use tokio::io::{AsyncRead, AsyncSeekExt};
use tracing::{error, info, instrument};

use crate::oci::Digest;
use crate::registry::blob_store::hashing_reader::HashingReader;
use crate::registry::blob_store::sha256_ext::Sha256Ext;
use crate::registry::blob_store::{BlobStore, BoxedReader, Error};
use crate::registry::{data_store, pagination, path_builder};

#[derive(Clone)]
pub struct Backend {
    store: data_store::fs::Backend,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &data_store::fs::BackendConfig) -> Self {
        info!("Using filesystem blob-store backend");
        Self {
            store: data_store::fs::Backend::new(config),
        }
    }

    async fn save_hasher(
        &self,
        name: &str,
        uuid: &str,
        offset: u64,
        state: &[u8],
    ) -> Result<(), Error> {
        let state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        self.store.write(&state_path, state).await?;
        Ok(())
    }

    async fn load_hasher(&self, name: &str, uuid: &str, offset: u64) -> Result<Sha256, Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        let state = self.store.read(&hash_state_path).await?;
        Sha256::from_state(&state)
    }

    async fn file_size_or_err(&self, path: &str, not_found: Error) -> Result<u64, Error> {
        match self.store.file_size(path).await {
            Ok(size) => Ok(size),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(not_found),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl BlobStore for Backend {
    #[instrument(skip(self))]
    async fn list_blobs(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        let path = PathBuf::new()
            .join(path_builder::blobs_root_dir())
            .join("sha256")
            .to_string_lossy()
            .to_string();

        let all_prefixes = self.store.list_dir(&path).await?;

        let mut digests = Vec::new();

        for prefix in all_prefixes {
            let blob_path = PathBuf::from(&path)
                .join(&prefix)
                .to_string_lossy()
                .to_string();

            let all_digests = self.store.list_dir(&blob_path).await?;

            for digest in all_digests {
                digests.push(Digest::Sha256(digest.into()));
            }
        }

        Ok(pagination::paginate(
            &digests,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self))]
    async fn list_uploads(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let path = path_builder::uploads_root_dir(namespace);
        let uploads = self.store.list_dir(&path).await?;

        Ok(pagination::paginate(
            &uploads,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self))]
    async fn create_upload(&self, name: &str, uuid: &str) -> Result<String, Error> {
        let content_path = path_builder::upload_path(name, uuid);
        self.store.write(&content_path, &[]).await?;

        let date_path = path_builder::upload_start_date_path(name, uuid);
        self.store
            .write(&date_path, Utc::now().to_rfc3339().as_bytes())
            .await?;

        let state = Sha256::new().serialized_state();
        self.save_hasher(name, uuid, 0, &state).await?;

        Ok(uuid.to_string())
    }

    #[instrument(skip(self, stream))]
    async fn write_upload(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        let upload_size = if append {
            let path = path_builder::upload_path(name, uuid);
            self.file_size_or_err(&path, Error::UploadNotFound).await?
        } else {
            0
        };

        let file_path = path_builder::upload_path(name, uuid);
        let mut file = if append {
            self.store.open_file_append(&file_path).await
        } else {
            self.store.create_file(&file_path).await
        }
        .map_err(|error| {
            error!("Error opening upload file {file_path}: {error}");
            match error.kind() {
                ErrorKind::NotFound => Error::UploadNotFound,
                _ => error.into(),
            }
        })?;
        file.seek(SeekFrom::Start(upload_size)).await?;

        let hasher = self.load_hasher(name, uuid, upload_size).await?;
        let mut reader = HashingReader::with_hasher(stream, hasher);

        let written = tokio::io::copy(&mut reader, &mut file).await?;
        let total_size = upload_size + written;
        let digest = reader.digest();

        self.save_hasher(name, uuid, total_size, &reader.serialized_state())
            .await?;

        Ok((digest, total_size))
    }

    #[instrument(skip(self))]
    async fn get_upload_size(&self, name: &str, uuid: &str) -> Result<u64, Error> {
        let path = path_builder::upload_path(name, uuid);
        self.file_size_or_err(&path, Error::UploadNotFound).await
    }

    #[instrument(skip(self))]
    async fn read_upload_summary(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<(Digest, u64, DateTime<Utc>), Error> {
        let path = path_builder::upload_path(name, uuid);
        let size = self.file_size_or_err(&path, Error::UploadNotFound).await?;
        let digest = self.load_hasher(name, uuid, size).await?.digest();

        let date = path_builder::upload_start_date_path(name, uuid);
        let date_str = self.store.read_to_string(&date).await.unwrap_or_default();
        let start_date = DateTime::parse_from_rfc3339(&date_str)
            .ok()
            .unwrap_or_default() // Fallbacks to epoch
            .with_timezone(&Utc);

        Ok((digest, size, start_date))
    }

    #[instrument(skip(self))]
    async fn complete_upload(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let path = path_builder::upload_path(name, uuid);
        let size = self.file_size_or_err(&path, Error::UploadNotFound).await?;

        let digest = if let Some(digest) = digest {
            digest.clone()
        } else {
            self.load_hasher(name, uuid, size).await?.digest()
        };

        let upload_path = path_builder::upload_path(name, uuid);
        let blob_path = path_builder::blob_path(&digest);
        self.store.rename(&upload_path, &blob_path).await?;

        let path = path_builder::upload_container_path(name, uuid);
        self.store.delete_dir(&path).await?;
        let _ = self.store.delete_empty_parent_dirs(&path).await;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn delete_upload(&self, name: &str, uuid: &str) -> Result<(), Error> {
        let path = path_builder::upload_container_path(name, uuid);
        self.store.delete_dir(&path).await?;
        self.store.delete_empty_parent_dirs(&path).await?;
        Ok(())
    }

    #[instrument(skip(self, content))]
    async fn create_blob(&self, content: &[u8]) -> Result<Digest, Error> {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let digest = hasher.digest();

        let blob_path = path_builder::blob_path(&digest);
        self.store.write(&blob_path, content).await?;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn read_blob(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        let path = path_builder::blob_path(digest);
        Ok(self.store.read(&path).await?)
    }

    #[instrument(skip(self))]
    async fn get_blob_size(&self, digest: &Digest) -> Result<u64, Error> {
        let path = path_builder::blob_path(digest);
        Ok(self.file_size_or_err(&path, Error::BlobNotFound).await?)
    }

    #[instrument(skip(self))]
    async fn build_blob_reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let path = path_builder::blob_path(digest);
        let mut file = match self.store.open_file(&path).await {
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
    async fn get_blob_url(&self, _digest: &Digest) -> Result<Option<String>, Error> {
        Ok(None)
    }

    #[instrument(skip(self))]
    async fn delete_blob(&self, digest: &Digest) -> Result<(), Error> {
        let path = path_builder::blob_container_dir(digest);
        self.store.delete_dir(&path).await?;
        let _ = self.store.delete_empty_parent_dirs(&path).await;
        Ok(())
    }
}
