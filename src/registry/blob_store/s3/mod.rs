mod cache;
mod channel_body;
mod chunked_reader;
mod cleanup;
mod nonuniform;
#[cfg(test)]
pub mod tests;
mod uniform;

use std::{
    fmt::{self, Debug, Formatter},
    io,
    sync::Arc,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest as ShaDigestTrait, Sha256};
use tokio::io::AsyncRead;
use tracing::{debug, info, instrument};

use crate::{
    cache::Cache,
    oci::Digest,
    registry::{
        blob_store::{
            BlobStore, BoxedReader, Error, PresignedBlobStore, UploadStore, UploadSummary,
            sha256_ext::Sha256Ext,
        },
        data_store, pagination, path_builder,
    },
};

pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const FRAME_SIZE: usize = 256 * 1024;

/// A single part that has been successfully uploaded as part of a multipart upload.
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadedPart {
    pub part_number: i32,
    pub e_tag: String,
    pub size: i64,
}

/// S3-internal upload state, cached between `write_upload` calls to avoid
/// redundant round-trips to the S3 API.
#[derive(Debug, Serialize, Deserialize)]
pub struct S3UploadState {
    pub size: u64,
    pub multipart_upload_id: Option<String>,
    pub parts: Vec<UploadedPart>,
    pub pending_size: u64,
}

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    multipart_part_size: usize,
    uniform_parts: bool,
    cache: Option<Arc<dyn Cache>>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &data_store::s3::BackendConfig) -> Result<Self, Error> {
        info!("Using S3 blob-store backend");
        #[allow(clippy::cast_possible_truncation)]
        let multipart_part_size = config.multipart_part_size.as_u64() as usize;
        let store = data_store::s3::Backend::new(config)?;

        Ok(Self {
            store,
            multipart_part_size,
            uniform_parts: config.multipart_uniform_parts,
            cache: None,
        })
    }

    pub fn with_cache(mut self, cache: Arc<dyn Cache>) -> Self {
        self.cache = Some(cache);
        self
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
        debug!("Fetching {n} blob(s) with continuation token: {continuation_token:?}");
        let algorithm = "sha256";
        let path = path_builder::blobs_root_dir();
        let blob_prefix = format!("{path}/{algorithm}/");

        let mut all_blobs = Vec::new();
        let mut list_continuation_token = None;

        loop {
            let (objects, next_token) = self
                .store
                .list_objects(&blob_prefix, 1000, list_continuation_token)
                .await?;

            for key in objects {
                if !key.ends_with("/data") {
                    continue;
                }

                let key_without_data = &key[..key.len() - 5];
                if let Some(slash_pos) = key_without_data.rfind('/') {
                    let digest = &key_without_data[slash_pos + 1..];
                    all_blobs.push(Digest::Sha256(digest.into()));
                }
            }

            list_continuation_token = next_token;
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

    #[instrument(skip(self, content))]
    async fn create(&self, content: &[u8]) -> Result<Digest, Error> {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let digest = hasher.digest();

        let blob_path = path_builder::blob_path(&digest);
        self.store
            .put_object(&blob_path, Bytes::copy_from_slice(content))
            .await?;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        let path = path_builder::blob_path(digest);
        Ok(self.store.get_object_body(&path, None).await?)
    }

    #[instrument(skip(self))]
    async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        let path = path_builder::blob_path(digest);
        match self.store.object_size(&path).await {
            Ok(size) => Ok(size),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let path = path_builder::blob_path(digest);
        let res = self
            .store
            .get_object(&path, start_offset)
            .await
            .map_err(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    Error::BlobNotFound
                } else {
                    e.into()
                }
            })?;

        let remaining: u64 = res
            .content_length
            .unwrap_or_default()
            .try_into()
            .unwrap_or(0);
        let total_size = remaining + start_offset.unwrap_or(0);

        Ok((Box::new(res.body.into_async_read()), total_size))
    }

    #[instrument(skip(self))]
    async fn delete(&self, digest: &Digest) -> Result<(), Error> {
        let path = path_builder::blob_container_dir(digest);
        self.store.delete_prefix(&path).await?;
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
        debug!(
            "Fetching {n} upload(s) for namespace '{namespace}' with continuation token: {continuation_token:?}"
        );
        let uploads_dir = path_builder::uploads_root_dir(namespace);

        let (prefixes, _, next_continuation_token) = self
            .store
            .list_prefixes(&uploads_dir, "/", i32::from(n), continuation_token, None)
            .await?;

        Ok((prefixes, next_continuation_token))
    }

    #[instrument(skip(self))]
    async fn create(&self, name: &str, uuid: &str) -> Result<String, Error> {
        let date_path = path_builder::upload_start_date_path(name, uuid);
        self.store
            .put_object(&date_path, Utc::now().to_rfc3339())
            .await?;

        let state = Sha256::new().serialized_state();
        self.save_hasher(name, uuid, 0, state).await?;

        Ok(uuid.to_string())
    }

    #[instrument(skip(self, stream))]
    async fn write(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        let result = if self.uniform_parts {
            self.write_upload_uniform(name, uuid, stream, append).await
        } else {
            self.write_upload_nonuniform(name, uuid, stream, content_length)
                .await
        };
        if result.is_ok() {
            self.evict_upload_state(name, uuid).await;
        }
        result
    }

    #[instrument(skip(self))]
    async fn summary(&self, name: &str, uuid: &str) -> Result<UploadSummary, Error> {
        let state = if self.uniform_parts {
            self.get_upload_state_uniform(name, uuid).await?
        } else {
            self.get_upload_state_nonuniform(name, uuid).await?
        };
        self.build_upload_summary(name, uuid, state.size).await
    }

    #[instrument(skip(self))]
    async fn complete(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        if self.uniform_parts {
            self.complete_upload_uniform(name, uuid, digest).await
        } else {
            self.complete_upload_nonuniform(name, uuid, digest).await
        }
    }

    #[instrument(skip(self))]
    async fn delete(&self, name: &str, uuid: &str) -> Result<(), Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        self.evict_upload_id(&upload_path).await;
        self.evict_upload_state(name, uuid).await;
        self.store.abort_pending_uploads(&upload_path).await?;

        let upload_path = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&upload_path).await?;
        Ok(())
    }
}

#[async_trait]
impl PresignedBlobStore for Backend {
    #[instrument(skip(self))]
    async fn url(
        &self,
        digest: &Digest,
        content_type: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let path = path_builder::blob_path(digest);
        let url = self
            .store
            .generate_presigned_url(&path, StdDuration::from_mins(30), content_type)
            .await?;
        Ok(Some(url))
    }
}
