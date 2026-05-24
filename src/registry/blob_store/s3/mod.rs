mod cache;
mod cleanup;
mod multipart_helpers;
mod nonuniform;
#[cfg(test)]
pub mod tests;
mod uniform;

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
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
        pagination, path_builder,
        s3_connection::S3ConnectionConfig,
    },
};
use angos_s3_client::{self as s3_client, BackendConfig as S3TransportConfig};
use angos_storage::{
    Error as StorageError, MultipartStore, ObjectStore, Part, PresignedStore,
    s3::Backend as StorageS3Backend,
};

pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const FRAME_SIZE: usize = 1024 * 1024;
pub const FRAME_BUFFER_CAPACITY: usize = 8;

/// Configuration for the S3 blob-store backend.
///
/// Connection fields (`access_key_id`, `secret_key`, `endpoint`, `bucket`,
/// `region`, `key_prefix`) are embedded via `#[serde(flatten)]` so the
/// operator TOML shape under `[blob_store.s3]` is flat. Connection fields
/// are required (matching the documented schema); only `key_prefix` may be
/// omitted. Transport fields each default through `TransportFields`'
/// struct-level `#[serde(default)]`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct BackendConfig {
    #[serde(flatten)]
    pub connection: S3ConnectionConfig,
    #[serde(flatten)]
    pub transport: TransportFields,
}

/// Blob-store-specific transport knobs.
///
/// Mirrors the eight non-connection fields of [`S3TransportConfig`]. The
/// duplication is intentional: the upstream struct mixes connection and
/// transport in a single record, and the six credential/endpoint fields
/// there are plain `String` (no debug redaction). Pulling out just the
/// transport knobs lets the blob-store config use `Secret`-wrapped
/// credentials via [`S3ConnectionConfig`] while still exposing the same
/// flat TOML keys to operators. When a new transport knob is added in
/// `angos_s3_client::BackendConfig`, mirror it here and in
/// [`BackendConfig::to_transport_config`].
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(default)]
pub struct TransportFields {
    pub multipart_copy_threshold: ByteSize,
    pub multipart_copy_chunk_size: ByteSize,
    pub multipart_copy_jobs: usize,
    pub multipart_part_size: ByteSize,
    pub multipart_uniform_parts: bool,
    pub operation_timeout_secs: u64,
    pub operation_attempt_timeout_secs: u64,
    pub max_attempts: u32,
}

impl Default for TransportFields {
    fn default() -> Self {
        let t = S3TransportConfig::default();
        Self {
            multipart_copy_threshold: t.multipart_copy_threshold,
            multipart_copy_chunk_size: t.multipart_copy_chunk_size,
            multipart_copy_jobs: t.multipart_copy_jobs,
            multipart_part_size: t.multipart_part_size,
            multipart_uniform_parts: t.multipart_uniform_parts,
            operation_timeout_secs: t.operation_timeout_secs,
            operation_attempt_timeout_secs: t.operation_attempt_timeout_secs,
            max_attempts: t.max_attempts,
        }
    }
}

impl BackendConfig {
    /// Build the transport-level config by merging connection fields with the
    /// blob-store-specific multipart and retry settings.
    pub fn to_transport_config(&self) -> S3TransportConfig {
        let t = &self.transport;
        S3TransportConfig {
            multipart_copy_threshold: t.multipart_copy_threshold,
            multipart_copy_chunk_size: t.multipart_copy_chunk_size,
            multipart_copy_jobs: t.multipart_copy_jobs,
            multipart_part_size: t.multipart_part_size,
            multipart_uniform_parts: t.multipart_uniform_parts,
            operation_timeout_secs: t.operation_timeout_secs,
            operation_attempt_timeout_secs: t.operation_attempt_timeout_secs,
            max_attempts: t.max_attempts,
            ..self.connection.to_client_config()
        }
    }
}

/// S3-internal upload state, cached between `write_upload` calls to avoid
/// redundant round-trips to the S3 API.
#[derive(Debug, Serialize, Deserialize)]
pub struct S3UploadState {
    pub size: u64,
    pub multipart_upload_id: Option<String>,
    pub parts: Vec<Part>,
    pub pending_size: u64,
}

#[derive(Clone)]
pub struct Backend {
    pub store: StorageS3Backend,
    multipart_part_size: u64,
    uniform_parts: bool,
    cache: Option<Arc<Cache>>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using S3 blob-store backend");
        let multipart_part_size = config.transport.multipart_part_size.as_u64();
        let transport = config.to_transport_config();
        let http = s3_client::Backend::new(&transport)?;
        let store = StorageS3Backend::builder().client(Arc::new(http)).build()?;

        Ok(Self {
            store,
            multipart_part_size,
            uniform_parts: config.transport.multipart_uniform_parts,
            cache: None,
        })
    }

    pub fn with_cache(mut self, cache: Arc<Cache>) -> Self {
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
            let page = self
                .store
                .list(&blob_prefix, 1000, list_continuation_token)
                .await?;

            for key in page.items {
                if !key.ends_with("/data") {
                    continue;
                }

                let key_without_data = &key[..key.len() - 5];
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

    #[instrument(skip(self, content))]
    async fn create(&self, content: &[u8]) -> Result<Digest, Error> {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let digest = hasher.digest();

        let blob_path = path_builder::blob_path(&digest);
        self.store
            .put(&blob_path, Bytes::copy_from_slice(content))
            .await?;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        let path = path_builder::blob_path(digest);
        Ok(self.store.get(&path).await?)
    }

    #[instrument(skip(self))]
    async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        let path = path_builder::blob_path(digest);
        match self.store.head(&path).await {
            Ok(meta) => Ok(meta.size),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
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
        match self.store.get_stream(&path, start_offset).await {
            Ok((body, total)) => Ok((body, total)),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
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

        let page = self
            .store
            .list_children(&uploads_dir, n, continuation_token, None)
            .await?;

        Ok((page.sub_prefixes, page.next_token))
    }

    #[instrument(skip(self))]
    async fn create(&self, name: &str, uuid: &str) -> Result<String, Error> {
        let date_path = path_builder::upload_start_date_path(name, uuid);
        self.store
            .put(&date_path, Bytes::from(Utc::now().to_rfc3339()))
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
            self.write_upload_uniform(name, uuid, stream, content_length, append)
                .await
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

        let upload_container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&upload_container).await?;
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
            .presign_get(&path, Duration::from_mins(30), content_type)
            .await?;
        Ok(Some(url))
    }
}
