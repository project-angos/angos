mod multipart_helpers;
mod nonuniform;
pub mod session;
#[cfg(test)]
pub mod tests;
mod uniform;

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytesize::ByteSize;
use tokio::io::AsyncRead;
use tracing::{debug, info, instrument};

use angos_tx_engine::{executor::TransactionExecutor, transaction::Mutation};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            BlobStore, BoxedReader, Error, PresignedBlobStore, UploadStore, UploadSummary,
        },
        pagination, path_builder,
        s3_connection::S3ConnectionConfig,
    },
};
use angos_s3_client::{Backend as S3HttpBackend, BackendConfig as S3TransportConfig};
use angos_storage::{
    Error as StorageError, ObjectStore, PresignedStore, s3::Backend as StorageS3Backend,
};
use serde::Deserialize;

use crate::secret::Secret;

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
/// `angos_s3_client::BackendConfig`, mirror it here and add a corresponding
/// builder method on [`BackendBuilder`].
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

#[derive(Clone)]
pub struct Backend {
    pub store: StorageS3Backend,
    pub multipart_part_size: u64,
    pub uniform_parts: bool,
    executor: Arc<dyn TransactionExecutor>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish_non_exhaustive()
    }
}

impl Backend {
    pub fn builder() -> BackendBuilder {
        BackendBuilder::default()
    }

    /// Return the transaction executor shared across all session operations.
    pub fn executor(&self) -> &Arc<dyn TransactionExecutor> {
        &self.executor
    }
}

#[derive(Default)]
pub struct BackendBuilder {
    access_key_id: Option<Secret<String>>,
    secret_key: Option<Secret<String>>,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    key_prefix: Option<String>,
    multipart_copy_threshold: Option<ByteSize>,
    multipart_copy_chunk_size: Option<ByteSize>,
    multipart_copy_jobs: Option<usize>,
    multipart_part_size: Option<ByteSize>,
    multipart_uniform_parts: Option<bool>,
    operation_timeout_secs: Option<u64>,
    operation_attempt_timeout_secs: Option<u64>,
    max_attempts: Option<u32>,
    executor: Option<Arc<dyn TransactionExecutor>>,
}

impl BackendBuilder {
    #[must_use]
    pub fn access_key_id(mut self, value: Secret<String>) -> Self {
        self.access_key_id = Some(value);
        self
    }

    #[must_use]
    pub fn secret_key(mut self, value: Secret<String>) -> Self {
        self.secret_key = Some(value);
        self
    }

    #[must_use]
    pub fn endpoint(mut self, value: impl Into<String>) -> Self {
        self.endpoint = Some(value.into());
        self
    }

    #[must_use]
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }

    #[must_use]
    pub fn region(mut self, value: impl Into<String>) -> Self {
        self.region = Some(value.into());
        self
    }

    #[must_use]
    pub fn key_prefix(mut self, value: impl Into<String>) -> Self {
        self.key_prefix = Some(value.into());
        self
    }

    #[must_use]
    pub fn multipart_copy_threshold(mut self, value: ByteSize) -> Self {
        self.multipart_copy_threshold = Some(value);
        self
    }

    #[must_use]
    pub fn multipart_copy_chunk_size(mut self, value: ByteSize) -> Self {
        self.multipart_copy_chunk_size = Some(value);
        self
    }

    #[must_use]
    pub fn multipart_copy_jobs(mut self, value: usize) -> Self {
        self.multipart_copy_jobs = Some(value);
        self
    }

    #[must_use]
    pub fn multipart_part_size(mut self, value: ByteSize) -> Self {
        self.multipart_part_size = Some(value);
        self
    }

    #[must_use]
    pub fn multipart_uniform_parts(mut self, value: bool) -> Self {
        self.multipart_uniform_parts = Some(value);
        self
    }

    #[must_use]
    pub fn operation_timeout_secs(mut self, value: u64) -> Self {
        self.operation_timeout_secs = Some(value);
        self
    }

    #[must_use]
    pub fn operation_attempt_timeout_secs(mut self, value: u64) -> Self {
        self.operation_attempt_timeout_secs = Some(value);
        self
    }

    #[must_use]
    pub fn max_attempts(mut self, value: u32) -> Self {
        self.max_attempts = Some(value);
        self
    }

    #[must_use]
    pub fn executor(mut self, executor: Arc<dyn TransactionExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        info!("Using S3 blob-store backend");
        let defaults = S3TransportConfig::default();

        let access_key_id = self.access_key_id.ok_or_else(|| {
            Error::StorageBackend("S3 blob-store builder requires access_key_id".into())
        })?;
        let secret_key = self.secret_key.ok_or_else(|| {
            Error::StorageBackend("S3 blob-store builder requires secret_key".into())
        })?;
        let endpoint = self.endpoint.ok_or_else(|| {
            Error::StorageBackend("S3 blob-store builder requires endpoint".into())
        })?;
        let bucket = self
            .bucket
            .ok_or_else(|| Error::StorageBackend("S3 blob-store builder requires bucket".into()))?;
        let region = self
            .region
            .ok_or_else(|| Error::StorageBackend("S3 blob-store builder requires region".into()))?;
        let key_prefix = self.key_prefix.unwrap_or_default();

        let multipart_part_size = self
            .multipart_part_size
            .unwrap_or(defaults.multipart_part_size);
        let multipart_uniform_parts = self
            .multipart_uniform_parts
            .unwrap_or(defaults.multipart_uniform_parts);

        let transport = S3TransportConfig {
            access_key_id: access_key_id.expose().clone(),
            secret_key: secret_key.expose().clone(),
            endpoint,
            bucket,
            region,
            key_prefix,
            multipart_copy_threshold: self
                .multipart_copy_threshold
                .unwrap_or(defaults.multipart_copy_threshold),
            multipart_copy_chunk_size: self
                .multipart_copy_chunk_size
                .unwrap_or(defaults.multipart_copy_chunk_size),
            multipart_copy_jobs: self
                .multipart_copy_jobs
                .unwrap_or(defaults.multipart_copy_jobs),
            multipart_part_size,
            multipart_uniform_parts,
            operation_timeout_secs: self
                .operation_timeout_secs
                .unwrap_or(defaults.operation_timeout_secs),
            operation_attempt_timeout_secs: self
                .operation_attempt_timeout_secs
                .unwrap_or(defaults.operation_attempt_timeout_secs),
            max_attempts: self.max_attempts.unwrap_or(defaults.max_attempts),
        };

        let executor = self.executor.ok_or_else(|| {
            Error::StorageBackend("S3 blob-store builder requires an executor".into())
        })?;

        let http =
            S3HttpBackend::new(&transport).map_err(|e| Error::StorageBackend(e.to_string()))?;
        let store = StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .map_err(|e| Error::StorageBackend(e.to_string()))?;

        Ok(Backend {
            store,
            multipart_part_size: multipart_part_size.as_u64(),
            uniform_parts: multipart_uniform_parts,
            executor,
        })
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
        self.list_via_session(namespace, n, continuation_token)
            .await
    }

    #[instrument(skip(self))]
    async fn create(&self, name: &str, uuid: &str) -> Result<String, Error> {
        self.create_via_session(name, uuid).await
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
        self.write_via_session(name, uuid, stream, content_length, append)
            .await
    }

    #[instrument(skip(self))]
    async fn summary(&self, name: &str, uuid: &str) -> Result<UploadSummary, Error> {
        self.summary_via_session(name, uuid).await
    }

    #[instrument(skip(self))]
    async fn complete(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        self.complete_via_session(name, uuid, digest).await
    }

    #[instrument(skip(self))]
    async fn finalize_mutations(
        &self,
        name: &str,
        uuid: &str,
        expected_digest: Option<&Digest>,
    ) -> Result<(Digest, Vec<Mutation>), Error> {
        self.finalize_mutations_via_session(name, uuid, expected_digest)
            .await
    }

    #[instrument(skip(self))]
    async fn delete(&self, name: &str, uuid: &str) -> Result<(), Error> {
        self.delete_via_session(name, uuid).await
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
