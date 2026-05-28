use std::sync::Arc;

use serde::Deserialize;

use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{
    ConditionalStore, ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_tx_engine::{executor::build_executor, lock::LockStrategy};

use crate::registry::blob_store::{BlobStore, Error, PresignedBlobStore, UploadStore, fs, s3};

pub struct BlobStoreHandles {
    pub blob: Arc<dyn BlobStore>,
    pub upload: Arc<dyn UploadStore>,
    pub presigned: Option<Arc<dyn PresignedBlobStore>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum BlobStorageConfig {
    #[serde(rename = "fs")]
    FS(fs::BackendConfig),
    #[serde(rename = "s3")]
    S3(s3::BackendConfig),
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        BlobStorageConfig::FS(fs::BackendConfig::default())
    }
}

impl BlobStorageConfig {
    /// Build the backend handles.
    ///
    /// For the FS backend the transaction executor is constructed via
    /// `build_executor` with an in-process memory lock strategy. The backend
    /// receives a shared `Arc<dyn TransactionExecutor>` so all concurrent
    /// `complete` calls coordinate through the same lock.
    ///
    /// For S3, atomicity is provided by the S3 multipart API itself; no
    /// additional lock executor is needed.
    ///
    /// Stale upload sessions (including their S3 multipart uploads) are reaped
    /// by `scrub`'s `UploadChecker`, not by a background task.
    pub fn to_backend(&self) -> Result<BlobStoreHandles, Error> {
        match self {
            BlobStorageConfig::FS(config) => {
                let executor_storage: Arc<dyn ObjectStore> = Arc::new(
                    StorageFsBackend::builder()
                        .root_dir(&config.root_dir)
                        .sync_to_disk(config.sync_to_disk)
                        .build()
                        .map_err(|e| Error::StorageBackend(e.to_string()))?,
                );
                let executor = build_executor(
                    executor_storage,
                    None,
                    LockStrategy::Memory,
                    None,
                    false,
                    false,
                )
                .map_err(|e| Error::StorageBackend(e.to_string()))?;

                let backend = Arc::new(
                    fs::Backend::builder()
                        .root_dir(&config.root_dir)
                        .sync_to_disk(config.sync_to_disk)
                        .executor(executor)
                        .build()?,
                );

                Ok(BlobStoreHandles {
                    blob: backend.clone(),
                    upload: backend,
                    presigned: None,
                })
            }
            BlobStorageConfig::S3(config) => {
                let http = S3HttpBackend::new(&config.connection.to_client_config())
                    .map_err(|e| Error::StorageBackend(e.to_string()))?;
                let raw_storage = Arc::new(
                    StorageS3Backend::builder()
                        .client(Arc::new(http))
                        .build()
                        .map_err(|e| Error::StorageBackend(e.to_string()))?,
                );
                let executor_store: Arc<dyn ObjectStore> = raw_storage.clone();
                let conditional_store: Arc<dyn ConditionalStore> = raw_storage;
                let executor = build_executor(
                    executor_store,
                    Some(conditional_store),
                    LockStrategy::Memory,
                    None,
                    false,
                    false,
                )
                .map_err(|e| Error::StorageBackend(e.to_string()))?;

                let backend = Arc::new(
                    s3::Backend::builder()
                        .access_key_id(config.connection.access_key_id.clone())
                        .secret_key(config.connection.secret_key.clone())
                        .endpoint(&config.connection.endpoint)
                        .bucket(&config.connection.bucket)
                        .region(&config.connection.region)
                        .key_prefix(&config.connection.key_prefix)
                        .multipart_copy_threshold(config.transport.multipart_copy_threshold)
                        .multipart_copy_chunk_size(config.transport.multipart_copy_chunk_size)
                        .multipart_copy_jobs(config.transport.multipart_copy_jobs)
                        .multipart_part_size(config.transport.multipart_part_size)
                        .multipart_uniform_parts(config.transport.multipart_uniform_parts)
                        .operation_timeout_secs(config.transport.operation_timeout_secs)
                        .operation_attempt_timeout_secs(
                            config.transport.operation_attempt_timeout_secs,
                        )
                        .max_attempts(config.transport.max_attempts)
                        .executor(executor)
                        .build()?,
                );
                Ok(BlobStoreHandles {
                    blob: backend.clone(),
                    upload: backend.clone(),
                    presigned: Some(backend as Arc<dyn PresignedBlobStore>),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::registry::s3_connection::S3ConnectionConfig;
    use crate::secret::Secret;

    #[tokio::test]
    async fn fs_backend_handles_build() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlobStorageConfig::FS(fs::BackendConfig {
            root_dir: temp_dir.path().to_string_lossy().to_string(),
            sync_to_disk: false,
        });

        assert!(config.to_backend().is_ok());
    }

    #[test]
    fn s3_backend_handles_build() {
        let config = BlobStorageConfig::S3(s3::BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("minioadmin".to_string()),
                secret_key: Secret::new("minioadmin".to_string()),
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                key_prefix: String::new(),
            },
            ..s3::BackendConfig::default()
        });

        let handles = config.to_backend().unwrap();
        assert!(handles.presigned.is_some());
    }
}
