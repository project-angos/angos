use std::sync::Arc;

use serde::Deserialize;

use crate::{
    cache::Cache,
    registry::{
        blob_store::{BlobStore, Error, MultipartCleanup, PresignedBlobStore, UploadStore, fs, s3},
        data_store,
    },
};

pub struct BlobStoreHandles {
    pub blob_store: Arc<dyn BlobStore>,
    pub upload_store: Arc<dyn UploadStore>,
    pub presigned_store: Option<Arc<dyn PresignedBlobStore>>,
    pub multipart_cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum BlobStorageConfig {
    #[serde(rename = "fs")]
    FS(data_store::fs::BackendConfig),
    #[serde(rename = "s3")]
    S3(data_store::s3::BackendConfig),
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        BlobStorageConfig::FS(data_store::fs::BackendConfig::default())
    }
}

impl BlobStorageConfig {
    pub fn to_backend(&self, cache: Option<Arc<dyn Cache>>) -> Result<BlobStoreHandles, Error> {
        match self {
            BlobStorageConfig::FS(config) => {
                let backend = Arc::new(fs::Backend::new(config));
                Ok(BlobStoreHandles {
                    blob_store: backend.clone(),
                    upload_store: backend.clone(),
                    presigned_store: None,
                    multipart_cleanup: backend,
                })
            }
            BlobStorageConfig::S3(config) => {
                let mut backend = s3::Backend::new(config)?;
                if let Some(cache) = cache {
                    backend = backend.with_cache(cache);
                }
                let backend = Arc::new(backend);
                Ok(BlobStoreHandles {
                    blob_store: backend.clone(),
                    upload_store: backend.clone(),
                    presigned_store: Some(backend.clone() as Arc<dyn PresignedBlobStore>),
                    multipart_cleanup: backend,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use tempfile::TempDir;

    use super::*;
    use crate::{registry::data_store, secret::Secret};

    #[tokio::test]
    async fn fs_backend_provides_multipart_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlobStorageConfig::FS(data_store::fs::BackendConfig {
            root_dir: temp_dir.path().to_string_lossy().to_string(),
            sync_to_disk: false,
        });

        let handles = config.to_backend(None).unwrap();
        let orphans = handles
            .multipart_cleanup
            .list_orphan_multipart_uploads(Duration::hours(1))
            .await
            .unwrap();
        assert!(orphans.is_empty());
    }

    #[test]
    fn s3_backend_provides_multipart_cleanup() {
        let config = BlobStorageConfig::S3(data_store::s3::BackendConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Secret::new("minioadmin".to_string()),
            secret_key: Secret::new("minioadmin".to_string()),
            ..Default::default()
        });

        let handles = config.to_backend(None).unwrap();
        // Verifies the handle is present; no live S3 call needed.
        let _ = handles.multipart_cleanup;
    }
}
