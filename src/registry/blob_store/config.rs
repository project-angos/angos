use std::sync::Arc;

use serde::Deserialize;
use tracing::warn;

use crate::{
    cache::Cache,
    registry::{
        blob_store::{BlobStore, Error, MultipartCleanup, PresignedBlobStore, UploadStore, fs, s3},
        data_store,
    },
};

type BlobStoreTriple = (
    Arc<dyn BlobStore>,
    Arc<dyn UploadStore>,
    Option<Arc<dyn PresignedBlobStore>>,
);

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
    pub fn to_backend(&self, cache: Option<Arc<dyn Cache>>) -> Result<BlobStoreTriple, Error> {
        match self {
            BlobStorageConfig::FS(config) => {
                let backend = Arc::new(fs::Backend::new(config));
                Ok((backend.clone(), backend, None))
            }
            BlobStorageConfig::S3(config) => {
                let mut backend = s3::Backend::new(config)?;
                if let Some(cache) = cache {
                    backend = backend.with_cache(cache);
                }
                let backend = Arc::new(backend);
                Ok((
                    backend.clone(),
                    backend.clone(),
                    Some(backend as Arc<dyn PresignedBlobStore>),
                ))
            }
        }
    }

    pub fn to_multipart_cleanup(&self) -> Option<Arc<dyn MultipartCleanup + Send + Sync>> {
        match self {
            BlobStorageConfig::FS(_) => None,
            BlobStorageConfig::S3(config) => match s3::Backend::new(config) {
                Ok(backend) => Some(Arc::new(backend) as _),
                Err(e) => {
                    warn!("Failed to create S3 backend for multipart cleanup: {e}");
                    None
                }
            },
        }
    }
}
