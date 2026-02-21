use std::sync::Arc;

use serde::Deserialize;
use tracing::warn;

use crate::registry::{
    blob_store::{BlobStore, Error, MultipartCleanup, fs, s3},
    data_store,
};

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
    pub fn to_backend(&self) -> Result<Arc<dyn BlobStore>, Error> {
        match self {
            BlobStorageConfig::FS(config) => Ok(Arc::new(fs::Backend::new(config))),
            BlobStorageConfig::S3(config) => Ok(Arc::new(s3::Backend::new(config)?)),
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
