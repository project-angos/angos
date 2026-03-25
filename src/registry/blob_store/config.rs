use std::sync::Arc;

use serde::Deserialize;
use tracing::warn;

use crate::{
    cache::Cache,
    registry::{
        blob_store::{BlobStore, Error, MultipartCleanup, fs, s3},
        data_store,
    },
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
    pub fn to_backend(&self, cache: Option<Arc<dyn Cache>>) -> Result<Arc<dyn BlobStore>, Error> {
        match self {
            BlobStorageConfig::FS(config) => Ok(Arc::new(fs::Backend::new(config))),
            BlobStorageConfig::S3(config) => {
                let mut backend = s3::Backend::new(config)?;
                if let Some(cache) = cache {
                    backend = backend.with_cache(cache);
                }
                Ok(Arc::new(backend))
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
