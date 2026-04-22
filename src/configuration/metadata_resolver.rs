use tracing::info;

use super::Configuration;
use crate::registry::{blob_store, metadata_store};

impl Configuration {
    pub fn resolve_metadata_config(&self) -> metadata_store::MetadataStoreConfig {
        match &self.metadata_store {
            Some(config) => config.clone(),
            None => match &self.blob_store {
                blob_store::BlobStorageConfig::FS(config) => {
                    metadata_store::MetadataStoreConfig::FS(metadata_store::fs::BackendConfig {
                        root_dir: config.root_dir.clone(),
                        sync_to_disk: config.sync_to_disk,
                        ..Default::default()
                    })
                }
                blob_store::BlobStorageConfig::S3(config) => {
                    info!("Auto-configuring S3 metadata-store from blob-store");
                    metadata_store::MetadataStoreConfig::S3(metadata_store::s3::BackendConfig {
                        bucket: config.bucket.clone(),
                        region: config.region.clone(),
                        endpoint: config.endpoint.clone(),
                        access_key_id: config.access_key_id.clone(),
                        secret_key: config.secret_key.clone(),
                        key_prefix: config.key_prefix.clone(),
                        ..Default::default()
                    })
                }
            },
        }
    }
}
