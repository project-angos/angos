use super::Configuration;
use crate::registry::metadata_store::MetadataStoreConfig;

impl Configuration {
    pub fn resolve_metadata_config(&self) -> MetadataStoreConfig {
        self.metadata_store
            .clone()
            .unwrap_or_else(|| MetadataStoreConfig::from_blob_store(&self.blob_store))
    }
}
