use std::sync::Arc;

use serde::Deserialize;

use crate::{
    cache::Cache,
    registry::{
        metadata_store,
        metadata_store::{Error, MetadataStore},
    },
};

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataStoreConfig {
    #[serde(rename = "fs")]
    FS(metadata_store::fs::BackendConfig),
    #[serde(rename = "s3")]
    S3(metadata_store::s3::BackendConfig),
}

impl MetadataStoreConfig {
    pub fn to_backend(
        &self,
        cache: Option<Arc<dyn Cache>>,
    ) -> Result<Arc<dyn MetadataStore + Send + Sync>, Error> {
        match self {
            MetadataStoreConfig::FS(config) => {
                Ok(Arc::new(metadata_store::fs::Backend::new(config)?))
            }
            MetadataStoreConfig::S3(config) => {
                let backend = metadata_store::s3::Backend::new(config)?;
                let backend = match cache {
                    Some(c) => backend.with_cache(c),
                    None => backend,
                };
                Ok(Arc::new(backend))
            }
        }
    }
}
