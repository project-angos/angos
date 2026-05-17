use std::sync::Arc;

use serde::Deserialize;
use tracing::info;

use crate::{
    cache::Cache,
    registry::{
        blob_store, metadata_store,
        metadata_store::{ConditionalCapabilities, Error, LockStrategy, MetadataStore},
    },
    s3_client,
};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataStoreConfig {
    /// Inherit blob-store credentials and root path.
    ///
    /// Resolved via [`Configuration::resolve_metadata_config`] before reaching
    /// [`MetadataStoreConfig::to_backend`] or [`MetadataStoreConfig::probe`].
    /// Reaching either method with this variant is a programming error.
    #[default]
    Inherit,
    #[serde(rename = "fs")]
    FS(metadata_store::fs::BackendConfig),
    #[serde(rename = "s3")]
    S3(metadata_store::s3::BackendConfig),
}

impl MetadataStoreConfig {
    pub fn from_blob_store(blob: &blob_store::BlobStorageConfig) -> Self {
        match blob {
            blob_store::BlobStorageConfig::FS(config) => {
                MetadataStoreConfig::FS(metadata_store::fs::BackendConfig {
                    root_dir: config.root_dir.clone(),
                    sync_to_disk: config.sync_to_disk,
                    ..Default::default()
                })
            }
            blob_store::BlobStorageConfig::S3(config) => {
                info!("Auto-configuring S3 metadata-store from blob-store");
                MetadataStoreConfig::S3(metadata_store::s3::BackendConfig {
                    bucket: config.bucket.clone(),
                    region: config.region.clone(),
                    endpoint: config.endpoint.clone(),
                    access_key_id: config.access_key_id.clone(),
                    secret_key: config.secret_key.clone(),
                    key_prefix: config.key_prefix.clone(),
                    ..Default::default()
                })
            }
        }
    }

    pub async fn probe(&self) -> Result<Option<ConditionalCapabilities>, Error> {
        match self {
            MetadataStoreConfig::Inherit => unreachable!(
                "MetadataStoreConfig::Inherit must be resolved via \
                 Configuration::resolve_metadata_config before probe"
            ),
            MetadataStoreConfig::S3(config) => {
                let store = s3_client::Backend::new(&config.to_data_store_config())
                    .map_err(|e| Error::StorageBackend(e.to_string()))?;
                let caps = metadata_store::s3::probe_conditional_capabilities(&store).await?;
                if matches!(config.lock_strategy, LockStrategy::S3(_)) && !caps.supports_cas() {
                    return Err(Error::Lock(format!(
                        "S3 lock strategy requires If-None-Match and If-Match support, \
                         but probe found: If-None-Match={}, If-Match={}. \
                         Use lock_strategy = redis or lock_strategy = memory instead.",
                        caps.put_if_none_match, caps.put_if_match
                    )));
                }
                Ok(Some(caps))
            }
            MetadataStoreConfig::FS(_) => Ok(None),
        }
    }

    pub async fn to_backend(
        &self,
        cache: Option<Arc<Cache>>,
    ) -> Result<
        (
            Arc<dyn MetadataStore + Send + Sync>,
            Option<ConditionalCapabilities>,
        ),
        Error,
    > {
        match self {
            MetadataStoreConfig::Inherit => unreachable!(
                "MetadataStoreConfig::Inherit must be resolved via \
                 Configuration::resolve_metadata_config before to_backend"
            ),
            MetadataStoreConfig::FS(config) => {
                Ok((Arc::new(metadata_store::fs::Backend::new(config)?), None))
            }
            MetadataStoreConfig::S3(config) => {
                let caps = match &config.capabilities {
                    Some(declared) => {
                        if matches!(config.lock_strategy, LockStrategy::S3(_))
                            && !declared.supports_cas()
                        {
                            return Err(Error::Lock(format!(
                                "S3 lock strategy requires If-None-Match and If-Match support, \
                                 but config declares: put_if_none_match={}, put_if_match={}. \
                                 Use lock_strategy = redis or lock_strategy = memory instead.",
                                declared.put_if_none_match, declared.put_if_match
                            )));
                        }
                        Some(declared.clone())
                    }
                    None => self.probe().await?,
                };
                let backend = metadata_store::s3::Backend::new(config, caps.clone())?;
                let backend = match cache {
                    Some(c) => backend.with_cache(c),
                    None => backend,
                };
                Ok((Arc::new(backend), caps))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::Secret;

    fn s3_config_with_lock_strategy(lock_strategy: LockStrategy) -> MetadataStoreConfig {
        MetadataStoreConfig::S3(metadata_store::s3::BackendConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "registry".to_string(),
            region: "us-east-1".to_string(),
            key_prefix: format!("probe-test-{}", uuid::Uuid::new_v4()),
            lock_strategy,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            capabilities: None,
        })
    }

    #[tokio::test]
    async fn test_probe_s3_lock_strategy_detects_minio_capabilities() {
        use crate::registry::metadata_store::lock::s3::S3LockConfig;

        let config = s3_config_with_lock_strategy(LockStrategy::S3(S3LockConfig::default()));
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should succeed against MinIO with S3 lock strategy: {result:?}"
        );
        let caps = result
            .unwrap()
            .expect("S3 lock strategy should return capabilities");
        assert!(caps.put_if_none_match, "MinIO should support If-None-Match");
        assert!(caps.put_if_match, "MinIO should support If-Match");
    }

    #[tokio::test]
    async fn test_probe_memory_lock_strategy_detects_capabilities() {
        let config = s3_config_with_lock_strategy(LockStrategy::Memory);
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should succeed for Memory lock strategy: {result:?}"
        );
        let caps = result
            .unwrap()
            .expect("S3 metadata store should return detected capabilities");
        assert!(caps.put_if_none_match, "MinIO should support If-None-Match");
        assert!(caps.put_if_match, "MinIO should support If-Match");
    }

    #[tokio::test]
    async fn test_probe_fs_config_is_noop() {
        let config = MetadataStoreConfig::FS(metadata_store::fs::BackendConfig {
            root_dir: "/tmp/probe-test".to_string(),
            lock_strategy: LockStrategy::Memory,
            sync_to_disk: false,
        });
        let result = config.probe().await;
        assert!(result.is_ok(), "Probe should be no-op for FS config");
        assert!(
            result.unwrap().is_none(),
            "FS config should return no capabilities"
        );
    }

    #[test]
    fn test_from_blob_store_fs_copies_paths_and_sync() {
        let blob = blob_store::BlobStorageConfig::FS(blob_store::fs::BackendConfig {
            root_dir: "/var/lib/registry".to_string(),
            sync_to_disk: true,
        });
        match MetadataStoreConfig::from_blob_store(&blob) {
            MetadataStoreConfig::FS(c) => {
                assert_eq!(c.root_dir, "/var/lib/registry");
                assert!(c.sync_to_disk);
            }
            MetadataStoreConfig::Inherit | MetadataStoreConfig::S3(_) => {
                panic!("expected FS metadata config")
            }
        }
    }

    #[test]
    fn test_from_blob_store_s3_copies_credentials_and_bucket() {
        let blob = blob_store::BlobStorageConfig::S3(s3_client::BackendConfig {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: Secret::new("key".to_string()),
            secret_key: Secret::new("secret".to_string()),
            key_prefix: "foo".to_string(),
            ..Default::default()
        });
        match MetadataStoreConfig::from_blob_store(&blob) {
            MetadataStoreConfig::S3(c) => {
                assert_eq!(c.bucket, "test-bucket");
                assert_eq!(c.region, "us-east-1");
                assert_eq!(c.endpoint, "http://localhost:9000");
                assert_eq!(c.access_key_id.expose(), "key");
                assert_eq!(c.secret_key.expose(), "secret");
                assert_eq!(c.key_prefix, "foo");
            }
            MetadataStoreConfig::Inherit | MetadataStoreConfig::FS(_) => {
                panic!("expected S3 metadata config")
            }
        }
    }
}
