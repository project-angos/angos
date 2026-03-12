use std::sync::Arc;

use serde::Deserialize;

use crate::{
    cache::Cache,
    registry::{
        data_store, metadata_store,
        metadata_store::{ConditionalCapabilities, Error, LockStrategy, MetadataStore},
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
    pub async fn probe(&self) -> Result<Option<ConditionalCapabilities>, Error> {
        match self {
            MetadataStoreConfig::S3(config) => match &config.lock_strategy {
                LockStrategy::S3(lock_config) => {
                    let store =
                        data_store::s3::Backend::new(&config.to_lock_store_config(lock_config))
                            .map_err(|e| Error::StorageBackend(e.to_string()))?;
                    let caps =
                        metadata_store::s3::Backend::probe_conditional_capabilities(&store).await?;
                    if !caps.supports_cas() {
                        return Err(Error::Lock(format!(
                            "S3 lock strategy requires If-None-Match and If-Match support, \
                             but probe found: If-None-Match={}, If-Match={}. \
                             Use lock_strategy = redis or lock_strategy = memory instead.",
                            caps.put_if_none_match, caps.put_if_match
                        )));
                    }
                    Ok(Some(caps))
                }
                _ => Ok(None),
            },
            MetadataStoreConfig::FS(_) => Ok(None),
        }
    }

    pub async fn to_backend(
        &self,
        cache: Option<Arc<dyn Cache>>,
    ) -> Result<Arc<dyn MetadataStore + Send + Sync>, Error> {
        match self {
            MetadataStoreConfig::FS(config) => {
                Ok(Arc::new(metadata_store::fs::Backend::new(config)?))
            }
            MetadataStoreConfig::S3(config) => {
                let conditional = match &config.capabilities {
                    Some(caps) => {
                        if matches!(config.lock_strategy, LockStrategy::S3(_))
                            && !caps.supports_cas()
                        {
                            return Err(Error::Lock(format!(
                                "S3 lock strategy requires If-None-Match and If-Match support, \
                                 but config declares: put_if_none_match={}, put_if_match={}. \
                                 Use lock_strategy = redis or lock_strategy = memory instead.",
                                caps.put_if_none_match, caps.put_if_match
                            )));
                        }
                        Some(caps.clone())
                    }
                    None => self.probe().await?,
                };
                let backend = metadata_store::s3::Backend::new(config, conditional)?;
                let backend = match cache {
                    Some(c) => backend.with_cache(c),
                    None => backend,
                };
                Ok(Arc::new(backend))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::metadata_store::lock::s3::S3LockConfig;

    fn s3_config_with_s3_lock() -> MetadataStoreConfig {
        MetadataStoreConfig::S3(metadata_store::s3::BackendConfig {
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "registry".to_string(),
            region: "us-east-1".to_string(),
            key_prefix: format!("probe-test-{}", uuid::Uuid::new_v4()),
            lock_strategy: LockStrategy::S3(S3LockConfig::default()),
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            capabilities: None,
        })
    }

    #[tokio::test]
    async fn test_probe_s3_lock_strategy() {
        let config = s3_config_with_s3_lock();
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should pass for S3 lock strategy on MinIO: {result:?}"
        );
        let caps = result.unwrap();
        assert!(
            caps.is_some(),
            "S3 lock strategy should return capabilities"
        );
        let caps = caps.unwrap();
        assert!(caps.put_if_none_match, "MinIO should support If-None-Match");
        assert!(caps.put_if_match, "MinIO should support If-Match");
    }

    #[tokio::test]
    async fn test_probe_memory_lock_strategy_is_noop() {
        let config = MetadataStoreConfig::S3(metadata_store::s3::BackendConfig {
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "registry".to_string(),
            region: "us-east-1".to_string(),
            key_prefix: "probe-noop".to_string(),
            lock_strategy: LockStrategy::Memory,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            capabilities: None,
        });
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should be no-op for Memory lock strategy"
        );
        assert!(
            result.unwrap().is_none(),
            "Memory lock strategy should return no capabilities"
        );
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
}
