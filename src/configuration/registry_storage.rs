use std::sync::Arc;

use serde::{Deserialize, Deserializer};
use tracing::info;

use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{
    ConditionalStore, ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_tx_engine::{
    ConditionalCapabilities,
    executor::{TransactionExecutor, build_executor},
    lock::{
        LockStrategy, resolve_lock_strategy, storage::redis::RedisLockStorageConfig as LockConfig,
    },
    probe::probe_conditional_capabilities,
    store::Store,
};

use crate::registry::{blob_store, s3_connection::S3ConnectionConfig};

// Error

/// Errors from building the shared storage layer (object store, transaction
/// executor, lock primitive, capabilities probe) from a [`RegistryStorageConfig`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage backend failed: {0}")]
    StorageBackend(String),
    #[error("coordination/configuration error: {0}")]
    Coordination(String),
}

impl From<angos_storage::Error> for Error {
    fn from(e: angos_storage::Error) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

impl From<angos_s3_client::Error> for Error {
    fn from(e: angos_s3_client::Error) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

// FS backend config

#[derive(Clone, Debug, PartialEq)]
pub struct FsBackendConfig {
    pub root_dir: String,
    pub lock_strategy: LockStrategy,
    pub sync_to_disk: bool,
}

impl Default for FsBackendConfig {
    fn default() -> Self {
        Self {
            root_dir: String::new(),
            lock_strategy: LockStrategy::Memory,
            sync_to_disk: false,
        }
    }
}

impl<'de> Deserialize<'de> for FsBackendConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            root_dir: String,
            #[serde(default)]
            redis: Option<LockConfig>,
            #[serde(default)]
            lock_strategy: Option<LockStrategy>,
            #[serde(default)]
            sync_to_disk: bool,
        }

        let raw = Raw::deserialize(deserializer)?;
        let lock_strategy = resolve_lock_strategy(raw.lock_strategy, raw.redis, false)?;

        Ok(FsBackendConfig {
            root_dir: raw.root_dir,
            lock_strategy,
            sync_to_disk: raw.sync_to_disk,
        })
    }
}

// S3 backend config

#[derive(Clone, Debug, PartialEq)]
pub struct S3BackendConfig {
    pub connection: S3ConnectionConfig,
    pub lock_strategy: LockStrategy,
    pub link_cache_ttl: u64,
    pub access_time_debounce_secs: u64,
    /// Declared conditional-write support, one flag per HTTP conditional header:
    /// `put_if_none_match` (If-None-Match: *, create-only), `put_if_match`
    /// (If-Match, update-only), and `delete_if_match` (If-Match delete). When set,
    /// the startup probe is skipped and these values are used; when absent, the
    /// probe auto-detects. Both `put_if_none_match` and `put_if_match` must be true
    /// for compare-and-swap; set all to `false` under memory/redis locking to skip
    /// the probe and route blob-index updates through the lock backend.
    pub capabilities: Option<ConditionalCapabilities>,
}

impl Default for S3BackendConfig {
    fn default() -> Self {
        Self {
            connection: S3ConnectionConfig::default(),
            lock_strategy: LockStrategy::Memory,
            link_cache_ttl: default_link_cache_ttl(),
            access_time_debounce_secs: default_access_time_debounce(),
            capabilities: None,
        }
    }
}

impl<'de> Deserialize<'de> for S3BackendConfig {
    // Custom impl to resolve `lock_strategy` from optional `redis` /
    // `lock_strategy` keys and to flatten the connection fields.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(flatten)]
            connection: S3ConnectionConfig,
            #[serde(default)]
            redis: Option<LockConfig>,
            #[serde(default)]
            lock_strategy: Option<LockStrategy>,
            #[serde(default = "default_link_cache_ttl")]
            link_cache_ttl: u64,
            #[serde(default = "default_access_time_debounce")]
            access_time_debounce_secs: u64,
            #[serde(default)]
            capabilities: Option<ConditionalCapabilities>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let lock_strategy = resolve_lock_strategy(raw.lock_strategy, raw.redis, true)?;

        Ok(S3BackendConfig {
            connection: raw.connection,
            lock_strategy,
            link_cache_ttl: raw.link_cache_ttl,
            access_time_debounce_secs: raw.access_time_debounce_secs,
            capabilities: raw.capabilities,
        })
    }
}

fn default_link_cache_ttl() -> u64 {
    30
}

fn default_access_time_debounce() -> u64 {
    60
}

// RegistryStorageConfig

/// Unified storage configuration for the metadata store and the job store, which
/// share the `ObjectStore` and `TransactionExecutor` pair built by
/// [`RegistryStorageConfig::build_store`]. The operator-facing TOML key is
/// `[metadata_store]` (with `.fs` or `.s3` sub-tables); the default `Inherit`
/// variant resolves to the blob-store configuration at startup.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum RegistryStorageConfig {
    /// Inherit blob-store credentials and root path. Must be resolved via
    /// [`crate::configuration::Configuration::resolve_registry_storage`] before
    /// reaching `build_store` or `probe`; reaching them here is a programming error.
    #[default]
    Inherit,
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[serde(rename = "s3")]
    S3(S3BackendConfig),
}

impl RegistryStorageConfig {
    /// S3 lock strategy requires compare-and-swap (If-None-Match + If-Match). Returns
    /// a Coordination error when the strategy is S3 but the provider lacks CAS.
    fn ensure_s3_cas_supported(
        lock_strategy: &LockStrategy,
        caps: &ConditionalCapabilities,
    ) -> Result<(), Error> {
        if matches!(lock_strategy, LockStrategy::S3(_)) && !caps.supports_cas() {
            return Err(Error::Coordination(format!(
                "S3 lock strategy requires If-None-Match and If-Match support, but the provider \
                 reports put_if_none_match={}, put_if_match={}. Use lock_strategy = redis or \
                 lock_strategy = memory instead.",
                caps.put_if_none_match, caps.put_if_match
            )));
        }
        Ok(())
    }

    /// Build a `RegistryStorageConfig` that mirrors the given blob-store config.
    pub fn from_blob_store(blob: &blob_store::BlobStoreConfig) -> Self {
        match blob {
            blob_store::BlobStoreConfig::FS(config) => RegistryStorageConfig::FS(FsBackendConfig {
                root_dir: config.root_dir.clone(),
                sync_to_disk: config.sync_to_disk,
                ..Default::default()
            }),
            blob_store::BlobStoreConfig::S3(config) => {
                info!("Auto-configuring S3 metadata-store from blob-store");
                RegistryStorageConfig::S3(S3BackendConfig {
                    connection: config.connection.clone(),
                    ..Default::default()
                })
            }
        }
    }

    /// Probe the underlying S3 store for conditional-write capabilities. Returns
    /// `None` for FS configs and [`Error::Coordination`] for the unresolved
    /// `Inherit` variant.
    pub async fn probe(&self) -> Result<Option<ConditionalCapabilities>, Error> {
        match self {
            RegistryStorageConfig::Inherit => Err(Error::Coordination(
                "RegistryStorageConfig::Inherit reached probe(); callers must \
                 resolve via Configuration::resolve_registry_storage first"
                    .to_string(),
            )),
            RegistryStorageConfig::S3(config) => {
                let http = S3HttpBackend::new(&config.connection.to_client_config())?;
                let storage = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
                let caps = probe_conditional_capabilities(storage.as_ref())
                    .await
                    .map_err(|e| Error::StorageBackend(e.to_string()))?;
                Self::ensure_s3_cas_supported(&config.lock_strategy, &caps)?;
                Ok(Some(caps))
            }
            RegistryStorageConfig::FS(_) => Ok(None),
        }
    }

    /// Build the [`Store`] façade shared by the metadata store, job store, and
    /// engine-maintenance loops. For S3 without declared capabilities this probes
    /// the endpoint; callers can inject resolved capabilities to skip the probe.
    #[allow(clippy::too_many_lines)]
    pub async fn build_store(&self) -> Result<Arc<Store>, Error> {
        // One `ObjectStore` handle backs both the `Store` façade and the executor.
        let (object, executor): (Arc<dyn ObjectStore>, Arc<dyn TransactionExecutor>) = match self {
            RegistryStorageConfig::Inherit => {
                return Err(Error::Coordination(
                    "RegistryStorageConfig::Inherit reached build_store(); callers must \
                     resolve via Configuration::resolve_registry_storage first"
                        .to_string(),
                ));
            }
            RegistryStorageConfig::FS(config) => {
                if matches!(config.lock_strategy, LockStrategy::S3(_)) {
                    return Err(Error::Coordination(
                        "S3 lock strategy is not supported for filesystem storage".to_string(),
                    ));
                }
                info!(
                    "Using filesystem storage backend with lock_strategy={:?}",
                    config.lock_strategy
                );
                let object: Arc<dyn ObjectStore> = Arc::new(
                    StorageFsBackend::builder(&config.root_dir)
                        .sync_to_disk(config.sync_to_disk)
                        .build(),
                );
                let executor = build_executor(
                    object.clone(),
                    None,
                    config.lock_strategy.clone(),
                    None,
                    false,
                    false,
                )
                .map_err(|e| Error::Coordination(e.to_string()))?;
                (object, executor)
            }
            RegistryStorageConfig::S3(config) => {
                info!(
                    "Using S3 storage backend with lock_strategy={:?}",
                    config.lock_strategy
                );
                let caps = match &config.capabilities {
                    Some(declared) => Some(declared.clone()),
                    None => self.probe().await?,
                };

                let caps_resolved = caps.unwrap_or_default();
                Self::ensure_s3_cas_supported(&config.lock_strategy, &caps_resolved)?;

                let http = S3HttpBackend::new(&config.connection.to_client_config())?;
                let backend = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
                let object: Arc<dyn ObjectStore> = backend.clone();
                let conditional_store: Arc<dyn ConditionalStore> = backend;

                let s3_lock_store: Option<Arc<dyn ConditionalStore>> = match &config.lock_strategy {
                    LockStrategy::S3(s3_lock_config) => {
                        let lock_http = S3HttpBackend::new(
                            &config.connection.to_lock_client_config(s3_lock_config),
                        )
                        .map_err(|e| {
                            Error::Coordination(format!("Failed to initialize S3 lock client: {e}"))
                        })?;
                        let lock_backend = StorageS3Backend::builder(Arc::new(lock_http)).build();
                        Some(Arc::new(lock_backend))
                    }
                    LockStrategy::Redis(_) | LockStrategy::Memory => None,
                };

                let executor = build_executor(
                    object.clone(),
                    Some(conditional_store),
                    config.lock_strategy.clone(),
                    s3_lock_store,
                    caps_resolved.delete_if_match,
                    caps_resolved.supports_cas(),
                )
                .map_err(|e| Error::Coordination(e.to_string()))?;

                (object, executor)
            }
        };

        Ok(Arc::new(Store::builder(object, executor).build()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        registry::{blob_store, s3_connection::S3ConnectionConfig},
        secret::Secret,
    };
    use angos_tx_engine::lock::S3LockConfig;

    fn s3_config_with_lock_strategy(lock_strategy: LockStrategy) -> RegistryStorageConfig {
        RegistryStorageConfig::S3(S3BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("root".to_string()),
                secret_key: Secret::new("roottoor".to_string()),
                endpoint: crate::registry::test_utils::test_s3_endpoint(),
                bucket: "registry".to_string(),
                region: "region".to_string(),
                key_prefix: format!("probe-test-{}", uuid::Uuid::new_v4()),
            },
            lock_strategy,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            capabilities: None,
        })
    }

    #[tokio::test]
    async fn test_probe_s3_lock_strategy_detects_s3_capabilities() {
        let config = s3_config_with_lock_strategy(LockStrategy::S3(S3LockConfig::default()));
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should succeed against the S3 backend with S3 lock strategy: {result:?}"
        );
        let caps = result
            .unwrap()
            .expect("S3 lock strategy should return capabilities");
        assert!(
            caps.put_if_none_match,
            "the S3 backend should support If-None-Match"
        );
        assert!(caps.put_if_match, "the S3 backend should support If-Match");
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
        assert!(
            caps.put_if_none_match,
            "the S3 backend should support If-None-Match"
        );
        assert!(caps.put_if_match, "the S3 backend should support If-Match");
    }

    #[tokio::test]
    async fn test_probe_fs_config_is_noop() {
        let config = RegistryStorageConfig::FS(FsBackendConfig {
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
        let blob = blob_store::BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: "/var/lib/registry".to_string(),
            sync_to_disk: true,
        });
        match RegistryStorageConfig::from_blob_store(&blob) {
            RegistryStorageConfig::FS(c) => {
                assert_eq!(c.root_dir, "/var/lib/registry");
                assert!(c.sync_to_disk);
            }
            RegistryStorageConfig::Inherit | RegistryStorageConfig::S3(_) => {
                panic!("expected FS storage config")
            }
        }
    }

    #[test]
    fn test_from_blob_store_s3_copies_credentials_and_bucket() {
        let blob = blob_store::BlobStoreConfig::S3(blob_store::S3BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("key".to_string()),
                secret_key: Secret::new("secret".to_string()),
                endpoint: "http://localhost:9000".to_string(),
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                key_prefix: "foo".to_string(),
            },
            ..blob_store::S3BackendConfig::default()
        });
        match RegistryStorageConfig::from_blob_store(&blob) {
            RegistryStorageConfig::S3(c) => {
                assert_eq!(c.connection.bucket, "test-bucket");
                assert_eq!(c.connection.region, "us-east-1");
                assert_eq!(c.connection.endpoint, "http://localhost:9000");
                assert_eq!(c.connection.access_key_id.expose(), "key");
                assert_eq!(c.connection.secret_key.expose(), "secret");
                assert_eq!(c.connection.key_prefix, "foo");
            }
            RegistryStorageConfig::Inherit | RegistryStorageConfig::FS(_) => {
                panic!("expected S3 storage config")
            }
        }
    }

    /// `[metadata_store.s3]` round-trip: flat TOML deserialises into a
    /// `S3BackendConfig` whose `connection` carries the right values and whose
    /// metadata-specific keys override their defaults.
    #[test]
    fn s3_backend_config_toml_round_trip() {
        let toml = r#"
            access_key_id            = "meta-key"
            secret_key               = "meta-secret"
            endpoint                 = "https://meta.s3.example.com"
            bucket                   = "meta-bucket"
            region                   = "eu-central-1"
            key_prefix               = "_meta"
            link_cache_ttl           = 60
            access_time_debounce_secs = 120
        "#;

        let cfg: S3BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.access_key_id.expose(), "meta-key");
        assert_eq!(cfg.connection.secret_key.expose(), "meta-secret");
        assert_eq!(cfg.connection.endpoint, "https://meta.s3.example.com");
        assert_eq!(cfg.connection.bucket, "meta-bucket");
        assert_eq!(cfg.connection.region, "eu-central-1");
        assert_eq!(cfg.connection.key_prefix, "_meta");
        assert_eq!(cfg.link_cache_ttl, 60);
        assert_eq!(cfg.access_time_debounce_secs, 120);
    }

    /// Regression: `region` must be required, matching the documented schema
    /// and the behaviour before consolidation.
    #[test]
    fn s3_backend_config_requires_region() {
        let toml = r#"
            access_key_id = "k"
            secret_key    = "s"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
        "#;
        let err = toml::from_str::<S3BackendConfig>(toml).expect_err("region must be required");
        assert!(
            err.to_string().contains("region"),
            "error should mention the missing `region` field, got: {err}"
        );
    }
}
