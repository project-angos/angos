#[cfg(feature = "s3-backend")]
use std::sync::PoisonError;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Deserializer};
use tracing::info;

#[cfg(feature = "s3-backend")]
use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::ObjectStore;
#[cfg(feature = "fs-backend")]
use angos_storage::fs::Backend as StorageFsBackend;
#[cfg(feature = "s3-backend")]
use angos_storage::{ConditionalStore, s3::Backend as StorageS3Backend};
#[cfg(feature = "s3-backend")]
use angos_tx_engine::probe::probe_conditional_capabilities;
use angos_tx_engine::{
    ConditionalCapabilities,
    error::Error as EngineError,
    lock::{
        LockStrategy, RedisLockStorageConfig as LockConfig, default_lock_strategy,
        resolve_lock_strategy,
    },
    store::Store,
};

use crate::registry::blob_store;
#[cfg(feature = "s3-backend")]
use crate::registry::s3_connection::S3ConnectionConfig;

// Error

/// Errors produced while building the shared storage layer (object store,
/// transaction executor, lock primitive, capabilities probe) from a
/// [`RegistryStorageConfig`].
///
/// This is intentionally narrower than the metadata-store error type:
/// `RegistryStorageConfig::build_store` and `RegistryStorageConfig::probe`
/// don't perform any metadata-store-specific work, so they don't borrow
/// that subsystem's error vocabulary.
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

#[cfg(feature = "s3-backend")]
impl From<angos_s3_client::Error> for Error {
    fn from(e: angos_s3_client::Error) -> Self {
        Error::StorageBackend(e.to_string())
    }
}

impl From<EngineError> for Error {
    fn from(e: EngineError) -> Self {
        match &e {
            EngineError::Storage(_) => Error::StorageBackend(e.to_string()),
            _ => Error::Coordination(e.to_string()),
        }
    }
}

// FS backend config

#[cfg(feature = "fs-backend")]
#[derive(Clone, Debug, PartialEq)]
pub struct FsBackendConfig {
    pub root_dir: String,
    pub lock_strategy: LockStrategy,
    pub sync_to_disk: bool,
}

#[cfg(feature = "fs-backend")]
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

#[cfg(feature = "s3-backend")]
#[derive(Clone, Debug, PartialEq)]
pub struct S3BackendConfig {
    pub connection: S3ConnectionConfig,
    pub lock_strategy: LockStrategy,
    pub link_cache_ttl: u64,
    pub access_time_debounce_secs: u64,
    /// Explicitly declare which conditional S3 operations the provider supports.
    /// Each boolean flag corresponds to a distinct HTTP conditional header:
    /// - `put_if_none_match`: `PutObject` with If-None-Match: * (create-only)
    /// - `put_if_match`: `PutObject` with If-Match: <etag> (update-only, enables CAS optimizations)
    /// - `delete_if_match`: `DeleteObject` with If-Match: <etag> (atomic lock release)
    ///
    /// When set, the startup probe is skipped entirely and your declared values are used.
    /// When absent, the probe runs automatically for S3 metadata storage to auto-detect
    /// capabilities. With `lock_strategy = "memory"` or `"redis"`, set every flag to
    /// `false` to skip the probe and force blob-index updates through the configured
    /// lock backend instead of S3 CAS.
    ///
    /// Set explicitly to avoid startup latency from probing, or to handle S3-compatible
    /// providers where probe results may be inaccurate. Both `put_if_none_match` and
    /// `put_if_match` must be true for compare-and-swap (CAS) operations to be used.
    pub capabilities: Option<ConditionalCapabilities>,
}

#[cfg(feature = "s3-backend")]
impl<'de> Deserialize<'de> for S3BackendConfig {
    // Custom impl because `lock_strategy` must be resolved from optional
    // `redis` / `lock_strategy` keys via `resolve_lock_strategy`.
    // The connection fields come in flat alongside the metadata-specific
    // keys; flattening `S3ConnectionConfig` preserves its required/optional
    // contract (all required except `key_prefix`).
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

#[cfg(feature = "s3-backend")]
impl S3BackendConfig {
    /// S3 lock strategy requires compare-and-swap (If-None-Match + If-Match).
    /// Returns a Coordination error when the strategy is S3 but the provider
    /// lacks CAS.
    fn ensure_cas_supported(&self, caps: &ConditionalCapabilities) -> Result<(), Error> {
        if matches!(self.lock_strategy, LockStrategy::S3(_)) && !caps.supports_cas() {
            return Err(Error::Coordination(format!(
                "S3 lock strategy requires If-None-Match and If-Match support, but the provider \
                 reports put_if_none_match={}, put_if_match={}. Use lock_strategy = redis or \
                 lock_strategy = memory instead.",
                caps.put_if_none_match, caps.put_if_match
            )));
        }
        Ok(())
    }

    /// Probe the endpoint for conditional-write capabilities and validate
    /// them against the configured lock strategy.
    async fn probe(&self) -> Result<ConditionalCapabilities, Error> {
        let http = S3HttpBackend::new(&self.connection.to_client_config())?;
        let storage = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
        let caps = probe_conditional_capabilities(storage.as_ref()).await?;
        self.ensure_cas_supported(&caps)?;
        Ok(caps)
    }
}

#[cfg(feature = "s3-backend")]
fn default_link_cache_ttl() -> u64 {
    30
}

#[cfg(feature = "s3-backend")]
fn default_access_time_debounce() -> u64 {
    60
}

// RegistryStorageConfig

/// Unified storage configuration for both the metadata store and the job store.
///
/// Both subsystems share the same `ObjectStore` and `TransactionExecutor` pair
/// built once at startup via [`RegistryStorageConfig::build_store`].
///
/// The operator-facing TOML key remains `[metadata_store]` (with `.fs` or
/// `.s3` sub-tables). The `Inherit` variant is the default and resolves to
/// the blob-store configuration at startup.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum RegistryStorageConfig {
    /// Inherit blob-store credentials and root path.
    ///
    /// Resolved via [`crate::configuration::Configuration::resolve_registry_storage`]
    /// before reaching [`RegistryStorageConfig::build_store`].
    /// Reaching that method with this variant is a programming error.
    #[default]
    Inherit,
    #[cfg(feature = "fs-backend")]
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[cfg(feature = "s3-backend")]
    #[serde(rename = "s3")]
    S3(S3BackendConfig),
}

impl RegistryStorageConfig {
    /// Build a `RegistryStorageConfig` that mirrors the given blob-store config.
    ///
    /// # Errors
    ///
    /// [`Error::Coordination`] when this build has no default lock strategy
    /// (`memory-lock` disabled): inheritance cannot pick a lock backend, so
    /// `[metadata_store]` must be configured explicitly.
    pub fn from_blob_store(blob: &blob_store::BlobStoreConfig) -> Result<Self, Error> {
        let lock_strategy = default_lock_strategy().ok_or_else(|| {
            Error::Coordination(
                "[metadata_store] inherits the blob store using the memory lock strategy, \
                 which is not compiled into this build; configure [metadata_store.fs] or \
                 [metadata_store.s3] with an explicit lock_strategy"
                    .to_string(),
            )
        })?;
        Ok(match blob {
            #[cfg(feature = "fs-backend")]
            blob_store::BlobStoreConfig::FS(config) => RegistryStorageConfig::FS(FsBackendConfig {
                root_dir: config.root_dir.clone(),
                sync_to_disk: config.sync_to_disk,
                lock_strategy,
            }),
            #[cfg(feature = "s3-backend")]
            blob_store::BlobStoreConfig::S3(config) => {
                info!("Auto-configuring S3 metadata-store from blob-store");
                RegistryStorageConfig::S3(S3BackendConfig {
                    connection: config.connection.clone(),
                    lock_strategy,
                    link_cache_ttl: default_link_cache_ttl(),
                    access_time_debounce_secs: default_access_time_debounce(),
                    capabilities: None,
                })
            }
        })
    }

    /// The configured lock strategy; `None` for the unresolved `Inherit`
    /// variant.
    pub fn lock_strategy(&self) -> Option<&LockStrategy> {
        match self {
            RegistryStorageConfig::Inherit => None,
            #[cfg(feature = "fs-backend")]
            RegistryStorageConfig::FS(config) => Some(&config.lock_strategy),
            #[cfg(feature = "s3-backend")]
            RegistryStorageConfig::S3(config) => Some(&config.lock_strategy),
        }
    }

    /// Metadata-store cache tuning as `(link_cache_ttl,
    /// access_time_debounce_secs)`; zero (disabled) for backends without
    /// link caching.
    pub fn link_cache_tuning(&self) -> (u64, u64) {
        match self {
            #[cfg(feature = "s3-backend")]
            RegistryStorageConfig::S3(config) => {
                (config.link_cache_ttl, config.access_time_debounce_secs)
            }
            _ => (0, 0),
        }
    }

    /// Resolve S3 conditional-write capabilities once, memoizing them in
    /// `cache` so config hot-reloads rebuild stores without re-probing the
    /// endpoint. Injecting the result means [`RegistryStorageConfig::build_store`]
    /// skips its own probe. No-op unless this is an S3 config without
    /// operator-declared capabilities.
    ///
    /// # Errors
    ///
    /// Any probe failure, surfaced as this module's [`Error`].
    #[cfg_attr(
        not(feature = "s3-backend"),
        allow(
            unused_variables,
            clippy::unused_async,
            clippy::unnecessary_wraps,
            reason = "without s3-backend there are no capabilities to resolve"
        )
    )]
    pub async fn memoize_capabilities(
        &mut self,
        cache: &Mutex<Option<ConditionalCapabilities>>,
    ) -> Result<(), Error> {
        #[cfg(feature = "s3-backend")]
        if let RegistryStorageConfig::S3(config) = self
            && config.capabilities.is_none()
        {
            let cached = cache.lock().unwrap_or_else(PoisonError::into_inner).clone();
            let caps = if let Some(caps) = cached {
                caps
            } else {
                let probed = config.probe().await?;
                *cache.lock().unwrap_or_else(PoisonError::into_inner) = Some(probed.clone());
                probed
            };
            config.capabilities = Some(caps);
        }
        Ok(())
    }

    /// Build the [`Store`] façade shared by the metadata store, the job store,
    /// and the engine-maintenance loops.
    ///
    /// For S3 without operator-declared capabilities this probes the endpoint
    /// to configure the executor. Server callers that want to memoize the probe
    /// across hot-reloads should resolve capabilities up front (see
    /// `setup::build_metadata_store`) and inject them into the config so this
    /// path skips the probe.
    #[cfg_attr(
        not(feature = "s3-backend"),
        allow(clippy::unused_async, reason = "only the S3 arm awaits (the probe)")
    )]
    pub async fn build_store(&self) -> Result<Arc<Store>, Error> {
        let store = match self {
            RegistryStorageConfig::Inherit => {
                return Err(Error::Coordination(
                    "RegistryStorageConfig::Inherit reached build_store(); callers must \
                     resolve via Configuration::resolve_registry_storage first"
                        .to_string(),
                ));
            }
            #[cfg(feature = "fs-backend")]
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
                Store::new(
                    object,
                    None,
                    config.lock_strategy.clone(),
                    None,
                    false,
                    false,
                )?
            }
            #[cfg(feature = "s3-backend")]
            RegistryStorageConfig::S3(config) => {
                info!(
                    "Using S3 storage backend with lock_strategy={:?}",
                    config.lock_strategy
                );
                let caps_resolved = match &config.capabilities {
                    Some(declared) => declared.clone(),
                    None => config.probe().await?,
                };
                config.ensure_cas_supported(&caps_resolved)?;

                let http = S3HttpBackend::new(&config.connection.to_client_config())?;
                let backend = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
                let object: Arc<dyn ObjectStore> = backend.clone();
                let conditional_store: Arc<dyn ConditionalStore> = backend;

                let s3_lock_store: Option<Arc<dyn ConditionalStore>> = match &config.lock_strategy {
                    LockStrategy::S3(s3_lock_config) => {
                        let lock_http = S3HttpBackend::new(
                            &config.connection.to_lock_client_config(s3_lock_config),
                        )?;
                        let lock_backend = StorageS3Backend::builder(Arc::new(lock_http)).build();
                        Some(Arc::new(lock_backend))
                    }
                    LockStrategy::Redis(_) | LockStrategy::Memory => None,
                };

                Store::new(
                    object,
                    Some(conditional_store),
                    config.lock_strategy.clone(),
                    s3_lock_store,
                    caps_resolved.delete_if_match,
                    caps_resolved.supports_cas(),
                )?
            }
        };

        Ok(Arc::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        registry::{blob_store, s3_connection::S3ConnectionConfig, test_utils::s3_test_connection},
        secret::Secret,
    };
    use angos_tx_engine::lock::S3LockConfig;

    fn s3_config_with_lock_strategy(lock_strategy: LockStrategy) -> S3BackendConfig {
        S3BackendConfig {
            connection: s3_test_connection(format!("probe-test-{}", uuid::Uuid::new_v4())),
            lock_strategy,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            capabilities: None,
        }
    }

    #[tokio::test]
    async fn test_probe_s3_lock_strategy_detects_s3_capabilities() {
        let config = s3_config_with_lock_strategy(LockStrategy::S3(S3LockConfig::default()));
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should succeed against the S3 backend with S3 lock strategy: {result:?}"
        );
        let caps = result.unwrap();
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
        let caps = result.unwrap();
        assert!(
            caps.put_if_none_match,
            "the S3 backend should support If-None-Match"
        );
        assert!(caps.put_if_match, "the S3 backend should support If-Match");
    }

    #[tokio::test]
    async fn test_memoize_capabilities_is_noop_for_fs() {
        let mut config = RegistryStorageConfig::FS(FsBackendConfig {
            root_dir: "/tmp/probe-test".to_string(),
            lock_strategy: LockStrategy::Memory,
            sync_to_disk: false,
        });
        let cache = Mutex::new(None);
        config
            .memoize_capabilities(&cache)
            .await
            .expect("memoize must be a no-op for FS configs");
        assert!(
            cache.lock().unwrap().is_none(),
            "FS config must not populate the capabilities cache"
        );
    }

    #[test]
    fn test_from_blob_store_fs_copies_paths_and_sync() {
        let blob = blob_store::BlobStoreConfig::FS(blob_store::FsBackendConfig {
            root_dir: "/var/lib/registry".to_string(),
            sync_to_disk: true,
        });
        match RegistryStorageConfig::from_blob_store(&blob).unwrap() {
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
        match RegistryStorageConfig::from_blob_store(&blob).unwrap() {
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
