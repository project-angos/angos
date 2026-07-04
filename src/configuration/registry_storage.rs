use std::sync::Arc;

use serde::{Deserialize, Deserializer, de::Error as _};
use tracing::info;

use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{
    ConditionalStore, ObjectStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};
use angos_tx_engine::{
    error::Error as EngineError,
    lock::{
        LockStrategy, S3LockConfig, resolve_lock_strategy,
        storage::redis::RedisLockStorageConfig as LockConfig,
    },
    probe::probe_cas_support,
    store::Store,
};

use crate::registry::{blob_store, s3_connection::S3ConnectionConfig};

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
        let lock_strategy = resolve_lock_strategy(raw.lock_strategy, raw.redis, false)?
            .unwrap_or(LockStrategy::Memory);

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
    /// Operator-configured lock backend. `None` means unset: the effective
    /// strategy then follows the provider's conditional-write support (see
    /// [`S3BackendConfig::resolved_lock_strategy`]).
    pub lock_strategy: Option<LockStrategy>,
    pub link_cache_ttl: u64,
    pub access_time_debounce_secs: u64,
    /// Explicitly declare whether the provider supports the conditional
    /// operations CAS coordination requires, as one all-or-nothing set:
    /// `PutObject` with If-None-Match: *, `PutObject` with If-Match: <etag>,
    /// and `DeleteObject` with If-Match: <etag>.
    ///
    /// When set, the startup probe is skipped entirely and the declared value
    /// is used. When absent, the probe runs automatically for S3 metadata
    /// storage. Set it to `false` to skip the probe and force coordination
    /// through the configured lock backend instead of S3 CAS; with an unset
    /// `lock_strategy` this also pins the default lock to `memory`.
    ///
    /// Set explicitly to avoid startup latency from probing, or for
    /// S3-compatible providers where probe results may be inaccurate.
    ///
    /// The legacy `capabilities` table (three per-operation booleans) is
    /// still accepted and maps to `true` only when all three flags are set.
    pub conditional_operations: Option<bool>,
}

impl Default for S3BackendConfig {
    fn default() -> Self {
        Self {
            connection: S3ConnectionConfig::default(),
            lock_strategy: None,
            link_cache_ttl: default_link_cache_ttl(),
            access_time_debounce_secs: default_access_time_debounce(),
            conditional_operations: None,
        }
    }
}

impl S3BackendConfig {
    /// The effective lock strategy given the provider's conditional-write
    /// support. An unset `lock_strategy` defaults to the S3 lock backend when
    /// CAS is available, so coordination works across processes out of the
    /// box; without CAS it falls back to the in-process memory lock.
    pub fn resolved_lock_strategy(&self, cas: bool) -> LockStrategy {
        match &self.lock_strategy {
            Some(strategy) => strategy.clone(),
            None if cas => LockStrategy::S3(S3LockConfig::default()),
            None => LockStrategy::Memory,
        }
    }
}

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
            conditional_operations: Option<bool>,
            #[serde(default)]
            capabilities: Option<LegacyCapabilities>,
        }

        /// Legacy `[metadata_store.s3.capabilities]` table. Folded into
        /// `conditional_operations`: CAS requires all three operations.
        #[derive(Deserialize)]
        struct LegacyCapabilities {
            put_if_none_match: bool,
            put_if_match: bool,
            delete_if_match: bool,
        }

        let raw = Raw::deserialize(deserializer)?;
        // `None` (nothing configured) survives here: the effective default
        // depends on CAS support, resolved at build time.
        let lock_strategy = resolve_lock_strategy(raw.lock_strategy, raw.redis, true)?;
        let conditional_operations = match (raw.conditional_operations, raw.capabilities) {
            (Some(_), Some(_)) => {
                return Err(D::Error::custom(
                    "cannot set both 'conditional_operations' and the legacy 'capabilities' \
                     table; use conditional_operations",
                ));
            }
            (Some(declared), None) => Some(declared),
            (None, Some(caps)) => {
                Some(caps.put_if_none_match && caps.put_if_match && caps.delete_if_match)
            }
            (None, None) => None,
        };

        Ok(S3BackendConfig {
            connection: raw.connection,
            lock_strategy,
            link_cache_ttl: raw.link_cache_ttl,
            access_time_debounce_secs: raw.access_time_debounce_secs,
            conditional_operations,
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
    /// before reaching [`RegistryStorageConfig::build_store`] or
    /// [`RegistryStorageConfig::probe`].
    /// Reaching either method with this variant is a programming error.
    #[default]
    Inherit,
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[serde(rename = "s3")]
    S3(S3BackendConfig),
}

impl RegistryStorageConfig {
    /// S3 lock strategy requires the full conditional set (If-None-Match and
    /// If-Match on PUT, If-Match on DELETE). Returns a Coordination error when
    /// the strategy is S3 but the provider lacks it.
    fn ensure_s3_cas_supported(lock_strategy: &LockStrategy, cas: bool) -> Result<(), Error> {
        if matches!(lock_strategy, LockStrategy::S3(_)) && !cas {
            return Err(Error::Coordination(
                "S3 lock strategy requires conditional writes (If-None-Match and If-Match on \
                 PUT, If-Match on DELETE), but the provider does not support them all. Use \
                 lock_strategy = redis or lock_strategy = memory instead."
                    .to_string(),
            ));
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

    /// Probe the underlying S3 store for conditional-write support.
    ///
    /// Returns `None` for FS configs (nothing to probe). Returns
    /// [`Error::Coordination`] when called on the `Inherit` variant: callers
    /// must resolve first via
    /// [`crate::configuration::Configuration::resolve_registry_storage`].
    pub async fn probe(&self) -> Result<Option<bool>, Error> {
        match self {
            RegistryStorageConfig::Inherit => Err(Error::Coordination(
                "RegistryStorageConfig::Inherit reached probe(); callers must \
                 resolve via Configuration::resolve_registry_storage first"
                    .to_string(),
            )),
            RegistryStorageConfig::S3(config) => {
                let http = S3HttpBackend::new(&config.connection.to_client_config())?;
                let storage = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
                let cas = probe_cas_support(storage.as_ref()).await?;
                if let Some(strategy) = &config.lock_strategy {
                    Self::ensure_s3_cas_supported(strategy, cas)?;
                }
                Ok(Some(cas))
            }
            RegistryStorageConfig::FS(_) => Ok(None),
        }
    }

    /// Build the [`Store`] façade shared by the metadata store, the job store,
    /// and the engine-maintenance loops.
    ///
    /// For S3 without an operator-declared `conditional_operations` this probes
    /// the endpoint to configure the executor. Server callers that want to
    /// memoize the probe across hot-reloads should resolve it up front (see
    /// `setup::build_metadata_store`) and inject it into the config so this
    /// path skips the probe.
    pub async fn build_store(&self) -> Result<Arc<Store>, Error> {
        let store = match self {
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
                Store::new(object, None, config.lock_strategy.clone(), None)?
            }
            RegistryStorageConfig::S3(config) => {
                let cas = match config.conditional_operations {
                    Some(declared) => declared,
                    None => self.probe().await?.unwrap_or_default(),
                };
                let lock_strategy = config.resolved_lock_strategy(cas);
                Self::ensure_s3_cas_supported(&lock_strategy, cas)?;
                info!("Using S3 storage backend with lock_strategy={lock_strategy:?}");

                let http = S3HttpBackend::new(&config.connection.to_client_config())?;
                let backend = Arc::new(StorageS3Backend::builder(Arc::new(http)).build());
                let object: Arc<dyn ObjectStore> = backend.clone();
                let conditional_store: Option<Arc<dyn ConditionalStore>> =
                    cas.then_some(backend as Arc<dyn ConditionalStore>);

                let s3_lock_store: Option<Arc<dyn ConditionalStore>> = match &lock_strategy {
                    LockStrategy::S3(s3_lock_config) => {
                        let lock_http = S3HttpBackend::new(
                            &config.connection.to_lock_client_config(s3_lock_config),
                        )?;
                        let lock_backend = StorageS3Backend::builder(Arc::new(lock_http)).build();
                        Some(Arc::new(lock_backend))
                    }
                    LockStrategy::Redis(_) | LockStrategy::Memory => None,
                };

                Store::new(object, conditional_store, lock_strategy, s3_lock_store)?
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

    fn s3_config_with_lock_strategy(lock_strategy: Option<LockStrategy>) -> RegistryStorageConfig {
        RegistryStorageConfig::S3(S3BackendConfig {
            connection: s3_test_connection(format!("probe-test-{}", uuid::Uuid::new_v4())),
            lock_strategy,
            link_cache_ttl: 30,
            access_time_debounce_secs: 0,
            conditional_operations: None,
        })
    }

    #[tokio::test]
    async fn test_probe_s3_lock_strategy_detects_s3_capabilities() {
        let config = s3_config_with_lock_strategy(Some(LockStrategy::S3(S3LockConfig::default())));
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should succeed against the S3 backend with S3 lock strategy: {result:?}"
        );
        let cas = result.unwrap().expect("S3 lock strategy should probe");
        assert!(
            cas,
            "the S3 test backend should support the full conditional set"
        );
    }

    #[tokio::test]
    async fn test_probe_memory_lock_strategy_detects_capabilities() {
        let config = s3_config_with_lock_strategy(Some(LockStrategy::Memory));
        let result = config.probe().await;
        assert!(
            result.is_ok(),
            "Probe should succeed for Memory lock strategy: {result:?}"
        );
        let cas = result
            .unwrap()
            .expect("S3 metadata store should return a probe verdict");
        assert!(
            cas,
            "the S3 test backend should support the full conditional set"
        );
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

    fn s3_toml_with(extra: &str) -> String {
        format!(
            r#"
            access_key_id = "k"
            secret_key    = "s"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
            region        = "r"
            {extra}
        "#
        )
    }

    #[test]
    fn legacy_capabilities_full_set_maps_to_conditional_operations_true() {
        let toml = s3_toml_with(
            r"
            [capabilities]
            put_if_none_match = true
            put_if_match      = true
            delete_if_match   = true
        ",
        );
        let cfg: S3BackendConfig = toml::from_str(&toml).expect("deserialize");
        assert_eq!(cfg.conditional_operations, Some(true));
    }

    #[test]
    fn legacy_capabilities_partial_set_maps_to_conditional_operations_false() {
        // Conditional deletes are part of the required set, so a legacy config
        // that lacks them declares a provider that cannot run CAS.
        let toml = s3_toml_with(
            r"
            [capabilities]
            put_if_none_match = true
            put_if_match      = true
            delete_if_match   = false
        ",
        );
        let cfg: S3BackendConfig = toml::from_str(&toml).expect("deserialize");
        assert_eq!(cfg.conditional_operations, Some(false));
    }

    #[test]
    fn conditional_operations_and_legacy_capabilities_conflict_is_rejected() {
        let toml = s3_toml_with(
            r"
            conditional_operations = true

            [capabilities]
            put_if_none_match = true
            put_if_match      = true
            delete_if_match   = true
        ",
        );
        let err = toml::from_str::<S3BackendConfig>(&toml).expect_err("conflict must be rejected");
        assert!(
            err.to_string().contains("conditional_operations"),
            "error should point at the conflicting keys, got: {err}"
        );
    }

    #[test]
    fn conditional_operations_round_trips() {
        let cfg: S3BackendConfig =
            toml::from_str(&s3_toml_with("conditional_operations = false")).expect("deserialize");
        assert_eq!(cfg.conditional_operations, Some(false));
    }

    #[test]
    fn unset_lock_strategy_resolves_from_cas_support() {
        let cfg: S3BackendConfig = toml::from_str(&s3_toml_with("")).expect("deserialize");
        assert_eq!(cfg.lock_strategy, None);
        assert_eq!(
            cfg.resolved_lock_strategy(true),
            LockStrategy::S3(S3LockConfig::default())
        );
        assert_eq!(cfg.resolved_lock_strategy(false), LockStrategy::Memory);
    }

    #[test]
    fn explicit_lock_strategy_wins_over_cas_default() {
        let cfg: S3BackendConfig =
            toml::from_str(&s3_toml_with(r#"lock_strategy = "memory""#)).expect("deserialize");
        assert_eq!(cfg.resolved_lock_strategy(true), LockStrategy::Memory);
    }

    #[tokio::test]
    async fn test_build_store_defaults_to_s3_lock_when_cas_is_supported() {
        let config = s3_config_with_lock_strategy(None);
        let store = config.build_store().await.expect("build store");
        assert_eq!(
            store.lock_backend(),
            "s3",
            "a CAS-capable provider with no configured lock strategy must default to the S3 lock"
        );
    }
}
