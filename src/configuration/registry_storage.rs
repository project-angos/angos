use serde::{Deserialize, Deserializer};
use tracing::info;

use angos_tx_engine::lock::{
    LockStrategy, S3LockConfig, resolve_lock_strategy,
    storage::redis::RedisLockStorageConfig as LockConfig,
};

use crate::registry::{blob_store, s3_connection::S3ConnectionConfig};

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
        }

        let raw = Raw::deserialize(deserializer)?;
        // `None` (nothing configured) survives here: the effective default
        // depends on CAS support, resolved at build time.
        let lock_strategy = resolve_lock_strategy(raw.lock_strategy, raw.redis, true)?;

        Ok(S3BackendConfig {
            connection: raw.connection,
            lock_strategy,
            link_cache_ttl: raw.link_cache_ttl,
            access_time_debounce_secs: raw.access_time_debounce_secs,
            conditional_operations: raw.conditional_operations,
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
/// built once at startup by the CLI bootstrap
/// (`crate::command::bootstrap::build_store`); this module carries only the
/// parsed configuration.
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
    /// before any backend is built or probed. Reaching the bootstrap's
    /// `build_store`/`probe_storage` with this variant is a programming error.
    #[default]
    Inherit,
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[serde(rename = "s3")]
    S3(S3BackendConfig),
}

impl RegistryStorageConfig {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        registry::{blob_store, s3_connection::S3ConnectionConfig},
        secret::Secret,
    };

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
}
