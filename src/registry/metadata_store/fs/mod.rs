use std::sync::Arc;

use serde::Deserialize;
use tracing::info;

use crate::registry::metadata_store::{
    Backend, Error, LockConfig, LockStrategy,
    backend::Coordinator,
    lock::{self, LockBackend, MemoryBackend},
};
use angos_storage::fs::Backend as StorageFsBackend;

#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    pub lock_strategy: LockStrategy,
    pub sync_to_disk: bool,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            root_dir: String::new(),
            lock_strategy: LockStrategy::Memory,
            sync_to_disk: false,
        }
    }
}

impl<'de> Deserialize<'de> for BackendConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
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
        let lock_strategy = lock::resolve_lock_strategy(raw.lock_strategy, raw.redis, false)?;

        Ok(BackendConfig {
            root_dir: raw.root_dir,
            lock_strategy,
            sync_to_disk: raw.sync_to_disk,
        })
    }
}

impl BackendConfig {
    /// Build the unified metadata-store backend for a filesystem deployment.
    ///
    /// Default `lock_strategy = "memory"` is single-process scope;
    /// multi-process worker pools on a shared FS mount must configure
    /// `lock_strategy.redis`.
    pub fn to_backend(&self) -> Result<Backend, Error> {
        info!("Using filesystem metadata-store backend");

        let storage = Arc::new(
            StorageFsBackend::builder()
                .root_dir(&self.root_dir)
                .sync_to_disk(self.sync_to_disk)
                .build()?,
        );

        let lock: Arc<dyn LockBackend + Send + Sync> = match &self.lock_strategy {
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock store for filesystem metadata-store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(backend)
            }
            LockStrategy::S3(_) => {
                return Err(Error::Lock(
                    "S3 lock strategy is not supported for filesystem metadata store".to_string(),
                ));
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for filesystem metadata-store");
                Arc::new(MemoryBackend::new())
            }
        };

        let coordinator = Arc::new(Coordinator::Locked {
            store: storage,
            lock,
        });

        Backend::builder().coordinator(coordinator).build()
    }
}
