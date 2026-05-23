//! TOML config + factory for filesystem-backed job queues.
//!
//! Wires up two backends from a single shared `LockBackend`:
//!
//! - `locked::Backend` ([`JobStore`]) for the pending/failed/dedup primitives.
//! - `lease::locked::Backend` ([`LeaseBackend`]) for the per-`lock_key` lease
//!   lifecycle.
//!
//! Both layers run over the same `angos_storage::fs::Backend` and the same
//! `LockBackend`, so operators only configure storage and lock strategy
//! once.

use std::sync::Arc;

use serde::Deserialize;
use tracing::info;

use crate::registry::{
    job_store::{
        Error, JobBackends, JobStore,
        lease::{self, LeaseBackend},
        locked,
    },
    metadata_store::{
        LockConfig, LockStrategy,
        lock::{self, LockBackend, MemoryBackend},
    },
};
use angos_storage::fs::Backend as StorageFsBackend;

#[derive(Clone, Debug, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    pub lock_strategy: LockStrategy,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            root_dir: String::new(),
            lock_strategy: LockStrategy::Memory,
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
        }

        let raw = Raw::deserialize(deserializer)?;

        let lock_strategy = lock::resolve_lock_strategy(raw.lock_strategy, raw.redis, false)?;

        Ok(BackendConfig {
            root_dir: raw.root_dir,
            lock_strategy,
        })
    }
}

impl BackendConfig {
    /// Build the storage + lease backends for this FS deployment.
    ///
    /// Default `lock_strategy = "memory"` is single-process scope;
    /// multi-process worker pools on a shared FS mount must configure
    /// `lock_strategy.redis`.
    pub fn to_backends(&self) -> Result<JobBackends, Error> {
        let storage = Arc::new(
            StorageFsBackend::builder()
                .root_dir(&self.root_dir)
                .build()
                .map_err(|e| Error::Initialization(e.to_string()))?,
        );

        let lock: Arc<dyn LockBackend + Send + Sync> = match &self.lock_strategy {
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock store for filesystem job store");
                let backend = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Initialization(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(backend)
            }
            LockStrategy::S3(_) => {
                return Err(Error::Initialization(
                    "S3 lock strategy is not supported for filesystem job store".to_string(),
                ));
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for filesystem job store");
                Arc::new(MemoryBackend::new())
            }
        };

        let store: Arc<dyn JobStore> = Arc::new(
            locked::Backend::builder()
                .store(storage.clone())
                .lock(lock.clone())
                .build()?,
        );

        let leases: Arc<dyn LeaseBackend> = Arc::new(
            lease::locked::Backend::builder()
                .store(storage)
                .lock(lock)
                .build()?,
        );

        Ok(JobBackends { store, leases })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::TempDir;

    use crate::{
        metrics_provider,
        registry::{
            job_store::{
                JobEnvelope, JobQueue,
                durable::{DurableJobConsumer, DurableJobQueue},
            },
            path_builder,
        },
    };

    use super::*;

    fn smoke_envelope(lock_key: &str) -> JobEnvelope {
        JobEnvelope::new("cache", "test.noop", lock_key, &()).expect("envelope")
    }

    #[tokio::test]
    async fn config_builds_a_working_job_store() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("tempdir");
        let backends = BackendConfig {
            root_dir: dir.path().to_string_lossy().into_owned(),
            ..BackendConfig::default()
        }
        .to_backends()
        .expect("build");

        let queue = DurableJobQueue::new(backends.store.clone());
        let consumer =
            DurableJobConsumer::new(backends.store, backends.leases, 30, "smoke".to_string());

        queue
            .enqueue(smoke_envelope("smoke.fs.lock"))
            .await
            .expect("enqueue");

        let claimed = consumer
            .claim_one("cache")
            .await
            .expect("claim")
            .claimed
            .expect("Some");
        consumer.complete(claimed).await.expect("complete");

        assert!(
            consumer
                .claim_one("cache")
                .await
                .expect("claim")
                .claimed
                .is_none(),
            "queue must be empty after complete",
        );
    }

    /// The lock-key dedup index lives at a stable path. Once `try_claim`
    /// succeeds, the index object exists on disk and a sibling lookup sees
    /// it.
    #[tokio::test]
    async fn lock_key_index_persisted_at_expected_path() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().expect("tempdir");
        let backends = BackendConfig {
            root_dir: dir.path().to_string_lossy().into_owned(),
            ..BackendConfig::default()
        }
        .to_backends()
        .expect("build");

        let env = smoke_envelope("cache.ns:sha256:fs-index");
        let storage_key = backends
            .store
            .put_pending("cache", &env, Utc::now())
            .await
            .expect("put");
        assert!(
            backends
                .store
                .try_claim_lock_key("cache", &env.lock_key, &storage_key)
                .await
                .expect("claim"),
            "fresh lock_key must be claimable",
        );

        let index_path = dir
            .path()
            .join(path_builder::job_lock_key_index_path("cache", &env.lock_key));
        let raw = tokio::fs::read(&index_path).await.expect("read index");
        assert!(!raw.is_empty(), "index file must have content");

        assert!(
            backends
                .store
                .find_pending_with_lock_key("cache", &env.lock_key)
                .await
                .expect("dedup"),
            "lookup must hit via the index",
        );
    }
}
