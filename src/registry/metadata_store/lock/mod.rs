use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::registry::metadata_store::Error;

pub mod memory;
pub mod redis;
pub mod s3;

pub use memory::MemoryBackend;
pub use redis::RedisBackend;
pub use s3::S3LockBackend;

type AsyncReleaseFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Holds a distributed lock acquired via [`LockBackend::acquire`].
///
/// Call [`release()`](Self::release) to explicitly release the lock. If dropped without
/// calling `release()`, the heartbeat is aborted synchronously and a fire-and-forget async
/// task is spawned to clean up remote lock objects.
pub struct LockGuard {
    sync_guard: Option<Box<dyn Send>>,
    async_release: Option<AsyncReleaseFn>,
    valid: Option<Arc<AtomicBool>>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl LockGuard {
    pub fn sync(guard: Box<dyn Send>) -> Self {
        Self {
            sync_guard: Some(guard),
            async_release: None,
            valid: None,
            heartbeat_handle: None,
        }
    }

    pub fn with_async_release(
        release_fn: impl FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
    ) -> Self {
        Self {
            sync_guard: None,
            async_release: Some(Box::new(release_fn)),
            valid: None,
            heartbeat_handle: None,
        }
    }

    pub fn with_async_release_and_heartbeat(
        release_fn: impl FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
        valid: Arc<AtomicBool>,
        heartbeat_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            sync_guard: None,
            async_release: Some(Box::new(release_fn)),
            valid: Some(valid),
            heartbeat_handle: Some(heartbeat_handle),
        }
    }

    pub fn is_valid(&self) -> bool {
        self.valid
            .as_ref()
            .is_none_or(|v| v.load(Ordering::Acquire))
    }

    pub async fn release(mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        if let Some(release_fn) = self.async_release.take() {
            release_fn().await;
        }
        if let Some(valid) = &self.valid {
            valid.store(false, Ordering::Release);
        }
        drop(self.sync_guard.take());
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        if let Some(valid) = &self.valid {
            valid.store(false, Ordering::Release);
        }
        if let Some(release_fn) = self.async_release.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    release_fn().await;
                });
            } else {
                warn!("No tokio runtime available for async lock release on drop");
            }
        }
    }
}

#[async_trait]
pub trait LockBackend: Send + Sync + Debug {
    async fn acquire(&self, keys: &[String]) -> Result<LockGuard, Error>;
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LockStrategy {
    Memory,
    Redis(redis::LockConfig),
    S3(s3::S3LockConfig),
}

pub fn resolve_lock_strategy<E: serde::de::Error>(
    lock_strategy: Option<LockStrategy>,
    redis: Option<redis::LockConfig>,
    allow_s3: bool,
) -> Result<LockStrategy, E> {
    match (lock_strategy, redis) {
        (Some(_), Some(_)) => Err(E::custom(
            "cannot set both 'lock_strategy' and 'redis'; use lock_strategy.redis instead",
        )),
        (Some(LockStrategy::S3(_)), None) if !allow_s3 => Err(E::custom(
            "S3 lock strategy is not supported for filesystem metadata store",
        )),
        (Some(strategy), None) => Ok(strategy),
        (None, Some(redis_config)) => Ok(LockStrategy::Redis(redis_config)),
        (None, None) => Ok(LockStrategy::Memory),
    }
}
