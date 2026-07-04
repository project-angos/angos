//! Distributed-lock primitive for the transaction engine.
//!
//! The single concrete type is [`Lock`]. Callers never see a RAII guard:
//! they call [`Lock::acquire`] (blocking retry) or [`Lock::try_acquire`]
//! (non-blocking, single attempt) with the keys to lock, run their work, then
//! await [`LockSession::release`], always on the calling task's own call
//! path. A background heartbeat task refreshes the lock TTL and fires the
//! session's [`CancellationToken`] when ownership is lost, so a caller racing
//! its operation against [`LockSession::cancellation`] can short-circuit to
//! [`Error::Invalidated`] at the next await point.
//!
//! ## Lifetime contract
//!
//! - **Release**: the happy path awaits [`LockSession::release`] on the
//!   calling task's own call path. If the session is instead dropped before
//!   `release` runs (outer task cancellation), [`LockSession::Drop`]
//!   best-effort spawns the async release on the current Tokio runtime so the
//!   remote lock is freed promptly instead of waiting on TTL. The spawn is
//!   fire-and-forget; if no runtime is available the lock expires via TTL.
//! - **Heartbeat failure**: the heartbeat task fires the
//!   [`CancellationToken`] when the heartbeat tick fails (ownership lost,
//!   refresh failed, max hold exceeded). Callers that race their operation
//!   against the token short-circuit to [`Error::Invalidated`] at the next
//!   await point.
//!
//! ## Storage flavours
//!
//! [`Lock`] is parameterised by a [`LockStorage`] implementation selected at
//! startup from the operator's `lock_strategy` config:
//!
//! | `lock_strategy` | [`LockStorage`] impl | Notes |
//! |---|---|---|
//! | `memory` | [`MemoryLockStorage`] | In-process; single-process only (default for FS deployments) |
//! | `redis`  | [`RedisLockStorage`] | Feature `redis`; suitable for FS stores under heavy load |
//! | `s3`     | [`S3LockStorage`] | CAS-capable S3; uses `.tx-locks/<shard>/<key>` objects |

use std::{fmt::Debug, future::Future, pin::Pin};

use serde::Deserialize;
use tokio::{runtime::Handle, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub mod metrics;
pub mod primitive;
pub mod storage;

/// Errors produced by lock operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("lock error: {0}")]
    Lock(String),
    #[error("invalid data: {0}")]
    InvalidData(String),
    #[error("storage backend error: {0}")]
    StorageBackend(String),
    /// The heartbeat fired mid-operation.
    #[error("lock invalidated mid-operation")]
    Invalidated,
    /// Key was not found (used by `LockStorage` implementations).
    #[error("lock object not found")]
    NotFound,
}

#[cfg(feature = "redis")]
impl From<::redis::RedisError> for Error {
    fn from(err: ::redis::RedisError) -> Self {
        Error::Lock(format!("Redis error: {err}"))
    }
}

type AsyncReleaseFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Opaque bookkeeping returned by [`Lock::acquire`].
///
/// Backends construct one of these from their `acquire` impl; callers consume
/// it by awaiting [`release`](Self::release) and nothing else should reach for
/// `release` / `cancellation` directly.
///
/// **Release contract:** the happy path runs through
/// [`release`](Self::release), awaited on the calling task's call path before
/// returning. [`Drop`] is a best-effort fallback that fires when the session is
/// dropped without an explicit release (outer task cancellation): it aborts the
/// heartbeat synchronously and spawns the async release on the current Tokio
/// runtime so the remote lock is freed without waiting on TTL. If no runtime is
/// available the remote lock expires via the backend's TTL.
pub struct LockSession {
    sync_guard: Option<Box<dyn Send>>,
    async_release: Option<AsyncReleaseFn>,
    /// Fired by the backend's heartbeat task to signal lock-ownership
    /// loss.
    cancellation: CancellationToken,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl LockSession {
    /// In-process session. The `Drop` of `guard` is the entire release.
    /// No heartbeat, no remote release.
    #[must_use]
    pub fn sync(guard: Box<dyn Send>) -> Self {
        Self {
            sync_guard: Some(guard),
            async_release: None,
            cancellation: CancellationToken::new(),
            heartbeat_handle: None,
        }
    }

    /// Distributed session backed by an async release and a heartbeat task.
    pub fn with_async_release_and_heartbeat(
        release_fn: impl FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
        cancellation: CancellationToken,
        heartbeat_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            sync_guard: None,
            async_release: Some(Box::new(release_fn)),
            cancellation,
            heartbeat_handle: Some(heartbeat_handle),
        }
    }

    /// Clone the session's cancellation token so callers can race their
    /// operation against heartbeat-loss events.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Release the lock on the calling task's call path.
    pub async fn release(mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        if let Some(release_fn) = self.async_release.take() {
            release_fn().await;
        }
        drop(self.sync_guard.take());
    }
}

impl Drop for LockSession {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        if let Some(release_fn) = self.async_release.take() {
            if let Ok(runtime) = Handle::try_current() {
                runtime.spawn(release_fn());
            } else {
                debug!("LockSession::drop: no Tokio runtime; remote lock will expire via TTL");
            }
        }
    }
}

// Lock strategy config

/// Parsed configuration for the S3-backed lock storage.
///
/// This is a DTO: deserialized from operator config and used to construct an
/// [`S3LockStorage`]. Not held as a field on any runtime struct.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct S3LockConfig {
    #[serde(
        default = "S3LockConfig::default_ttl_secs",
        deserialize_with = "deserialize_ttl_secs"
    )]
    pub ttl_secs: u64,
    #[serde(default = "S3LockConfig::default_max_retries")]
    pub max_retries: u32,
    #[serde(
        default = "S3LockConfig::default_retry_delay_ms",
        deserialize_with = "deserialize_retry_delay_ms"
    )]
    pub retry_delay_ms: u64,
    #[serde(default = "S3LockConfig::default_max_hold_secs")]
    pub max_hold_secs: u64,
    /// Timeout (seconds) for a single storage operation. Defaults to 15.
    #[serde(default = "S3LockConfig::default_operation_timeout_secs")]
    pub operation_timeout_secs: u64,
    /// Timeout (seconds) per attempt inside a retried storage operation. Defaults to 4.
    #[serde(default = "S3LockConfig::default_operation_attempt_timeout_secs")]
    pub operation_attempt_timeout_secs: u64,
    /// Maximum attempts per storage operation. Defaults to 2.
    #[serde(default = "S3LockConfig::default_max_attempts")]
    pub max_attempts: u32,
}

fn deserialize_ttl_secs<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 9 {
        return Err(serde::de::Error::custom("ttl_secs must be at least 9"));
    }
    Ok(value)
}

fn deserialize_retry_delay_ms<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value < 1 {
        return Err(serde::de::Error::custom(
            "retry_delay_ms must be at least 1",
        ));
    }
    Ok(value)
}

impl S3LockConfig {
    fn default_ttl_secs() -> u64 {
        30
    }
    fn default_max_retries() -> u32 {
        100
    }
    fn default_retry_delay_ms() -> u64 {
        50
    }
    fn default_max_hold_secs() -> u64 {
        300
    }
    fn default_operation_timeout_secs() -> u64 {
        15
    }
    fn default_operation_attempt_timeout_secs() -> u64 {
        4
    }
    fn default_max_attempts() -> u32 {
        2
    }
}

impl Default for S3LockConfig {
    fn default() -> Self {
        Self {
            ttl_secs: Self::default_ttl_secs(),
            max_retries: Self::default_max_retries(),
            retry_delay_ms: Self::default_retry_delay_ms(),
            max_hold_secs: Self::default_max_hold_secs(),
            operation_timeout_secs: Self::default_operation_timeout_secs(),
            operation_attempt_timeout_secs: Self::default_operation_attempt_timeout_secs(),
            max_attempts: Self::default_max_attempts(),
        }
    }
}

/// Lock strategy configuration.
///
/// Determines which [`LockStorage`] implementation is constructed at startup.
/// Deserialized from operator configuration; selection is per-deployment.
/// `lock_strategy = "memory" | "redis" | "s3"` selects the lock-object storage
/// backend.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LockStrategy {
    Memory,
    #[cfg(feature = "redis")]
    Redis(storage::redis::RedisLockStorageConfig),
    S3(S3LockConfig),
}

/// Resolve the configured [`LockStrategy`] from operator config, applying
/// precedence rules and validating constraints.
///
/// Returns `Ok(None)` when the operator configured nothing; the caller picks
/// the backend-appropriate default (memory for FS, CAS-dependent for S3).
/// Returns a `serde::de::Error` for any configuration conflict so this
/// function can be called from a custom `Deserialize` impl.
///
/// # Errors
///
/// Returns `Err(E::custom(...))` when both `lock_strategy` and `redis` are
/// provided, when S3 lock strategy is requested on a non-S3 metadata store,
/// or when the Redis feature is not enabled.
#[allow(
    clippy::ignored_unit_patterns,
    reason = "redis param is Option<()> when feature is disabled; () wildcard is correct in that branch"
)]
pub fn resolve_lock_strategy<E: serde::de::Error>(
    lock_strategy: Option<LockStrategy>,
    #[cfg(feature = "redis")] redis: Option<storage::redis::RedisLockStorageConfig>,
    #[cfg(not(feature = "redis"))] redis: Option<()>,
    allow_s3: bool,
) -> Result<Option<LockStrategy>, E> {
    match (lock_strategy, redis) {
        (Some(_), Some(_)) => Err(E::custom(
            "cannot set both 'lock_strategy' and 'redis'; use lock_strategy.redis instead",
        )),
        (Some(LockStrategy::S3(_)), None) if !allow_s3 => Err(E::custom(
            "S3 lock strategy is not supported for filesystem metadata store",
        )),
        (Some(strategy), None) => Ok(Some(strategy)),
        #[cfg(feature = "redis")]
        (None, Some(redis_config)) => Ok(Some(LockStrategy::Redis(redis_config))),
        #[cfg(not(feature = "redis"))]
        (None, Some(_)) => Err(E::custom(
            "redis lock strategy requested but the 'redis' feature is not enabled",
        )),
        (None, None) => Ok(None),
    }
}
