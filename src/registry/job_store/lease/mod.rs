//! Lease engine for the durable job queue.
//!
//! Concerns the lifecycle of "I own this `lock_key` for the next ttl seconds":
//! acquire → heartbeat → release. The implementation is intentionally hidden
//! behind [`LeaseGuard`] — callers never see the token, the body shape, or
//! the heartbeat task — so the same [`DurableJobConsumer`] code drives
//! whichever backend was wired at construction.
//!
//! [`DurableJobConsumer`]: crate::registry::job_store::durable::DurableJobConsumer
//!
//! Two backends exist today:
//!
//! - [`locked::Backend`] coordinates lease writes through a
//!   [`LockBackend`](crate::registry::metadata_store::lock::LockBackend);
//!   used by the FS storage backend (and any other non-CAS store).
//! - [`conditional::Backend`] uses real `If-Match` / `If-None-Match` CAS;
//!   used by the S3 storage backend.

use std::{future::Future, pin::Pin};

use async_trait::async_trait;
use tokio::{runtime::Handle, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::registry::job_store::Error;

pub mod conditional;
pub mod locked;

type AsyncReleaseFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Handle to an acquired lease. Owns the heartbeat task and the release
/// closure; aborts/releases on [`release`](Self::release) or [`Drop`].
///
/// Consumers observe [`lost_token`](Self::lost_token) to detect heartbeat
/// failures (and drop the guard once the work is done so the lease cleanup
/// runs). Mirrors the shape of
/// [`LockGuard`](crate::registry::metadata_store::lock::LockGuard).
pub struct LeaseGuard {
    lost: CancellationToken,
    heartbeat: Option<JoinHandle<()>>,
    release_fn: Option<AsyncReleaseFn>,
}

impl LeaseGuard {
    /// Build a guard from a heartbeat task and an async release closure.
    /// The `lost` token is cancelled by the heartbeat task on terminal
    /// failure; consumers `select!` on it alongside their handler future.
    pub fn new(
        lost: CancellationToken,
        heartbeat: JoinHandle<()>,
        release_fn: impl FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
    ) -> Self {
        Self {
            lost,
            heartbeat: Some(heartbeat),
            release_fn: Some(Box::new(release_fn)),
        }
    }

    /// Token cancelled when the heartbeat task gives up on the lease.
    /// `select!` on it alongside the handler future to abort work whose
    /// lease has been lost; `.is_cancelled()` doubles as an `is_valid` probe.
    pub fn lost_token(&self) -> CancellationToken {
        self.lost.clone()
    }

    /// Stop the heartbeat and run the release closure. Always prefer this
    /// over relying on [`Drop`] — drop spawns the release fire-and-forget,
    /// which means errors can't be observed and the runtime might exit
    /// before the spawned task runs.
    pub async fn release(mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
        if let Some(f) = self.release_fn.take() {
            f().await;
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
        if let Some(f) = self.release_fn.take() {
            if let Ok(rt) = Handle::try_current() {
                rt.spawn(async move {
                    f().await;
                });
            } else {
                warn!("No tokio runtime available for async lease release on drop");
            }
        }
    }
}

/// Lease acquisition surface. Implementations encapsulate the storage shape
/// of the lease body, the atomicity primitive (lock vs. CAS), and the
/// heartbeat task that keeps the lease alive.
#[async_trait]
pub trait LeaseBackend: Send + Sync {
    /// Try to acquire the lease for `lock_key` for `ttl_secs`. Returns
    /// `Some(guard)` when the caller has exclusive ownership (a fresh
    /// acquisition or a successful steal of a stale lease); `None` when
    /// another worker holds a non-stale lease.
    ///
    /// `worker_id` is recorded in the lease body for diagnostics.
    async fn try_acquire(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<LeaseGuard>, Error>;
}

/// Consecutive heartbeat failures tolerated before the lease is treated as
/// lost. Combined with the `ttl / 3` tick this gives roughly one TTL window
/// of grace before the worker bails out. Shared by both backend impls so
/// behaviour stays uniform.
pub const MAX_HEARTBEAT_FAILURES: u32 = 3;
