//! Lock-coordinated [`LeaseBackend`] for non-CAS stores.
//!
//! Built over an [`ObjectStore`] for body persistence and a [`LockBackend`]
//! for serialising read-modify-write on the lease body. Used by the FS
//! storage backend; works with any [`LockBackend`] (Memory / Redis / …)
//! so multi-process FS deployments can share a Redis lock.
//!
//! Lease body shape: `{ instance_id, refreshed_at, worker_id, ttl_secs }`.
//! `instance_id` is a fresh UUID written on every successful acquire/steal
//! and compared under the lock during heartbeat/release to detect ownership
//! loss — the analogue of the storage `Etag` token used by the conditional
//! backend.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    select, spawn,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::registry::{
    job_store::{
        Error,
        lease::{LeaseBackend, LeaseGuard, MAX_HEARTBEAT_FAILURES},
        with_lock,
    },
    metadata_store::lock::LockBackend,
    path_builder,
};
use angos_storage::{Error as StorageError, ObjectStore};

/// Builder for [`Backend`].
pub struct Builder {
    store: Option<Arc<dyn ObjectStore>>,
    lock: Option<Arc<dyn LockBackend + Send + Sync>>,
}

impl Builder {
    fn new() -> Self {
        Self {
            store: None,
            lock: None,
        }
    }

    /// The underlying object store (required).
    pub fn store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// The lock backend used to serialise read-modify-write on the lease
    /// body (required).
    pub fn lock(mut self, lock: Arc<dyn LockBackend + Send + Sync>) -> Self {
        self.lock = Some(lock);
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let store = self.store.ok_or_else(|| {
            Error::Initialization("lease::locked::Backend requires a store".to_string())
        })?;
        let lock = self.lock.ok_or_else(|| {
            Error::Initialization("lease::locked::Backend requires a lock".to_string())
        })?;
        Ok(Backend { store, lock })
    }
}

/// Lock-coordinated [`LeaseBackend`] implementation.
pub struct Backend {
    store: Arc<dyn ObjectStore>,
    lock: Arc<dyn LockBackend + Send + Sync>,
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::new()
    }
}

#[derive(Serialize, Deserialize)]
struct LeaseFile {
    /// Owner token. Defaults to `""` so any pre-existing body without this
    /// field is immediately treated as "ownership lost" on heartbeat.
    #[serde(default)]
    instance_id: String,
    /// Wall-clock instant the body was last written. Defaults to `MIN_UTC`
    /// so any body missing the field is immediately stealable.
    #[serde(default)]
    refreshed_at: DateTime<Utc>,
    worker_id: String,
    ttl_secs: u64,
}

fn serialize_lease(instance_id: &str, worker_id: &str, ttl_secs: u64) -> Result<Bytes, Error> {
    let body = LeaseFile {
        instance_id: instance_id.to_string(),
        refreshed_at: Utc::now(),
        worker_id: worker_id.to_string(),
        ttl_secs,
    };
    serde_json::to_vec(&body)
        .map(Bytes::from)
        .map_err(|e| Error::Storage(format!("lease serialization failed: {e}")))
}

fn parse_lease(bytes: &[u8]) -> Result<LeaseFile, Error> {
    serde_json::from_slice(bytes).map_err(|e| Error::Storage(format!("corrupt lease: {e}")))
}

fn lease_lock_key(lock_key: &str) -> String {
    format!("lease:{lock_key}")
}

/// Issue a single heartbeat write under the lease lock. Returns `Ok(())`
/// when the body still has our `instance_id` and we successfully rewrote
/// it; returns `Err` on ownership loss or storage error.
async fn heartbeat_once(
    store: &dyn ObjectStore,
    lock: &(dyn LockBackend + Send + Sync),
    lock_key: &str,
    token: &str,
    worker_id: &str,
    ttl_secs: u64,
) -> Result<(), Error> {
    let lock_keys = [lease_lock_key(lock_key)];
    with_lock(
        lock,
        &lock_keys,
        "lock invalidated during heartbeat",
        || async {
            let path = path_builder::job_lease_path(lock_key);
            let bytes = match store.get(&path).await {
                Ok(b) => b,
                Err(StorageError::NotFound) => {
                    return Err(Error::Storage(
                        "heartbeat failed: lease vanished".to_string(),
                    ));
                }
                Err(e) => return Err(Error::from(e)),
            };
            let existing = parse_lease(&bytes)?;
            if existing.instance_id != token {
                return Err(Error::Storage(
                    "heartbeat failed: lease ownership changed".to_string(),
                ));
            }
            let payload = serialize_lease(token, worker_id, ttl_secs)?;
            store.put(&path, payload).await?;
            Ok(())
        },
    )
    .await
}

/// Best-effort release under the lock: delete the lease body only when its
/// `instance_id` still matches our token. Mismatch or absent body is a
/// no-op (a successful stealer or our own earlier cleanup).
async fn release_once(
    store: &dyn ObjectStore,
    lock: &(dyn LockBackend + Send + Sync),
    lock_key: &str,
    token: &str,
) {
    let lock_keys = [lease_lock_key(lock_key)];
    let result = with_lock(
        lock,
        &lock_keys,
        "lock invalidated during lease release",
        || async {
            let path = path_builder::job_lease_path(lock_key);
            let bytes = match store.get(&path).await {
                Ok(b) => b,
                Err(StorageError::NotFound) => return Ok(()),
                Err(e) => return Err(Error::from(e)),
            };
            if let Ok(existing) = parse_lease(&bytes)
                && existing.instance_id == token
            {
                store.delete(&path).await?;
            }
            Ok(())
        },
    )
    .await;
    if let Err(e) = result {
        warn!(lock_key, error = %e, "Lease release failed");
    }
}

#[async_trait]
impl LeaseBackend for Backend {
    async fn try_acquire(
        &self,
        lock_key: &str,
        worker_id: &str,
        ttl_secs: u64,
    ) -> Result<Option<LeaseGuard>, Error> {
        let lock_keys = [lease_lock_key(lock_key)];
        let token = with_lock(
            &*self.lock,
            &lock_keys,
            "lock invalidated during lease acquire",
            || async {
                let path = path_builder::job_lease_path(lock_key);
                match self.store.get(&path).await {
                    Ok(bytes) => {
                        let existing = parse_lease(&bytes)?;
                        let ttl =
                            ChronoDuration::seconds(existing.ttl_secs.min(3600).cast_signed());
                        if Utc::now() <= existing.refreshed_at + ttl {
                            return Ok(None);
                        }
                        debug!(
                            lock_key,
                            worker_id = existing.worker_id,
                            refreshed_at = %existing.refreshed_at,
                            "Stealing stale lease"
                        );
                        let instance_id = Uuid::new_v4().to_string();
                        let payload = serialize_lease(&instance_id, worker_id, ttl_secs)?;
                        self.store.put(&path, payload).await?;
                        Ok(Some(instance_id))
                    }
                    Err(StorageError::NotFound) => {
                        let instance_id = Uuid::new_v4().to_string();
                        let payload = serialize_lease(&instance_id, worker_id, ttl_secs)?;
                        self.store.put(&path, payload).await?;
                        Ok(Some(instance_id))
                    }
                    Err(e) => Err(Error::from(e)),
                }
            },
        )
        .await?;

        let Some(token) = token else {
            return Ok(None);
        };

        let lost = CancellationToken::new();
        let heartbeat = spawn_heartbeat(
            self.store.clone(),
            self.lock.clone(),
            lock_key.to_string(),
            token.clone(),
            worker_id.to_string(),
            ttl_secs,
            lost.clone(),
        );

        let release_store = self.store.clone();
        let release_lock = self.lock.clone();
        let release_lock_key = lock_key.to_string();
        let release_token = token;
        let release_fn = move || -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                release_once(
                    &*release_store,
                    &*release_lock,
                    &release_lock_key,
                    &release_token,
                )
                .await;
            })
        };

        Ok(Some(LeaseGuard::new(lost, heartbeat, release_fn)))
    }
}

fn spawn_heartbeat(
    store: Arc<dyn ObjectStore>,
    lock: Arc<dyn LockBackend + Send + Sync>,
    lock_key: String,
    token: String,
    worker_id: String,
    ttl_secs: u64,
    lost: CancellationToken,
) -> JoinHandle<()> {
    let tick = Duration::from_secs(ttl_secs / 3);
    spawn(async move {
        let mut timer = interval(tick);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Consume immediate first tick so first heartbeat happens after `tick`.
        timer.tick().await;

        let mut consecutive_failures: u32 = 0;
        loop {
            select! {
                () = lost.cancelled() => return,
                _ = timer.tick() => {}
            }
            match heartbeat_once(&*store, &*lock, &lock_key, &token, &worker_id, ttl_secs).await {
                Ok(()) => consecutive_failures = 0,
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    warn!(lock_key, error = %e, consecutive_failures, "Lease heartbeat failed");
                    if consecutive_failures >= MAX_HEARTBEAT_FAILURES {
                        warn!(lock_key, "Too many heartbeat failures, marking lease lost");
                        lost.cancel();
                        return;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests;
