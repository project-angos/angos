//! Dedicated single-instance maintenance lock shared by `scrub`, `policy`, and
//! `replication`.
//!
//! Two overlapping maintenance runs would corrupt each other's mutations, so
//! this builds a long-hold [`Lock`] from the metadata store's [`LockStrategy`]
//! and acquires `maintenance:registry` at run entry, refusing if another
//! maintenance command holds it.
//!
//! Unlike the commit-path executor lock (300s max-hold), this pairs a small `ttl`
//! (fast heartbeat) with a long `max_hold` and a wide recovery margin, so a live
//! holder is never stolen while a crashed one is still reclaimed.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
};

use angos_tx_engine::lock::{
    LockSession, LockStrategy,
    primitive::Lock,
    storage::{LockStorage, memory::MemoryLockStorage, redis::RedisLockStorage, s3::S3LockStorage},
};

use crate::{command::scrub::error::Error, registry::metadata_store::MetadataStore};

/// Lock object key guarding the whole registry against a concurrent
/// maintenance run (`scrub`, `policy`, or `replication`).
pub const MAINTENANCE_LOCK_KEY: &str = "maintenance:registry";

/// Small TTL so the heartbeat fires every `ttl/3` seconds (builder minimum 9),
/// keeping the crash-recovery window tight.
const MAINTENANCE_LOCK_TTL_SECS: u64 = 9;

/// Steal a contended lock only after the holder has been stale this long: wide
/// enough that a briefly-lagging live holder is never stolen.
const MAINTENANCE_LOCK_RECOVERY_MARGIN_SECS: u64 = 3_600;

/// Process-wide memory lock storages, keyed by the registry identity, so two
/// in-process maintenance commands over the same registry contend while
/// distinct registries (and parallel tests) stay isolated.
static MEMORY_LOCK_STORAGES: LazyLock<Mutex<HashMap<String, Arc<MemoryLockStorage>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The shared in-process lock storage for `registry_id`.
fn memory_storage_for(registry_id: &str) -> Arc<MemoryLockStorage> {
    let mut storages = match MEMORY_LOCK_STORAGES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    storages
        .entry(registry_id.to_string())
        .or_insert_with(|| Arc::new(MemoryLockStorage::new()))
        .clone()
}

/// Build the long-hold [`Lock`] from `lock_strategy`, reusing the metadata
/// store's conditional store for the S3 path. `registry_id` scopes the shared
/// in-process memory storage; `max_hold_secs` is the operator's
/// `maintenance_lock_max_hold_secs`.
pub fn build(
    lock_strategy: &LockStrategy,
    metadata_store: &MetadataStore,
    registry_id: &str,
    max_hold_secs: u64,
) -> Result<Lock, Error> {
    let storage: Arc<dyn LockStorage> = match lock_strategy {
        LockStrategy::Memory => memory_storage_for(registry_id),
        LockStrategy::Redis(config) => Arc::new(RedisLockStorage::new(config).map_err(|e| {
            Error::Initialization(format!(
                "failed to build maintenance redis lock storage: {e}"
            ))
        })?),
        LockStrategy::S3(_) => {
            let conditional = metadata_store
                .executor()
                .conditional_store()
                .ok_or_else(|| {
                    Error::Initialization(
                    "S3 lock strategy requires a conditional store on the metadata executor, but \
                     none is wired"
                        .into(),
                )
                })?;
            Arc::new(S3LockStorage::new(conditional, true))
        }
    };

    Lock::builder(storage)
        .ttl_secs(MAINTENANCE_LOCK_TTL_SECS)
        .max_hold_secs(max_hold_secs)
        .recovery_margin_secs(MAINTENANCE_LOCK_RECOVERY_MARGIN_SECS)
        .build()
        .map_err(|e| Error::Initialization(format!("failed to build maintenance lock: {e}")))
}

/// Acquire the single-instance maintenance lock, refusing when another
/// maintenance command holds it. Returns [`Error::Initialization`] on
/// contention, or the storage error on a hard failure.
pub async fn acquire(
    lock_strategy: &LockStrategy,
    metadata_store: &MetadataStore,
    registry_id: &str,
    max_hold_secs: u64,
) -> Result<LockSession, Error> {
    let lock = build(lock_strategy, metadata_store, registry_id, max_hold_secs)?;
    match lock.try_acquire(&[MAINTENANCE_LOCK_KEY.to_string()]).await {
        Ok(Some(session)) => Ok(session),
        Ok(None) => Err(Error::Initialization(
            "another maintenance command (scrub, policy, or replication) already holds the \
             registry lock (maintenance:registry); refusing to start a concurrent run"
                .into(),
        )),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use angos_storage::MemoryObjectStore;

    use super::*;
    use crate::registry::test_utils::{locked_executor_over, metadata_store_over};

    fn memory_metadata_store() -> Arc<MetadataStore> {
        let raw = Arc::new(MemoryObjectStore::new());
        metadata_store_over(raw.clone(), locked_executor_over(raw))
    }

    /// Two builds over the same registry id share one in-process storage: the
    /// second `try_acquire` sees the held key. A distinct registry id stays
    /// isolated and acquires freely.
    #[tokio::test]
    async fn memory_storage_is_shared_per_registry_id() {
        let store = memory_metadata_store();
        let id = format!("fs:{}", uuid::Uuid::new_v4());

        let first = build(&LockStrategy::Memory, &store, &id, 86_400).unwrap();
        let session = first
            .try_acquire(&[MAINTENANCE_LOCK_KEY.to_string()])
            .await
            .unwrap()
            .expect("first acquire succeeds");

        let second = build(&LockStrategy::Memory, &store, &id, 86_400).unwrap();
        assert!(
            second
                .try_acquire(&[MAINTENANCE_LOCK_KEY.to_string()])
                .await
                .unwrap()
                .is_none(),
            "a second in-process build over the same registry must contend"
        );

        let other_id = format!("fs:{}", uuid::Uuid::new_v4());
        let other = build(&LockStrategy::Memory, &store, &other_id, 86_400).unwrap();
        let other_session = other
            .try_acquire(&[MAINTENANCE_LOCK_KEY.to_string()])
            .await
            .unwrap()
            .expect("a distinct registry id must not contend");

        other_session.release().await;
        session.release().await;
    }

    /// The operator knob reaches the lock builder: a max hold below the TTL is
    /// rejected at build.
    #[test]
    fn max_hold_below_ttl_is_rejected() {
        let store = memory_metadata_store();
        let result = build(&LockStrategy::Memory, &store, "fs:/tmp/x", 1);
        assert!(
            matches!(result, Err(Error::Initialization(_))),
            "max_hold_secs below the ttl must fail the lock build"
        );
    }
}
