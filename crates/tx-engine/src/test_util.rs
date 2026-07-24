//! Shared construction helpers for tx-engine tests.
//!
//! One home for the in-memory lock and executor fixtures the unit and
//! integration tests build over [`MemoryObjectStore`]-backed stores, plus the
//! stale-intent staging, one-shot recovery-sweep, and orphan-assertion
//! recipes, instead of each test module re-rolling the same builder chains.
//! Enabled for this crate's own tests and, through the `test-util` feature,
//! for downstream crates' dev-dependencies.

use std::sync::Arc;

use bytes::Bytes;
use chrono::{Duration, Utc};
use uuid::Uuid;

use angos_storage::{ConditionalStore, ObjectStore};

use crate::{
    executor::{cas::CasExecutor, locked::LockedExecutor},
    intent::{IntentRecord, MutationProgress, MutationRecord, body_ref_key},
    lock::{primitive::Lock, storage::memory::MemoryLockStorage},
    recovery::RecoveryLoop,
};

/// Build a fresh in-memory lock primitive.
///
/// # Panics
/// When the lock builder rejects its defaults, which cannot happen for the
/// in-memory storage.
#[must_use]
pub fn memory_lock() -> Arc<Lock> {
    Arc::new(
        Lock::builder(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("lock builder"),
    )
}

/// Build a `LockedExecutor` over `store`, serialising on `lock`.
pub fn locked_executor(store: Arc<dyn ObjectStore>, lock: Arc<Lock>) -> Arc<LockedExecutor> {
    Arc::new(LockedExecutor::builder(store, lock).build())
}

/// Build a `CasExecutor` over `store`, serialising coarse locks on `lock`.
pub fn cas_executor(store: Arc<dyn ConditionalStore>, lock: Arc<Lock>) -> Arc<CasExecutor> {
    Arc::new(CasExecutor::builder(store, lock).build())
}

/// Stage a mutation body at its `.tx-bodies/{tx_id}/{index}` key.
///
/// Returns the staging key, ready to use as the mutation's `body_ref`.
///
/// # Panics
/// When the staging write fails.
pub async fn stage_body(store: &dyn ObjectStore, tx_id: Uuid, index: usize, body: Bytes) -> String {
    let body_ref = body_ref_key(tx_id, index);
    store.put(&body_ref, body).await.expect("stage body");
    body_ref
}

/// Build an [`IntentRecord`] the recovery loop already treats as stale:
/// created an hour ago with a one-second TTL, no reads, no coarse lock keys.
#[must_use]
pub fn stale_intent(
    tx_id: Uuid,
    mutations: Vec<MutationRecord>,
    progress: Vec<MutationProgress>,
) -> IntentRecord {
    IntentRecord {
        id: tx_id,
        created_at: Utc::now() - Duration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations,
        coarse_lock_keys: vec![],
        progress,
    }
}

/// Serialise `intent` and write it at its `.tx-log/` key.
///
/// # Panics
/// When serialisation or the write fails.
pub async fn put_intent(store: &dyn ObjectStore, intent: &IntentRecord) {
    let body = serde_json::to_vec(intent).expect("serialise intent");
    store
        .put(&intent.log_key(), Bytes::from(body))
        .await
        .expect("put intent");
}

/// Run a single recovery sweep over `store`.
///
/// The lock is an explicit parameter so tests can share one primitive across
/// concurrent sweeps or with a live executor; single-process tests pass a
/// fresh [`memory_lock`] so the sweep still exercises the production
/// ownership-takeover path, uncontended.
pub async fn sweep_once(store: Arc<dyn ObjectStore>, lock: Arc<Lock>) {
    RecoveryLoop::builder(store, lock).build().sweep().await;
}

/// Run a single recovery sweep over `store`, replaying through its
/// conditional primitives (the CAS deployment shape, where one backend serves
/// both store roles).
pub async fn sweep_once_cas(store: Arc<dyn ConditionalStore>, lock: Arc<Lock>) {
    RecoveryLoop::builder(store.clone(), lock)
        .conditional_store(store)
        .build()
        .sweep()
        .await;
}

/// Assert `.tx-log/` and `.tx-bodies/` hold no objects.
///
/// # Panics
/// When either prefix still contains objects, or listing fails.
pub async fn assert_no_orphans(store: &dyn ObjectStore) {
    for prefix in [".tx-log/", ".tx-bodies/"] {
        let items = store.list(prefix, 1000, None).await.unwrap().items;
        assert!(
            items.is_empty(),
            "Orphan keys found under {prefix}: {items:?}"
        );
    }
}

/// Number of objects under `prefix` (first page, up to 1000 keys).
///
/// # Panics
/// When listing fails.
pub async fn list_count(store: &dyn ObjectStore, prefix: &str) -> usize {
    store.list(prefix, 1000, None).await.unwrap().items.len()
}
