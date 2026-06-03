//! Shared construction helpers for the tx-engine integration tests.
//!
//! Each integration-test file is compiled as its own crate and pulls these in
//! with `mod common;`, so not every helper is used by every file — hence the
//! blanket `dead_code` allow.
#![allow(dead_code)]

use std::sync::Arc;

use angos_storage::{ConditionalStore, ObjectStore};
use angos_tx_engine::{
    executor::{cas::CasExecutor, locked::LockedExecutor},
    lock::{primitive::Lock, storage::memory::MemoryLockStorage},
};

/// Build a fresh in-memory lock primitive.
pub fn memory_lock() -> Arc<Lock> {
    Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("lock builder"),
    )
}

/// Build a `LockedExecutor` over `store`, serialising on `lock`.
pub fn locked_executor(store: Arc<dyn ObjectStore>, lock: Arc<Lock>) -> Arc<LockedExecutor> {
    Arc::new(
        LockedExecutor::builder()
            .store(store)
            .lock(lock)
            .build()
            .expect("LockedExecutor builder"),
    )
}

/// Build a `CasExecutor` over `store`, serialising coarse locks on `lock`.
pub fn cas_executor(store: Arc<dyn ConditionalStore>, lock: Arc<Lock>) -> Arc<CasExecutor> {
    Arc::new(
        CasExecutor::builder()
            .store(store)
            .lock(lock)
            .build()
            .expect("CasExecutor builder"),
    )
}
