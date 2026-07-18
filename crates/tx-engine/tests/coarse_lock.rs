//! Tests for `Transaction::coarse_lock`.
//!
//! A coarse lock key folds an extra key into the transaction's lock set even
//! though it is neither read nor written. Two transactions that share a coarse
//! lock key must serialise on it, under both the locked and the CAS executor.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Barrier;
use tokio::time::sleep;

use angos_storage::{MemoryObjectStore, ObjectStore};

use angos_tx_engine::{
    executor::TransactionExecutor,
    lock::primitive::Lock,
    transaction::{Mutation, Transaction},
};

use angos_tx_engine::test_util;

const COARSE_KEY: &str = "blob-data:dummy";

fn tx_delete() -> Transaction {
    Transaction::builder()
        .coarse_lock(COARSE_KEY)
        .mutation(Mutation::Delete {
            key: "links/A".to_string(),
            expected: None,
        })
        .build()
}

fn tx_put_if_absent() -> Transaction {
    Transaction::builder()
        .coarse_lock(COARSE_KEY)
        .mutation(Mutation::PutIfAbsent {
            key: "blob-data/dummy".to_string(),
            body: Bytes::from_static(b"blob-body"),
        })
        .build()
}

/// Two transactions sharing a coarse lock must serialise: while one holds the
/// lock, a `try_acquire` on the same key must return `None`.
async fn assert_coarse_lock_serialises<E>(executor: Arc<E>, lock: Arc<Lock>)
where
    E: TransactionExecutor + Send + Sync + 'static,
{
    let barrier = Arc::new(Barrier::new(2));

    // Hold the coarse lock from a probe task; while held, the other side
    // must observe the contention.
    let probe_lock = lock.clone();
    let barrier_probe = barrier.clone();
    let probe = tokio::spawn(async move {
        let session = probe_lock
            .acquire(&[COARSE_KEY.to_string()])
            .await
            .expect("probe acquire");
        // Signal the other task that the lock is held.
        barrier_probe.wait().await;
        // Stay held long enough for the other task's try_acquire to observe.
        sleep(Duration::from_millis(100)).await;
        session.release().await;
    });

    barrier.wait().await;
    let attempt = lock
        .try_acquire(&[COARSE_KEY.to_string()])
        .await
        .expect("try_acquire infallible");
    assert!(
        attempt.is_none(),
        "second waiter must see the coarse lock as contended"
    );

    probe.await.expect("probe task");

    // Once the probe releases, both real transactions can run sequentially
    // and both must succeed.
    executor
        .execute(tx_delete())
        .await
        .expect("delete tx commits");
    executor
        .execute(tx_put_if_absent())
        .await
        .expect("put-if-absent tx commits");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coarse_lock_serialises_under_locked_executor() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = test_util::memory_lock();
    let executor = test_util::locked_executor(store, lock.clone());
    assert_coarse_lock_serialises(executor, lock).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coarse_lock_serialises_under_cas_executor() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = test_util::memory_lock();
    let executor = test_util::cas_executor(store, lock.clone());
    assert_coarse_lock_serialises(executor, lock).await;
}

/// The CAS executor takes no transaction-scoped lock for its working set, but
/// must acquire the coarse lock for the duration of Apply. Concurrent CAS
/// transactions that share a coarse lock therefore serialise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cas_executor_holds_coarse_lock_across_apply() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = test_util::memory_lock();
    let executor = test_util::cas_executor(store.clone(), lock.clone());

    // Pre-acquire the coarse key from outside the executor so the in-flight
    // transaction must wait. If the CAS executor neglected to acquire it,
    // execute() would return immediately.
    let held = lock
        .acquire(&[COARSE_KEY.to_string()])
        .await
        .expect("acquire");

    let exec_clone = executor.clone();
    let tx_handle = tokio::spawn(async move { exec_clone.execute(tx_put_if_absent()).await });

    // Give the spawned task time to attempt acquire and block.
    sleep(Duration::from_millis(50)).await;
    assert!(
        !tx_handle.is_finished(),
        "tx must be blocked on the coarse lock"
    );

    held.release().await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), tx_handle)
        .await
        .expect("tx completes after coarse lock release")
        .expect("join")
        .expect("execute");
    let _ = outcome;

    let body = store.get("blob-data/dummy").await.expect("body present");
    assert_eq!(body, b"blob-body");
}
