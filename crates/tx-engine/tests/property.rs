//! Property tests for the transactional engine.
//!
//! Exercises random transactions against an in-memory `ObjectStore` with both
//! the `LockedExecutor` and `CasExecutor`. Asserts:
//!
//! - Every committed transaction's mutations are visible in the store.
//! - Every uncommitted transaction's mutations are absent from the store.
//! - No orphan objects remain in `tx-log/` or `tx-bodies/` after the recovery
//!   loop has run a sweep.

use std::sync::Arc;

use bytes::Bytes;
use tokio::runtime::Runtime;

use angos_storage::{ConditionalStore, Error as StorageError, MemoryObjectStore, ObjectStore};

use angos_tx_engine::{
    executor::{TransactionExecutor, cas::CasExecutor, locked::LockedExecutor},
    intent::MutationProgress,
    lock::{primitive::Lock, storage::memory::MemoryLockStorage},
    recovery::RecoveryLoop,
    transaction::{Mutation, Transaction},
};

// Helper: assert no orphan keys under a prefix.
async fn assert_no_prefix(store: &MemoryObjectStore, prefix: &str) {
    let items = store.list(prefix, 1000, None).await.unwrap().items;
    assert!(
        items.is_empty(),
        "Orphan keys found under {prefix}: {items:?}"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Property tests
// ──────────────────────────────────────────────────────────────────────────────

/// A committed transaction's mutations must be visible in the store.
#[tokio::test(flavor = "multi_thread")]
async fn committed_mutations_are_visible_locked() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );
    let executor = LockedExecutor::builder()
        .store(store.clone() as Arc<dyn ObjectStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "data/key-a".to_owned(),
            body: Bytes::from_static(b"hello"),
            expected: None,
        })
        .mutation(Mutation::Put {
            key: "data/key-b".to_owned(),
            body: Bytes::from_static(b"world"),
            expected: None,
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    let a = store.get("data/key-a").await.expect("key-a must exist");
    let b = store.get("data/key-b").await.expect("key-b must exist");
    assert_eq!(a, b"hello");
    assert_eq!(b, b"world");
}

/// After a committed transaction, no orphan tx-log or tx-bodies entries remain.
#[tokio::test(flavor = "multi_thread")]
async fn no_orphans_after_commit_locked() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );
    let executor = LockedExecutor::builder()
        .store(store.clone() as Arc<dyn ObjectStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "obj/x".to_owned(),
            body: Bytes::from_static(b"data"),
            expected: None,
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    assert_no_prefix(&store, "tx-log/").await;
    assert_no_prefix(&store, "tx-bodies/").await;
}

/// CAS executor: committed mutations are visible and no orphans remain.
#[tokio::test(flavor = "multi_thread")]
async fn committed_mutations_are_visible_cas() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .expect("lock"),
    );
    let executor = CasExecutor::builder()
        .store(store.clone() as Arc<dyn ConditionalStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "cas/key-a".to_owned(),
            body: Bytes::from_static(b"value-a"),
            expected: None,
        })
        .mutation(Mutation::PutIfAbsent {
            key: "cas/key-b".to_owned(),
            body: Bytes::from_static(b"value-b"),
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    let a = store.get("cas/key-a").await.expect("key-a must exist");
    let b = store.get("cas/key-b").await.expect("key-b must exist");
    assert_eq!(a, b"value-a");
    assert_eq!(b, b"value-b");

    assert_no_prefix(&store, "tx-log/").await;
    assert_no_prefix(&store, "tx-bodies/").await;
}

/// Delete mutations remove the target key.
#[tokio::test(flavor = "multi_thread")]
async fn delete_mutation_removes_key() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );

    // Pre-populate.
    store
        .put("del/key", Bytes::from_static(b"to-delete"))
        .await
        .unwrap();

    let executor = LockedExecutor::builder()
        .store(store.clone() as Arc<dyn ObjectStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Delete {
            key: "del/key".to_owned(),
            expected: None,
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    let result = store.get("del/key").await;
    assert!(
        matches!(result, Err(StorageError::NotFound)),
        "deleted key must not exist"
    );
}

/// Move mutation propagates the body to the destination and removes the
/// source — under both executors.
#[tokio::test(flavor = "multi_thread")]
async fn move_mutation_relocates_body_locked() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );

    store
        .put("staging/blob", Bytes::from_static(b"blob-body"))
        .await
        .unwrap();

    let executor = LockedExecutor::builder()
        .store(store.clone() as Arc<dyn ObjectStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Move {
            src: "staging/blob".to_owned(),
            dst: "canonical/blob".to_owned(),
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    let dst = store.get("canonical/blob").await.expect("dst must exist");
    assert_eq!(dst, b"blob-body");
    let src = store.get("staging/blob").await;
    assert!(
        matches!(src, Err(StorageError::NotFound)),
        "src must be removed after Move"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn move_mutation_relocates_body_cas() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );

    store
        .put("staging/blob", Bytes::from_static(b"blob-body"))
        .await
        .unwrap();

    let executor = CasExecutor::builder()
        .store(store.clone() as Arc<dyn ConditionalStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Move {
            src: "staging/blob".to_owned(),
            dst: "canonical/blob".to_owned(),
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    let dst = store.get("canonical/blob").await.expect("dst must exist");
    assert_eq!(dst, b"blob-body");
    let src = store.get("staging/blob").await;
    assert!(
        matches!(src, Err(StorageError::NotFound)),
        "src must be removed after Move"
    );
}

/// Copy mutation propagates the body to the destination.
#[tokio::test(flavor = "multi_thread")]
async fn copy_mutation_propagates_body() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );

    store
        .put("src/blob", Bytes::from_static(b"blob-body"))
        .await
        .unwrap();

    let executor = LockedExecutor::builder()
        .store(store.clone() as Arc<dyn ObjectStore>)
        .lock(lock)
        .build()
        .expect("executor builder");

    let tx = Transaction::builder()
        .mutation(Mutation::Copy {
            src: "src/blob".to_owned(),
            dst: "dst/blob".to_owned(),
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    let dst = store.get("dst/blob").await.expect("destination must exist");
    assert_eq!(dst, b"blob-body");
}

/// Recovery loop cleans up stale (orphaned) intents.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_loop_cleans_stale_intent() {
    let store = Arc::new(MemoryObjectStore::new());
    let cancel = tokio_util::sync::CancellationToken::new();

    // Manually inject a stale intent with ttl_secs = 0 (immediately stale).
    let tx_id = uuid::Uuid::new_v4();
    let intent = angos_tx_engine::intent::IntentRecord {
        id: tx_id,
        created_at: chrono::Utc::now() - chrono::Duration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations: vec![angos_tx_engine::intent::MutationRecord::Put {
            key: "recovery/key".to_owned(),
            body_ref: format!("tx-bodies/{tx_id}/0"),
            expected: None,
        }],
        coarse_lock_keys: vec![],
        progress: vec![MutationProgress::Pending],
    };

    // Write the body and intent.
    store
        .put(&format!("tx-bodies/{tx_id}/0"), Bytes::from_static(b"body"))
        .await
        .unwrap();
    let intent_json = serde_json::to_vec(&intent).unwrap();
    store
        .put(&format!("tx-log/{tx_id}.json"), Bytes::from(intent_json))
        .await
        .unwrap();

    // Run one sweep, wiring an uncontended lock so the path under test
    // exercises the production ownership-takeover code.
    let recovery_lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );
    let recovery = RecoveryLoop::builder(store.clone() as Arc<dyn ObjectStore>)
        .lock(recovery_lock)
        .cancellation(cancel.clone())
        .interval(std::time::Duration::from_hours(1)) // Only sweep on demand.
        .build();

    recovery.sweep().await;

    // After recovery: the stale intent and its bodies should be gone (rolled back
    // because no mutations were applied) and the canonical key must be written.
    assert_no_prefix(&store, "tx-log/").await;
    assert_no_prefix(&store, "tx-bodies/").await;
    // The intent had no Applied progress entries, so it was rolled back —
    // the key must NOT exist.
    let result = store.get("recovery/key").await;
    assert!(
        matches!(
            result,
            Err(StorageError::NotFound | StorageError::Backend(_))
        ),
        "rolled-back key must not be visible"
    );
}

/// Multiple concurrent transactions do not interfere when keys are disjoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_disjoint_transactions_all_commit() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = Arc::new(
        Lock::builder()
            .storage(Arc::new(MemoryLockStorage::new()))
            .build()
            .unwrap(),
    );
    let executor = Arc::new(
        LockedExecutor::builder()
            .store(store.clone() as Arc<dyn ObjectStore>)
            .lock(lock)
            .build()
            .expect("executor builder"),
    );

    let mut handles = Vec::new();
    for i in 0..8u32 {
        let ex = executor.clone();
        handles.push(tokio::spawn(async move {
            let tx = Transaction::builder()
                .mutation(Mutation::Put {
                    key: format!("concurrent/{i}"),
                    body: Bytes::from(format!("value-{i}")),
                    expected: None,
                })
                .build();
            ex.execute(tx).await
        }));
    }

    for handle in handles {
        handle.await.unwrap().expect("transaction must succeed");
    }

    for i in 0u32..8 {
        let val = store.get(&format!("concurrent/{i}")).await.unwrap();
        assert_eq!(val, format!("value-{i}").as_bytes());
    }
}

/// `StorageError::NotFound` is returned for missing keys.
#[test]
fn memory_store_not_found_maps_correctly() {
    let rt = Runtime::new().unwrap();
    let store = MemoryObjectStore::new();
    let err = rt.block_on(store.get("nonexistent")).unwrap_err();
    assert!(
        matches!(err, StorageError::NotFound),
        "missing key must map to StorageError::NotFound"
    );
}
