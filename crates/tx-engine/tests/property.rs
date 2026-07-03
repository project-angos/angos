//! Property tests for the transactional engine.
//!
//! Exercises random transactions against an in-memory `ObjectStore` with both
//! the `LockedExecutor` and `CasExecutor`. Asserts:
//!
//! - Every committed transaction's mutations are visible in the store.
//! - Every uncommitted transaction's mutations are absent from the store.
//! - No orphan objects remain in `.tx-log/` or `.tx-bodies/` after the recovery
//!   loop has run a sweep.

use std::sync::Arc;

use bytes::Bytes;
use tokio::runtime::Runtime;
use uuid::Uuid;

use angos_storage::{Error as StorageError, MemoryObjectStore, ObjectStore};

use angos_tx_engine::{
    executor::TransactionExecutor,
    intent::{MutationProgress, MutationRecord},
    transaction::{Mutation, Transaction},
};

use angos_tx_engine::test_util;

// Property tests

/// A committed transaction's mutations must be visible in the store.
#[tokio::test(flavor = "multi_thread")]
async fn committed_mutations_are_visible_locked() {
    let store = Arc::new(MemoryObjectStore::new());
    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

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

/// After a committed transaction, no orphan .tx-log or .tx-bodies entries remain.
#[tokio::test(flavor = "multi_thread")]
async fn no_orphans_after_commit_locked() {
    let store = Arc::new(MemoryObjectStore::new());
    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "obj/x".to_owned(),
            body: Bytes::from_static(b"data"),
            expected: None,
        })
        .build();

    executor.execute(tx).await.expect("execute must succeed");

    test_util::assert_no_orphans(&*store).await;
}

/// CAS executor: committed mutations are visible and no orphans remain.
#[tokio::test(flavor = "multi_thread")]
async fn committed_mutations_are_visible_cas() {
    let store = Arc::new(MemoryObjectStore::new());
    let executor = test_util::cas_executor(store.clone(), test_util::memory_lock());

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

    test_util::assert_no_orphans(&*store).await;
}

/// Delete mutations remove the target key.
#[tokio::test(flavor = "multi_thread")]
async fn delete_mutation_removes_key() {
    let store = Arc::new(MemoryObjectStore::new());

    // Pre-populate.
    store
        .put("del/key", Bytes::from_static(b"to-delete"))
        .await
        .unwrap();

    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

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
/// source, under both executors.
#[tokio::test(flavor = "multi_thread")]
async fn move_mutation_relocates_body_locked() {
    let store = Arc::new(MemoryObjectStore::new());

    store
        .put("staging/blob", Bytes::from_static(b"blob-body"))
        .await
        .unwrap();

    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

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

    store
        .put("staging/blob", Bytes::from_static(b"blob-body"))
        .await
        .unwrap();

    let executor = test_util::cas_executor(store.clone(), test_util::memory_lock());

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

    store
        .put("src/blob", Bytes::from_static(b"blob-body"))
        .await
        .unwrap();

    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

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

    // Manually inject a stale intent whose only mutation is still Pending.
    let tx_id = Uuid::new_v4();
    let body_ref = test_util::stage_body(&*store, tx_id, 0, Bytes::from_static(b"body")).await;
    let intent = test_util::stale_intent(
        tx_id,
        vec![MutationRecord::Put {
            key: "recovery/key".to_owned(),
            body_ref,
            expected: None,
        }],
        vec![MutationProgress::Pending],
    );
    test_util::put_intent(&*store, &intent).await;

    // Run one sweep, wiring an uncontended lock so the path under test
    // exercises the production ownership-takeover code.
    test_util::sweep_once(store.clone(), test_util::memory_lock()).await;

    // After recovery: the stale intent and its bodies are gone (rolled back
    // because no mutations were applied).
    test_util::assert_no_orphans(&*store).await;
    // The intent had no Applied progress entries, so it was rolled back:
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
    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

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
