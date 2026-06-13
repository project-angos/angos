//! Regression test for the manifest-push vs manifest-delete race on the CAS
//! executor.
//!
//! `src/registry/metadata_store/link_ops.rs::execute_links_tx` declares the
//! synthetic coarse lock key `blob-data:{digest}` on both branches of its
//! `LinksTxExtras` (push and delete). Under the CAS executor, which takes no
//! working-set lock, the coarse lock is the only thing that prevents a delete
//! planner's "is the blob unreferenced?" observation from racing a concurrent
//! push that lands its `Put blob-data/{digest}` between the planner's HEAD and
//! the executor's Apply.
//!
//! This test exercises the engine-level invariant: two transactions that share
//! `blob-data:{digest}` as a coarse lock key must serialise under the CAS
//! executor, so a Put and a Delete on the same key cannot interleave their
//! Apply phases.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Barrier;
use tokio::time::sleep;

use angos_storage::{MemoryObjectStore, ObjectStore};

use angos_tx_engine::{
    executor::TransactionExecutor,
    transaction::{Mutation, Transaction},
};

mod common;

const BLOB_KEY: &str = "blob-data/sha256:dead";
const COARSE_KEY: &str = "blob-data:sha256:dead";

fn push_tx(body: Bytes) -> Transaction {
    Transaction::builder()
        .mutation(Mutation::Put {
            key: BLOB_KEY.to_string(),
            body,
            expected: None,
        })
        .coarse_lock(COARSE_KEY)
        .build()
}

fn delete_tx() -> Transaction {
    Transaction::builder()
        .mutation(Mutation::Delete {
            key: BLOB_KEY.to_string(),
            expected: None,
        })
        .coarse_lock(COARSE_KEY)
        .build()
}

/// Both push and delete must declare the same coarse lock key. This is the
/// invariant `execute_links_tx` relies on: if either side drops it under the
/// CAS executor, Apply phases can interleave and a dangling-link state becomes
/// observable.
#[test]
fn push_and_delete_declare_matching_coarse_lock() {
    let push = push_tx(Bytes::from_static(b"manifest"));
    let delete = delete_tx();
    assert!(
        push.coarse_lock_keys.iter().any(|k| k == COARSE_KEY),
        "push transaction must declare blob-data:{{digest}} coarse lock"
    );
    assert!(
        delete.coarse_lock_keys.iter().any(|k| k == COARSE_KEY),
        "delete transaction must declare blob-data:{{digest}} coarse lock"
    );
    assert!(
        push.lock_set().contains(&COARSE_KEY.to_string()),
        "push lock_set must include the coarse key"
    );
    assert!(
        delete.lock_set().contains(&COARSE_KEY.to_string()),
        "delete lock_set must include the coarse key"
    );
}

/// Under the CAS executor, push and delete that share `blob-data:{digest}`
/// serialise. The committed state is therefore consistent with one or the
/// other landing last, never an interleaved partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cas_executor_serialises_push_and_delete() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();
    let executor = common::cas_executor(store.clone(), lock.clone());

    // Hold the coarse lock from outside so the in-flight push must wait.
    let held = lock
        .acquire(&[COARSE_KEY.to_string()])
        .await
        .expect("acquire");

    let exec_clone = executor.clone();
    let push_handle = tokio::spawn(async move {
        exec_clone
            .execute(push_tx(Bytes::from_static(b"manifest")))
            .await
    });

    sleep(Duration::from_millis(50)).await;
    assert!(
        !push_handle.is_finished(),
        "push must block on the coarse lock"
    );

    held.release().await;

    tokio::time::timeout(Duration::from_secs(5), push_handle)
        .await
        .expect("push completes after coarse lock release")
        .expect("join")
        .expect("execute");

    executor
        .execute(delete_tx())
        .await
        .expect("delete tx commits");

    // After both commit (push then delete), the blob must be absent.
    let final_head = store.as_ref().head(BLOB_KEY).await;
    assert!(final_head.is_err(), "blob-data should be deleted");
}

/// Cross-task concurrent push and delete on a shared CAS executor: both
/// commit in some order; the final state matches one of the two legal
/// outcomes (blob present, or blob absent).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_push_and_delete_converge_to_consistent_state() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();
    let executor = common::cas_executor(store.clone(), lock);

    let barrier = Arc::new(Barrier::new(2));

    let push_exec = executor.clone();
    let push_barrier = barrier.clone();
    let push_task = tokio::spawn(async move {
        push_barrier.wait().await;
        push_exec
            .execute(push_tx(Bytes::from_static(b"manifest")))
            .await
    });

    let delete_exec = executor.clone();
    let delete_barrier = barrier.clone();
    let delete_task = tokio::spawn(async move {
        delete_barrier.wait().await;
        delete_exec.execute(delete_tx()).await
    });

    let (push_res, delete_res) = tokio::join!(push_task, delete_task);
    push_res.expect("push join").expect("push execute");
    delete_res.expect("delete join").expect("delete execute");

    // Either order is legal; the resulting state must reflect the winner.
    // We don't know which won, but the state must be either present-with-the-
    // written-body or absent, never a partially-written or corrupted body.
    if let Ok(body) = store.as_ref().get(BLOB_KEY).await {
        assert_eq!(body, b"manifest", "blob present must hold the pushed body");
    }
}
