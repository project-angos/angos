//! Tests for the idempotent `MergeSet` mutation.
//!
//! Concurrent writers converge on the CAS executor with no lost update, and a
//! committed-then-contended merge is completed by the recovery loop instead of
//! stranding a permanent partial commit (the failure mode an etag-pinned `Put`
//! on a hot blob-index shard produced).

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{Value, json};
use uuid::Uuid;

use angos_storage::test_util::{HookedStore, StoreHook, StoreOp};
use angos_storage::{
    ConditionalStore, Error as StorageError, Etag, MemoryObjectStore, ObjectStore,
};

use angos_tx_engine::error::Error;
use angos_tx_engine::executor::TransactionExecutor;
use angos_tx_engine::executor::common::merge_json_set;
use angos_tx_engine::intent::{MutationProgress, MutationRecord};
use angos_tx_engine::recovery::RecoveryLoop;
use angos_tx_engine::test_util;
use angos_tx_engine::transaction::{Mutation, Transaction};

const SHARD: &str = "blob-index/refs/shard";

fn member(tag: &str) -> Value {
    json!({ "Layer": tag })
}

fn encode(members: &[Value]) -> Bytes {
    Bytes::from(serde_json::to_vec(members).expect("serialise members"))
}

async fn read_set(store: &dyn ObjectStore, key: &str) -> BTreeSet<String> {
    match store.get(key).await {
        Ok(body) => serde_json::from_slice::<Vec<Value>>(&body)
            .expect("shard is a json array")
            .into_iter()
            .map(|value| value.to_string())
            .collect(),
        Err(_) => BTreeSet::new(),
    }
}

// merge_json_set unit coverage

#[test]
fn merge_adds_new_member_and_dedups_existing() {
    let current = encode(&[member("a")]);
    let out = merge_json_set(&current, &[member("a"), member("b")], &[])
        .expect("merge")
        .expect("non-empty");
    let set: Vec<Value> = serde_json::from_slice(&out).expect("parse");
    assert_eq!(set.len(), 2);
    assert!(set.contains(&member("a")) && set.contains(&member("b")));
}

#[test]
fn merge_emptying_the_set_returns_none() {
    let current = encode(&[member("a")]);
    let out = merge_json_set(&current, &[], &[member("a")]).expect("merge");
    assert!(out.is_none(), "an emptied set signals a delete");
}

#[test]
fn merge_over_absent_key_creates_the_set() {
    let out = merge_json_set(&[], &[member("a")], &[])
        .expect("merge")
        .expect("non-empty");
    let set: Vec<Value> = serde_json::from_slice(&out).expect("parse");
    assert_eq!(set, vec![member("a")]);
}

// Executor behaviour

/// Concurrent single-mutation merges on one hot key must all land: the CAS apply
/// loop re-reads and retries on each precondition miss, so no add is lost and no
/// intent is stranded.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_merge_sets_all_converge_on_cas() {
    let store = Arc::new(MemoryObjectStore::new());
    let executor = test_util::cas_executor(store.clone(), test_util::memory_lock());

    let writers = 12;
    let mut handles = Vec::new();
    for i in 0..writers {
        let executor = executor.clone();
        handles.push(tokio::spawn(async move {
            let tx = Transaction::builder()
                .mutation(Mutation::MergeSet {
                    key: SHARD.to_string(),
                    add: vec![member(&format!("l{i}"))],
                    remove: vec![],
                })
                .build();
            executor.execute(tx).await.expect("merge commits");
        }));
    }
    for handle in handles {
        handle.await.expect("writer task");
    }

    assert_eq!(
        read_set(store.as_ref(), SHARD).await.len(),
        writers,
        "every concurrent add survives"
    );
    test_util::assert_no_orphans(store.as_ref()).await;
}

/// Fails every conditional write on the shard with `PreconditionFailed`,
/// modelling contention that outlasts the merge's attempt budget.
struct ShardAlwaysContended;

#[async_trait]
impl StoreHook for ShardAlwaysContended {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        match op {
            StoreOp::PutIfAbsent { key, .. } | StoreOp::PutIfMatch { key, .. } if key == SHARD => {
                Err(StorageError::PreconditionFailed)
            }
            _ => Ok(()),
        }
    }
}

/// A first-mutation merge that exhausts its attempt budget has applied nothing,
/// so the executor rolls the transaction back and returns the retriable
/// `Conflict` rather than surfacing `PartialCommit`.
#[tokio::test(flavor = "multi_thread")]
async fn exhausted_merge_with_nothing_applied_rolls_back_as_conflict() {
    let inner = Arc::new(MemoryObjectStore::new());
    let store: Arc<dyn ConditionalStore> = Arc::new(HookedStore::new(
        inner.clone() as Arc<dyn ConditionalStore>,
        ShardAlwaysContended,
    ));
    let executor = test_util::cas_executor(store, test_util::memory_lock());

    let tx = Transaction::builder()
        .mutation(Mutation::MergeSet {
            key: SHARD.to_string(),
            add: vec![member("a")],
            remove: vec![],
        })
        .build();

    let err = executor.execute(tx).await.expect_err("merge cannot commit");
    assert!(
        matches!(err, Error::Conflict),
        "attempt-budget exhaustion with nothing applied must surface a retriable Conflict, \
         got {err:?}"
    );
    test_util::assert_no_orphans(inner.as_ref()).await;
}

/// The locked executor applies a merge as a read-modify-write under the key's
/// lock, deleting the key once the set empties.
#[tokio::test(flavor = "multi_thread")]
async fn merge_set_writes_then_deletes_on_locked() {
    let store = Arc::new(MemoryObjectStore::new());
    let executor = test_util::locked_executor(store.clone(), test_util::memory_lock());

    let add = Transaction::builder()
        .mutation(Mutation::MergeSet {
            key: SHARD.to_string(),
            add: vec![member("a")],
            remove: vec![],
        })
        .build();
    executor.execute(add).await.expect("add commits");
    assert_eq!(
        read_set(store.as_ref(), SHARD).await,
        BTreeSet::from([member("a").to_string()])
    );

    let remove = Transaction::builder()
        .mutation(Mutation::MergeSet {
            key: SHARD.to_string(),
            add: vec![],
            remove: vec![member("a")],
        })
        .build();
    executor.execute(remove).await.expect("remove commits");
    assert!(
        store.get(SHARD).await.is_err(),
        "emptied shard is deleted, not left as []"
    );
    test_util::assert_no_orphans(store.as_ref()).await;
}

/// A committed transaction whose trailing merge is still `Pending` converges on
/// recovery even though a concurrent writer moved the shard past what the
/// original attempt observed. An etag-pinned `Put` could never reconcile this
/// and would log "precondition failed" on every sweep forever.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_completes_merge_against_a_concurrently_changed_shard() {
    let store = Arc::new(MemoryObjectStore::new());

    store
        .put(SHARD, encode(&[member("other")]))
        .await
        .expect("seed shard with a concurrent writer's entry");

    // Mutation 0 is already Applied (skipped on replay); mutation 1 is the
    // Pending merge the recovery loop must complete.
    let intent = test_util::stale_intent(
        Uuid::new_v4(),
        vec![
            MutationRecord::MergeSet {
                key: "blob-index/refs/already-applied".to_string(),
                add: vec![member("seed")],
                remove: vec![],
            },
            MutationRecord::MergeSet {
                key: SHARD.to_string(),
                add: vec![member("mine")],
                remove: vec![],
            },
        ],
        vec![MutationProgress::Applied, MutationProgress::Pending],
    );
    test_util::put_intent(store.as_ref(), &intent).await;

    test_util::sweep_once_cas(store.clone(), test_util::memory_lock()).await;

    let set = read_set(store.as_ref(), SHARD).await;
    assert!(
        set.contains(&member("other").to_string()),
        "the concurrent writer's entry is preserved"
    );
    assert!(
        set.contains(&member("mine").to_string()),
        "the stalled merge is completed against live state"
    );
    test_util::assert_no_orphans(store.as_ref()).await;
}

/// A legacy committed intent whose etag-pinned `Put` can never reconcile (the
/// live object diverged) is abandoned once it is older than the grace, so it
/// stops re-logging forever. This clears the pre-`MergeSet` stranded intents
/// after an upgrade.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_abandons_unreconcilable_committed_intent_past_grace() {
    let store = Arc::new(MemoryObjectStore::new());

    store
        .put("k", Bytes::from_static(b"live"))
        .await
        .expect("seed diverged live object");

    // Committed intent (mutation 0 Applied) whose Pending mutation 1 is an
    // etag-pinned Put that can never match: the live object carries a different
    // etag and a different body than the staged one.
    let tx_id = Uuid::new_v4();
    let body_ref =
        test_util::stage_body(store.as_ref(), tx_id, 1, Bytes::from_static(b"staged")).await;
    let intent = test_util::stale_intent(
        tx_id,
        vec![
            MutationRecord::MergeSet {
                key: "blob-index/refs/applied".to_string(),
                add: vec![member("seed")],
                remove: vec![],
            },
            MutationRecord::Put {
                key: "k".to_string(),
                body_ref,
                expected: Some(Etag::new("\"stale\"")),
            },
        ],
        vec![MutationProgress::Applied, MutationProgress::Pending],
    );
    test_util::put_intent(store.as_ref(), &intent).await;

    // The stale intent was created an hour ago; a one-second grace makes it
    // abandonment-eligible.
    RecoveryLoop::builder(store.clone())
        .conditional_store(store.clone())
        .lock(test_util::memory_lock())
        .abandon_after_secs(1)
        .build()
        .sweep()
        .await;

    assert_eq!(
        store.get("k").await.expect("live object untouched"),
        b"live",
        "abandonment does not overwrite the diverged object"
    );
    test_util::assert_no_orphans(store.as_ref()).await;
}
