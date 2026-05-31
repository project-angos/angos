//! Recovery takeover and conditional-replay tests.
//!
//! Covers the Phase 3 correctness invariants:
//!
//! 1. Two concurrent recovery loops sharing the same lock primitive must
//!    serialise their apply on a stale intent — at most one of them gets to
//!    re-apply each mutation.
//! 2. Mutations whose `progress` slot is already `Applied` are skipped on
//!    replay (recovery never overwrites a committed write).
//! 3. On a CAS deployment, a `Put { expected: Some(etag) }` whose etag has
//!    moved but whose body matches the staged body is treated as
//!    already-applied (stale stamp recovery).
//! 4. On a CAS deployment, true contention (live body ≠ staged body) stops
//!    the replay and leaves the intent for the next sweep.
//!
//! Executor-path tests (new with the mid-apply Precondition fix):
//!
//! 5. When mutation[0] lands and is stamped, then mutation[1] returns
//!    `PreconditionFailed` but the live body matches the staged body (stale
//!    stamp from the healthy path), the executor stamps mutation[1] and
//!    continues forward — the transaction fully commits and the intent is reaped.
//! 6. When mutation[0] lands and is stamped, then mutation[1] returns
//!    `PreconditionFailed` with a live body that does NOT match (true
//!    contention), the executor returns `Error::PartialCommit` and preserves
//!    the intent in `.tx-log/` for the recovery loop.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use uuid::Uuid;

use angos_storage::{
    BoxedReader, ChildrenPage, ConditionalStore, Error as StorageError, Etag, MemoryObjectStore,
    ObjectMeta, ObjectStore, Page,
};

use angos_tx_engine::{
    error::Error as TxError,
    executor::TransactionExecutor,
    intent::{IntentRecord, MutationProgress, MutationRecord},
    recovery::RecoveryLoop,
    transaction::{Mutation, Transaction},
};

mod common;

/// Wrapper that counts `put` calls per key. Used to prove a recovery
/// mutation applied at most once across two racing sweeps.
#[derive(Debug)]
struct PutCountingStore {
    inner: Arc<MemoryObjectStore>,
    target_key: String,
    target_puts: AtomicUsize,
}

impl PutCountingStore {
    fn new(inner: Arc<MemoryObjectStore>, target_key: impl Into<String>) -> Self {
        Self {
            inner,
            target_key: target_key.into(),
            target_puts: AtomicUsize::new(0),
        }
    }

    fn target_put_count(&self) -> usize {
        self.target_puts.load(Ordering::Acquire)
    }
}

#[async_trait]
impl ObjectStore for PutCountingStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get(key).await
    }
    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), StorageError> {
        self.inner.get_stream(key, offset).await
    }
    async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        if key == self.target_key {
            self.target_puts.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.put(key, data).await
    }
    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }
    async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
        self.inner.delete_prefix(prefix).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
        self.inner.head(key).await
    }
    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, StorageError> {
        self.inner.list(prefix, n, token).await
    }
    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, StorageError> {
        self.inner
            .list_children(prefix, n, token, start_after)
            .await
    }
    async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
        self.inner.copy(source, destination).await
    }
}

/// Two recovery loops on the same backing store and the same `Arc<Lock>` race
/// to take over a stale intent. The intent has one mutation marked `Applied`
/// (so recovery enters replay-forward) and one `Pending` mutation that writes
/// to a sentinel destination key. We wrap the store with a `PutCountingStore`
/// to assert the sentinel was `put` exactly once across both racing sweeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recovery_loops_apply_each_mutation_exactly_once() {
    let inner = Arc::new(MemoryObjectStore::new());
    let counting = Arc::new(PutCountingStore::new(inner.clone(), "race/dst"));
    let lock = common::memory_lock();

    let tx_id = Uuid::new_v4();
    inner
        .put(
            &format!(".tx-bodies/{tx_id}/0"),
            Bytes::from_static(b"sibling-body"),
        )
        .await
        .unwrap();
    inner
        .put(
            &format!(".tx-bodies/{tx_id}/1"),
            Bytes::from_static(b"staged-body"),
        )
        .await
        .unwrap();

    let intent = IntentRecord {
        id: tx_id,
        created_at: Utc::now() - ChronoDuration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations: vec![
            // Already-applied sibling so `any_applied()` is true and recovery
            // enters replay-forward.
            MutationRecord::PutIfAbsent {
                key: "race/sibling".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/0"),
            },
            // Pending mutation under test.
            MutationRecord::Put {
                key: "race/dst".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/1"),
                expected: None,
            },
        ],
        coarse_lock_keys: vec![],
        progress: vec![
            MutationProgress::Applied { etag: None },
            MutationProgress::Pending,
        ],
    };
    inner
        .put(
            &format!(".tx-log/{tx_id}.json"),
            Bytes::from(serde_json::to_vec(&intent).unwrap()),
        )
        .await
        .unwrap();

    let recovery_a = RecoveryLoop::builder(counting.clone() as Arc<dyn ObjectStore>)
        .lock(lock.clone())
        .interval(std::time::Duration::from_hours(1))
        .build();
    let recovery_b = RecoveryLoop::builder(counting.clone() as Arc<dyn ObjectStore>)
        .lock(lock.clone())
        .interval(std::time::Duration::from_hours(1))
        .build();

    tokio::join!(recovery_a.sweep(), recovery_b.sweep());

    assert_eq!(
        counting.target_put_count(),
        1,
        "exactly one sweep must have applied the pending mutation"
    );
    let body = inner.get("race/dst").await.expect("dst written");
    assert_eq!(body, b"staged-body");
}

/// An intent whose `progress[0]` is already `Applied` must not have its
/// canonical key 0 touched by recovery. We pre-write a sentinel body at key 0
/// and assert it survives the sweep.
#[tokio::test(flavor = "multi_thread")]
async fn already_applied_mutations_are_skipped() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();

    // Pre-write a sentinel at key 0; recovery must not overwrite it.
    store
        .put(
            "applied/k0",
            Bytes::from_static(b"sentinel-do-not-overwrite"),
        )
        .await
        .unwrap();

    let tx_id = Uuid::new_v4();
    store
        .put(
            &format!(".tx-bodies/{tx_id}/0"),
            Bytes::from_static(b"would-overwrite-sentinel"),
        )
        .await
        .unwrap();
    store
        .put(
            &format!(".tx-bodies/{tx_id}/1"),
            Bytes::from_static(b"pending-body"),
        )
        .await
        .unwrap();

    let intent = IntentRecord {
        id: tx_id,
        created_at: Utc::now() - ChronoDuration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations: vec![
            MutationRecord::Put {
                key: "applied/k0".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/0"),
                expected: None,
            },
            MutationRecord::Put {
                key: "applied/k1".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/1"),
                expected: None,
            },
        ],
        coarse_lock_keys: vec![],
        progress: vec![
            MutationProgress::Applied { etag: None },
            MutationProgress::Pending,
        ],
    };
    store
        .put(
            &format!(".tx-log/{tx_id}.json"),
            Bytes::from(serde_json::to_vec(&intent).unwrap()),
        )
        .await
        .unwrap();

    let recovery = RecoveryLoop::builder(store.clone() as Arc<dyn ObjectStore>)
        .lock(lock)
        .interval(std::time::Duration::from_hours(1))
        .build();
    recovery.sweep().await;

    let k0 = store.get("applied/k0").await.expect("k0 must survive");
    assert_eq!(
        k0, b"sentinel-do-not-overwrite",
        "applied-slot mutation was overwritten by recovery"
    );
    let k1 = store.get("applied/k1").await.expect("k1 was applied");
    assert_eq!(k1, b"pending-body");
}

/// CAS-mode recovery: a `Put { expected: Some(etag) }` whose etag has moved
/// but whose live body's hash matches the staged body must be treated as
/// already-applied. Recovery stamps progress and reaps the intent without
/// erroring.
///
/// We make the intent enter `replay_forward` by including a sibling mutation
/// already stamped `Applied`; the mutation under test is left `Pending` so
/// the CAS replay path actually runs.
#[tokio::test(flavor = "multi_thread")]
async fn cas_recovery_treats_stale_etag_with_matching_body_as_applied() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();

    // Land the final body at the destination first; capture its etag (different
    // from any "stale" etag we record in the intent).
    let live_etag = store
        .put_if_absent("cas/dst", Bytes::from_static(b"final-body"))
        .await
        .expect("seed dst")
        .expect("etag");

    // Stage the same body in .tx-bodies (so its sha256 matches the live body).
    let tx_id = Uuid::new_v4();
    store
        .put(
            &format!(".tx-bodies/{tx_id}/0"),
            Bytes::from_static(b"final-body"),
        )
        .await
        .unwrap();
    // Sibling already-applied mutation: write a tombstone body and pre-land it
    // so its replay is a CAS no-op.
    store
        .put(
            &format!(".tx-bodies/{tx_id}/1"),
            Bytes::from_static(b"sibling"),
        )
        .await
        .unwrap();
    store
        .put_if_absent("cas/sibling", Bytes::from_static(b"sibling"))
        .await
        .expect("seed sibling")
        .expect("etag");

    // Record a stale etag (anything not matching `live_etag`) so put_if_match
    // returns PreconditionFailed at replay time.
    let stale_etag = Etag::new("stale-etag-from-build-time");
    assert_ne!(stale_etag, live_etag, "stale etag must differ from live");

    let intent = IntentRecord {
        id: tx_id,
        created_at: Utc::now() - ChronoDuration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations: vec![
            MutationRecord::PutIfAbsent {
                key: "cas/sibling".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/1"),
            },
            MutationRecord::Put {
                key: "cas/dst".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/0"),
                expected: Some(stale_etag),
            },
        ],
        coarse_lock_keys: vec![],
        progress: vec![
            MutationProgress::Applied { etag: None },
            MutationProgress::Pending,
        ],
    };
    store
        .put(
            &format!(".tx-log/{tx_id}.json"),
            Bytes::from(serde_json::to_vec(&intent).unwrap()),
        )
        .await
        .unwrap();

    let cs: Arc<dyn ConditionalStore> = store.clone();
    let recovery = RecoveryLoop::builder(store.clone() as Arc<dyn ObjectStore>)
        .conditional_store(cs)
        .lock(lock)
        .interval(std::time::Duration::from_hours(1))
        .build();
    recovery.sweep().await;

    // Even though the etag mismatched, the live body matches the staged body,
    // so recovery treats the mutation as already-applied: intent is reaped,
    // dst body is unchanged.
    let body = store.get("cas/dst").await.expect("dst still present");
    assert_eq!(body, b"final-body");
    assert!(
        store
            .list(".tx-log/", 10, None)
            .await
            .unwrap()
            .items
            .is_empty(),
        "intent must be reaped after stale-stamp recovery"
    );
}

/// CAS-mode recovery: when the live body's hash does NOT match the staged
/// body, recovery must stop the replay and leave the intent for the next
/// sweep (true contention, not an already-applied case).
#[tokio::test(flavor = "multi_thread")]
async fn cas_recovery_stops_on_true_contention() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();

    // Live body at the destination has different content from the staged body.
    let _live_etag = store
        .put_if_absent("cas/dst", Bytes::from_static(b"someone-elses-body"))
        .await
        .expect("seed dst")
        .expect("etag");

    let tx_id = Uuid::new_v4();
    store
        .put(
            &format!(".tx-bodies/{tx_id}/0"),
            Bytes::from_static(b"our-staged-body"),
        )
        .await
        .unwrap();
    // Sibling already-applied so replay_forward runs.
    store
        .put(
            &format!(".tx-bodies/{tx_id}/1"),
            Bytes::from_static(b"sibling"),
        )
        .await
        .unwrap();
    store
        .put_if_absent("cas/sibling", Bytes::from_static(b"sibling"))
        .await
        .expect("seed sibling")
        .expect("etag");

    let stale_etag = Etag::new("stale-etag-from-build-time");
    let intent = IntentRecord {
        id: tx_id,
        created_at: Utc::now() - ChronoDuration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations: vec![
            MutationRecord::PutIfAbsent {
                key: "cas/sibling".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/1"),
            },
            MutationRecord::Put {
                key: "cas/dst".to_string(),
                body_ref: format!(".tx-bodies/{tx_id}/0"),
                expected: Some(stale_etag),
            },
        ],
        coarse_lock_keys: vec![],
        progress: vec![
            MutationProgress::Applied { etag: None },
            MutationProgress::Pending,
        ],
    };
    store
        .put(
            &format!(".tx-log/{tx_id}.json"),
            Bytes::from(serde_json::to_vec(&intent).unwrap()),
        )
        .await
        .unwrap();

    let cs: Arc<dyn ConditionalStore> = store.clone();
    let recovery = RecoveryLoop::builder(store.clone() as Arc<dyn ObjectStore>)
        .conditional_store(cs)
        .lock(lock)
        .interval(std::time::Duration::from_hours(1))
        .build();
    recovery.sweep().await;

    // Live body is untouched (CAS would have refused), and the intent stays in
    // place for the next sweep.
    let body = store.get("cas/dst").await.expect("dst still present");
    assert_eq!(body, b"someone-elses-body");
    let logs = store.list(".tx-log/", 10, None).await.unwrap().items;
    assert_eq!(
        logs.len(),
        1,
        "true contention must leave intent for next sweep, got {logs:?}"
    );
}

/// Executor-path: mutation[0] lands and is stamped `Applied`. mutation[1]
/// returns `PreconditionFailed` from a stale-etag scenario where the
/// healthy-path write already landed (live body matches staged body). The
/// executor must stamp mutation[1] as applied, finish the loop, reap the
/// intent, and return `Ok(Outcome)`.
///
/// We simulate the stale-stamp scenario by:
/// 1. Pre-landing the mutation[1] destination key with the same body as the
///    staged body (simulating the healthy-path write that landed but whose
///    stamp was lost).
/// 2. Setting `expected` to a stale etag so `put_if_match` returns
///    `PreconditionFailed`.
///
/// The `MemoryObjectStore` is also a `ConditionalStore`, so we can use it
/// directly as both the store and the CAS store for the executor.
#[tokio::test]
async fn cas_executor_stale_stamp_mid_apply_continues_and_commits() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();

    // mutation[0]: unconditional Put — will succeed normally.
    // mutation[1]: conditional Put with a stale etag — will return
    //   PreconditionFailed, but the live body matches the staged body.
    let first_body = Bytes::from_static(b"first-body");
    let second_body = Bytes::from_static(b"second-body");

    // Pre-land mutation[1]'s destination with the same body the transaction
    // will stage, simulating the healthy-path apply that completed but whose
    // stamp was never written back to the intent.
    store
        .put_if_absent("exec/dst1", second_body.clone())
        .await
        .expect("seed dst1")
        .expect("etag");

    let stale_etag = Etag::new("\"stale-etag-never-matches\"");

    let executor = common::cas_executor(store.clone(), lock);

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "exec/dst0".to_owned(),
            body: first_body,
            expected: None,
        })
        .mutation(Mutation::Put {
            key: "exec/dst1".to_owned(),
            body: second_body,
            expected: Some(stale_etag),
        })
        .build();

    let result = executor.execute(tx).await;
    assert!(
        result.is_ok(),
        "stale-stamp mid-apply must commit: {result:?}"
    );

    // Both canonical keys must be present.
    store.get("exec/dst0").await.expect("dst0 present");
    store.get("exec/dst1").await.expect("dst1 present");

    // Intent must have been reaped (no .tx-log/ entries).
    let logs = store.list(".tx-log/", 10, None).await.unwrap().items;
    assert!(
        logs.is_empty(),
        "intent must be reaped after successful commit: {logs:?}"
    );
}

/// Executor-path: mutation[0] lands and is stamped `Applied`. mutation[1]
/// returns `PreconditionFailed` and the live body at the destination does NOT
/// match the staged body (true contention). The executor must return
/// `Error::PartialCommit` and leave the intent in `.tx-log/` for the recovery
/// loop.
#[tokio::test]
async fn cas_executor_true_contention_mid_apply_leaves_intent() {
    let store = Arc::new(MemoryObjectStore::new());
    let lock = common::memory_lock();

    // Pre-land mutation[1]'s destination with a *different* body from what
    // the transaction will stage — this is true contention.
    store
        .put_if_absent("exec/contend", Bytes::from_static(b"someone-elses-body"))
        .await
        .expect("seed contend")
        .expect("etag");

    let stale_etag = Etag::new("\"stale-etag-never-matches\"");
    let our_staged_body = Bytes::from_static(b"our-staged-body");

    let executor = common::cas_executor(store.clone(), lock);

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "exec/first".to_owned(),
            body: Bytes::from_static(b"first-body"),
            expected: None,
        })
        .mutation(Mutation::Put {
            key: "exec/contend".to_owned(),
            body: our_staged_body,
            expected: Some(stale_etag),
        })
        .build();

    let result = executor.execute(tx).await;
    assert!(
        matches!(result, Err(TxError::PartialCommit)),
        "true contention must return PartialCommit, got: {result:?}"
    );

    // The contention key must still hold the original body.
    let body = store.get("exec/contend").await.expect("contend present");
    assert_eq!(body, b"someone-elses-body");

    // Intent must NOT have been reaped — recovery must be able to converge it.
    let logs = store.list(".tx-log/", 10, None).await.unwrap().items;
    assert_eq!(
        logs.len(),
        1,
        "intent must be preserved for recovery on true contention: {logs:?}"
    );
}
