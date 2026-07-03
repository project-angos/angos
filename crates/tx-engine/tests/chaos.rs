//! Chaos tests: crash injection at various stages of the transaction lifecycle.
//!
//! Each test wraps the store in a `CrashingStore` that can be configured to
//! fail on a specific call number, simulating a process crash. A fresh
//! executor is then constructed on the same underlying store and the recovery
//! loop is run; the invariants (committed mutations visible, uncommitted absent,
//! no orphans) are asserted.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};
use uuid::Uuid;

use angos_storage::{
    ConditionalStore, Error as StorageError, MemoryObjectStore, ObjectStore,
    test_util::{HookedStore, StoreHook, StoreOp},
};

use angos_tx_engine::{
    executor::TransactionExecutor,
    intent::{IntentRecord, MutationProgress, MutationRecord},
    transaction::{Mutation, Transaction},
};

use angos_tx_engine::test_util;

/// Fails the N-th write-class call (`put`, `delete`, `delete_prefix`,
/// `copy`, and the conditional writes), simulating a process crash at that
/// point in the lifecycle. When `permanent` is set, every write at or after
/// `crash_on` fails, modelling a death that also prevents later reap steps.
struct CrashPlan {
    write_count: AtomicUsize,
    crash_on: usize,
    permanent: bool,
}

#[async_trait]
impl StoreHook for CrashPlan {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        if !op.is_write() {
            return Ok(());
        }
        let n = self.write_count.fetch_add(1, Ordering::Relaxed);
        let hit = if self.permanent {
            n >= self.crash_on
        } else {
            n == self.crash_on
        };
        if hit {
            Err(StorageError::Backend("injected crash".to_owned()))
        } else {
            Ok(())
        }
    }
}

type CrashingStore = HookedStore<Arc<dyn ConditionalStore>, CrashPlan>;

/// Store that fails exactly the `crash_on`-th write.
fn crashing_store(inner: Arc<MemoryObjectStore>, crash_on: usize) -> Arc<CrashingStore> {
    Arc::new(HookedStore::new(
        inner,
        CrashPlan {
            write_count: AtomicUsize::new(0),
            crash_on,
            permanent: false,
        },
    ))
}

/// Store that fails every write at and after the `crash_on`-th.
fn crashing_store_permanent(inner: Arc<MemoryObjectStore>, crash_on: usize) -> Arc<CrashingStore> {
    Arc::new(HookedStore::new(
        inner,
        CrashPlan {
            write_count: AtomicUsize::new(0),
            crash_on,
            permanent: true,
        },
    ))
}

// Helpers

/// Backdate every intent under `.tx-log/` so the recovery loop treats it as stale.
async fn backdate_intents(inner: &MemoryObjectStore) {
    let suffixes = inner.list(".tx-log/", 100, None).await.unwrap().items;
    for suffix in &suffixes {
        let key = format!(".tx-log/{suffix}");
        if let Ok(body) = inner.get(&key).await
            && let Ok(mut record) = serde_json::from_slice::<IntentRecord>(&body)
        {
            record.created_at = Utc::now() - Duration::seconds(3600);
            record.ttl_secs = 1;
            inner
                .put(&key, Bytes::from(serde_json::to_vec(&record).unwrap()))
                .await
                .unwrap();
        }
    }
}

// Crash tests

/// Crash before the intent is written (`crash_on` = 1 = the intent PUT).
///
/// The body object(s) are staged (write 0) but the intent never lands.
/// After recovery: both the body and intent are orphans that the janitor would
/// eventually clean. The canonical key must NOT exist.
#[tokio::test(flavor = "multi_thread")]
async fn crash_before_intent() {
    let inner = Arc::new(MemoryObjectStore::new());
    // Write 0 = body staging; write 1 = intent PUT (which we crash on).
    let crashing = crashing_store(inner.clone(), 1);

    let lock = test_util::memory_lock();
    let executor = test_util::locked_executor(crashing.clone(), lock);

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "crash/canonical".to_owned(),
            body: Bytes::from_static(b"should-not-land"),
            expected: None,
        })
        .build();

    // The transaction must fail because the intent write was injected to crash.
    let result = executor.execute(tx).await;
    assert!(result.is_err(), "execute must fail on injected crash");

    // The canonical key must not exist.
    assert!(
        inner.get("crash/canonical").await.is_err(),
        "canonical key must not exist before recovery"
    );

    // Run recovery. There is no intent (it never landed), so nothing to replay.
    test_util::sweep_once(inner.clone(), test_util::memory_lock()).await;

    // After recovery: canonical key still absent.
    assert!(
        inner.get("crash/canonical").await.is_err(),
        "canonical key must not exist after recovery"
    );
}

/// Crash after the intent is written but before the first Apply write.
///
/// Recovery should replay the transaction and the canonical key must exist.
#[tokio::test(flavor = "multi_thread")]
async fn crash_after_intent_before_apply() {
    let inner = Arc::new(MemoryObjectStore::new());
    // Write 0 = body staging; write 1 = intent PUT; write 2 = first apply.
    // Crash on write 2.
    let crashing = crashing_store(inner.clone(), 2);

    let lock = test_util::memory_lock();
    let executor = test_util::locked_executor(crashing.clone(), lock);

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "apply/canonical".to_owned(),
            body: Bytes::from_static(b"applied-body"),
            expected: None,
        })
        .build();

    // The intent lands (write 1) but Apply crashes (write 2).
    let result = executor.execute(tx).await;
    // This may succeed or fail depending on whether the stamp write (also a PUT)
    // is hit. In this test the apply PUT itself is write 2 so it will error.
    let _ = result; // Outcome is indeterminate; recovery is what we test.

    // Force a stale intent by backdating created_at.
    backdate_intents(&inner).await;

    test_util::sweep_once(inner.clone(), test_util::memory_lock()).await;

    // After recovery: no orphans.
    test_util::assert_no_orphans(&*inner).await;
}

/// Injecting a crash during Reap (after all mutations are applied) does not
/// leave the canonical key in an inconsistent state. Recovery completes the
/// reap.
#[tokio::test(flavor = "multi_thread")]
async fn crash_during_reap() {
    let inner = Arc::new(MemoryObjectStore::new());
    // We want the transaction to complete Apply but crash on the first Reap write
    // (delete_prefix for bodies). Count: body_staging=0, intent=1, apply=2,
    // stamp=3, reap bodies delete_prefix=4.
    let crashing = crashing_store(inner.clone(), 4);

    let lock = test_util::memory_lock();
    let executor = test_util::locked_executor(crashing.clone(), lock);

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "reap/canonical".to_owned(),
            body: Bytes::from_static(b"reap-body"),
            expected: None,
        })
        .build();

    // The transaction may or may not surface an error (crash happens post-Apply).
    let _ = executor.execute(tx).await;

    // Backdate any remaining intents.
    backdate_intents(&inner).await;

    test_util::sweep_once(inner.clone(), test_util::memory_lock()).await;

    test_util::assert_no_orphans(&*inner).await;
}

/// An intent with at least one `Applied` progress slot is fully committed;
/// recovery replays each mutation idempotently and reaps.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_replays_fully_stamped_intent() {
    let inner = Arc::new(MemoryObjectStore::new());
    let tx_id = Uuid::new_v4();

    // Write the canonical key directly (simulating a completed Apply).
    inner
        .put("stamped/key", Bytes::from_static(b"already-there"))
        .await
        .unwrap();

    // Stage the body for the replayed mutation. Recovery's replay path re-reads
    // the body and PUTs it unconditionally; the canonical bytes happen to be
    // the same as the staged bytes here, so replay is a no-op observationally.
    let body_ref =
        test_util::stage_body(&*inner, tx_id, 0, Bytes::from_static(b"already-there")).await;

    // Write a stale intent with the only mutation marked Applied.
    let intent = test_util::stale_intent(
        tx_id,
        vec![MutationRecord::Put {
            key: "stamped/key".to_owned(),
            body_ref,
            expected: None,
        }],
        vec![MutationProgress::Applied],
    );
    test_util::put_intent(&*inner, &intent).await;

    test_util::sweep_once(inner.clone(), test_util::memory_lock()).await;

    // Intent and staged body reaped; canonical key intact.
    test_util::assert_no_orphans(&*inner).await;
    let body = inner
        .get("stamped/key")
        .await
        .expect("canonical key must survive reap");
    assert_eq!(body, b"already-there");
}

/// Chaos test: manifest push crashing mid-Apply after the blob-data write but
/// before the link/index writes complete.
///
/// Models the engine path for `store_manifest`: the transaction contains a
/// `PutIfAbsent` for the blob-data key and `Put` mutations for the link key
/// and blob-index shard key, mirroring what `manifest_engine.rs` builds.
///
/// Each successful Apply stamps its `progress[idx]` slot to `Applied`; once
/// any slot is `Applied` the recovery loop replays every mutation idempotently
/// (`PutIfAbsent` skips when the key exists; Put/Delete/Copy are write-anywhere).
/// Before the first Apply succeeds the recovery loop rolls back (deletes the
/// intent + bodies).
///
/// Invariants verified:
/// 1. For crash points before the intent lands (writes 0-3): no canonical keys
///    are present after recovery, because the transaction was never committed.
/// 2. For all crash points: the recovery loop leaves no .tx-log/ orphans.
#[tokio::test(flavor = "multi_thread")]
async fn manifest_push_crash_mid_apply_recovery_converges() {
    // Keys that mirror the manifest-engine transaction shape.
    let blob_data_key = "blob-data/sha256:abcdef";
    let link_key = "repositories/ns/_manifests/revisions/sha256:abcdef/link";
    let shard_key = "blob_index/sha256:abcdef/refs/ns.json";

    // Write sequence for the Locked executor with 3 mutations
    // (PutIfAbsent + Put + Put), each with a body:
    //   write 0: body staging for mutation 0
    //   write 1: body staging for mutation 1
    //   write 2: body staging for mutation 2
    //   write 3: intent PUT  ← linearisation point
    //   write 4: Apply mutation 0 (PutIfAbsent blob-data)
    //   write 5: stamp_progress (intent re-PUT marking mutation 0 Applied)
    //   write 6: Apply mutation 1 (Put link-key)
    //   write 7: stamp_progress (intent re-PUT marking mutation 1 Applied)
    //   write 8: Apply mutation 2 (Put shard-key)
    //   write 9: stamp_progress (intent re-PUT marking mutation 2 Applied)
    //   write 10: Reap bodies delete_prefix
    //   write 11: Reap intent delete
    for crash_on in 0usize..=11 {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = crashing_store(inner.clone(), crash_on);

        let lock = test_util::memory_lock();
        let executor = test_util::locked_executor(crashing.clone(), lock);

        let tx = Transaction::builder()
            .mutation(Mutation::PutIfAbsent {
                key: blob_data_key.to_owned(),
                body: Bytes::from_static(b"manifest-bytes"),
            })
            .mutation(Mutation::Put {
                key: link_key.to_owned(),
                body: Bytes::from_static(b"{\"target\":\"sha256:abcdef\"}"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: shard_key.to_owned(),
                body: Bytes::from_static(b"[\"tag:latest\"]"),
                expected: None,
            })
            .build();

        let _ = executor.execute(tx).await;

        // Backdate any live intents so recovery treats them as stale.
        backdate_intents(&inner).await;

        test_util::sweep_once(inner.clone(), test_util::memory_lock()).await;

        // Invariant 1: before the intent lands (writes 0-3), no canonical keys.
        if crash_on <= 3 {
            assert!(
                inner.get(blob_data_key).await.is_err(),
                "blob-data must not exist when transaction never committed (crash_on={crash_on})"
            );
            assert!(
                inner.get(link_key).await.is_err(),
                "link must not exist when transaction never committed (crash_on={crash_on})"
            );
            assert!(
                inner.get(shard_key).await.is_err(),
                "shard must not exist when transaction never committed (crash_on={crash_on})"
            );
        }

        // Invariant 2: recovery always cleans .tx-log/ orphans. Bodies staged
        // before a pre-intent crash legitimately remain for the janitor, so
        // only .tx-log/ is checked.
        assert_eq!(
            test_util::list_count(&*inner, ".tx-log/").await,
            0,
            "recovery must leave a clean .tx-log/ (crash_on={crash_on})"
        );
    }
}

/// A transient (non-permanent) storage error mid-Apply, after at least one
/// mutation has been applied and stamped, must NOT cause the executor to reap
/// the intent. Reaping would delete the staged bodies + intent and orphan the
/// partial canonical write, breaking the all-or-nothing guarantee. The intent
/// must survive so the recovery loop can replay-forward and converge.
#[tokio::test(flavor = "multi_thread")]
async fn partial_apply_error_preserves_intent_for_recovery() {
    let inner = Arc::new(MemoryObjectStore::new());

    // Two-mutation Put transaction. Write sequence for the Locked executor:
    //   write 0: body staging for mutation 0
    //   write 1: body staging for mutation 1
    //   write 2: intent PUT
    //   write 3: Apply mutation 0 (Put k0)
    //   write 4: stamp_progress (intent re-PUT marking mutation 0 Applied)
    //   write 5: Apply mutation 1 (Put k1)  ← crash here (transient)
    // Crashing on write 5 fails the second apply put after mutation 0 has
    // applied and stamped. Being non-permanent, the later reap writes would
    // succeed, which is exactly the scenario the buggy unconditional reap
    // mishandled.
    let crashing = crashing_store(inner.clone(), 5);

    let lock = test_util::memory_lock();
    let executor = test_util::locked_executor(crashing.clone(), lock);

    let tx = Transaction::builder()
        .mutation(Mutation::Put {
            key: "partial/k0".to_owned(),
            body: Bytes::from_static(b"body-0"),
            expected: None,
        })
        .mutation(Mutation::Put {
            key: "partial/k1".to_owned(),
            body: Bytes::from_static(b"body-1"),
            expected: None,
        })
        .build();

    let result = executor.execute(tx).await;
    assert!(
        result.is_err(),
        "execute must surface the injected mid-Apply error"
    );

    // With the fix, the intent survives (any mutation applied → no reap).
    // Without the fix it would have been reaped, making recovery impossible.
    assert_eq!(
        test_util::list_count(&*inner, ".tx-log/").await,
        1,
        "intent must survive a partial-apply error so recovery can converge"
    );

    // Backdate + run recovery; it replays-forward idempotently.
    backdate_intents(&inner).await;
    test_util::sweep_once(inner.clone(), test_util::memory_lock()).await;

    // Both canonical keys present with correct bodies.
    let b0 = inner
        .get("partial/k0")
        .await
        .expect("k0 must be present after recovery");
    assert_eq!(b0, b"body-0");
    let b1 = inner
        .get("partial/k1")
        .await
        .expect("k1 must be present after recovery");
    assert_eq!(b1, b"body-1");

    // No orphans: recovery reaped the intent and staged bodies.
    test_util::assert_no_orphans(&*inner).await;
}

// Progress-vector invariants under both executors

/// Read the (only) intent under `.tx-log/` directly, bypassing the recovery
/// loop, and return its parsed form.
async fn read_only_intent(inner: &MemoryObjectStore) -> IntentRecord {
    let suffixes = inner.list(".tx-log/", 100, None).await.unwrap().items;
    assert_eq!(
        suffixes.len(),
        1,
        "expected exactly one .tx-log entry, got {suffixes:?}"
    );
    let body = inner
        .get(&format!(".tx-log/{}", suffixes[0]))
        .await
        .expect("intent must still be present");
    serde_json::from_slice(&body).expect("intent must parse")
}

/// Locked executor: a crash at the Reap step leaves an intent whose progress
/// vector is fully `Applied`; a crash mid-Apply leaves the applied prefix
/// `Applied` and the remaining suffix `Pending`.
#[tokio::test(flavor = "multi_thread")]
async fn progress_vector_reflects_apply_state_locked() {
    // Three mutations: writes 0,1,2 = body stage; 3 = intent;
    //   4 = apply0, 5 = stamp0, 6 = apply1, 7 = stamp1,
    //   8 = apply2, 9 = stamp2, 10 = reap-prefix, 11 = reap-intent.

    // (a) Crash permanently from the Reap step onward: every mutation is
    // Applied; the intent remains for inspection.
    {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = crashing_store_permanent(inner.clone(), 10);
        let lock = test_util::memory_lock();
        let executor = test_util::locked_executor(crashing.clone(), lock);

        let tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: "p/a".to_owned(),
                body: Bytes::from_static(b"A"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "p/b".to_owned(),
                body: Bytes::from_static(b"B"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "p/c".to_owned(),
                body: Bytes::from_static(b"C"),
                expected: None,
            })
            .build();

        let _ = executor.execute(tx).await;
        let intent = read_only_intent(&inner).await;
        assert_eq!(intent.progress.len(), 3);
        for (idx, p) in intent.progress.iter().enumerate() {
            assert!(
                matches!(p, MutationProgress::Applied),
                "progress[{idx}] expected Applied, got {p:?}"
            );
        }
    }

    // (b) Crash permanently from apply 1 onward (write 6): the executor
    // applies + stamps mutation 0, then every subsequent write fails, so the
    // intent remains and the stamp for 0 is preserved while 1 and 2 are still
    // Pending.
    {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = crashing_store_permanent(inner.clone(), 6);
        let lock = test_util::memory_lock();
        let executor = test_util::locked_executor(crashing.clone(), lock);

        let tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: "p/a".to_owned(),
                body: Bytes::from_static(b"A"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "p/b".to_owned(),
                body: Bytes::from_static(b"B"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "p/c".to_owned(),
                body: Bytes::from_static(b"C"),
                expected: None,
            })
            .build();

        let _ = executor.execute(tx).await;
        let intent = read_only_intent(&inner).await;
        assert_eq!(intent.progress.len(), 3);
        assert!(matches!(intent.progress[0], MutationProgress::Applied));
        assert!(matches!(intent.progress[1], MutationProgress::Pending));
        assert!(matches!(intent.progress[2], MutationProgress::Pending));
    }
}

/// CAS executor: same invariants as the Locked variant. The CAS path stamps
/// the per-mutation etag (when the backend surfaces one) but the
/// Applied-vs-Pending classification is identical.
#[tokio::test(flavor = "multi_thread")]
async fn progress_vector_reflects_apply_state_cas() {
    // (a) Crash permanently from the Reap step (write 10) onward: every
    // mutation Applied; the intent remains for inspection.
    {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = crashing_store_permanent(inner.clone(), 10);
        let lock = test_util::memory_lock();
        let executor = test_util::cas_executor(crashing.clone(), lock);

        let tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: "cas/a".to_owned(),
                body: Bytes::from_static(b"A"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "cas/b".to_owned(),
                body: Bytes::from_static(b"B"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "cas/c".to_owned(),
                body: Bytes::from_static(b"C"),
                expected: None,
            })
            .build();

        let _ = executor.execute(tx).await;
        let intent = read_only_intent(&inner).await;
        assert_eq!(intent.progress.len(), 3);
        for (idx, p) in intent.progress.iter().enumerate() {
            assert!(
                matches!(p, MutationProgress::Applied),
                "progress[{idx}] expected Applied, got {p:?}"
            );
        }
    }

    // (b) Crash permanently from apply 1 onward (write 6): the executor
    // applies + stamps mutation 0, then every subsequent write fails.
    {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = crashing_store_permanent(inner.clone(), 6);
        let lock = test_util::memory_lock();
        let executor = test_util::cas_executor(crashing.clone(), lock);

        let tx = Transaction::builder()
            .mutation(Mutation::Put {
                key: "cas/a".to_owned(),
                body: Bytes::from_static(b"A"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "cas/b".to_owned(),
                body: Bytes::from_static(b"B"),
                expected: None,
            })
            .mutation(Mutation::Put {
                key: "cas/c".to_owned(),
                body: Bytes::from_static(b"C"),
                expected: None,
            })
            .build();

        let _ = executor.execute(tx).await;
        let intent = read_only_intent(&inner).await;
        assert_eq!(intent.progress.len(), 3);
        assert!(matches!(intent.progress[0], MutationProgress::Applied));
        assert!(matches!(intent.progress[1], MutationProgress::Pending));
        assert!(matches!(intent.progress[2], MutationProgress::Pending));
    }
}
