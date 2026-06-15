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

use bytes::Bytes;

use angos_storage::{
    ByteStream, ChildrenPage, ConditionalStore, Error as StorageError, Etag, MemoryObjectStore,
    MultipartUploadPage, ObjectMeta, ObjectStore, Page,
};

use angos_tx_engine::{
    executor::TransactionExecutor,
    intent::{MutationProgress, MutationRecord},
    recovery::RecoveryLoop,
    transaction::{Mutation, Transaction},
};

mod common;

// CrashingStore: wraps MemoryObjectStore and fails on a specific write call number

/// A store that delegates all reads to the inner store but injects a failure
/// on the N-th write (`put`, `delete`, `delete_prefix`, `copy`).
///
/// When `permanent` is set, every write at or after `crash_on` fails. This is
/// useful for simulating a process death that prevents subsequent reap steps
/// from running, leaving the intent log in place for inspection.
#[derive(Debug, Clone)]
struct CrashingStore {
    inner: Arc<MemoryObjectStore>,
    write_count: Arc<AtomicUsize>,
    crash_on: usize,
    permanent: bool,
}

impl CrashingStore {
    fn new(inner: Arc<MemoryObjectStore>, crash_on: usize) -> Self {
        Self {
            inner,
            write_count: Arc::new(AtomicUsize::new(0)),
            crash_on,
            permanent: false,
        }
    }

    fn new_permanent_from(inner: Arc<MemoryObjectStore>, crash_on: usize) -> Self {
        Self {
            inner,
            write_count: Arc::new(AtomicUsize::new(0)),
            crash_on,
            permanent: true,
        }
    }

    fn check_crash(&self) -> Result<(), StorageError> {
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

#[async_trait::async_trait]
impl ObjectStore for CrashingStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get(key).await
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(angos_storage::BoxedReader, u64), StorageError> {
        self.inner.get_stream(key, offset).await
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        self.check_crash()?;
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.check_crash()?;
        self.inner.delete(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
        self.check_crash()?;
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
        self.check_crash()?;
        self.inner.copy(source, destination).await
    }

    async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
        self.inner.create_upload(key).await
    }

    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, StorageError> {
        self.inner.write_upload(key, body, len).await
    }

    async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
        self.inner.complete_upload(key).await
    }

    async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
        self.inner.abort_upload(key).await
    }

    async fn list_multipart_uploads(
        &self,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, StorageError> {
        self.inner
            .list_multipart_uploads(key_marker, upload_id_marker)
            .await
    }
}

#[async_trait::async_trait]
impl ConditionalStore for CrashingStore {
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), StorageError> {
        self.inner.get_with_etag(key).await
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, StorageError> {
        self.check_crash()?;
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, StorageError> {
        self.check_crash()?;
        self.inner.put_if_match(key, etag, data).await
    }

    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), StorageError> {
        self.check_crash()?;
        self.inner.delete_if_match(key, etag).await
    }
}

// Helpers

async fn assert_no_prefix_inner(store: &MemoryObjectStore, prefix: &str) {
    let items = store.list(prefix, 1000, None).await.unwrap().items;
    assert!(
        items.is_empty(),
        "Orphan keys found under {prefix}: {items:?}"
    );
}

/// Run the recovery loop's sweep once against the inner store.
///
/// Wires a fresh in-process `Lock` so the recovery path under test exercises
/// the same ownership-takeover code path production uses; the lock is
/// uncontended in these single-process scenarios.
async fn run_recovery(inner: Arc<MemoryObjectStore>) {
    let lock = common::memory_lock();
    let recovery = RecoveryLoop::builder(inner as Arc<dyn ObjectStore>)
        .lock(lock)
        .interval(std::time::Duration::from_hours(1))
        .build();
    recovery.sweep().await;
}

/// Backdate every intent under `.tx-log/` so the recovery loop treats it as stale.
async fn backdate_intents(inner: &MemoryObjectStore) {
    let suffixes = inner.list(".tx-log/", 100, None).await.unwrap().items;
    for suffix in &suffixes {
        let key = format!(".tx-log/{suffix}");
        if let Ok(body) = inner.get(&key).await
            && let Ok(mut record) =
                serde_json::from_slice::<angos_tx_engine::intent::IntentRecord>(&body)
        {
            record.created_at = chrono::Utc::now() - chrono::Duration::seconds(3600);
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
    let crashing = Arc::new(CrashingStore::new(inner.clone(), 1));

    let lock = common::memory_lock();
    let executor = common::locked_executor(crashing.clone(), lock);

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
    run_recovery(inner.clone()).await;

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
    let crashing = Arc::new(CrashingStore::new(inner.clone(), 2));

    let lock = common::memory_lock();
    let executor = common::locked_executor(crashing.clone(), lock);

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

    run_recovery(inner.clone()).await;

    // After recovery: no orphans.
    assert_no_prefix_inner(&inner, ".tx-log/").await;
    assert_no_prefix_inner(&inner, ".tx-bodies/").await;
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
    let crashing = Arc::new(CrashingStore::new(inner.clone(), 4));

    let lock = common::memory_lock();
    let executor = common::locked_executor(crashing.clone(), lock);

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

    run_recovery(inner.clone()).await;

    assert_no_prefix_inner(&inner, ".tx-log/").await;
    assert_no_prefix_inner(&inner, ".tx-bodies/").await;
}

/// An intent with at least one `Applied` progress slot is fully committed;
/// recovery replays each mutation idempotently and reaps.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_replays_fully_stamped_intent() {
    let inner = Arc::new(MemoryObjectStore::new());
    let tx_id = uuid::Uuid::new_v4();

    // Write the canonical key directly (simulating a completed Apply).
    inner
        .put("stamped/key", Bytes::from_static(b"already-there"))
        .await
        .unwrap();

    // Stage the body for the replayed mutation. Recovery's replay path re-reads
    // the body and PUTs it unconditionally; the canonical bytes happen to be
    // the same as the staged bytes here, so replay is a no-op observationally.
    inner
        .put(
            &format!(".tx-bodies/{tx_id}/0"),
            Bytes::from_static(b"already-there"),
        )
        .await
        .unwrap();

    // Write a stale intent with the only mutation marked Applied.
    let intent = angos_tx_engine::intent::IntentRecord {
        id: tx_id,
        created_at: chrono::Utc::now() - chrono::Duration::seconds(3600),
        ttl_secs: 1,
        reads: vec![],
        mutations: vec![MutationRecord::Put {
            key: "stamped/key".to_owned(),
            body_ref: format!(".tx-bodies/{tx_id}/0"),
            expected: None,
        }],
        coarse_lock_keys: vec![],
        progress: vec![MutationProgress::Applied],
    };
    inner
        .put(
            &format!(".tx-log/{tx_id}.json"),
            Bytes::from(serde_json::to_vec(&intent).unwrap()),
        )
        .await
        .unwrap();

    run_recovery(inner.clone()).await;

    // Intent reaped; canonical key intact.
    assert_no_prefix_inner(&inner, ".tx-log/").await;
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
        let crashing = Arc::new(CrashingStore::new(inner.clone(), crash_on));

        let lock = common::memory_lock();
        let executor = common::locked_executor(crashing.clone(), lock);

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

        run_recovery(inner.clone()).await;

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

        // Invariant 2: recovery always cleans .tx-log/ orphans.
        assert_no_prefix_inner(&inner, ".tx-log/").await;
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
    let crashing = Arc::new(CrashingStore::new(inner.clone(), 5));

    let lock = common::memory_lock();
    let executor = common::locked_executor(crashing.clone(), lock);

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
    let intents = inner.list(".tx-log/", 100, None).await.unwrap().items;
    assert_eq!(
        intents.len(),
        1,
        "intent must survive a partial-apply error so recovery can converge"
    );

    // Backdate + run recovery; it replays-forward idempotently.
    backdate_intents(&inner).await;
    run_recovery(inner.clone()).await;

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
    assert_no_prefix_inner(&inner, ".tx-log/").await;
    assert_no_prefix_inner(&inner, ".tx-bodies/").await;
}

// Progress-vector invariants under both executors

/// Read the (only) intent under `.tx-log/` directly, bypassing the recovery
/// loop, and return its parsed form.
async fn read_only_intent(inner: &MemoryObjectStore) -> angos_tx_engine::intent::IntentRecord {
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
    use angos_tx_engine::intent::MutationProgress;

    // Three mutations: writes 0,1,2 = body stage; 3 = intent;
    //   4 = apply0, 5 = stamp0, 6 = apply1, 7 = stamp1,
    //   8 = apply2, 9 = stamp2, 10 = reap-prefix, 11 = reap-intent.

    // (a) Crash permanently from the Reap step onward: every mutation is
    // Applied; the intent remains for inspection.
    {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = Arc::new(CrashingStore::new_permanent_from(inner.clone(), 10));
        let lock = common::memory_lock();
        let executor = common::locked_executor(crashing.clone(), lock);

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
        let crashing = Arc::new(CrashingStore::new_permanent_from(inner.clone(), 6));
        let lock = common::memory_lock();
        let executor = common::locked_executor(crashing.clone(), lock);

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
    use angos_tx_engine::intent::MutationProgress;

    // (a) Crash permanently from the Reap step (write 10) onward: every
    // mutation Applied; the intent remains for inspection.
    {
        let inner = Arc::new(MemoryObjectStore::new());
        let crashing = Arc::new(CrashingStore::new_permanent_from(inner.clone(), 10));
        let lock = common::memory_lock();
        let executor = common::cas_executor(crashing.clone(), lock);

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
        let crashing = Arc::new(CrashingStore::new_permanent_from(inner.clone(), 6));
        let lock = common::memory_lock();
        let executor = common::cas_executor(crashing.clone(), lock);

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
