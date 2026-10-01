use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone as _, Utc};
use serde_json::json;
use tempfile::TempDir;
use tokio::{sync::Semaphore, time::sleep};
use tokio_util::sync::CancellationToken;

use angos_storage::{
    Error as StorageError, ObjectStore,
    fs::Backend as StorageFsBackend,
    test_util::{HookedStore, StoreHook, StoreOp},
};

use crate::jobs::store::{
    ClaimCheck, ClaimMode, ClaimedJob, CompleteOutcome, DeadLetterRecord, Error, FailOutcome,
    JOBS_ROOT, JobEnvelope, JobQueueConfig, JobRetryPolicy, JobState, JobStore, LockKey,
    MAX_REPORTED_PENDING, Queue, QueueDepthRefresh, STORAGE_KEY_PREFIX_LEN, ensure_claim_support,
    job_claim_path, job_lock_key_index_path, job_pending_path, make_storage_key,
    parse_lock_key_index, parse_not_before, queue_depth_refresh_loop, serialize_lock_key_index,
    should_cancel_claim,
};
use crate::metrics_provider;
use crate::registry::{Error as RegistryError, test_utils::metadata_store_over};

struct Harness {
    store: Arc<JobStore>,
    // Raw handle lets tests stage deliberate fixture state (count-cap
    // stress, orphan indexes) that the engine would never produce naturally.
    raw: Arc<dyn ObjectStore>,
    // Keeps the backing directory alive for as long as the harness.
    _dir: TempDir,
}

/// A job store over a private fs backend rooted in its own temp directory.
fn harness() -> Harness {
    metrics_provider::init_for_tests();
    let (raw, dir) = fs_store();
    let store = Arc::new(JobStore::new(
        &metadata_store_over(raw.clone()),
        "test-worker",
        ClaimMode::Atomic,
    ));
    Harness {
        store,
        raw,
        _dir: dir,
    }
}

/// A bare fs object store and the temp directory that must outlive it. Tests
/// that wrap the store in a [`HookedStore`] build their `JobStore` by hand.
fn fs_store() -> (Arc<dyn ObjectStore>, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let store: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
    (store, dir)
}

fn lock_key(value: &str) -> LockKey {
    LockKey::new(value).expect("test lock key must be valid")
}

fn dummy_envelope(key: &str) -> JobEnvelope {
    JobEnvelope::new(Queue::Cache, "test.noop", key, &()).expect("envelope")
}

// =========================================================================
// End-to-end claim cycle
// =========================================================================

#[tokio::test]
async fn enqueue_then_claim_succeeds() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:aaa"))
        .await
        .expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim_one")
        .claimed
        .expect("Some");
    assert_eq!(claimed.envelope.lock_key, lock_key("cache.ns:sha256:aaa"));
    h.store.complete(claimed).await.expect("complete");
    assert!(
        h.store
            .claim_one(Queue::Cache)
            .await
            .expect("claim_one")
            .claimed
            .is_none(),
        "queue must be empty after complete",
    );
}

// =========================================================================
// Retry + dead-letter (consumer-driven storage behaviour)
// =========================================================================

#[tokio::test]
async fn retry_writes_pending_with_backoff() {
    let h = harness();
    let mut env = dummy_envelope("cache.ns:sha256:retry");
    env.max_attempts = Some(3);
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(pending.len(), 1, "exactly one retry envelope expected");
    let storage_key = &pending[0];
    let not_before = parse_not_before(storage_key).expect("parse prefix");
    assert!(not_before > Utc::now(), "retry must be backed off");

    let updated = h
        .store
        .read_pending(Queue::Cache, storage_key)
        .await
        .expect("read updated");
    assert_eq!(updated.attempts, 1);
}

#[tokio::test]
async fn dead_letter_after_max_attempts() {
    let h = harness();
    let mut env = dummy_envelope("cache.ns:sha256:dl");
    env.max_attempts = Some(1);
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let storage_key = claimed.storage_key.clone();

    assert!(matches!(
        h.store.fail(claimed, "final error").await.expect("fail"),
        FailOutcome::MovedToDeadLetter
    ));
    assert!(matches!(
        h.store.read_pending(Queue::Cache, &storage_key).await,
        Err(crate::jobs::store::Error::NotFound)
    ));
}

// =========================================================================
// queue_depth_refresh_loop
// =========================================================================

/// The pending gauge once it reads `expected`, else whatever it last read
/// within the budget, so the assertion reports the value instead of racing the
/// ticker.
async fn settled_pending_gauge(expected: i64) -> i64 {
    let mut value = i64::MIN;
    for _ in 0..200 {
        value = metrics_provider::metrics_provider()
            .job_queue_pending
            .with_label_values(&[Queue::Cache.as_str()])
            .get();
        if value == expected {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    value
}

fn depth_refresh(store: &Arc<JobStore>, period: Duration) -> QueueDepthRefresh {
    QueueDepthRefresh {
        store: store.clone(),
        period,
        ready_horizon_secs: 600,
    }
}

/// A configuration reload replaces what the ticker reads, so the gauges
/// describe the store the reloaded server enqueues into rather than the one it
/// booted with, and a changed cadence rebuilds the ticker rather than pinning
/// it to the boot-time period.
#[tokio::test]
async fn queue_depth_gauges_follow_a_reloaded_store() {
    let booted = harness();
    booted
        .store
        .enqueue(dummy_envelope("cache.ns:sha256:booted"))
        .await
        .expect("enqueue");
    let reloaded = harness();
    for key in ["cache.ns:sha256:one", "cache.ns:sha256:two"] {
        reloaded
            .store
            .enqueue(dummy_envelope(key))
            .await
            .expect("enqueue");
    }

    let refresh = Arc::new(ArcSwap::from_pointee(depth_refresh(
        &booted.store,
        Duration::from_millis(10),
    )));
    let shutdown = CancellationToken::new();
    let ticker = tokio::spawn(queue_depth_refresh_loop(
        Arc::clone(&refresh),
        Queue::Cache,
        shutdown.clone(),
    ));

    assert_eq!(
        settled_pending_gauge(1).await,
        1,
        "the booted store holds one pending job"
    );

    refresh.store(Arc::new(depth_refresh(
        &reloaded.store,
        Duration::from_millis(10),
    )));
    assert_eq!(
        settled_pending_gauge(2).await,
        2,
        "the reloaded store's depth must replace the booted one"
    );

    // A new cadence as well as a new store: the ticker rebuilds and keeps
    // publishing.
    refresh.store(Arc::new(depth_refresh(
        &booted.store,
        Duration::from_millis(20),
    )));
    assert_eq!(
        settled_pending_gauge(1).await,
        1,
        "a reload changing the period must keep refreshing"
    );

    shutdown.cancel();
    ticker.await.expect("the ticker stops on cancellation");
}

// =========================================================================
// count_pending
// =========================================================================

#[tokio::test]
async fn count_pending_saturates_at_cap() {
    let h = harness();
    let now = Utc::now();
    for i in 0..(MAX_REPORTED_PENDING + 5) {
        let key = make_storage_key(now, &format!("stub-{i}"));
        h.raw
            .put(
                &crate::jobs::store::job_pending_path("cache", &key),
                Bytes::from_static(b"{}"),
            )
            .await
            .expect("stub");
    }
    let count = h
        .store
        .count_pending(Queue::Cache, 600)
        .await
        .expect("count");
    assert_eq!(count, MAX_REPORTED_PENDING);
}

#[tokio::test]
async fn count_pending_excludes_envelopes_past_readiness_horizon() {
    let h = harness();
    let now = Utc::now();
    for i in 0..2 {
        let key = make_storage_key(now, &format!("ready-{i}"));
        h.raw
            .put(
                &crate::jobs::store::job_pending_path("cache", &key),
                Bytes::from_static(b"{}"),
            )
            .await
            .expect("ready");
    }
    let far_future = now + chrono::Duration::hours(1);
    for i in 0..2 {
        let key = make_storage_key(far_future, &format!("future-{i}"));
        h.raw
            .put(
                &crate::jobs::store::job_pending_path("cache", &key),
                Bytes::from_static(b"{}"),
            )
            .await
            .expect("future");
    }

    let count = h
        .store
        .count_pending(Queue::Cache, 60)
        .await
        .expect("count");
    assert_eq!(count, 2, "only ready envelopes must count");
}

// =========================================================================
// count_failed
// =========================================================================

#[tokio::test]
async fn count_failed_reflects_dead_letters() {
    let h = harness();
    assert_eq!(h.store.count_failed(Queue::Cache).await.expect("count"), 0);

    let mut env = dummy_envelope("cache.ns:sha256:dl-count");
    env.max_attempts = Some(1);
    h.store.enqueue(env).await.expect("enqueue");
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    assert!(matches!(
        h.store.fail(claimed, "final error").await.expect("fail"),
        FailOutcome::MovedToDeadLetter
    ));

    assert_eq!(
        h.store.count_failed(Queue::Cache).await.expect("count"),
        1,
        "a dead-lettered job must be counted by count_failed",
    );
    assert_eq!(
        h.store
            .count_pending(Queue::Cache, 600)
            .await
            .expect("count"),
        0,
        "a dead-lettered job is no longer pending",
    );
}

#[tokio::test]
async fn future_storage_key_yields_next_ready_without_claiming() {
    let h = harness();
    let mut env = dummy_envelope("cache.ns:sha256:future");
    env.max_attempts = Some(5);
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let scheduled = match h.store.fail(claimed, "scheduled").await.expect("fail") {
        FailOutcome::Retried { next_at } => next_at,
        FailOutcome::MovedToDeadLetter => panic!("expected retry"),
    };

    let outcome = h.store.claim_one(Queue::Cache).await.expect("claim");
    assert!(
        outcome.claimed.is_none(),
        "future-scheduled job must not be claimed",
    );
    let next = outcome.next_ready.expect("next_ready must be set");
    let diff = (scheduled - next).num_milliseconds().abs();
    assert!(
        diff < 2,
        "next_ready ({next}) must match scheduled ({scheduled})"
    );
}

// =========================================================================
// Dedup index (lock-key index)
// =========================================================================

#[tokio::test]
async fn orphan_index_is_self_healed_on_next_lookup() {
    let h = harness();
    let lock_key = lock_key("cache.ns:sha256:orphan");

    let storage_key = make_storage_key(Utc::now(), "phantom-id");
    let index_data = serialize_lock_key_index(&storage_key).expect("serialize");
    let index_path = crate::jobs::store::job_lock_key_index_path("cache", &lock_key);
    h.raw
        .put(&index_path, Bytes::from(index_data))
        .await
        .expect("seed index");

    let hit = h
        .store
        .find_pending_with_lock_key(Queue::Cache, &lock_key)
        .await
        .expect("lookup");
    assert!(!hit, "orphan index must not register as a hit");

    assert!(
        h.raw.head(&index_path).await.is_err(),
        "orphan index must be self-healed",
    );
}

/// Fails the plain `Delete` of one key, modelling a transient backend error on
/// the orphan-index self-heal while leaving every other op untouched.
struct FailIndexDelete {
    index_path: String,
}

#[async_trait]
impl StoreHook for FailIndexDelete {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        match op {
            StoreOp::Delete { key } if key == self.index_path => Err(StorageError::Backend(
                "injected orphan-delete failure".to_string(),
            )),
            _ => Ok(()),
        }
    }
}

#[tokio::test]
async fn orphan_index_transient_delete_failure_does_not_drop_enqueue() {
    metrics_provider::init_for_tests();
    let lock_key = lock_key("cache.ns:sha256:orphan-transient");
    let index_path = crate::jobs::store::job_lock_key_index_path("cache", &lock_key);

    let (inner, _dir) = fs_store();
    let hook = FailIndexDelete {
        index_path: index_path.clone(),
    };
    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(inner.clone(), hook));
    let store = Arc::new(JobStore::new(
        &metadata_store_over(hooked),
        "test-worker",
        ClaimMode::Atomic,
    ));

    // Seed an orphan index (index present, pending file absent) through the
    // inner store so the fault hook does not intercept the fixture write.
    let storage_key = make_storage_key(Utc::now(), "phantom-id");
    let index_data = serialize_lock_key_index(&storage_key).expect("serialize");
    inner
        .put(&index_path, Bytes::from(index_data))
        .await
        .expect("seed index");

    // The self-heal delete fails transiently, so the lookup surfaces the error
    // rather than reporting a false miss.
    assert!(
        store
            .find_pending_with_lock_key(Queue::Cache, &lock_key)
            .await
            .is_err(),
        "a transient orphan-cleanup failure must surface as an error",
    );

    // And the enqueue must not silently drop the distinct job: it propagates the
    // failure instead of colliding with the lingering index on `PutIfAbsent` and
    // returning a false dedup hit.
    assert!(
        store
            .enqueue(dummy_envelope(lock_key.as_str()))
            .await
            .is_err(),
        "enqueue must not silently drop a job behind an un-retired orphan index",
    );
}

/// Fails the second `get` of a pending-job file with `NotFound`, modelling
/// another worker completing the job (deleting its pending file) between the
/// claim's first read and its post-acquire re-read.
struct VanishOnSecondPendingRead {
    pending_reads: AtomicUsize,
}

#[async_trait]
impl StoreHook for VanishOnSecondPendingRead {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        if let StoreOp::Get { key } = op
            && key.contains("/pending/")
            && self.pending_reads.fetch_add(1, Ordering::SeqCst) >= 1
        {
            return Err(StorageError::NotFound);
        }
        Ok(())
    }
}

#[tokio::test]
async fn claim_rechecks_pending_under_lock_and_skips_a_vanished_job() {
    metrics_provider::init_for_tests();
    let (inner, _dir) = fs_store();
    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner,
        VanishOnSecondPendingRead {
            pending_reads: AtomicUsize::new(0),
        },
    ));
    let store = Arc::new(JobStore::new(
        &metadata_store_over(hooked),
        "test-worker",
        ClaimMode::Atomic,
    ));

    store
        .enqueue(dummy_envelope("cache.ns:sha256:vanish"))
        .await
        .expect("enqueue");

    // The claim reads the pending file, acquires the lock, then re-reads: the
    // re-read finds it gone, so the job is skipped rather than claimed as stale.
    let outcome = store.claim_one(Queue::Cache).await.expect("claim");
    assert!(
        outcome.claimed.is_none(),
        "a job whose pending file vanished after lock acquisition must not be claimed"
    );
}

/// Regression: the retire is what stops a same-`lock_key` enqueue coalescing
/// into a job that is already running. When it fails, claiming anyway leaves
/// that stale index in place for the whole execution, so the claim must be
/// abandoned and retried by the next scan instead.
#[tokio::test]
async fn claim_is_skipped_when_the_dedup_index_cannot_be_retired() {
    metrics_provider::init_for_tests();
    let lock_key = lock_key("cache.ns:sha256:retire-fails");
    let index_path = crate::jobs::store::job_lock_key_index_path("cache", &lock_key);

    let (inner, _dir) = fs_store();
    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner.clone(),
        FailIndexDelete {
            index_path: index_path.clone(),
        },
    ));
    let store = Arc::new(JobStore::new(
        &metadata_store_over(hooked),
        "test-worker",
        ClaimMode::Atomic,
    ));

    store
        .enqueue(dummy_envelope(lock_key.as_str()))
        .await
        .expect("enqueue");

    let outcome = store.claim_one(Queue::Cache).await.expect("claim");
    assert!(
        outcome.claimed.is_none(),
        "a job whose dedup index could not be retired must not be claimed"
    );
    assert!(
        inner.head(&index_path).await.is_ok(),
        "the index the retire failed to remove is still there for the next scan"
    );
}

/// Regression: the execution lock keeps other workers out but not producers,
/// so a reschedule must not overwrite an index a concurrent enqueue just
/// pointed at its own fresh job.
#[tokio::test]
async fn retry_leaves_a_concurrent_producers_index_alone() {
    let h = harness();
    let lock_key = lock_key("cache.ns:sha256:retry-vs-producer");

    let mut env = dummy_envelope(lock_key.as_str());
    env.max_attempts = Some(3);
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    // The claim retired the index, so a producer enqueueing the same lock_key
    // mid-execution indexes its own fresh pending file.
    h.store
        .enqueue(dummy_envelope(lock_key.as_str()))
        .await
        .expect("producer enqueue");
    let index_path = crate::jobs::store::job_lock_key_index_path("cache", &lock_key);
    let produced = parse_lock_key_index(&h.raw.get(&index_path).await.expect("read index"))
        .expect("parse")
        .storage_key;

    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    let after = parse_lock_key_index(&h.raw.get(&index_path).await.expect("read index"))
        .expect("parse")
        .storage_key;
    assert_eq!(
        after, produced,
        "the reschedule clobbered the index of a job enqueued while it ran, \
         stranding that job's pending file unindexed"
    );
}

#[tokio::test]
async fn retry_updates_lock_key_index_to_new_storage_key() {
    let h = harness();
    let lock_key = lock_key("cache.ns:sha256:retry-index");

    let mut env = dummy_envelope(lock_key.as_str());
    env.max_attempts = Some(3);
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let old_storage_key = claimed.storage_key.clone();
    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(pending.len(), 1);
    let new_storage_key = &pending[0];
    assert_ne!(new_storage_key, &old_storage_key);

    let index_path = crate::jobs::store::job_lock_key_index_path("cache", &lock_key);
    let data = h.raw.get(&index_path).await.expect("read index");
    let index = parse_lock_key_index(&data).expect("parse");
    assert_eq!(&index.storage_key, new_storage_key);
}

#[tokio::test]
async fn enqueue_dedup_skips_existing_lock_key() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:dup"))
        .await
        .expect("enqueue 1");
    let before = h
        .store
        .count_pending(Queue::Cache, 600)
        .await
        .expect("count");
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:dup"))
        .await
        .expect("enqueue 2");
    assert_eq!(
        before,
        h.store
            .count_pending(Queue::Cache, 600)
            .await
            .expect("count")
    );
}

#[tokio::test]
async fn enqueue_after_claim_creates_second_pending() {
    let h = harness();
    let lock_key = lock_key("cache.ns:sha256:inflight");
    h.store
        .enqueue(dummy_envelope(lock_key.as_str()))
        .await
        .expect("enqueue 1");

    // Claim without completing: the job is now executing and holds the lock.
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim_one")
        .claimed
        .expect("Some");
    assert_eq!(claimed.envelope.lock_key, lock_key);

    // A same-lock_key enqueue mid-execution must not coalesce into the
    // already-resolved job; it gets its own pending file.
    h.store
        .enqueue(dummy_envelope(lock_key.as_str()))
        .await
        .expect("enqueue 2");
    assert_eq!(
        h.store
            .count_pending(Queue::Cache, 600)
            .await
            .expect("count"),
        2,
        "enqueue during execution must create a second pending job",
    );

    // The execution lock serialises the two: the second is unclaimable until
    // the first releases the lock on complete.
    assert!(
        h.store
            .claim_one(Queue::Cache)
            .await
            .expect("claim")
            .claimed
            .is_none(),
        "second job must wait on the execution lock",
    );
    h.store.complete(claimed).await.expect("complete 1");

    let second = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("second job claimable after the first completes");
    assert_eq!(second.envelope.lock_key, lock_key);
    h.store.complete(second).await.expect("complete 2");
    assert!(
        h.store
            .claim_one(Queue::Cache)
            .await
            .expect("claim")
            .claimed
            .is_none(),
        "queue empty after both jobs complete",
    );
}

#[tokio::test]
async fn concurrent_enqueue_dedup() {
    let h = harness();
    let lock_key = lock_key("cache.ns:sha256:concurrent");
    let mut handles = Vec::with_capacity(8);
    for _ in 0..8 {
        let store = Arc::clone(&h.store);
        let key = lock_key.to_string();
        handles.push(tokio::spawn(async move {
            store.enqueue(dummy_envelope(&key)).await
        }));
    }
    for handle in handles {
        handle.await.expect("join").expect("enqueue");
    }

    let pending = h.store.list_pending(Queue::Cache, 64).await.expect("list");
    assert_eq!(
        pending.len(),
        1,
        "concurrent enqueues for the same lock_key must produce exactly one pending file, got: {pending:?}"
    );
}

// =========================================================================
// complete() commit-failure fail-over (no hot loop)
// =========================================================================

/// The claim key serialises a `lock_key` across workers: while one holds it,
/// a second scan finds the pending job but cannot claim it, and a release
/// (here via `complete`) frees the key for the next claimant.
#[tokio::test]
async fn a_held_claim_blocks_a_second_claimant() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:claimed-once"))
        .await
        .expect("enqueue");
    let first = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("first claim")
        .claimed
        .expect("Some");

    let second = h.store.claim_one(Queue::Cache).await.expect("second claim");
    assert!(
        second.claimed.is_none(),
        "a held claim key must block a second claimant of the same lock_key"
    );

    h.store.complete(first).await.expect("complete");
    let after = h.store.claim_one(Queue::Cache).await.expect("after");
    assert!(
        after.claimed.is_none(),
        "the completed job must be gone from pending"
    );
}

// =========================================================================
// Keyset pagination + administrative mutations
// =========================================================================

#[tokio::test]
async fn list_pending_page_is_keyset_ordered() {
    let h = harness();
    for i in 0..5 {
        h.store
            .enqueue(dummy_envelope(&format!("cache.ns:sha256:page-{i}")))
            .await
            .expect("enqueue");
    }

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = h
            .store
            .list_pending_page(Queue::Cache, 2, after.as_deref())
            .await
            .expect("page");
        let keys = page.items;
        assert!(keys.len() <= 2, "page must not exceed n");
        seen.extend(keys.iter().cloned());
        match page.next_token {
            Some(cursor) => {
                assert_eq!(
                    Some(&cursor),
                    keys.last(),
                    "cursor must be the last key of the page"
                );
                after = Some(cursor);
            }
            None => break,
        }
    }

    assert_eq!(seen.len(), 5, "every envelope is paged exactly once");
    let mut ordered = seen.clone();
    ordered.sort();
    ordered.dedup();
    assert_eq!(
        ordered, seen,
        "keys are returned ascending with no duplicates"
    );
}

#[tokio::test]
async fn retry_failed_resets_attempts() {
    let h = harness();
    // Seed a dead-letter record carrying a non-zero attempt count so the reset
    // is observable.
    let mut env = dummy_envelope("cache.ns:sha256:retry-failed");
    env.attempts = 3;
    let key = make_storage_key(Utc::now(), &env.id);
    let body = serde_json::to_vec(&DeadLetterRecord {
        envelope: env,
        last_error: "boom".to_string(),
        failed_at: Utc::now(),
    })
    .expect("serialize");
    h.raw
        .put(
            &crate::jobs::store::job_failed_path("cache", &key),
            Bytes::from(body),
        )
        .await
        .expect("seed failed");

    let record = h
        .store
        .read_failed(Queue::Cache, &key)
        .await
        .expect("read failed");
    assert_eq!(record.last_error, "boom");
    assert_eq!(record.envelope.attempts, 3);

    h.store
        .retry_failed(Queue::Cache, &key)
        .await
        .expect("retry");

    assert!(
        matches!(
            h.store.read_failed(Queue::Cache, &key).await,
            Err(crate::jobs::store::Error::NotFound)
        ),
        "failed record is consumed by retry",
    );

    let pending = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list pending")
        .items;
    assert_eq!(pending.len(), 1, "exactly one requeued envelope");
    let restored = h
        .store
        .read_pending(Queue::Cache, &pending[0])
        .await
        .expect("read pending");
    assert_eq!(restored.attempts, 0, "retry resets attempts to zero");
    assert_eq!(restored.lock_key, lock_key("cache.ns:sha256:retry-failed"));

    assert!(
        matches!(
            h.store.retry_failed(Queue::Cache, &key).await,
            Err(crate::jobs::store::Error::NotFound)
        ),
        "retrying a consumed key is a stale 404",
    );
}

#[tokio::test]
async fn delete_failed_record() {
    let h = harness();
    let env = dummy_envelope("cache.ns:sha256:del-failed");
    let key = make_storage_key(Utc::now(), &env.id);
    let body = serde_json::to_vec(&DeadLetterRecord {
        envelope: env,
        last_error: "boom".to_string(),
        failed_at: Utc::now(),
    })
    .expect("serialize");
    h.raw
        .put(
            &crate::jobs::store::job_failed_path("cache", &key),
            Bytes::from(body),
        )
        .await
        .expect("seed failed");

    h.store
        .delete_job(Queue::Cache, JobState::Failed, &key)
        .await
        .expect("delete");
    assert!(matches!(
        h.store.read_failed(Queue::Cache, &key).await,
        Err(crate::jobs::store::Error::NotFound)
    ));
    assert!(
        matches!(
            h.store
                .delete_job(Queue::Cache, JobState::Failed, &key)
                .await,
            Err(crate::jobs::store::Error::NotFound)
        ),
        "deleting a consumed key is a stale 404",
    );
}

#[tokio::test]
async fn delete_pending_removes_record_and_index() {
    let h = harness();
    let lock_key = lock_key("cache.ns:sha256:del-pending");
    h.store
        .enqueue(dummy_envelope(lock_key.as_str()))
        .await
        .expect("enqueue");
    assert!(
        h.store
            .find_pending_with_lock_key(Queue::Cache, &lock_key)
            .await
            .expect("find"),
        "enqueue establishes the dedup index",
    );

    let pending = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list")
        .items;
    assert_eq!(pending.len(), 1);
    let key = pending[0].clone();

    h.store
        .delete_job(Queue::Cache, JobState::Pending, &key)
        .await
        .expect("delete");

    let after = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list")
        .items;
    assert!(after.is_empty(), "pending file removed");
    assert!(
        !h.store
            .find_pending_with_lock_key(Queue::Cache, &lock_key)
            .await
            .expect("find"),
        "dedup index is removed alongside the pending file",
    );

    assert!(
        matches!(
            h.store
                .delete_job(Queue::Cache, JobState::Pending, &key)
                .await,
            Err(crate::jobs::store::Error::NotFound)
        ),
        "deleting a consumed key is a stale 404",
    );
}

// =========================================================================
// Handler-facing helpers
// =========================================================================

/// Neither a foreign kind nor a payload that does not decode can change on
/// retry, so both must dead-letter at once.
#[test]
fn a_foreign_kind_or_undecodable_payload_is_terminal() {
    let envelope = JobEnvelope::new(Queue::Cache, "cache.fetch_blob", "lock", &json!({"a": 1}))
        .expect("envelope");
    assert!(
        envelope
            .payload::<serde_json::Value>(&["cache.fetch_blob"])
            .is_ok()
    );
    assert!(matches!(
        envelope.payload::<serde_json::Value>(&["scan.image"]),
        Err(Error::Terminal(_))
    ));
    assert!(matches!(
        envelope.payload::<u32>(&["cache.fetch_blob"]),
        Err(Error::Terminal(_))
    ));
}

/// An authorization denial dead-letters; any other registry error is retried.
#[test]
fn only_a_denied_registry_error_is_terminal() {
    assert!(matches!(
        Error::from(RegistryError::Denied("forbidden".to_string())),
        Error::Terminal(_)
    ));
    assert!(matches!(
        Error::from(RegistryError::BlobUnknown),
        Error::Execution(_)
    ));
}

// =========================================================================
// Key helpers
// =========================================================================

#[test]
fn storage_key_roundtrips_through_parse_not_before() {
    let when = Utc
        .timestamp_millis_opt(1_700_000_000_123)
        .single()
        .unwrap();
    let key = make_storage_key(when, "abc-123");
    assert_eq!(parse_not_before(&key), Some(when));
    assert!(key.ends_with("-abc-123"));
    assert_eq!(&key[..STORAGE_KEY_PREFIX_LEN], "0000018bcfe5687b");
}

#[test]
fn storage_key_prefix_sorts_by_time() {
    let earlier = Utc
        .timestamp_millis_opt(1_700_000_000_000)
        .single()
        .unwrap();
    let later = Utc
        .timestamp_millis_opt(1_700_000_001_000)
        .single()
        .unwrap();
    let id = "uuid";
    assert!(make_storage_key(earlier, id) < make_storage_key(later, id));
}

#[test]
fn parse_not_before_rejects_malformed_keys() {
    assert!(parse_not_before("").is_none());
    assert!(parse_not_before("not-a-storage-key").is_none());
    assert!(
        parse_not_before("zzzzzzzzzzzzzzzz-uuid").is_none(),
        "non-hex prefix"
    );
    assert!(
        parse_not_before("0000000000000000uuid").is_none(),
        "missing separator"
    );
}

#[test]
fn negative_timestamp_clamps_to_zero() {
    let pre_epoch = Utc.timestamp_millis_opt(-1).single().unwrap();
    let key = make_storage_key(pre_epoch, "id");
    assert!(key.starts_with("0000000000000000-"));
}

#[test]
fn pending_refresh_interval_below_floor_is_rejected() {
    let toml_with_zero = r"
        pending_refresh_interval_secs = 0
        pending_ready_horizon_secs = 600
    ";
    let err = toml::from_str::<JobQueueConfig>(toml_with_zero)
        .expect_err("pending_refresh_interval_secs = 0 must be rejected");
    assert!(
        err.to_string().contains("pending_refresh_interval_secs"),
        "error must name the field: {err}"
    );

    let toml_with_four = r"
        pending_refresh_interval_secs = 4
        pending_ready_horizon_secs = 600
    ";
    toml::from_str::<JobQueueConfig>(toml_with_four)
        .expect_err("pending_refresh_interval_secs = 4 must be rejected");

    let toml_with_five = r"
        pending_refresh_interval_secs = 5
        pending_ready_horizon_secs = 600
    ";
    let cfg = toml::from_str::<JobQueueConfig>(toml_with_five)
        .expect("the floor value itself must parse");
    assert_eq!(cfg.pending_refresh_interval_secs, 5);
}

#[test]
fn test_job_paths() {
    use crate::jobs::store::{job_failed_path, job_lock_key_index_path, job_pending_path};

    assert_eq!(
        job_pending_path("cache", "01HABCDE"),
        "_jobs/pending/cache/01HABCDE.json"
    );
    assert_eq!(
        job_failed_path("cache", "01HABCDE"),
        "_jobs/failed/cache/01HABCDE.json"
    );
    assert_eq!(
        job_lock_key_index_path("cache", &lock_key("cache.ns:sha256:abc")),
        "_jobs/index/cache/cache.ns%3Asha256%3Aabc.json"
    );
}

/// `%` is the escape character of the index-path encoding, so a key carrying
/// one could encode onto another key's path and falsely dedup. The type makes
/// that key unrepresentable rather than leaving the encoding non-injective.
#[test]
fn a_lock_key_containing_the_escape_character_is_rejected() {
    assert!(matches!(
        LockKey::new("a%3Ab"),
        Err(crate::jobs::store::Error::InvalidLockKey(_))
    ));
    assert!(matches!(
        LockKey::new(""),
        Err(crate::jobs::store::Error::InvalidLockKey(_))
    ));
}

/// A pinned budget of zero used to be indistinguishable from "not set", so the
/// queue's own budget overwrote it. Zero now means zero: the job dead-letters
/// on its first failure rather than retrying five times.
#[tokio::test]
async fn a_pinned_zero_budget_is_not_overwritten_by_the_queue_default() {
    let h = harness();

    let mut env = dummy_envelope("cache.ns:sha256:no-retries");
    env.max_attempts = Some(0);
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    assert_eq!(
        claimed.envelope.max_attempts,
        Some(0),
        "enqueue must leave a pinned budget alone"
    );

    let outcome = h.store.fail(claimed, "boom").await.expect("fail");
    assert!(
        matches!(outcome, FailOutcome::MovedToDeadLetter),
        "a zero budget must dead-letter on the first failure"
    );
}

/// An envelope that pins nothing takes the queue's configured budget at
/// enqueue, which is what every production caller relies on.
#[tokio::test]
async fn an_unpinned_budget_takes_the_queue_default() {
    let h = harness();

    let env = dummy_envelope("cache.ns:sha256:default-budget");
    assert_eq!(env.max_attempts, None, "a fresh envelope pins nothing");
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    assert_eq!(
        claimed.envelope.max_attempts,
        Some(JobRetryPolicy::default().max_attempts),
        "enqueue must stamp the queue's budget when the caller pinned none"
    );
}

/// The queue is durable, so a record written before the budget became optional
/// must still claim and still carry the same budget.
#[test]
fn a_stored_envelope_keeps_its_budget() {
    let stored = r#"{"id":"j1","queue":"cache","kind":"test.noop",
        "lock_key":"cache.ns:sha256:aaa","created_at":"2026-01-01T00:00:00Z",
        "attempts":2,"max_attempts":5,"payload":null}"#;

    let envelope: JobEnvelope =
        serde_json::from_str(stored).expect("a stored envelope must still parse");
    assert_eq!(envelope.max_attempts, Some(5));
    assert_eq!(envelope.attempts, 2);

    let json = serde_json::to_value(&envelope).expect("serialize");
    assert_eq!(
        json["max_attempts"], 5,
        "the budget must stay a plain number on the wire: {json}"
    );
}

/// The scan walks pending keys in ascending time order, so a body it cannot
/// read used to strand every job queued after it. The record is discarded
/// rather than stepped over: it can no longer name the work it stood for, and
/// both queues re-enqueue what was lost.
#[tokio::test]
async fn a_poison_pending_record_is_discarded_and_does_not_wedge_the_queue() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:poison"))
        .await
        .expect("enqueue poison");
    let poisoned = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list")
        .items
        .first()
        .cloned()
        .expect("the first job must be listed");
    let path = crate::jobs::store::job_pending_path("cache", &poisoned);
    h.raw
        .put(&path, Bytes::from_static(b"{ truncated"))
        .await
        .expect("corrupt the body");

    // Phase one: the scan meets the poison record and drops it. Ordering
    // against a second job is not assumed: two enqueues can share a
    // millisecond, so their storage keys may tie.
    let outcome = h.store.claim_one(Queue::Cache).await.expect("claim");
    assert!(
        outcome.claimed.is_none(),
        "an unreadable record must not claim as a job"
    );
    assert!(
        h.raw.head(&path).await.is_err(),
        "the unreadable record must be gone, not re-warned on every scan"
    );

    // Phase two: the queue still drains, which the old abort prevented for
    // every job queued after the poison record.
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:healthy"))
        .await
        .expect("enqueue healthy");
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("the queue must keep draining");
    assert_eq!(
        claimed.envelope.lock_key.to_string(),
        "cache.ns:sha256:healthy"
    );
}

/// Retiring the record through the admin API is the only recovery path, and it
/// needed the body it cannot read: only the `lock_key` came from there, so the
/// delete proceeds without the index fold.
#[tokio::test]
async fn a_poison_pending_record_can_be_deleted() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:poison"))
        .await
        .expect("enqueue");
    let poisoned = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list")
        .items
        .first()
        .cloned()
        .expect("the job must be listed");
    let path = crate::jobs::store::job_pending_path("cache", &poisoned);
    h.raw
        .put(&path, Bytes::from_static(b"{ truncated"))
        .await
        .expect("corrupt the body");

    h.store
        .delete_job(Queue::Cache, JobState::Pending, &poisoned)
        .await
        .expect("an unreadable pending record must still be deletable");

    assert!(
        h.raw.head(&path).await.is_err(),
        "the pending object must be gone"
    );
}

/// A worker whose claim lapsed mid-execution must not reschedule or bury the
/// job: the pending file, dedup index, and dead-letter store belong to the
/// key's new holder, exactly as on the `complete` path.
#[tokio::test]
async fn fail_with_lost_claim_leaves_pending_and_index_untouched() {
    let h = harness();
    let mut env = dummy_envelope("cache.ns:sha256:lostfail");
    env.max_attempts = Some(5);
    h.store.enqueue(env).await.expect("enqueue");
    let storage_key = h
        .store
        .list_pending(Queue::Cache, 10)
        .await
        .expect("list")
        .pop()
        .expect("one pending job");
    let envelope = h
        .store
        .read_pending(Queue::Cache, &storage_key)
        .await
        .expect("read pending");

    let lost = CancellationToken::new();
    lost.cancel();
    let claimed = ClaimedJob::for_test(envelope.clone(), storage_key.clone(), lost);
    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    // Same guarantee on the dead-letter path.
    let lost = CancellationToken::new();
    lost.cancel();
    let claimed = ClaimedJob::for_test(envelope, storage_key.clone(), lost);
    assert!(matches!(
        h.store
            .fail_terminal(claimed, "boom")
            .await
            .expect("fail_terminal"),
        FailOutcome::MovedToDeadLetter
    ));

    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(
        pending,
        vec![storage_key.clone()],
        "the original pending file must be the only one"
    );
    let pending_path = job_pending_path("cache", &storage_key);
    assert!(
        h.raw.head(&pending_path).await.is_ok(),
        "the pending file must be untouched"
    );
    let index_path = job_lock_key_index_path("cache", &lock_key("cache.ns:sha256:lostfail"));
    assert!(
        h.raw.head(&index_path).await.is_ok(),
        "the dedup index must be untouched"
    );
    assert_eq!(
        h.store.count_failed(Queue::Cache).await.expect("count"),
        0,
        "nothing may be dead-lettered by a lost claim"
    );
}

/// The refresher's loss predicate: a positive loss cancels immediately, and
/// a transient read error only cancels once the last verified expiry passed.
#[test]
fn should_cancel_claim_tolerates_transient_errors_inside_the_lease() {
    let now = Utc::now();
    let future = now + ChronoDuration::seconds(40);
    let past = now - ChronoDuration::seconds(1);
    assert!(
        !should_cancel_claim(&ClaimCheck::Unverifiable, future, now),
        "a read error inside the lease must not cancel"
    );
    assert!(
        should_cancel_claim(&ClaimCheck::Unverifiable, past, now),
        "read errors past the last verified expiry must cancel"
    );
    assert!(
        should_cancel_claim(&ClaimCheck::Lost, future, now),
        "a record showing another instance or a lapse must cancel"
    );
    assert!(
        !should_cancel_claim(&ClaimCheck::Owned { expires_at: future }, past, now),
        "a verified own record must never cancel"
    );
}

/// A lapsed claim must not wedge its lock key: a later claimant sees the
/// stale record, takes the key over, and claims the job.
#[tokio::test]
async fn claim_one_takes_over_a_stale_claim() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:stale"))
        .await
        .expect("enqueue");
    let claim_path = job_claim_path(&lock_key("cache.ns:sha256:stale"));
    h.raw
        .put(
            &claim_path,
            Bytes::from_static(
                br#"{"instance":"departed-worker","expires_at":"2020-01-01T00:00:00Z"}"#,
            ),
        )
        .await
        .expect("write stale claim");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("a stale claim must be taken over");
    assert_eq!(claimed.envelope.lock_key, lock_key("cache.ns:sha256:stale"));
    h.store.complete(claimed).await.expect("complete");
}

/// A release by an instance that no longer owns the claim key must leave the
/// new owner's record in place.
#[tokio::test]
async fn release_leaves_a_foreign_claim_intact() {
    let h = harness();
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:foreign"))
        .await
        .expect("enqueue");
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    // Another worker took the key over: overwrite the claim record with a
    // foreign instance before this holder releases it.
    let claim_path = job_claim_path(&lock_key("cache.ns:sha256:foreign"));
    let foreign = format!(
        r#"{{"instance":"foreign-instance","expires_at":"{}"}}"#,
        (Utc::now() + ChronoDuration::seconds(300)).to_rfc3339(),
    );
    h.raw
        .put(&claim_path, Bytes::from(foreign))
        .await
        .expect("overwrite claim");

    assert!(matches!(
        h.store.complete(claimed).await.expect("complete"),
        CompleteOutcome::Completed
    ));

    let body = h
        .raw
        .get(&claim_path)
        .await
        .expect("the foreign claim must survive the release");
    let record: serde_json::Value = serde_json::from_slice(&body).expect("claim json");
    assert_eq!(record["instance"], "foreign-instance");
}

/// The startup probe must pass on the fs backend and leave no scratch key
/// behind.
#[tokio::test]
async fn ensure_claim_support_probe_succeeds_and_cleans_up() {
    metrics_provider::init_for_tests();
    let (raw, _dir) = fs_store();
    assert_eq!(
        ensure_claim_support(&metadata_store_over(raw.clone()))
            .await
            .expect("probe"),
        ClaimMode::Atomic,
        "the fs backend is honest and must probe as atomic",
    );
    let page = raw
        .list(&format!("{JOBS_ROOT}/claims/"), 10, None)
        .await
        .expect("list claims");
    assert!(
        page.items.is_empty(),
        "the probe must clean up its scratch key, found {:?}",
        page.items
    );
}

/// Deletes the key ahead of every write, so `create_if_absent` never sees an
/// existing object: a fake backend whose atomic create is dishonest.
struct DishonestCreate {
    inner: Arc<dyn ObjectStore>,
}

#[async_trait]
impl StoreHook for DishonestCreate {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        if let StoreOp::Put { key, .. } = op {
            let _ = self.inner.delete(key).await;
        }
        Ok(())
    }
}

/// A backend whose second create of an existing key succeeds must no longer
/// fail startup: the probe degrades it to advisory claims.
#[tokio::test]
async fn a_dishonest_backend_probes_as_advisory() {
    metrics_provider::init_for_tests();
    let (inner, _dir) = fs_store();
    let hooked: Arc<dyn ObjectStore> =
        Arc::new(HookedStore::new(inner.clone(), DishonestCreate { inner }));
    assert_eq!(
        ensure_claim_support(&metadata_store_over(hooked.clone()))
            .await
            .expect("a dishonest backend must probe cleanly"),
        ClaimMode::Advisory,
    );
}

/// Gates one advisory claimant's claim-key ops on semaphores so its interleave
/// with the other claimant is deterministic instead of timing-dependent.
struct AdvisoryRaceGate {
    claim_key: String,
    gets: AtomicUsize,
    signal_before_put: Option<Arc<Semaphore>>,
    wait_before_put: Option<Arc<Semaphore>>,
    signal_before_verify: Option<Arc<Semaphore>>,
    wait_before_verify: Option<Arc<Semaphore>>,
}

#[async_trait]
impl StoreHook for AdvisoryRaceGate {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        match op {
            StoreOp::Put { key, .. } if key == self.claim_key => {
                if let Some(gate) = &self.signal_before_put {
                    gate.add_permits(1);
                }
                if let Some(gate) = &self.wait_before_put {
                    gate.acquire().await.expect("gate").forget();
                }
            }
            // The claim key's second read is the post-settle verify; the
            // first is the pre-read.
            StoreOp::Get { key }
                if key == self.claim_key && self.gets.fetch_add(1, Ordering::SeqCst) == 1 =>
            {
                if let Some(gate) = &self.signal_before_verify {
                    gate.add_permits(1);
                }
                if let Some(gate) = &self.wait_before_verify {
                    gate.acquire().await.expect("gate").forget();
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Advisory race, driven deterministically: both claimants pre-read the key
/// as absent, A puts its record first, B overwrites it before A's verify.
/// A's verify must lose, B's must win, and exactly one claimant proceeds.
#[tokio::test]
async fn advisory_claim_race_admits_exactly_one_winner() {
    metrics_provider::init_for_tests();
    let (inner, _dir) = fs_store();
    let claim_key = job_claim_path(&lock_key("cache.ns:sha256:advisory-race"));

    // Gate order: B pre-reads, A puts, B puts, then both verify.
    let b_preread_done = Arc::new(Semaphore::new(0));
    let a_put_done = Arc::new(Semaphore::new(0));
    let b_put_done = Arc::new(Semaphore::new(0));

    let hooked_a: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner.clone(),
        AdvisoryRaceGate {
            claim_key: claim_key.clone(),
            gets: AtomicUsize::new(0),
            signal_before_put: None,
            wait_before_put: Some(b_preread_done.clone()),
            signal_before_verify: Some(a_put_done.clone()),
            wait_before_verify: Some(b_put_done.clone()),
        },
    ));
    let hooked_b: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner.clone(),
        AdvisoryRaceGate {
            claim_key: claim_key.clone(),
            gets: AtomicUsize::new(0),
            signal_before_put: Some(b_preread_done),
            wait_before_put: Some(a_put_done),
            signal_before_verify: Some(b_put_done),
            wait_before_verify: None,
        },
    ));

    let a = {
        let store = JobStore::new(
            &metadata_store_over(hooked_a),
            "worker-a",
            ClaimMode::Advisory,
        );
        let key = claim_key.clone();
        tokio::spawn(async move { store.put_claim_advisory(&key, "instance-a").await })
    };
    let b = {
        let store = JobStore::new(
            &metadata_store_over(hooked_b),
            "worker-b",
            ClaimMode::Advisory,
        );
        let key = claim_key.clone();
        tokio::spawn(async move { store.put_claim_advisory(&key, "instance-b").await })
    };

    let a_won = a.await.expect("join a").expect("a acquire").is_some();
    let b_won = b.await.expect("join b").expect("b acquire").is_some();
    assert!(!a_won, "A's verify must lose to B's overwrite");
    assert!(b_won, "B's verify must find its own record and win");

    let body = inner.get(&claim_key).await.expect("claim record");
    let record: serde_json::Value = serde_json::from_slice(&body).expect("claim json");
    assert_eq!(
        record["instance"], "instance-b",
        "the surviving record must carry the winner's instance"
    );
}

/// Advisory mode end to end: a claim is acquired via put-settle-verify, its
/// release on complete frees the key, and the next claim wins again.
#[tokio::test]
async fn advisory_mode_claims_completes_and_reclaims() {
    metrics_provider::init_for_tests();
    let (raw, _dir) = fs_store();
    let store = Arc::new(JobStore::new(
        &metadata_store_over(raw),
        "advisory-worker",
        ClaimMode::Advisory,
    ));

    store
        .enqueue(dummy_envelope("cache.ns:sha256:advisory"))
        .await
        .expect("enqueue");
    let claimed = store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    store.complete(claimed).await.expect("complete");

    store
        .enqueue(dummy_envelope("cache.ns:sha256:advisory"))
        .await
        .expect("re-enqueue");
    let reclaimed = store
        .claim_one(Queue::Cache)
        .await
        .expect("re-claim")
        .claimed
        .expect("the released key must be claimable again");
    store.complete(reclaimed).await.expect("complete 2");
}

/// Fails the first delete under the pending prefix, modelling a transient
/// backend error on `complete`'s cleanup.
struct FailPendingDeleteOnce {
    remaining: AtomicUsize,
}

#[async_trait]
impl StoreHook for FailPendingDeleteOnce {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
        if let StoreOp::Delete { key } = op
            && key.starts_with("_jobs/pending/")
            && self.remaining.swap(0, Ordering::SeqCst) == 1
        {
            return Err(StorageError::Backend(
                "injected pending-delete failure".to_string(),
            ));
        }
        Ok(())
    }
}

/// A failed pending-delete in `complete` must fail the job over to a retry
/// under a new storage key rather than leave it re-claimable in a hot loop.
#[tokio::test]
async fn complete_cleanup_failure_fails_over_to_retry() {
    metrics_provider::init_for_tests();
    let (inner, _dir) = fs_store();
    let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
        inner.clone(),
        FailPendingDeleteOnce {
            remaining: AtomicUsize::new(1),
        },
    ));
    let store = Arc::new(JobStore::new(
        &metadata_store_over(hooked),
        "test-worker",
        ClaimMode::Atomic,
    ));

    store
        .enqueue(dummy_envelope("cache.ns:sha256:cleanup"))
        .await
        .expect("enqueue");
    let claimed = store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let original_key = claimed.storage_key.clone();

    assert!(matches!(
        store.complete(claimed).await.expect("complete"),
        CompleteOutcome::FailedOver(FailOutcome::Retried { .. })
    ));

    let pending = store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(pending.len(), 1, "the job must be re-queued exactly once");
    assert_ne!(
        pending[0], original_key,
        "the retry must land under a new storage key"
    );
    let updated = store
        .read_pending(Queue::Cache, &pending[0])
        .await
        .expect("read retry");
    assert_eq!(updated.attempts, 1);
}

#[test]
fn claim_ttl_defaults_and_floor() {
    let cfg = toml::from_str::<JobQueueConfig>("").expect("all fields default");
    assert_eq!(cfg.claim_ttl_secs, 60);

    let err = toml::from_str::<JobQueueConfig>("claim_ttl_secs = 2")
        .expect_err("claim_ttl_secs below the floor must be rejected");
    assert!(
        err.to_string().contains("claim_ttl_secs"),
        "error must name the field: {err}"
    );

    let cfg = toml::from_str::<JobQueueConfig>("claim_ttl_secs = 3").expect("floor value parses");
    assert_eq!(cfg.claim_ttl_secs, 3);
}

/// The configured lease must reach the stored claim record.
#[tokio::test]
async fn claim_ttl_knob_stamps_the_claim_lease() {
    metrics_provider::init_for_tests();
    let (raw, _dir) = fs_store();
    let store = Arc::new(JobStore::with_retry_policy(
        &metadata_store_over(raw.clone()),
        "test-worker",
        ClaimMode::Atomic,
        JobRetryPolicy {
            claim_ttl_secs: 7,
            ..JobRetryPolicy::default()
        },
    ));
    store
        .enqueue(dummy_envelope("cache.ns:sha256:ttl"))
        .await
        .expect("enqueue");
    let _claimed = store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    let body = raw
        .get(&job_claim_path(&lock_key("cache.ns:sha256:ttl")))
        .await
        .expect("claim record");
    let record: serde_json::Value = serde_json::from_slice(&body).expect("claim json");
    let expires_at: DateTime<Utc> = record["expires_at"]
        .as_str()
        .expect("expires_at")
        .parse()
        .expect("timestamp");
    let lease = (expires_at - Utc::now()).num_seconds();
    assert!(
        (1..=7).contains(&lease),
        "the lease must reflect the 7s knob, got {lease}s"
    );
}

/// A horizon no timestamp can hold saturates the cutoff instead of overflowing
/// the arithmetic, so every pending envelope counts.
#[tokio::test]
async fn count_pending_counts_everything_past_an_unrepresentable_horizon() {
    let h = harness();
    let now = Utc::now();
    for (offset, name) in [(0, "ready"), (1, "future")] {
        let key = make_storage_key(now + chrono::Duration::hours(offset), name);
        h.raw
            .put(
                &crate::jobs::store::job_pending_path("cache", &key),
                Bytes::from_static(b"{}"),
            )
            .await
            .expect("stub");
    }

    let count = h
        .store
        .count_pending(Queue::Cache, u64::MAX)
        .await
        .expect("count");
    assert_eq!(count, 2, "an unbounded horizon must exclude nothing");
}
