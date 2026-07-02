use std::sync::Arc;

use bytes::Bytes;
use chrono::{TimeZone as _, Utc};
use tempfile::TempDir;

use angos_storage::{MemoryObjectStore, ObjectStore, fs::Backend as StorageFsBackend};
use angos_tx_engine::transaction::{Mutation, Transaction};

use crate::registry::job_store::{
    CompleteOutcome, FailOutcome, JobEnvelope, JobQueueConfig, JobState, JobStore,
    MAX_REPORTED_PENDING, Queue, STORAGE_KEY_PREFIX_LEN, make_storage_key, parse_lock_key_index,
    parse_not_before, serialize_dead_letter, serialize_lock_key_index,
};
use crate::registry::test_utils::{build_store, locked_executor_over};
use crate::{metrics_provider, registry::path_builder};

struct Harness {
    store: Arc<JobStore>,
    // Raw handle lets tests stage fixture state the engine would never produce.
    raw: Arc<dyn ObjectStore>,
}

fn harness(dir: &TempDir) -> Harness {
    metrics_provider::init_for_tests();
    let raw: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
    build_harness(raw)
}

fn harness_memory() -> Harness {
    metrics_provider::init_for_tests();
    let raw: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::new());
    build_harness(raw)
}

fn build_harness(raw: Arc<dyn ObjectStore>) -> Harness {
    let facade = build_store(raw.clone(), locked_executor_over(raw.clone()));
    let store = Arc::new(JobStore::new(facade, "test-worker"));
    Harness { store, raw }
}

fn dummy_envelope(lock_key: &str) -> JobEnvelope {
    JobEnvelope::new(Queue::Cache, "test.noop", lock_key, &()).expect("envelope")
}

// =========================================================================
// Shared test bodies
// =========================================================================

async fn run_enqueue_then_claim_succeeds(h: Harness) {
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
    assert_eq!(claimed.envelope.lock_key, "cache.ns:sha256:aaa");
    h.store
        .complete(claimed, Transaction::builder().build())
        .await
        .expect("complete");
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

async fn run_retry_writes_pending_with_backoff(h: Harness) {
    let mut env = dummy_envelope("cache.ns:sha256:retry");
    env.max_attempts = 3;
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

async fn run_dead_letter_after_max_attempts(h: Harness) {
    let mut env = dummy_envelope("cache.ns:sha256:dl");
    env.max_attempts = 1;
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
        Err(crate::registry::job_store::Error::NotFound)
    ));
}

async fn run_count_failed_reflects_dead_letters(h: Harness) {
    assert_eq!(h.store.count_failed(Queue::Cache).await.expect("count"), 0);

    let mut env = dummy_envelope("cache.ns:sha256:dl-count");
    env.max_attempts = 1;
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

async fn run_count_pending_saturates_at_cap(h: Harness) {
    let now = Utc::now();
    for i in 0..(MAX_REPORTED_PENDING + 5) {
        let key = make_storage_key(now, &format!("stub-{i}"));
        h.raw
            .put(
                &path_builder::job_pending_path("cache", &key),
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

async fn run_count_pending_excludes_envelopes_past_readiness_horizon(h: Harness) {
    let now = Utc::now();
    for i in 0..2 {
        let key = make_storage_key(now, &format!("ready-{i}"));
        h.raw
            .put(
                &path_builder::job_pending_path("cache", &key),
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
                &path_builder::job_pending_path("cache", &key),
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

async fn run_future_storage_key_yields_next_ready_without_claiming(h: Harness) {
    let mut env = dummy_envelope("cache.ns:sha256:future");
    env.max_attempts = 5;
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

async fn run_orphan_index_is_self_healed_on_next_lookup(h: Harness) {
    let lock_key = "cache.ns:sha256:orphan";

    let storage_key = make_storage_key(Utc::now(), "phantom-id");
    let index_data = serialize_lock_key_index(&storage_key).expect("serialize");
    let index_path = path_builder::job_lock_key_index_path("cache", lock_key);
    h.raw
        .put(&index_path, Bytes::from(index_data))
        .await
        .expect("seed index");
    assert!(h.raw.head(&index_path).await.is_ok());

    let hit = h
        .store
        .find_pending_with_lock_key(Queue::Cache, lock_key)
        .await
        .expect("lookup");
    assert!(!hit, "orphan index must not register as a hit");

    assert!(
        h.raw.head(&index_path).await.is_err(),
        "orphan index must be self-healed",
    );
}

async fn run_retry_updates_lock_key_index_to_new_storage_key(h: Harness) {
    let lock_key = "cache.ns:sha256:retry-index";

    let mut env = dummy_envelope(lock_key);
    env.max_attempts = 3;
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

    let index_path = path_builder::job_lock_key_index_path("cache", lock_key);
    let data = h.raw.get(&index_path).await.expect("read index");
    let index = parse_lock_key_index(&data).expect("parse");
    assert_eq!(&index.storage_key, new_storage_key);
}

async fn run_enqueue_dedup_skips_existing_lock_key(h: Harness) {
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

async fn run_enqueue_after_claim_creates_second_pending(h: Harness) {
    let lock_key = "cache.ns:sha256:inflight";
    h.store
        .enqueue(dummy_envelope(lock_key))
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
        .enqueue(dummy_envelope(lock_key))
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
    h.store
        .complete(claimed, Transaction::builder().build())
        .await
        .expect("complete 1");

    let second = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("second job claimable after the first completes");
    assert_eq!(second.envelope.lock_key, lock_key);
    h.store
        .complete(second, Transaction::builder().build())
        .await
        .expect("complete 2");
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

// =========================================================================
// End-to-end claim cycle: FS store
// =========================================================================

#[tokio::test]
async fn enqueue_then_claim_succeeds() {
    let dir = TempDir::new().expect("temp dir");
    run_enqueue_then_claim_succeeds(harness(&dir)).await;
}

#[tokio::test]
async fn enqueue_then_claim_succeeds_memory() {
    run_enqueue_then_claim_succeeds(harness_memory()).await;
}

// =========================================================================
// Retry + dead-letter (consumer-driven storage behaviour)
// =========================================================================

#[tokio::test]
async fn retry_writes_pending_with_backoff() {
    let dir = TempDir::new().expect("temp dir");
    run_retry_writes_pending_with_backoff(harness(&dir)).await;
}

#[tokio::test]
async fn retry_writes_pending_with_backoff_memory() {
    run_retry_writes_pending_with_backoff(harness_memory()).await;
}

#[tokio::test]
async fn dead_letter_after_max_attempts() {
    let dir = TempDir::new().expect("temp dir");
    run_dead_letter_after_max_attempts(harness(&dir)).await;
}

#[tokio::test]
async fn dead_letter_after_max_attempts_memory() {
    run_dead_letter_after_max_attempts(harness_memory()).await;
}

// =========================================================================
// count_pending
// =========================================================================

#[tokio::test]
async fn count_pending_saturates_at_cap() {
    let dir = TempDir::new().expect("temp dir");
    run_count_pending_saturates_at_cap(harness(&dir)).await;
}

#[tokio::test]
async fn count_pending_saturates_at_cap_memory() {
    run_count_pending_saturates_at_cap(harness_memory()).await;
}

#[tokio::test]
async fn count_pending_excludes_envelopes_past_readiness_horizon() {
    let dir = TempDir::new().expect("temp dir");
    run_count_pending_excludes_envelopes_past_readiness_horizon(harness(&dir)).await;
}

#[tokio::test]
async fn count_pending_excludes_envelopes_past_readiness_horizon_memory() {
    run_count_pending_excludes_envelopes_past_readiness_horizon(harness_memory()).await;
}

// =========================================================================
// count_failed
// =========================================================================

#[tokio::test]
async fn count_failed_reflects_dead_letters() {
    let dir = TempDir::new().expect("temp dir");
    run_count_failed_reflects_dead_letters(harness(&dir)).await;
}

#[tokio::test]
async fn count_failed_reflects_dead_letters_memory() {
    run_count_failed_reflects_dead_letters(harness_memory()).await;
}

#[tokio::test]
async fn future_storage_key_yields_next_ready_without_claiming() {
    let dir = TempDir::new().expect("temp dir");
    run_future_storage_key_yields_next_ready_without_claiming(harness(&dir)).await;
}

#[tokio::test]
async fn future_storage_key_yields_next_ready_without_claiming_memory() {
    run_future_storage_key_yields_next_ready_without_claiming(harness_memory()).await;
}

// =========================================================================
// Dedup index (lock-key index)
// =========================================================================

#[tokio::test]
async fn orphan_index_is_self_healed_on_next_lookup() {
    let dir = TempDir::new().expect("temp dir");
    run_orphan_index_is_self_healed_on_next_lookup(harness(&dir)).await;
}

#[tokio::test]
async fn orphan_index_is_self_healed_on_next_lookup_memory() {
    run_orphan_index_is_self_healed_on_next_lookup(harness_memory()).await;
}

#[tokio::test]
async fn retry_updates_lock_key_index_to_new_storage_key() {
    let dir = TempDir::new().expect("temp dir");
    run_retry_updates_lock_key_index_to_new_storage_key(harness(&dir)).await;
}

#[tokio::test]
async fn retry_updates_lock_key_index_to_new_storage_key_memory() {
    run_retry_updates_lock_key_index_to_new_storage_key(harness_memory()).await;
}

#[tokio::test]
async fn enqueue_dedup_skips_existing_lock_key() {
    let dir = TempDir::new().expect("temp dir");
    run_enqueue_dedup_skips_existing_lock_key(harness(&dir)).await;
}

#[tokio::test]
async fn enqueue_dedup_skips_existing_lock_key_memory() {
    run_enqueue_dedup_skips_existing_lock_key(harness_memory()).await;
}

#[tokio::test]
async fn enqueue_after_claim_creates_second_pending() {
    let dir = TempDir::new().expect("temp dir");
    run_enqueue_after_claim_creates_second_pending(harness(&dir)).await;
}

#[tokio::test]
async fn enqueue_after_claim_creates_second_pending_memory() {
    run_enqueue_after_claim_creates_second_pending(harness_memory()).await;
}

async fn run_concurrent_enqueue_dedup(h: Harness) {
    let lock_key = "cache.ns:sha256:concurrent";
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

#[tokio::test]
async fn concurrent_enqueue_dedup() {
    let dir = TempDir::new().expect("temp dir");
    run_concurrent_enqueue_dedup(harness(&dir)).await;
}

#[tokio::test]
async fn concurrent_enqueue_dedup_memory() {
    run_concurrent_enqueue_dedup(harness_memory()).await;
}

// =========================================================================
// Corrupt dedup-index recovery
// =========================================================================

/// Plant unparseable bytes at the dedup-index path of `lock_key`.
async fn plant_corrupt_index(h: &Harness, lock_key: &str) -> String {
    let index_path = path_builder::job_lock_key_index_path("cache", lock_key);
    h.raw
        .put(&index_path, Bytes::from_static(b"{not json"))
        .await
        .expect("seed corrupt index");
    index_path
}

/// The reconcile pass counts and retires a corrupt index instead of erroring.
async fn run_corrupt_index_is_counted_and_retired_by_reconcile(h: Harness) {
    let index_path = plant_corrupt_index(&h, "cache.ns:sha256:corrupt").await;

    let stems = h
        .store
        .list_lock_key_index_keys(Queue::Cache)
        .await
        .expect("list stems");
    assert_eq!(stems.len(), 1, "the corrupt index must be enumerated");

    assert!(
        h.store
            .is_orphan_lock_key_index(Queue::Cache, &stems[0])
            .await
            .expect("dry-run probe must classify, not error"),
        "the dry-run probe must count a corrupt index as retirable",
    );
    assert!(
        h.raw.head(&index_path).await.is_ok(),
        "the read-only probe must not retire the index",
    );

    assert!(
        h.store
            .reconcile_orphan_lock_key_index(Queue::Cache, &stems[0])
            .await
            .expect("reconcile must retire, not error"),
        "the reconcile must report the corrupt index as retired",
    );
    assert!(
        h.raw.head(&index_path).await.is_err(),
        "the corrupt index must be gone after the reconcile",
    );
}

/// An enqueue whose `lock_key` is blackholed by a corrupt index recovers: the
/// index is retired, the job lands, and the rebuilt index points at it.
async fn run_enqueue_recovers_from_corrupt_index(h: Harness) {
    let lock_key = "cache.ns:sha256:corrupt-enqueue";
    plant_corrupt_index(&h, lock_key).await;

    h.store
        .enqueue(dummy_envelope(lock_key))
        .await
        .expect("enqueue must recover from a corrupt index");

    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(pending.len(), 1, "the job must not be silently dropped");
    let (index_storage_key, _) = h
        .store
        .get_lock_key_index_raw(Queue::Cache, lock_key)
        .await
        .expect("index must parse after the recovery")
        .expect("index must be rebuilt");
    assert_eq!(
        index_storage_key, pending[0],
        "the rebuilt index must point at the enqueued body",
    );
    assert!(
        h.store
            .find_pending_with_lock_key(Queue::Cache, lock_key)
            .await
            .expect("probe"),
        "the recovered entry must dedup subsequent enqueues",
    );
}

/// A present body whose index is corrupt is flagged and corrected by the
/// re-index direction of the reconcile.
async fn run_reindex_corrects_corrupt_index(h: Harness) {
    let lock_key = "cache.ns:sha256:corrupt-reindex";
    h.store
        .enqueue(dummy_envelope(lock_key))
        .await
        .expect("enqueue");
    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    let storage_key = pending[0].clone();
    plant_corrupt_index(&h, lock_key).await;

    assert!(
        h.store
            .needs_reindex_pending(Queue::Cache, &storage_key)
            .await
            .expect("probe must classify, not error"),
        "a present body with a corrupt index must need a re-index",
    );
    assert!(
        h.store
            .reindex_orphan_pending_body(Queue::Cache, &storage_key)
            .await
            .expect("re-index must correct, not error"),
        "the re-index must report the corrupt entry as corrected",
    );
    let (index_storage_key, _) = h
        .store
        .get_lock_key_index_raw(Queue::Cache, lock_key)
        .await
        .expect("index must parse after the correction")
        .expect("index present");
    assert_eq!(
        index_storage_key, storage_key,
        "the corrected entry must point at the present body",
    );
}

/// `fail` on a retryable job must not clobber the fresh index a producer
/// created for a same-`lock_key` enqueue during execution.
async fn run_fail_retry_leaves_concurrent_producers_index(h: Harness) {
    let lock_key = "cache.ns:sha256:retry-vs-producer";
    let mut env = dummy_envelope(lock_key);
    env.max_attempts = 3;
    h.store.enqueue(env).await.expect("enqueue 1");

    // Claim without completing: the claim retires the dedup index.
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    // A producer enqueues the same lock_key mid-execution: fresh pending +
    // fresh index owned by that enqueue.
    h.store
        .enqueue(dummy_envelope(lock_key))
        .await
        .expect("enqueue 2");
    let (producer_key, _) = h
        .store
        .get_lock_key_index_raw(Queue::Cache, lock_key)
        .await
        .expect("read index")
        .expect("the mid-execution enqueue re-creates the index");

    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(
        pending.len(),
        2,
        "the producer's job and the retried job must both be pending",
    );
    assert!(
        pending.contains(&producer_key),
        "the producer's pending body must survive the retry",
    );
    let (index_storage_key, _) = h
        .store
        .get_lock_key_index_raw(Queue::Cache, lock_key)
        .await
        .expect("read index")
        .expect("index present");
    assert_eq!(
        index_storage_key, producer_key,
        "the retry must not clobber the producer's fresh index",
    );
}

/// A corrupt index met on the dead-letter path is retired inside the same
/// transaction; the job still dead-letters instead of erroring out.
async fn run_dead_letter_with_corrupt_index_still_dead_letters(h: Harness) {
    let lock_key = "cache.ns:sha256:corrupt-dl";
    let mut env = dummy_envelope(lock_key);
    env.max_attempts = 1;
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let storage_key = claimed.storage_key.clone();
    // The claim retired the index; plant a corrupt one in its place.
    let index_path = plant_corrupt_index(&h, lock_key).await;

    assert!(matches!(
        h.store
            .fail(claimed, "final error")
            .await
            .expect("fail must dead-letter, not propagate the parse error"),
        FailOutcome::MovedToDeadLetter
    ));

    assert!(
        matches!(
            h.store.read_pending(Queue::Cache, &storage_key).await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "the pending file must be consumed by the dead-letter",
    );
    let record = h
        .store
        .read_failed(Queue::Cache, &storage_key)
        .await
        .expect("the failed record must be written");
    assert_eq!(record.last_error, "final error");
    assert!(
        h.raw.head(&index_path).await.is_err(),
        "the corrupt index must be retired alongside the dead-letter",
    );
}

/// A corrupt index met on the completion path is retired inside the same
/// transaction; the job still completes instead of failing over.
async fn run_complete_with_corrupt_index_still_completes(h: Harness) {
    let lock_key = "cache.ns:sha256:corrupt-complete";
    h.store
        .enqueue(dummy_envelope(lock_key))
        .await
        .expect("enqueue");
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let index_path = plant_corrupt_index(&h, lock_key).await;

    assert!(matches!(
        h.store
            .complete(claimed, Transaction::builder().build())
            .await
            .expect("complete"),
        CompleteOutcome::Completed
    ));
    assert!(
        h.raw.head(&index_path).await.is_err(),
        "the corrupt index must be retired alongside the completion",
    );
    assert!(
        h.store
            .list_pending(Queue::Cache, 10)
            .await
            .expect("list")
            .is_empty(),
        "the queue must be empty after the completion",
    );
}

#[tokio::test]
async fn corrupt_index_is_counted_and_retired_by_reconcile() {
    let dir = TempDir::new().expect("temp dir");
    run_corrupt_index_is_counted_and_retired_by_reconcile(harness(&dir)).await;
}

#[tokio::test]
async fn corrupt_index_is_counted_and_retired_by_reconcile_memory() {
    run_corrupt_index_is_counted_and_retired_by_reconcile(harness_memory()).await;
}

#[tokio::test]
async fn enqueue_recovers_from_corrupt_index() {
    let dir = TempDir::new().expect("temp dir");
    run_enqueue_recovers_from_corrupt_index(harness(&dir)).await;
}

#[tokio::test]
async fn enqueue_recovers_from_corrupt_index_memory() {
    run_enqueue_recovers_from_corrupt_index(harness_memory()).await;
}

#[tokio::test]
async fn reindex_corrects_corrupt_index() {
    let dir = TempDir::new().expect("temp dir");
    run_reindex_corrects_corrupt_index(harness(&dir)).await;
}

#[tokio::test]
async fn reindex_corrects_corrupt_index_memory() {
    run_reindex_corrects_corrupt_index(harness_memory()).await;
}

#[tokio::test]
async fn fail_retry_leaves_concurrent_producers_index() {
    let dir = TempDir::new().expect("temp dir");
    run_fail_retry_leaves_concurrent_producers_index(harness(&dir)).await;
}

#[tokio::test]
async fn fail_retry_leaves_concurrent_producers_index_memory() {
    run_fail_retry_leaves_concurrent_producers_index(harness_memory()).await;
}

#[tokio::test]
async fn dead_letter_with_corrupt_index_still_dead_letters() {
    let dir = TempDir::new().expect("temp dir");
    run_dead_letter_with_corrupt_index_still_dead_letters(harness(&dir)).await;
}

#[tokio::test]
async fn dead_letter_with_corrupt_index_still_dead_letters_memory() {
    run_dead_letter_with_corrupt_index_still_dead_letters(harness_memory()).await;
}

#[tokio::test]
async fn complete_with_corrupt_index_still_completes() {
    let dir = TempDir::new().expect("temp dir");
    run_complete_with_corrupt_index_still_completes(harness(&dir)).await;
}

#[tokio::test]
async fn complete_with_corrupt_index_still_completes_memory() {
    run_complete_with_corrupt_index_still_completes(harness_memory()).await;
}

// =========================================================================
// complete() commit-failure fail-over (no hot loop)
// =========================================================================

/// When the work-product commit fails, `complete` must fail the job over
/// (retry/dead-letter) rather than leaving the pending file re-claimable forever.
async fn run_complete_commit_failure_fails_job_over(h: Harness) {
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:commitfail"))
        .await
        .expect("enqueue");
    let claimed = h
        .store
        .claim_one(Queue::Cache)
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let original_key = claimed.storage_key.clone();

    // Hand `complete` a work-product `PutIfAbsent` on an existing key so the
    // merged commit aborts.
    let collide_key = "collide-key";
    h.raw
        .put(collide_key, Bytes::from_static(b"x"))
        .await
        .expect("seed collide key");
    let handler_tx = Transaction::builder()
        .mutation(Mutation::PutIfAbsent {
            key: collide_key.to_string(),
            body: Bytes::from_static(b"y"),
        })
        .build();

    let outcome = h
        .store
        .complete(claimed, handler_tx)
        .await
        .expect("complete returns an outcome, not an error");
    assert!(
        matches!(
            outcome,
            CompleteOutcome::FailedOver(FailOutcome::Retried { .. })
        ),
        "a commit failure must fail the job over to a backed-off retry",
    );

    // Re-queued under a new backed-off key, not deleted or left re-claimable.
    let pending = h.store.list_pending(Queue::Cache, 10).await.expect("list");
    assert_eq!(
        pending.len(),
        1,
        "job must be re-queued after commit failure"
    );
    assert_ne!(
        pending[0], original_key,
        "retry must use a new backed-off storage key",
    );
    let env = h
        .store
        .read_pending(Queue::Cache, &pending[0])
        .await
        .expect("read requeued envelope");
    assert_eq!(env.attempts, 1, "attempt count bumped on fail-over");
}

#[tokio::test]
async fn complete_commit_failure_fails_job_over() {
    let dir = TempDir::new().expect("temp dir");
    run_complete_commit_failure_fails_job_over(harness(&dir)).await;
}

#[tokio::test]
async fn complete_commit_failure_fails_job_over_memory() {
    run_complete_commit_failure_fails_job_over(harness_memory()).await;
}

// =========================================================================
// Keyset pagination + administrative mutations
// =========================================================================

async fn run_list_pending_page_is_keyset_ordered(h: Harness) {
    for i in 0..5 {
        h.store
            .enqueue(dummy_envelope(&format!("cache.ns:sha256:page-{i}")))
            .await
            .expect("enqueue");
    }

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let (keys, next) = h
            .store
            .list_pending_page(Queue::Cache, 2, after.as_deref())
            .await
            .expect("page");
        assert!(keys.len() <= 2, "page must not exceed n");
        seen.extend(keys.iter().cloned());
        match next {
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

async fn run_retry_failed_resets_attempts(h: Harness) {
    // Seed a dead-letter record carrying a non-zero attempt count so the reset
    // is observable.
    let mut env = dummy_envelope("cache.ns:sha256:retry-failed");
    env.attempts = 3;
    let key = make_storage_key(Utc::now(), &env.id);
    let body = serialize_dead_letter(&env, "boom").expect("serialize");
    h.raw
        .put(
            &path_builder::job_failed_path("cache", &key),
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
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "failed record is consumed by retry",
    );

    let (pending, _) = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list pending");
    assert_eq!(pending.len(), 1, "exactly one requeued envelope");
    let restored = h
        .store
        .read_pending(Queue::Cache, &pending[0])
        .await
        .expect("read pending");
    assert_eq!(restored.attempts, 0, "retry resets attempts to zero");
    assert_eq!(restored.lock_key, "cache.ns:sha256:retry-failed");

    assert!(
        matches!(
            h.store.retry_failed(Queue::Cache, &key).await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "retrying a consumed key is a stale 404",
    );
}

async fn run_delete_failed_record(h: Harness) {
    let env = dummy_envelope("cache.ns:sha256:del-failed");
    let key = make_storage_key(Utc::now(), &env.id);
    let body = serialize_dead_letter(&env, "boom").expect("serialize");
    h.raw
        .put(
            &path_builder::job_failed_path("cache", &key),
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
        Err(crate::registry::job_store::Error::NotFound)
    ));
    assert!(
        matches!(
            h.store
                .delete_job(Queue::Cache, JobState::Failed, &key)
                .await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "deleting a consumed key is a stale 404",
    );
}

async fn run_delete_pending_removes_record_and_index(h: Harness) {
    let lock_key = "cache.ns:sha256:del-pending";
    h.store
        .enqueue(dummy_envelope(lock_key))
        .await
        .expect("enqueue");
    assert!(
        h.store
            .find_pending_with_lock_key(Queue::Cache, lock_key)
            .await
            .expect("find"),
        "enqueue establishes the dedup index",
    );

    let (pending, _) = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list");
    assert_eq!(pending.len(), 1);
    let key = pending[0].clone();

    h.store
        .delete_job(Queue::Cache, JobState::Pending, &key)
        .await
        .expect("delete");

    let (after, _) = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list");
    assert!(after.is_empty(), "pending file removed");
    assert!(
        !h.store
            .find_pending_with_lock_key(Queue::Cache, lock_key)
            .await
            .expect("find"),
        "dedup index is removed alongside the pending file",
    );

    assert!(
        matches!(
            h.store
                .delete_job(Queue::Cache, JobState::Pending, &key)
                .await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "deleting a consumed key is a stale 404",
    );
}

/// A corrupt dedup index must not block an admin pending delete: the delete
/// succeeds and retires the corrupt index in the same transaction, rather than
/// propagating the parse error.
async fn run_delete_pending_with_corrupt_index_still_deletes(h: Harness) {
    let lock_key = "cache.ns:sha256:corrupt-del-pending";
    h.store
        .enqueue(dummy_envelope(lock_key))
        .await
        .expect("enqueue");
    let (pending, _) = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list");
    assert_eq!(pending.len(), 1);
    let key = pending[0].clone();

    // Overwrite the live index with unparseable bytes.
    let index_path = plant_corrupt_index(&h, lock_key).await;

    h.store
        .delete_job(Queue::Cache, JobState::Pending, &key)
        .await
        .expect("a corrupt index must not abort the pending delete");

    assert!(
        matches!(
            h.store.read_pending(Queue::Cache, &key).await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "the pending file must be deleted",
    );
    assert!(
        h.raw.head(&index_path).await.is_err(),
        "the corrupt index must be retired alongside the pending delete",
    );
}

// =========================================================================
// Structural enumerators
// =========================================================================

async fn run_list_queue_dirs_includes_unknown_queue(h: Harness) {
    // A real enqueue creates `_jobs/pending/cache/`.
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:known"))
        .await
        .expect("enqueue");

    // Plant an unknown queue directory the engine would never produce; the
    // structural-scrub target.
    h.raw
        .put(
            &path_builder::job_pending_path("bogus-queue", "0000000000000000-x"),
            Bytes::from_static(b"{}"),
        )
        .await
        .expect("seed unknown queue");

    let dirs = h
        .store
        .list_queue_dirs(JobState::Pending)
        .await
        .expect("list pending queue dirs");
    assert!(
        dirs.contains(&"cache".to_string()),
        "the known queue dir must be listed; got {dirs:?}"
    );
    assert!(
        dirs.contains(&"bogus-queue".to_string()),
        "an unknown queue dir must be listed so scrub can flag it; got {dirs:?}"
    );
    assert!(
        !dirs.contains(&"replication".to_string()),
        "a queue with no directory must not be listed; got {dirs:?}"
    );
}

async fn run_list_queue_dirs_empty_partition_is_empty(h: Harness) {
    let pending = h
        .store
        .list_queue_dirs(JobState::Pending)
        .await
        .expect("list pending queue dirs");
    assert!(
        pending.is_empty(),
        "an untouched pending partition must list no queue dirs; got {pending:?}"
    );

    let failed = h
        .store
        .list_queue_dirs(JobState::Failed)
        .await
        .expect("list failed queue dirs");
    assert!(
        failed.is_empty(),
        "an untouched failed partition must list no queue dirs; got {failed:?}"
    );
}

async fn run_list_lock_key_index_keys_enumerates_entries(h: Harness) {
    // Each enqueue writes one dedup-index file under `_jobs/index/cache/`.
    let lock_keys = [
        "cache.ns:sha256:a",
        "cache.ns:sha256:b",
        "cache.ns:sha256:c",
    ];
    for lock_key in lock_keys {
        h.store
            .enqueue(dummy_envelope(lock_key))
            .await
            .expect("enqueue");
    }

    let mut keys = h
        .store
        .list_lock_key_index_keys(Queue::Cache)
        .await
        .expect("list index keys");
    assert_eq!(
        keys.len(),
        lock_keys.len(),
        "one index entry per enqueued lock_key; got {keys:?}"
    );

    // Draining the keyset pager must yield the same stems and terminate: this
    // small set fits one page, so the first page carries no continuation.
    let (first_page, next) = h
        .store
        .list_lock_key_index_keys_page(Queue::Cache, None)
        .await
        .expect("first index page");
    assert_eq!(
        next, None,
        "a sub-page set must not report a continuation token; got {next:?}"
    );
    let mut paged = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let (page, cursor) = h
            .store
            .list_lock_key_index_keys_page(Queue::Cache, after.as_deref())
            .await
            .expect("index page");
        paged.extend(page);
        match cursor {
            Some(c) => after = Some(c),
            None => break,
        }
    }
    paged.sort();
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(
        paged, expected,
        "draining pages must enumerate the same stems as the collect-all"
    );
    assert_eq!(
        first_page.len(),
        keys.len(),
        "the single page must carry every stem"
    );

    // The enumerated stems are the encoded lock keys (`.json` stripped); each
    // must address a readable index file under the queue's index directory.
    keys.sort();
    let index_dir = path_builder::job_lock_key_index_dir("cache");
    for key in &keys {
        h.raw
            .get(&format!("{index_dir}/{key}.json"))
            .await
            .expect("enumerated index file must be readable");
    }

    // A different queue with no index entries is empty via both APIs.
    let replication = h
        .store
        .list_lock_key_index_keys(Queue::Replication)
        .await
        .expect("list index keys for empty queue");
    assert!(
        replication.is_empty(),
        "a queue with no dedup indexes must enumerate nothing; got {replication:?}"
    );
    let (empty_page, empty_next) = h
        .store
        .list_lock_key_index_keys_page(Queue::Replication, None)
        .await
        .expect("empty queue index page");
    assert!(
        empty_page.is_empty() && empty_next.is_none(),
        "an empty queue must yield an empty page with no continuation; got {empty_page:?}/{empty_next:?}"
    );
}

async fn run_list_lock_key_index_keys_detects_dangling_entry(h: Harness) {
    let lock_key = "cache.ns:sha256:dangling";
    // Seed only the index file, no pending envelope: the dangling-lock-key
    // reconcile target.
    h.raw
        .put(
            &path_builder::job_lock_key_index_path("cache", lock_key),
            Bytes::from(serialize_lock_key_index("0000000000000000-gone").expect("serialize")),
        )
        .await
        .expect("seed dangling index");

    let keys = h
        .store
        .list_lock_key_index_keys(Queue::Cache)
        .await
        .expect("list index keys");
    assert_eq!(
        keys.len(),
        1,
        "the dangling index must be enumerated; got {keys:?}"
    );

    // No pending body backs it.
    let (pending, _) = h
        .store
        .list_pending_page(Queue::Cache, 10, None)
        .await
        .expect("list pending");
    assert!(
        pending.is_empty(),
        "the dangling index has no pending envelope; got {pending:?}"
    );
}

#[tokio::test]
async fn list_queue_dirs_includes_unknown_queue() {
    let dir = TempDir::new().expect("temp dir");
    run_list_queue_dirs_includes_unknown_queue(harness(&dir)).await;
}

#[tokio::test]
async fn list_queue_dirs_includes_unknown_queue_memory() {
    run_list_queue_dirs_includes_unknown_queue(harness_memory()).await;
}

#[tokio::test]
async fn list_queue_dirs_empty_partition_is_empty() {
    let dir = TempDir::new().expect("temp dir");
    run_list_queue_dirs_empty_partition_is_empty(harness(&dir)).await;
}

#[tokio::test]
async fn list_queue_dirs_empty_partition_is_empty_memory() {
    run_list_queue_dirs_empty_partition_is_empty(harness_memory()).await;
}

#[tokio::test]
async fn list_lock_key_index_keys_enumerates_entries() {
    let dir = TempDir::new().expect("temp dir");
    run_list_lock_key_index_keys_enumerates_entries(harness(&dir)).await;
}

#[tokio::test]
async fn list_lock_key_index_keys_enumerates_entries_memory() {
    run_list_lock_key_index_keys_enumerates_entries(harness_memory()).await;
}

#[tokio::test]
async fn list_lock_key_index_keys_detects_dangling_entry() {
    let dir = TempDir::new().expect("temp dir");
    run_list_lock_key_index_keys_detects_dangling_entry(harness(&dir)).await;
}

#[tokio::test]
async fn list_lock_key_index_keys_detects_dangling_entry_memory() {
    run_list_lock_key_index_keys_detects_dangling_entry(harness_memory()).await;
}

#[tokio::test]
async fn list_pending_page_is_keyset_ordered() {
    let dir = TempDir::new().expect("temp dir");
    run_list_pending_page_is_keyset_ordered(harness(&dir)).await;
}

#[tokio::test]
async fn list_pending_page_is_keyset_ordered_memory() {
    run_list_pending_page_is_keyset_ordered(harness_memory()).await;
}

#[tokio::test]
async fn retry_failed_resets_attempts() {
    let dir = TempDir::new().expect("temp dir");
    run_retry_failed_resets_attempts(harness(&dir)).await;
}

#[tokio::test]
async fn retry_failed_resets_attempts_memory() {
    run_retry_failed_resets_attempts(harness_memory()).await;
}

#[tokio::test]
async fn delete_failed_record() {
    let dir = TempDir::new().expect("temp dir");
    run_delete_failed_record(harness(&dir)).await;
}

#[tokio::test]
async fn delete_failed_record_memory() {
    run_delete_failed_record(harness_memory()).await;
}

#[tokio::test]
async fn delete_pending_removes_record_and_index() {
    let dir = TempDir::new().expect("temp dir");
    run_delete_pending_removes_record_and_index(harness(&dir)).await;
}

#[tokio::test]
async fn delete_pending_removes_record_and_index_memory() {
    run_delete_pending_removes_record_and_index(harness_memory()).await;
}

#[tokio::test]
async fn delete_pending_with_corrupt_index_still_deletes() {
    let dir = TempDir::new().expect("temp dir");
    run_delete_pending_with_corrupt_index_still_deletes(harness(&dir)).await;
}

#[tokio::test]
async fn delete_pending_with_corrupt_index_still_deletes_memory() {
    run_delete_pending_with_corrupt_index_still_deletes(harness_memory()).await;
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
