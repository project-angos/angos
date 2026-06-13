use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{TimeZone as _, Utc};
use tempfile::TempDir;

use angos_storage::{MemoryObjectStore, ObjectStore, fs::Backend as StorageFsBackend};
use angos_tx_engine::transaction::{Mutation, Transaction};

use crate::registry::job_store::{
    CompleteOutcome, FailOutcome, JobEnvelope, JobQueueConfig, JobState, JobStore,
    MAX_REPORTED_PENDING, STORAGE_KEY_PREFIX_LEN, backoff, make_storage_key, parse_lock_key_index,
    parse_not_before, serialize_dead_letter, serialize_lock_key_index,
};
use crate::registry::test_utils::{build_store, locked_executor_over};
use crate::{metrics_provider, registry::path_builder};

struct Harness {
    store: Arc<JobStore>,
    // Raw handle lets tests stage deliberate fixture state (count-cap
    // stress, orphan indexes) that the engine would never produce naturally.
    raw: Arc<dyn ObjectStore>,
}

fn harness(dir: &TempDir) -> Harness {
    metrics_provider::init_for_tests();
    let raw: Arc<dyn ObjectStore> = Arc::new(
        StorageFsBackend::builder()
            .root_dir(dir.path())
            .build()
            .expect("storage backend"),
    );
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
    JobEnvelope::new("cache", "test.noop", lock_key, &()).expect("envelope")
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
        .claim_one("cache")
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
            .claim_one("cache")
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
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    let pending = h.store.list_pending("cache", 10).await.expect("list");
    assert_eq!(pending.len(), 1, "exactly one retry envelope expected");
    let storage_key = &pending[0];
    let not_before = parse_not_before(storage_key).expect("parse prefix");
    assert!(not_before > Utc::now(), "retry must be backed off");

    let updated = h
        .store
        .read_pending("cache", storage_key)
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
        .claim_one("cache")
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
        h.store.read_pending("cache", &storage_key).await,
        Err(crate::registry::job_store::Error::NotFound)
    ));
}

async fn run_count_failed_reflects_dead_letters(h: Harness) {
    assert_eq!(h.store.count_failed("cache").await.expect("count"), 0);

    let mut env = dummy_envelope("cache.ns:sha256:dl-count");
    env.max_attempts = 1;
    h.store.enqueue(env).await.expect("enqueue");
    let claimed = h
        .store
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    assert!(matches!(
        h.store.fail(claimed, "final error").await.expect("fail"),
        FailOutcome::MovedToDeadLetter
    ));

    assert_eq!(
        h.store.count_failed("cache").await.expect("count"),
        1,
        "a dead-lettered job must be counted by count_failed",
    );
    assert_eq!(
        h.store.count_pending("cache", 600).await.expect("count"),
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
    let count = h.store.count_pending("cache", 600).await.expect("count");
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

    let count = h.store.count_pending("cache", 60).await.expect("count");
    assert_eq!(count, 2, "only ready envelopes must count");
}

async fn run_future_storage_key_yields_next_ready_without_claiming(h: Harness) {
    let mut env = dummy_envelope("cache.ns:sha256:future");
    env.max_attempts = 5;
    h.store.enqueue(env).await.expect("enqueue");

    let claimed = h
        .store
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let scheduled = match h.store.fail(claimed, "scheduled").await.expect("fail") {
        FailOutcome::Retried { next_at } => next_at,
        FailOutcome::MovedToDeadLetter => panic!("expected retry"),
    };

    let outcome = h.store.claim_one("cache").await.expect("claim");
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
        .find_pending_with_lock_key("cache", lock_key)
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
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let old_storage_key = claimed.storage_key.clone();
    assert!(matches!(
        h.store.fail(claimed, "boom").await.expect("fail"),
        FailOutcome::Retried { .. }
    ));

    let pending = h.store.list_pending("cache", 10).await.expect("list");
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
    let before = h.store.count_pending("cache", 600).await.expect("count");
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:dup"))
        .await
        .expect("enqueue 2");
    assert_eq!(
        before,
        h.store.count_pending("cache", 600).await.expect("count")
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
        .claim_one("cache")
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
        h.store.count_pending("cache", 600).await.expect("count"),
        2,
        "enqueue during execution must create a second pending job",
    );

    // The execution lock serialises the two: the second is unclaimable until
    // the first releases the lock on complete.
    assert!(
        h.store
            .claim_one("cache")
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
        .claim_one("cache")
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
            .claim_one("cache")
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

    let pending = h.store.list_pending("cache", 64).await.expect("list");
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
// complete() commit-failure fail-over (no hot loop)
// =========================================================================

/// When the work-product commit fails, `complete` must fail the job over
/// (retry/dead-letter) rather than returning an error that leaves the pending
/// file re-claimable forever. Regression test for the in-process cache-fill
/// hot loop (doc/reviews/20260603-in-process-cache-fill-broken.md).
async fn run_complete_commit_failure_fails_job_over(h: Harness) {
    h.store
        .enqueue(dummy_envelope("cache.ns:sha256:commitfail"))
        .await
        .expect("enqueue");
    let claimed = h
        .store
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let original_key = claimed.storage_key.clone();

    // Seed a key, then hand `complete` a work-product transaction whose
    // `PutIfAbsent` on that key is guaranteed to fail (the key already exists),
    // forcing the merged commit to abort.
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

    // The job is re-queued (with backoff and a bumped attempt count) under a new
    // storage key, not deleted, and not left at the original key to be
    // re-claimed immediately.
    let pending = h.store.list_pending("cache", 10).await.expect("list");
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
        .read_pending("cache", &pending[0])
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
            .list_pending_page("cache", 2, after.as_deref())
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
        .read_failed("cache", &key)
        .await
        .expect("read failed");
    assert_eq!(record.last_error, "boom");
    assert_eq!(record.envelope.attempts, 3);

    h.store.retry_failed("cache", &key).await.expect("retry");

    assert!(
        matches!(
            h.store.read_failed("cache", &key).await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "failed record is consumed by retry",
    );

    let (pending, _) = h
        .store
        .list_pending_page("cache", 10, None)
        .await
        .expect("list pending");
    assert_eq!(pending.len(), 1, "exactly one requeued envelope");
    let restored = h
        .store
        .read_pending("cache", &pending[0])
        .await
        .expect("read pending");
    assert_eq!(restored.attempts, 0, "retry resets attempts to zero");
    assert_eq!(restored.lock_key, "cache.ns:sha256:retry-failed");

    assert!(
        matches!(
            h.store.retry_failed("cache", &key).await,
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
        .delete_job("cache", JobState::Failed, &key)
        .await
        .expect("delete");
    assert!(matches!(
        h.store.read_failed("cache", &key).await,
        Err(crate::registry::job_store::Error::NotFound)
    ));
    assert!(
        matches!(
            h.store.delete_job("cache", JobState::Failed, &key).await,
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
            .find_pending_with_lock_key("cache", lock_key)
            .await
            .expect("find"),
        "enqueue establishes the dedup index",
    );

    let (pending, _) = h
        .store
        .list_pending_page("cache", 10, None)
        .await
        .expect("list");
    assert_eq!(pending.len(), 1);
    let key = pending[0].clone();

    h.store
        .delete_job("cache", JobState::Pending, &key)
        .await
        .expect("delete");

    let (after, _) = h
        .store
        .list_pending_page("cache", 10, None)
        .await
        .expect("list");
    assert!(after.is_empty(), "pending file removed");
    assert!(
        !h.store
            .find_pending_with_lock_key("cache", lock_key)
            .await
            .expect("find"),
        "dedup index is removed alongside the pending file",
    );

    assert!(
        matches!(
            h.store.delete_job("cache", JobState::Pending, &key).await,
            Err(crate::registry::job_store::Error::NotFound)
        ),
        "deleting a consumed key is a stale 404",
    );
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
fn backoff_doubles_each_attempt_then_caps_at_ten_minutes() {
    assert_eq!(backoff(0), Duration::from_mins(1));
    assert_eq!(backoff(1), Duration::from_mins(2));
    assert_eq!(backoff(3), Duration::from_mins(8));
    assert_eq!(backoff(4), Duration::from_mins(10));
    assert_eq!(backoff(100), Duration::from_mins(10));
}
