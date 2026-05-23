//! Behavioural tests for [`locked::Backend`] (storage primitives) driven
//! end-to-end through a [`DurableJobConsumer`] backed by a sibling
//! [`lease::locked::Backend`] over the same in-memory [`LockBackend`].
//! They cover the FS-on-lock chain in a `TempDir`; the equivalent
//! S3-on-real-CAS path is covered via `MinIO` in
//! [`crate::registry::job_store::s3::tests`].

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use tempfile::TempDir;

use crate::{
    metrics_provider,
    registry::{
        job_store::{
            JobEnvelope, JobQueue, JobStore, MAX_REPORTED_PENDING, make_storage_key,
            parse_lock_key_index, parse_not_before, serialize_lock_key_index,
            durable::{DurableJobConsumer, DurableJobQueue, FailOutcome},
            lease::{self, LeaseBackend},
            locked::Backend,
        },
        metadata_store::lock::{LockBackend, MemoryBackend},
        path_builder,
    },
};
use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

/// Build the storage backend, lease backend, and a raw storage handle
/// for fixture-staging, all over the same `TempDir` root and the same
/// shared [`MemoryBackend`] lock.
struct Harness {
    store: Arc<dyn JobStore>,
    leases: Arc<dyn LeaseBackend>,
    raw: Arc<StorageFsBackend>,
}

fn harness(dir: &TempDir) -> Harness {
    metrics_provider::init_for_tests();
    let raw = Arc::new(
        StorageFsBackend::builder()
            .root_dir(dir.path())
            .build()
            .expect("storage backend"),
    );
    let lock: Arc<dyn LockBackend + Send + Sync> = Arc::new(MemoryBackend::new());
    let store: Arc<dyn JobStore> = Arc::new(
        Backend::builder()
            .store(raw.clone())
            .lock(lock.clone())
            .build()
            .expect("locked backend"),
    );
    let leases: Arc<dyn LeaseBackend> = Arc::new(
        lease::locked::Backend::builder()
            .store(raw.clone())
            .lock(lock)
            .build()
            .expect("lease backend"),
    );
    Harness { store, leases, raw }
}

fn dummy_envelope(lock_key: &str) -> JobEnvelope {
    JobEnvelope::new("cache", "test.noop", lock_key, &()).expect("envelope")
}

fn make_consumer(h: &Harness) -> DurableJobConsumer {
    DurableJobConsumer::new(h.store.clone(), h.leases.clone(), 30, "test-worker".to_string())
}

// =========================================================================
// End-to-end claim cycle
// =========================================================================

#[tokio::test]
async fn enqueue_then_claim_succeeds() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let queue = DurableJobQueue::new(h.store.clone());
    let consumer = make_consumer(&h);

    queue
        .enqueue(dummy_envelope("cache.ns:sha256:aaa"))
        .await
        .expect("enqueue");

    let claimed = consumer
        .claim_one("cache")
        .await
        .expect("claim_one")
        .claimed
        .expect("Some");
    assert_eq!(claimed.envelope.lock_key, "cache.ns:sha256:aaa");
    consumer.complete(claimed).await.expect("complete");
    assert!(
        consumer
            .claim_one("cache")
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
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let consumer = make_consumer(&h);

    let mut env = dummy_envelope("cache.ns:sha256:retry");
    env.max_attempts = 3;
    h.store
        .put_pending("cache", &env, Utc::now())
        .await
        .expect("put");

    let claimed = consumer
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");

    assert!(matches!(
        consumer.fail(claimed, "boom").await.expect("fail"),
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

#[tokio::test]
async fn dead_letter_after_max_attempts() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let consumer = make_consumer(&h);

    let mut env = dummy_envelope("cache.ns:sha256:dl");
    env.max_attempts = 1;
    h.store
        .put_pending("cache", &env, Utc::now())
        .await
        .expect("put");

    let claimed = consumer
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let storage_key = claimed.storage_key.clone();

    assert!(matches!(
        consumer.fail(claimed, "final error").await.expect("fail"),
        FailOutcome::MovedToDeadLetter
    ));
    assert!(matches!(
        h.store.read_pending("cache", &storage_key).await,
        Err(crate::registry::job_store::Error::NotFound)
    ));
}

// =========================================================================
// count_pending
// =========================================================================

#[tokio::test]
async fn count_pending_saturates_at_cap() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);

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

#[tokio::test]
async fn count_pending_excludes_envelopes_past_readiness_horizon() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);

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

#[tokio::test]
async fn future_storage_key_yields_next_ready_without_claiming() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let consumer = make_consumer(&h);

    let env = dummy_envelope("cache.ns:sha256:future");
    let scheduled = Utc::now() + chrono::Duration::seconds(3600);
    h.store
        .put_pending("cache", &env, scheduled)
        .await
        .expect("put");

    let outcome = consumer.claim_one("cache").await.expect("claim");
    assert!(
        outcome.claimed.is_none(),
        "future-scheduled job must not be claimed",
    );
    let next = outcome.next_ready.expect("next_ready must be set");
    let diff = (scheduled - next).num_milliseconds().abs();
    assert!(diff < 2, "next_ready ({next}) must match scheduled ({scheduled})");
}

// =========================================================================
// Dedup index (lock-key index)
// =========================================================================

#[tokio::test]
async fn dedup_lookup_uses_index_not_body_scan() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let lock_key = "cache.ns:sha256:nobody-reads-this";

    let env = dummy_envelope(lock_key);
    let storage_key = h
        .store
        .put_pending("cache", &env, Utc::now())
        .await
        .expect("put");
    assert!(
        h.store
            .try_claim_lock_key("cache", lock_key, &storage_key)
            .await
            .expect("claim"),
        "fresh lock_key must be claimable",
    );

    // Corrupt every pending body. If dedup ever scanned bodies, parsing
    // would fail.
    let pending_dir = path_builder::job_pending_dir("cache");
    let page = h.raw.list(&pending_dir, 1000, None).await.expect("list");
    for name in page.items {
        let path = format!("{pending_dir}/{name}");
        h.raw
            .put(&path, Bytes::from_static(b"NOT VALID JSON"))
            .await
            .expect("corrupt");
    }

    // Dedup should still report a hit — purely from the index file.
    assert!(
        h.store
            .find_pending_with_lock_key("cache", lock_key)
            .await
            .expect("dedup"),
        "find_pending_with_lock_key must succeed via the index",
    );
}

#[tokio::test]
async fn try_claim_lock_key_resolves_enqueue_race() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let lock_key = "cache.ns:sha256:race";

    let env1 = dummy_envelope(lock_key);
    let env2 = dummy_envelope(lock_key);
    let key1 = h
        .store
        .put_pending("cache", &env1, Utc::now())
        .await
        .expect("put1");
    let key2 = h
        .store
        .put_pending("cache", &env2, Utc::now())
        .await
        .expect("put2");
    assert_ne!(key1, key2);

    assert!(
        h.store
            .try_claim_lock_key("cache", lock_key, &key1)
            .await
            .expect("claim1"),
        "first claim wins",
    );
    assert!(
        !h.store
            .try_claim_lock_key("cache", lock_key, &key2)
            .await
            .expect("claim2"),
        "second claim must lose",
    );

    // Loser cleans up.
    h.store
        .remove_pending("cache", &key2)
        .await
        .expect("rm loser");

    let pending = h.store.list_pending("cache", 10).await.expect("list");
    assert_eq!(pending, vec![key1.clone()]);
}

#[tokio::test]
async fn orphan_index_is_self_healed_on_next_lookup() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let lock_key = "cache.ns:sha256:orphan";

    // Write an orphan: index points at a non-existent pending file.
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

#[tokio::test]
async fn retry_updates_lock_key_index_to_new_storage_key() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let consumer = make_consumer(&h);
    let lock_key = "cache.ns:sha256:retry-index";

    let mut env = dummy_envelope(lock_key);
    env.max_attempts = 3;
    h.store
        .put_pending("cache", &env, Utc::now())
        .await
        .expect("put");

    let claimed = consumer
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    let old_storage_key = claimed.storage_key.clone();
    assert!(matches!(
        consumer.fail(claimed, "boom").await.expect("fail"),
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

#[tokio::test]
async fn complete_removes_lock_key_index() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let consumer = make_consumer(&h);
    let lock_key = "cache.ns:sha256:complete-clears";

    let env = dummy_envelope(lock_key);
    let storage_key = h
        .store
        .put_pending("cache", &env, Utc::now())
        .await
        .expect("put");
    assert!(
        h.store
            .try_claim_lock_key("cache", lock_key, &storage_key)
            .await
            .expect("claim"),
    );
    let index_path = path_builder::job_lock_key_index_path("cache", lock_key);
    assert!(h.raw.head(&index_path).await.is_ok());

    let claimed = consumer
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    consumer.complete(claimed).await.expect("complete");

    assert!(
        h.raw.head(&index_path).await.is_err(),
        "index must be gone after complete",
    );
}

#[tokio::test]
async fn dead_letter_removes_lock_key_index() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let consumer = make_consumer(&h);
    let lock_key = "cache.ns:sha256:dlq-clears";

    let mut env = dummy_envelope(lock_key);
    env.max_attempts = 1;
    let storage_key = h
        .store
        .put_pending("cache", &env, Utc::now())
        .await
        .expect("put");
    assert!(
        h.store
            .try_claim_lock_key("cache", lock_key, &storage_key)
            .await
            .expect("claim"),
    );
    let index_path = path_builder::job_lock_key_index_path("cache", lock_key);
    assert!(h.raw.head(&index_path).await.is_ok());

    let claimed = consumer
        .claim_one("cache")
        .await
        .expect("claim")
        .claimed
        .expect("Some");
    assert!(matches!(
        consumer.fail(claimed, "final").await.expect("fail"),
        FailOutcome::MovedToDeadLetter
    ));

    assert!(
        h.raw.head(&index_path).await.is_err(),
        "index must be gone after DLQ",
    );
}

#[tokio::test]
async fn enqueue_dedup_skips_existing_lock_key() {
    let dir = TempDir::new().expect("temp dir");
    let h = harness(&dir);
    let queue = DurableJobQueue::new(h.store.clone());

    queue
        .enqueue(dummy_envelope("cache.ns:sha256:dup"))
        .await
        .expect("enqueue 1");
    let before = h.store.count_pending("cache", 600).await.expect("count");
    queue
        .enqueue(dummy_envelope("cache.ns:sha256:dup"))
        .await
        .expect("enqueue 2");
    assert_eq!(before, h.store.count_pending("cache", 600).await.expect("count"));
}
