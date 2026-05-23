//! Behavioural tests for [`lease::locked::Backend`] using an FS
//! [`ObjectStore`] and an in-memory [`LockBackend`]. Covers the lease
//! lifecycle (acquire / steal-stale / heartbeat-loss) on the lock-coordinated
//! path; the equivalent CAS path is covered via `MinIO` in
//! [`crate::registry::job_store::s3::tests::s3_stale_lease_recovery`].

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use chrono::Utc;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::{
    metrics_provider,
    registry::{
        job_store::lease::{LeaseBackend, locked::Backend},
        metadata_store::lock::MemoryBackend,
        path_builder,
    },
};
use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

/// Build a lease backend over a fresh `TempDir`, alongside a raw storage
/// handle pointed at the same root so tests can stage fixtures (stale
/// bodies, stolen leases, …) directly.
fn setup(dir: &TempDir) -> (Backend, Arc<StorageFsBackend>) {
    metrics_provider::init_for_tests();
    let raw = Arc::new(
        StorageFsBackend::builder()
            .root_dir(dir.path())
            .build()
            .expect("storage backend"),
    );
    let backend = Backend::builder()
        .store(raw.clone())
        .lock(Arc::new(MemoryBackend::new()))
        .build()
        .expect("lease backend");
    (backend, raw)
}

#[tokio::test]
async fn fresh_acquire_returns_a_guard() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:fresh";

    let guard = backend
        .try_acquire(lock_key, "worker-A", 30)
        .await
        .expect("acquire")
        .expect("first acquire wins");

    // Body must exist on disk now.
    assert!(
        raw.head(&path_builder::job_lease_path(lock_key)).await.is_ok(),
        "lease body must exist after acquire",
    );
    guard.release().await;
}

#[tokio::test]
async fn second_acquire_while_held_returns_none() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, _raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:held";

    let first = backend
        .try_acquire(lock_key, "worker-A", 30)
        .await
        .expect("acquire 1")
        .expect("Some");
    let second = backend
        .try_acquire(lock_key, "worker-B", 30)
        .await
        .expect("acquire 2");
    assert!(
        second.is_none(),
        "second acquire on a fresh lease must yield None",
    );
    first.release().await;
}

/// Body-driven staleness: a lease whose `refreshed_at` is past `ttl_secs`
/// ago must be stealable even when the storage object is fresh.
#[tokio::test]
async fn stale_lease_is_stolen() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:stale";

    let stale_body = serde_json::json!({
        "instance_id": "dead-uuid",
        "refreshed_at": Utc::now() - chrono::Duration::seconds(3600),
        "worker_id": "long-gone",
        "ttl_secs": 30u64,
    });
    raw.put(
        &path_builder::job_lease_path(lock_key),
        Bytes::from(serde_json::to_vec(&stale_body).expect("serialize")),
    )
    .await
    .expect("seed stale lease");

    let guard = backend
        .try_acquire(lock_key, "worker-B", 30)
        .await
        .expect("acquire after stale")
        .expect("Some — body refreshed_at must drive staleness");
    guard.release().await;
}

/// When the lease body is replaced under us (a successful steal by another
/// worker), the heartbeat task notices the `instance_id` mismatch and
/// cancels the lost token within roughly one TTL window.
#[tokio::test]
async fn heartbeat_marks_lease_lost_after_replacement() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:replaced";

    // ttl_secs=3 → heartbeat ticks at 1 s; three consecutive ownership
    // mismatches cancel the token in ~3 s.
    let guard = backend
        .try_acquire(lock_key, "worker-A", 3)
        .await
        .expect("acquire")
        .expect("Some");

    // Simulate a successful steal by another worker.
    let stealer_body = serde_json::json!({
        "instance_id": "worker-B-uuid",
        "refreshed_at": Utc::now(),
        "worker_id": "worker-B",
        "ttl_secs": 3u64,
    });
    raw.put(
        &path_builder::job_lease_path(lock_key),
        Bytes::from(serde_json::to_vec(&stealer_body).expect("serialize")),
    )
    .await
    .expect("write");

    let lost = guard.lost_token();
    timeout(Duration::from_secs(6), lost.cancelled())
        .await
        .expect("lost_token must fire within 6s of body replacement");
    guard.release().await;
}

/// When the lease body vanishes (e.g. operator deletion or shared-mount
/// corruption), the heartbeat task gives up after the same handful of
/// failures and cancels the lost token.
#[tokio::test]
async fn heartbeat_marks_lease_lost_when_body_vanishes() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:vanished";

    let guard = backend
        .try_acquire(lock_key, "worker-A", 3)
        .await
        .expect("acquire")
        .expect("Some");

    raw.delete(&path_builder::job_lease_path(lock_key))
        .await
        .expect("delete lease body");

    let lost = guard.lost_token();
    timeout(Duration::from_secs(6), lost.cancelled())
        .await
        .expect("lost_token must fire within 6s of body deletion");
    guard.release().await;
}

#[tokio::test]
async fn release_deletes_the_lease_body() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:cleanup";

    let guard = backend
        .try_acquire(lock_key, "worker-A", 30)
        .await
        .expect("acquire")
        .expect("Some");
    guard.release().await;

    assert!(
        raw.head(&path_builder::job_lease_path(lock_key))
            .await
            .is_err(),
        "lease body must be gone after release",
    );
}

/// Release after a successful steal must be a no-op on the new owner's
/// body — `instance_id` no longer matches, so the original holder leaves
/// the body alone.
#[tokio::test]
async fn release_after_steal_leaves_new_owner_body() {
    let dir = TempDir::new().expect("temp dir");
    let (backend, raw) = setup(&dir);
    let lock_key = "cache.ns:sha256:stolen";

    let guard = backend
        .try_acquire(lock_key, "worker-A", 30)
        .await
        .expect("acquire")
        .expect("Some");

    let stealer_body = serde_json::json!({
        "instance_id": "worker-B-uuid",
        "refreshed_at": Utc::now(),
        "worker_id": "worker-B",
        "ttl_secs": 30u64,
    });
    raw.put(
        &path_builder::job_lease_path(lock_key),
        Bytes::from(serde_json::to_vec(&stealer_body).expect("serialize")),
    )
    .await
    .expect("steal");

    guard.release().await;

    assert!(
        raw.head(&path_builder::job_lease_path(lock_key))
            .await
            .is_ok(),
        "stealer body must survive release by the original holder",
    );
}
