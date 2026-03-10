use std::{
    io::ErrorKind,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::Utc;

use super::{RecoveryOutcome, S3LockBackend, S3LockConfig, S3LockPayload};
use crate::registry::{data_store, metadata_store::lock::LockBackend};

fn test_store(prefix: &str) -> Arc<data_store::s3::Backend> {
    let config = data_store::s3::BackendConfig {
        access_key_id: "root".to_string(),
        secret_key: "roottoor".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "registry".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: prefix.to_string(),
        ..Default::default()
    };
    Arc::new(data_store::s3::Backend::new(&config).unwrap())
}

fn test_backend(store: Arc<data_store::s3::Backend>) -> S3LockBackend {
    S3LockBackend::new(
        store,
        &S3LockConfig {
            ttl_secs: 30,
            max_retries: 0,
            retry_delay_ms: 5,
            max_hold_secs: 300,
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn test_acquire_empty_keys() {
    let store = test_store("s3lock_empty_");
    let backend = test_backend(store);
    let guard = backend.acquire(&[]).await;
    assert!(guard.is_ok(), "Acquiring empty keys should succeed");
    drop(guard);
}

#[tokio::test]
async fn test_acquire_and_release() {
    let store = test_store("s3lock_acq_rel_");
    let backend1 = test_backend(store.clone());
    let backend2 = test_backend(store);

    let guard = backend1
        .acquire(&["test_key".to_string()])
        .await
        .expect("Failed to acquire lock");

    let attempt = backend2.acquire(&["test_key".to_string()]).await;
    assert!(attempt.is_err(), "Should not acquire an already held lock");

    guard.release().await;

    let new_guard = backend1
        .acquire(&["test_key".to_string()])
        .await
        .expect("Should acquire lock after release");
    drop(new_guard);
}

#[tokio::test]
async fn test_acquire_multiple_locks() {
    let store = test_store("s3lock_multi_");
    let backend1 = test_backend(store.clone());
    let backend2 = test_backend(store);

    let guard = backend1
        .acquire(&["key1".to_string(), "key2".to_string()])
        .await
        .expect("Failed to acquire locks");

    let attempt = backend2.acquire(&["key2".to_string()]).await;
    assert!(
        attempt.is_err(),
        "Should not acquire a key from a held lock set"
    );

    guard.release().await;

    let new_guard = backend2
        .acquire(&["key1".to_string(), "key2".to_string()])
        .await
        .expect("Should acquire after release");
    drop(new_guard);
}

#[tokio::test]
async fn test_rollback_on_partial_acquire() {
    let store = test_store("s3lock_rollback_");
    let backend1 = test_backend(store.clone());
    let backend2 = test_backend(store.clone());
    let backend3 = test_backend(store);

    let guard_a = backend1
        .acquire(&["keyA".to_string()])
        .await
        .expect("Failed to acquire keyA");

    let attempt = backend2
        .acquire(&["keyA".to_string(), "keyB".to_string()])
        .await;
    assert!(attempt.is_err(), "Should fail since keyA is held");

    // keyB should be available after rollback
    let guard_b = backend3
        .acquire(&["keyB".to_string()])
        .await
        .expect("keyB should be available after rollback");

    drop(guard_a);
    drop(guard_b);
}

#[tokio::test]
async fn test_stale_lock_recovery() {
    let store = test_store("s3lock_stale_");

    // Write a lock and wait for it to expire (short TTL)
    let stale_payload = S3LockPayload {
        instance_id: "dead-instance".to_string(),
        refreshed_at: Utc::now(),
        ttl_secs: 3,
    };
    let data = serde_json::to_vec(&stale_payload).unwrap();
    store
        .put_object("_locks/stale_key", data)
        .await
        .expect("Failed to write stale lock");

    // Wait for the lock to become stale (S3 LastModified-based expiry)
    tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

    let backend = S3LockBackend::new(
        store,
        &S3LockConfig {
            ttl_secs: 9,
            max_retries: 0,
            retry_delay_ms: 1,
            max_hold_secs: 300,
            ..Default::default()
        },
    )
    .unwrap();
    let guard = backend
        .acquire(&["stale_key".to_string()])
        .await
        .expect("Should recover stale lock and acquire");
    drop(guard);
}

#[tokio::test]
async fn test_non_stale_lock_not_recovered() {
    let store = test_store("s3lock_nonstale_");
    let backend = test_backend(store.clone());

    let fresh_payload = S3LockPayload {
        instance_id: "live-instance".to_string(),
        refreshed_at: Utc::now(),
        ttl_secs: 300,
    };
    let data = serde_json::to_vec(&fresh_payload).unwrap();
    store
        .put_object("_locks/fresh_key", data)
        .await
        .expect("Failed to write fresh lock");

    let attempt = backend.acquire(&["fresh_key".to_string()]).await;
    assert!(attempt.is_err(), "Should not recover a non-stale lock");

    let _ = store.delete("_locks/fresh_key").await;
}

#[test]
fn test_jitter_bounds() {
    for max_ms in [1, 10, 100, 500, 1000] {
        for _ in 0..20 {
            let val = super::simple_jitter(max_ms);
            assert!(val < max_ms, "jitter {val} should be < {max_ms}");
        }
    }
}

#[test]
fn test_jitter_zero_guard() {
    assert_eq!(super::simple_jitter(0), 0);
}

#[test]
fn test_jitter_non_constant() {
    let values: std::collections::HashSet<u64> =
        (0..10).map(|_| super::simple_jitter(100)).collect();
    assert!(
        values.len() >= 2,
        "Expected at least 2 distinct values in 10 calls, got {values:?}"
    );
}

#[tokio::test]
async fn test_heartbeat_refreshes_lock() {
    let store = test_store("s3lock_m6_hb_refresh_");
    let backend = S3LockBackend::new(
        store.clone(),
        &S3LockConfig {
            ttl_secs: 9,
            max_retries: 0,
            retry_delay_ms: 1,
            max_hold_secs: 300,
            ..Default::default()
        },
    )
    .unwrap();

    let guard = backend
        .acquire(&["hb_test".to_string()])
        .await
        .expect("Failed to acquire lock");

    // Wait longer than heartbeat interval (9/3 = 3s) so heartbeat fires at least once
    tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

    // Lock file should still exist (heartbeat refreshed it)
    let data = store
        .read("_locks/hb_test")
        .await
        .expect("Lock file should still exist after heartbeat refresh");
    assert!(!data.is_empty(), "Lock payload should not be empty");

    assert!(
        guard.is_valid(),
        "Guard should still be valid after heartbeat"
    );
    guard.release().await;
}

#[tokio::test]
async fn test_heartbeat_invalidates_guard_on_ownership_loss() {
    let store = test_store("s3lock_h1_hb_invalid_");
    let backend = S3LockBackend::new(
        store.clone(),
        &S3LockConfig {
            ttl_secs: 9,
            max_retries: 0,
            retry_delay_ms: 1,
            max_hold_secs: 300,
            ..Default::default()
        },
    )
    .unwrap();

    let guard = backend
        .acquire(&["test_key".to_string()])
        .await
        .expect("Failed to acquire lock");

    assert!(
        guard.is_valid(),
        "Guard should be valid immediately after acquire"
    );

    let stolen_payload = S3LockPayload {
        instance_id: "thief-instance".to_string(),
        refreshed_at: Utc::now(),
        ttl_secs: 30,
    };
    let data = serde_json::to_vec(&stolen_payload).unwrap();
    store
        .put_object("_locks/test_key", data)
        .await
        .expect("Failed to overwrite lock");

    // Heartbeat interval is ttl/3 = 3s; sleep 4s to ensure at least one tick fires
    tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

    assert!(
        !guard.is_valid(),
        "Guard should be invalid after heartbeat detects ownership loss"
    );

    guard.release().await;
}

#[tokio::test]
async fn test_explicit_release_deletes_lock() {
    let store = test_store("s3lock_h3_release_");
    let backend = test_backend(store.clone());

    let guard = backend
        .acquire(&["test_key".to_string()])
        .await
        .expect("Failed to acquire lock");

    guard.release().await;

    let result = store.read("_locks/test_key").await;
    assert_eq!(
        result.unwrap_err().kind(),
        ErrorKind::NotFound,
        "Lock object should be deleted after explicit release"
    );
}

#[tokio::test]
async fn test_recovery_returns_retry_on_missing_lock() {
    let store = test_store("s3lock_retry_vanished_");
    let backend = test_backend(store);
    let outcome = backend.test_try_recover("nonexistent_key").await;
    assert!(
        matches!(outcome, RecoveryOutcome::Retry),
        "Expected Retry for vanished lock, got {outcome:?}"
    );
}

#[tokio::test]
async fn test_release_skips_delete_when_ownership_lost() {
    let store = test_store("s3lock_skip_del_");
    let backend = S3LockBackend::new(
        store.clone(),
        &S3LockConfig {
            ttl_secs: 9,
            max_retries: 0,
            retry_delay_ms: 1,
            max_hold_secs: 300,
            ..Default::default()
        },
    )
    .unwrap();

    let guard = backend
        .acquire(&["owned_key".to_string()])
        .await
        .expect("Failed to acquire lock");

    let stolen_payload = S3LockPayload {
        instance_id: "thief-instance".to_string(),
        refreshed_at: Utc::now(),
        ttl_secs: 30,
    };
    let data = serde_json::to_vec(&stolen_payload).unwrap();
    store
        .put_object("_locks/owned_key", data)
        .await
        .expect("Failed to overwrite lock");

    // Heartbeat interval is ttl/3 = 3s; sleep 4s to ensure at least one tick fires
    tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
    assert!(
        !guard.is_valid(),
        "Guard should be invalid after ownership loss"
    );

    guard.release().await;

    let data = store
        .read("_locks/owned_key")
        .await
        .expect("Thief's lock should still exist after release");
    let payload: S3LockPayload = serde_json::from_slice(&data).unwrap();
    assert_eq!(
        payload.instance_id, "thief-instance",
        "Thief's lock should not have been deleted"
    );

    let _ = store.delete("_locks/owned_key").await;
}

/// Spawns N tasks that race for the same lock key with retries enabled.
/// Verifies mutual exclusion (at most 1 holder at any time) and that all
/// tasks eventually succeed (no starvation from retry exhaustion).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_contention_single_key() {
    let store = test_store("s3lock_contention_");
    let num_tasks: usize = 5;
    let iterations_per_task: usize = 3;
    let concurrent_holders = Arc::new(AtomicUsize::new(0));
    let max_observed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for task_id in 0..num_tasks {
        let store = store.clone();
        let concurrent_holders = concurrent_holders.clone();
        let max_observed = max_observed.clone();
        handles.push(tokio::spawn(async move {
            let backend = S3LockBackend::new(
                store,
                &S3LockConfig {
                    ttl_secs: 9,
                    max_retries: 50,
                    retry_delay_ms: 10,
                    max_hold_secs: 300,
                    ..Default::default()
                },
            )
            .unwrap();

            for iter in 0..iterations_per_task {
                let guard = backend
                    .acquire(&["contended_key".to_string()])
                    .await
                    .unwrap_or_else(|e| {
                        panic!("Task {task_id} iter {iter} failed to acquire: {e}")
                    });

                let prev = concurrent_holders.fetch_add(1, Ordering::SeqCst);
                max_observed.fetch_max(prev + 1, Ordering::SeqCst);
                assert_eq!(
                    prev,
                    0,
                    "Task {task_id} iter {iter}: mutual exclusion violated ({} concurrent holders)",
                    prev + 1
                );

                // Simulate brief work under lock
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                concurrent_holders.fetch_sub(1, Ordering::SeqCst);
                guard.release().await;
            }
        }));
    }

    let results = futures_util::future::join_all(handles).await;
    for (i, result) in results.into_iter().enumerate() {
        result.unwrap_or_else(|e| panic!("Task {i} panicked: {e}"));
    }

    assert_eq!(
        max_observed.load(Ordering::SeqCst),
        1,
        "At most 1 task should hold the lock at any time"
    );
}

/// Three tasks acquire overlapping multi-key lock sets, exercising the
/// parallel-to-sequential fallback that prevents livelock. Key sets:
///   Task A: [k1, k2]
///   Task B: [k2, k3]
///   Task C: [k1, k3]
/// Each pair shares exactly one key, maximising contention. All tasks
/// must eventually succeed — starvation would indicate a livelock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_contention_overlapping_keys() {
    let store = test_store("s3lock_overlap_");
    let concurrent_holders = Arc::new(AtomicUsize::new(0));
    let max_observed = Arc::new(AtomicUsize::new(0));

    let key_sets: Vec<Vec<String>> = vec![
        vec!["k1".to_string(), "k2".to_string()],
        vec!["k2".to_string(), "k3".to_string()],
        vec!["k1".to_string(), "k3".to_string()],
    ];

    let mut handles = Vec::new();
    for (task_id, keys) in key_sets.into_iter().enumerate() {
        let store = store.clone();
        let concurrent_holders = concurrent_holders.clone();
        let max_observed = max_observed.clone();
        handles.push(tokio::spawn(async move {
            let backend = S3LockBackend::new(
                store,
                &S3LockConfig {
                    ttl_secs: 9,
                    max_retries: 50,
                    retry_delay_ms: 10,
                    max_hold_secs: 300,
                    ..Default::default()
                },
            )
            .unwrap();

            for iter in 0..2 {
                let guard = backend.acquire(&keys).await.unwrap_or_else(|e| {
                    panic!("Task {task_id} iter {iter} failed to acquire: {e}")
                });

                let prev = concurrent_holders.fetch_add(1, Ordering::SeqCst);
                max_observed.fetch_max(prev + 1, Ordering::SeqCst);
                assert_eq!(
                    prev,
                    0,
                    "Task {task_id} iter {iter}: mutual exclusion violated ({} concurrent holders)",
                    prev + 1
                );

                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                concurrent_holders.fetch_sub(1, Ordering::SeqCst);
                guard.release().await;
            }
        }));
    }

    let results = futures_util::future::join_all(handles).await;
    for (i, result) in results.into_iter().enumerate() {
        result.unwrap_or_else(|e| panic!("Task {i} panicked: {e}"));
    }

    assert_eq!(
        max_observed.load(Ordering::SeqCst),
        1,
        "At most 1 task should hold the lock at any time"
    );
}
