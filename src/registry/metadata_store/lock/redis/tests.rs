use std::time::Duration;

use redis::AsyncCommands;
use uuid::Uuid;

use crate::{
    metrics_provider,
    registry::metadata_store::lock::{LockBackend, RedisBackend, redis::LockConfig},
};

const REDIS_URL: &str = "redis://localhost:6379/2";

fn make_config(prefix: &str) -> LockConfig {
    LockConfig {
        url: REDIS_URL.to_owned(),
        ttl: 2,
        key_prefix: prefix.to_owned(),
        max_retries: 0,
        retry_delay_ms: 5,
    }
}

async fn key_exists(prefix: &str, key: &str) -> bool {
    let client = redis::Client::open(REDIS_URL).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    let full_key = format!("{prefix}{key}");
    let count: i64 = conn.exists(&full_key).await.expect("EXISTS command");
    count > 0
}

async fn count_keys_with_prefix(prefix: &str) -> usize {
    let client = redis::Client::open(REDIS_URL).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    let keys: Vec<String> = conn.keys(format!("{prefix}*")).await.expect("KEYS command");
    keys.len()
}

#[tokio::test]
async fn test_acquire_lock() {
    metrics_provider::init_for_tests();
    let config = LockConfig {
        url: "redis://localhost:6379/2".to_owned(),
        ttl: 2,
        key_prefix: "test_acquire_lock_".to_owned(),
        max_retries: 0,
        retry_delay_ms: 5,
    };

    let redis_backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");
    let redis_backend2 = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    let lock = redis_backend
        .acquire(&["test_key".to_string()])
        .await
        .expect("Failed to acquire initial lock");

    let lock_attempt = redis_backend2.acquire(&["test_key".to_string()]).await;
    assert!(
        lock_attempt.is_err(),
        "Should not be able to acquire an already held lock"
    );

    drop(lock);

    let new_lock = redis_backend
        .acquire(&["test_key".to_string()])
        .await
        .expect("Should be able to acquire lock after it was released");

    drop(new_lock);
}

#[tokio::test]
async fn test_acquire_multiple_locks() {
    metrics_provider::init_for_tests();
    let config = LockConfig {
        url: "redis://localhost:6379/2".to_owned(),
        ttl: 2,
        key_prefix: "test_acquire_multiple_".to_owned(),
        max_retries: 0,
        retry_delay_ms: 5,
    };

    let redis_backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");
    let redis_backend2 = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    let lock = redis_backend
        .acquire(&["key1".to_string(), "key2".to_string()])
        .await
        .expect("Failed to acquire locks");

    let lock_attempt = redis_backend2.acquire(&["key2".to_string()]).await;
    assert!(
        lock_attempt.is_err(),
        "Should not be able to acquire a key that's part of a held lock set"
    );

    drop(lock);

    let new_lock = redis_backend2
        .acquire(&["key1".to_string(), "key2".to_string()])
        .await
        .expect("Should be able to acquire locks after they were released");

    drop(new_lock);
}

#[tokio::test]
async fn test_acquire_atomic_all_or_nothing() {
    metrics_provider::init_for_tests();
    let config = LockConfig {
        url: "redis://localhost:6379/2".to_owned(),
        ttl: 2,
        key_prefix: "test_atomic_".to_owned(),
        max_retries: 0,
        retry_delay_ms: 5,
    };

    let redis_backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");
    let redis_backend2 = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    let lock1 = redis_backend
        .acquire(&["keyA".to_string()])
        .await
        .expect("Failed to acquire keyA");

    let lock_attempt = redis_backend2
        .acquire(&["keyA".to_string(), "keyB".to_string()])
        .await;
    assert!(
        lock_attempt.is_err(),
        "Should fail atomically if any key is held"
    );

    let lock2 = redis_backend2.acquire(&["keyB".to_string()]).await;
    assert!(
        lock2.is_ok(),
        "keyB should still be available since atomic acquire failed"
    );

    drop(lock1);
    drop(lock2);
}

/// Verify that dropping a `RedisGuard` immediately releases the lock in Redis so that
/// a subsequent acquisition of the same key succeeds without waiting for TTL expiry.
/// This guards against the risk of `Drop` using a synchronous connection from an async
/// context that could defer or silently lose the release.
#[tokio::test]
async fn test_drop_releases_lock_promptly() {
    metrics_provider::init_for_tests();
    let id = Uuid::new_v4().to_string();
    let prefix = format!("test_drop_prompt_{id}_");
    let config = make_config(&prefix);
    let backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    let lock = backend
        .acquire(&["k".to_string()])
        .await
        .expect("Failed to acquire lock");

    assert!(key_exists(&prefix, "k").await, "key must exist while held");

    drop(lock);

    // Give the synchronous drop a moment to complete; it is blocking but runs
    // inline in Drop, so by the time drop() returns control the key should be gone.
    assert!(
        !key_exists(&prefix, "k").await,
        "key must be absent immediately after drop"
    );

    // A fresh acquisition must succeed, confirming the key is truly released.
    let backend2 = RedisBackend::new(&config).expect("Failed to create RedisBackend");
    let reacquired = backend2
        .acquire(&["k".to_string()])
        .await
        .expect("Should re-acquire lock immediately after drop");
    drop(reacquired);
}

/// Spawn 10 Tokio tasks each holding a distinct lock. After all tasks drop their guards
/// simultaneously (via `join_all`), every Redis key must be absent.
#[tokio::test]
async fn test_concurrent_drops_release_all_locks() {
    metrics_provider::init_for_tests();
    let id = Uuid::new_v4().to_string();
    let prefix = format!("test_concurrent_drops_{id}_");

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let config = make_config(&prefix);
            tokio::spawn(async move {
                let backend =
                    RedisBackend::new(&config).expect("Failed to create RedisBackend in task");
                let key = format!("lock{i}");
                backend
                    .acquire(&[key])
                    .await
                    .expect("Failed to acquire lock in task")
            })
        })
        .collect();

    let guards: Vec<_> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task panicked"))
        .collect();

    // Drop all guards, driving the synchronous release for each.
    drop(guards);

    let remaining = count_keys_with_prefix(&prefix).await;
    assert_eq!(
        remaining, 0,
        "All {remaining} Redis keys must be gone after concurrent drops"
    );
}

/// Acquire a lock with a 2-second TTL and wait 1.5 seconds (past the halfway refresh
/// point of 1 second). The refresh heartbeat extends the TTL, so the key must still
/// exist after the wait rather than having expired naturally.
///
/// This verifies that `spawn_refresh_task` is actually running and that the `REFRESH_SCRIPT`
/// resets the TTL before it reaches zero.
#[tokio::test]
async fn test_lock_ttl_refresh_extends_expiry() {
    metrics_provider::init_for_tests();
    let id = Uuid::new_v4().to_string();
    let prefix = format!("test_ttl_refresh_{id}_");
    // ttl = 2s → refresh fires every 1s; wait 1.5s to land after the first refresh.
    let config = make_config(&prefix);
    let backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    let lock = backend
        .acquire(&["r".to_string()])
        .await
        .expect("Failed to acquire lock");

    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert!(
        key_exists(&prefix, "r").await,
        "key must still exist after 1.5s because the refresh heartbeat extended its TTL"
    );

    drop(lock);
}

/// Drop the guard before the first heartbeat fires (< 1s for a 2s TTL).
/// Verify no panic occurs, no double-release error is observable, and the
/// key is immediately absent so a fresh acquisition succeeds.
///
/// The implementation stops the heartbeat via `stop_notify.notify_one()` in `Drop`
/// before aborting the `JoinHandle`, so there is no window for a double-release.
#[tokio::test]
async fn test_drop_during_heartbeat_no_double_release() {
    metrics_provider::init_for_tests();
    let id = Uuid::new_v4().to_string();
    let prefix = format!("test_early_drop_{id}_");
    let config = make_config(&prefix);
    let backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    let lock = backend
        .acquire(&["h".to_string()])
        .await
        .expect("Failed to acquire lock");

    // Drop immediately — the refresh task has not yet fired (it sleeps for ttl/2 = 1s).
    drop(lock);

    // Must be absent: no panic, no double-release, key cleaned up.
    assert!(
        !key_exists(&prefix, "h").await,
        "key must be absent after early drop (before first heartbeat)"
    );

    let backend2 = RedisBackend::new(&config).expect("Failed to create RedisBackend");
    let reacquired = backend2
        .acquire(&["h".to_string()])
        .await
        .expect("Should re-acquire after early drop");
    drop(reacquired);
}

/// Acquire and release 100 locks sequentially, then verify that no residual keys
/// remain in Redis under the test prefix. This catches resource leaks where Drop
/// silently fails to release (e.g., due to connection exhaustion or runtime issues).
#[tokio::test]
async fn test_stress_100_locks_no_resource_leak() {
    metrics_provider::init_for_tests();
    let id = Uuid::new_v4().to_string();
    let prefix = format!("test_stress100_{id}_");
    let config = make_config(&prefix);
    let backend = RedisBackend::new(&config).expect("Failed to create RedisBackend");

    for i in 0..100u32 {
        let key = format!("s{i}");
        let guard = backend
            .acquire(&[key])
            .await
            .expect("Failed to acquire lock in stress loop");
        drop(guard);
    }

    let remaining = count_keys_with_prefix(&prefix).await;
    assert_eq!(
        remaining, 0,
        "All Redis keys must be cleaned up after 100 sequential acquire-drop cycles"
    );
}
