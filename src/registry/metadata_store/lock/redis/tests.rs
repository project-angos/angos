use crate::registry::metadata_store::lock::{LockBackend, RedisBackend, redis::LockConfig};

#[tokio::test]
async fn test_acquire_lock() {
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
