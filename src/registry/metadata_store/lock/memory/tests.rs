use crate::{
    metrics_provider,
    registry::metadata_store::lock::{LockBackend, MemoryBackend},
};

#[tokio::test]
async fn test_acquire_lock() {
    metrics_provider::init_for_tests();
    let memory_backend = MemoryBackend::new();

    let lock = memory_backend.acquire(&["test".to_string()]).await.unwrap();
    assert_eq!(memory_backend.get_lock_count().await, 1);

    drop(lock);
    assert_eq!(memory_backend.get_lock_count().await, 0);
}

#[tokio::test]
async fn test_acquire_multiple_locks() {
    metrics_provider::init_for_tests();
    let memory_backend = MemoryBackend::new();

    let lock = memory_backend
        .acquire(&["key1".to_string(), "key2".to_string(), "key3".to_string()])
        .await
        .unwrap();
    assert_eq!(memory_backend.get_lock_count().await, 3);

    drop(lock);
    assert_eq!(memory_backend.get_lock_count().await, 0);
}

#[tokio::test]
async fn test_acquire_empty_keys() {
    metrics_provider::init_for_tests();
    let memory_backend = MemoryBackend::new();

    let lock = memory_backend.acquire(&[]).await.unwrap();
    assert_eq!(memory_backend.get_lock_count().await, 0);

    drop(lock);
}
