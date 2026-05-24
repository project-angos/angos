use std::{
    collections::HashSet,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::registry::metadata_store::backend::tests::{
    legacy_blob_index_with, put_legacy_index, test_config,
};
use crate::{
    oci::Digest,
    registry::{
        metadata_store::{
            Backend, BlobIndex, BlobIndexOperation, MetadataStore,
            backend::Coordinator,
            link_kind::LinkKind,
            lock::{LockBackend, MemoryBackend},
            lock_ops::blob_index_lock_key,
        },
        path_builder,
    },
};
use angos_s3_client as s3_client;
use angos_storage::s3::Backend as StorageS3Backend;

#[tokio::test]
async fn test_lock_coordinator_blob_index_update_uses_blob_lock() {
    let config = test_config();
    // Build a backend that shares a single in-memory lock with the test so
    // we can hold the blob-index lock externally and observe that the
    // backend's blob-index update blocks until we release.
    let shared_lock = Arc::new(MemoryBackend::new());

    let http = s3_client::Backend::new(&config.to_data_store_config()).unwrap();
    let storage = Arc::new(
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap(),
    );
    let coordinator = Arc::new(Coordinator::Locked {
        store: storage,
        lock: shared_lock.clone(),
    });
    let backend = Backend::builder()
        .coordinator(coordinator)
        .link_cache_ttl(0)
        .access_time_debounce_secs(0)
        .build()
        .unwrap();

    let digest =
        Digest::from_str("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
            .unwrap();
    let namespace = "locked-blob-index";

    // Acquire the blob-index lock externally on the same backend instance.
    let lock_key = blob_index_lock_key(&digest);
    let guard = shared_lock.acquire(&[lock_key]).await.unwrap();

    let task_backend = backend.clone();
    let task_digest = digest.clone();
    let task_link = LinkKind::Blob(digest.clone());
    let handle = tokio::spawn(async move {
        task_backend
            .update_blob_index(
                namespace,
                &task_digest,
                BlobIndexOperation::Insert(task_link),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !handle.is_finished(),
        "blob index update must wait for the blob lock"
    );

    guard.release().await;
    handle.await.unwrap().unwrap();

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_migrate_blob_index_layout_holds_blob_index_lock() {
    let config = test_config();
    let shared_lock = Arc::new(MemoryBackend::new());

    let http = s3_client::Backend::new(&config.to_data_store_config()).unwrap();
    let storage = Arc::new(
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap(),
    );
    let coordinator = Arc::new(Coordinator::Locked {
        store: storage,
        lock: shared_lock.clone(),
    });
    let backend = Backend::builder()
        .coordinator(coordinator)
        .link_cache_ttl(0)
        .access_time_debounce_secs(0)
        .build()
        .unwrap();

    let digest =
        Digest::from_str("sha256:3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a")
            .unwrap();
    let namespace = "migrate-lock-ns";
    let link = LinkKind::Tag("m1".into());

    let legacy = legacy_blob_index_with(vec![(namespace, vec![link.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    // Hold the blob-index lock externally so the migration is forced to wait.
    let lock_key = blob_index_lock_key(&digest);
    let guard = shared_lock.acquire(&[lock_key]).await.unwrap();

    let task_backend = backend.clone();
    let task_digest = digest.clone();
    let handle = tokio::spawn(async move { task_backend.migrate_blob_index(&task_digest).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !handle.is_finished(),
        "migrate_blob_index must wait for the blob-index lock"
    );

    guard.release().await;
    handle.await.unwrap().unwrap();

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_blob_index_cas_legacy_holds_blob_index_lock_when_legacy_present() {
    let config = test_config();
    let shared_lock = Arc::new(MemoryBackend::new());

    let http = s3_client::Backend::new(&config.to_data_store_config()).unwrap();
    let storage = Arc::new(
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap(),
    );
    let coordinator = Arc::new(Coordinator::Cas {
        store: storage,
        lock: shared_lock.clone(),
    });
    let backend = Backend::builder()
        .coordinator(coordinator)
        .link_cache_ttl(0)
        .access_time_debounce_secs(0)
        .build()
        .unwrap();

    let digest =
        Digest::from_str("sha256:4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b")
            .unwrap();
    let namespace = "cas-legacy-lock-ns";
    let seed_link = LinkKind::Tag("seed".into());
    let new_link = LinkKind::Tag("new".into());

    let legacy = legacy_blob_index_with(vec![(namespace, vec![seed_link.clone()])]);
    put_legacy_index(&backend, &digest, &legacy).await;

    // Hold the blob-index lock externally so the CAS-mode legacy update is
    // forced to wait (HEAD short-circuit must NOT skip the lock because the
    // legacy file is present).
    let lock_key = blob_index_lock_key(&digest);
    let guard = shared_lock.acquire(&[lock_key]).await.unwrap();

    let task_backend = backend.clone();
    let task_digest = digest.clone();
    let task_link = new_link.clone();
    let handle = tokio::spawn(async move {
        task_backend
            .update_blob_index(
                namespace,
                &task_digest,
                BlobIndexOperation::Insert(task_link),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !handle.is_finished(),
        "cas-mode legacy update must wait for the blob-index lock"
    );

    guard.release().await;
    handle.await.unwrap().unwrap();

    // Verify the legacy file picked up the new link.
    let raw = backend
        .store()
        .get(&path_builder::blob_index_path(&digest))
        .await
        .unwrap();
    let stored: BlobIndex = serde_json::from_slice(&raw).unwrap();
    let links = stored
        .namespace
        .get(namespace)
        .expect("namespace must still be present in legacy file");
    assert!(
        links.contains(&seed_link),
        "seed link must remain after cas-legacy update"
    );
    assert!(
        links.contains(&new_link),
        "new link must be present in legacy file after cas-legacy update"
    );

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_blob_index_cas_no_legacy_skips_blob_index_lock() {
    let config = test_config();
    let shared_lock = Arc::new(MemoryBackend::new());

    let http = s3_client::Backend::new(&config.to_data_store_config()).unwrap();
    let storage = Arc::new(
        StorageS3Backend::builder()
            .client(Arc::new(http))
            .build()
            .unwrap(),
    );
    let coordinator = Arc::new(Coordinator::Cas {
        store: storage,
        lock: shared_lock.clone(),
    });
    let backend = Backend::builder()
        .coordinator(coordinator)
        .link_cache_ttl(0)
        .access_time_debounce_secs(0)
        .build()
        .unwrap();

    let digest =
        Digest::from_str("sha256:5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c")
            .unwrap();
    let namespace = "cas-no-legacy-ns";
    let new_link = LinkKind::Tag("only".into());

    // Hold the blob-index lock externally so the lock is unavailable. The HEAD
    // short-circuit must skip the lock entirely when no legacy file exists.
    let lock_key = blob_index_lock_key(&digest);
    let _guard = shared_lock.acquire(&[lock_key]).await.unwrap();

    let start = Instant::now();
    backend
        .update_blob_index(
            namespace,
            &digest,
            BlobIndexOperation::Insert(new_link.clone()),
        )
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "cas-mode update with no legacy file must not block on the blob-index lock"
    );

    let raw = backend
        .store()
        .get(&path_builder::blob_index_shard_path(&digest, namespace))
        .await
        .unwrap();
    let links: HashSet<LinkKind> = serde_json::from_slice(&raw).unwrap();
    assert!(
        links.contains(&new_link),
        "shard file must contain the inserted link"
    );

    backend
        .store()
        .delete_prefix(&config.key_prefix)
        .await
        .unwrap();
}
