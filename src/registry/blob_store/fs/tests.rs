use tokio::fs;

use angos_storage::fs::Backend as StorageFsBackend;

use crate::registry::{
    blob_store::tests::{
        test_build_blob_reader_returns_size, test_build_blob_reader_with_offset_returns_full_size,
        test_datastore_blob_operations, test_datastore_list_blobs, test_datastore_list_uploads,
        test_datastore_upload_operations,
    },
    test_utils::FSRegistryTestCase,
};

#[tokio::test]
async fn test_prune_empty_ancestors() {
    let t = FSRegistryTestCase::new();
    let root = t.temp_dir().path().to_path_buf();

    // Build the backend pointing at root so we can call prune_empty_ancestors
    // through the Backend method.
    let store = StorageFsBackend::builder()
        .root_dir(&root)
        .build()
        .expect("backend must build");

    let nested_dir = root.join("a/b/c/d");
    fs::create_dir_all(&nested_dir).await.unwrap();
    let test_file = nested_dir.join("test.txt");
    fs::File::create(&test_file).await.unwrap();

    fs::remove_file(&test_file).await.unwrap();
    fs::remove_dir_all(&nested_dir).await.unwrap();
    store.prune_empty_ancestors("a/b/c/d", 4).await;

    assert!(!root.join("a/b/c").exists());
    assert!(!root.join("a/b").exists());
    assert!(!root.join("a").exists());
}

#[tokio::test]
async fn test_prune_empty_ancestors_respects_max_levels() {
    let t = FSRegistryTestCase::new();
    let root = t.temp_dir().path().to_path_buf();

    let store = StorageFsBackend::builder()
        .root_dir(&root)
        .build()
        .expect("backend must build");

    let nested_dir = root.join("a/b/c/d");
    fs::create_dir_all(&nested_dir).await.unwrap();
    let test_file = nested_dir.join("test.txt");
    fs::File::create(&test_file).await.unwrap();

    fs::remove_file(&test_file).await.unwrap();
    fs::remove_dir_all(&nested_dir).await.unwrap();

    // Only ascend one level: `c` gets removed, `b` survives.
    store.prune_empty_ancestors("a/b/c/d", 1).await;
    assert!(!root.join("a/b/c").exists());
    assert!(root.join("a/b").exists());
}

#[tokio::test]
async fn test_list_uploads() {
    let t = FSRegistryTestCase::new();
    test_datastore_list_uploads(t.blob_store()).await;
}

#[tokio::test]
async fn test_list_blobs() {
    let t = FSRegistryTestCase::new();
    test_datastore_list_blobs(t.blob_store()).await;
}

#[tokio::test]
async fn test_blob_operations() {
    let t = FSRegistryTestCase::new();
    test_datastore_blob_operations(t.blob_store()).await;
}

#[tokio::test]
async fn test_upload_operations() {
    let t = FSRegistryTestCase::new();
    test_datastore_upload_operations(t.blob_store()).await;
}

#[tokio::test]
async fn test_blob_reader_returns_size() {
    let t = FSRegistryTestCase::new();
    test_build_blob_reader_returns_size(t.blob_store()).await;
}

#[tokio::test]
async fn test_blob_reader_with_offset_returns_full_size() {
    let t = FSRegistryTestCase::new();
    test_build_blob_reader_with_offset_returns_full_size(t.blob_store()).await;
}
