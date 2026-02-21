use tokio::fs;

use crate::registry::{
    blob_store::tests::{
        test_build_blob_reader_returns_size, test_build_blob_reader_with_offset_returns_full_size,
        test_datastore_blob_operations, test_datastore_list_blobs, test_datastore_list_uploads,
        test_datastore_upload_operations,
    },
    tests::FSRegistryTestCase,
};

#[tokio::test]
async fn test_write_and_read_file() {
    let t = FSRegistryTestCase::new();
    let backend = t.blob_store();

    let test_path = "test_file.txt";
    let test_content = b"Hello, world!";

    backend.store.write(test_path, test_content).await.unwrap();

    let full_path = t.temp_dir().path().join(test_path);
    assert!(full_path.exists());

    let content = fs::read(&full_path).await.unwrap();
    assert_eq!(content, test_content);

    let test_string = "Hello world!";
    backend
        .store
        .write(test_path, test_string.as_bytes())
        .await
        .unwrap();
    let string_content = fs::read_to_string(&full_path).await.unwrap();
    assert_eq!(string_content, test_string);
}

#[tokio::test]
async fn test_delete_empty_parent_dirs() {
    let t = FSRegistryTestCase::new();
    let backend = t.blob_store();

    let nested_path = "a/b/c/d";
    let test_file_path = "a/b/c/d/test.txt";

    backend.store.write(test_file_path, b"test").await.unwrap();

    backend.store.delete(test_file_path).await.unwrap();

    backend.store.delete_dir(nested_path).await.unwrap();
    backend
        .store
        .delete_empty_parent_dirs(nested_path)
        .await
        .unwrap();

    let full_path = t.temp_dir().path().join(nested_path);
    assert!(!full_path.exists());
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
