use std::{io, path::Path};

use tokio::fs;

use super::classify_open_error;
use crate::registry::{
    blob_store::{
        Error,
        tests::{
            test_build_blob_reader_returns_size,
            test_build_blob_reader_with_offset_returns_full_size, test_datastore_blob_operations,
            test_datastore_list_blobs, test_datastore_list_uploads,
            test_datastore_upload_operations,
        },
    },
    fs_ops::{
        atomic_write, prune_empty_ancestors, remove_dir_all_if_exists, remove_file_if_exists,
    },
    test_utils::FSRegistryTestCase,
};

#[tokio::test]
async fn test_write_and_read_file() {
    let t = FSRegistryTestCase::new();

    let test_path = t.temp_dir().path().join("test_file.txt");
    atomic_write(&test_path, b"Hello, world!", false)
        .await
        .unwrap();

    let content = fs::read(&test_path).await.unwrap();
    assert_eq!(content, b"Hello, world!");

    atomic_write(&test_path, b"Hello world!", false)
        .await
        .unwrap();
    let string_content = fs::read_to_string(&test_path).await.unwrap();
    assert_eq!(string_content, "Hello world!");
}

#[tokio::test]
async fn test_prune_empty_ancestors() {
    let t = FSRegistryTestCase::new();
    let root = t.temp_dir().path().to_path_buf();

    let nested_dir = root.join("a/b/c/d");
    let test_file = nested_dir.join("test.txt");

    atomic_write(&test_file, b"test", false).await.unwrap();

    remove_file_if_exists(&test_file).await.unwrap();
    remove_dir_all_if_exists(&nested_dir).await.unwrap();
    prune_empty_ancestors(&nested_dir, &root, 4).await.unwrap();

    assert!(!root.join("a/b/c").exists());
    assert!(!root.join("a/b").exists());
    assert!(!root.join("a").exists());
}

#[tokio::test]
async fn test_prune_empty_ancestors_respects_max_levels() {
    let t = FSRegistryTestCase::new();
    let root = t.temp_dir().path().to_path_buf();

    let nested_dir = root.join("a/b/c/d");
    let test_file = nested_dir.join("test.txt");

    atomic_write(&test_file, b"test", false).await.unwrap();
    remove_file_if_exists(&test_file).await.unwrap();
    remove_dir_all_if_exists(&nested_dir).await.unwrap();

    // Only ascend one level: `c` gets removed, `b` survives.
    prune_empty_ancestors(&nested_dir, &root, 1).await.unwrap();
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

#[test]
fn classify_open_error_maps_not_found_to_upload_not_found() {
    let err = io::Error::from(io::ErrorKind::NotFound);
    let result = classify_open_error(err, Path::new("/some/upload/path"));
    assert!(matches!(result, Error::UploadNotFound));
}

#[test]
fn classify_open_error_maps_other_kinds_via_from_impl() {
    let err = io::Error::from(io::ErrorKind::PermissionDenied);
    let result = classify_open_error(err, Path::new("/some/upload/path"));
    assert!(matches!(result, Error::StorageBackend(_)));
}
