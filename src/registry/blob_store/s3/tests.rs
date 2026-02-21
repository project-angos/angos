use std::io::Cursor;

use sha2::{Digest as ShaDigest, Sha256};
use uuid::Uuid;

use crate::registry::{
    blob_store::{
        BlobStore,
        sha256_ext::Sha256Ext,
        tests::{
            test_build_blob_reader_returns_size,
            test_build_blob_reader_with_offset_returns_full_size, test_datastore_blob_operations,
            test_datastore_list_blobs, test_datastore_list_uploads,
            test_datastore_upload_operations,
        },
    },
    path_builder,
    tests::S3RegistryTestCase,
};

#[tokio::test]
async fn test_list_uploads() {
    let t = S3RegistryTestCase::new();
    test_datastore_list_uploads(t.blob_store()).await;
}

#[tokio::test]
async fn test_list_blobs() {
    let t = S3RegistryTestCase::new();
    test_datastore_list_blobs(t.blob_store()).await;
}

#[tokio::test]
async fn test_blob_operations() {
    let t = S3RegistryTestCase::new();
    test_datastore_blob_operations(t.blob_store()).await;
}

#[tokio::test]
async fn test_upload_operations() {
    let t = S3RegistryTestCase::new();
    test_datastore_upload_operations(t.blob_store()).await;
}

#[tokio::test]
async fn test_blob_reader_returns_size() {
    let t = S3RegistryTestCase::new();
    test_build_blob_reader_returns_size(t.blob_store()).await;
}

#[tokio::test]
async fn test_blob_reader_with_offset_returns_full_size() {
    let t = S3RegistryTestCase::new();
    test_build_blob_reader_with_offset_returns_full_size(t.blob_store()).await;
}

/// Tests multipart upload with staged chunks and S3 parts produces correct digest
#[tokio::test]
async fn test_multipart_upload_digest() {
    let t = S3RegistryTestCase::new();
    let store = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create_upload("ns", &uuid).await.unwrap();

    // 2MB + 4MB + 6MB = 12MB across 3 PATCH requests
    let chunks: Vec<Vec<u8>> = vec![
        vec![0x41; 2 * 1024 * 1024],
        vec![0x42; 4 * 1024 * 1024],
        vec![0x43; 6 * 1024 * 1024],
    ];

    let mut expected = Sha256::new();
    for chunk in &chunks {
        expected.update(chunk);
        store
            .write_upload("ns", &uuid, Box::new(Cursor::new(chunk.clone())), true)
            .await
            .unwrap();
    }

    let digest = store.complete_upload("ns", &uuid, None).await.unwrap();
    assert_eq!(digest, expected.digest());
}

#[tokio::test]
async fn test_delete_prefix_removes_all_objects() {
    let t = S3RegistryTestCase::new();
    let store = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create_upload("ns", &uuid).await.unwrap();

    let data = vec![0xAB; 1024];
    store
        .write_upload("ns", &uuid, Box::new(Cursor::new(data)), true)
        .await
        .unwrap();

    let summary = store.read_upload_summary("ns", &uuid).await;
    assert!(summary.is_ok(), "Upload should exist before deletion");

    store.delete_upload("ns", &uuid).await.unwrap();

    let summary_after = store.read_upload_summary("ns", &uuid).await;
    assert!(
        summary_after.is_err(),
        "Upload should not exist after delete_upload"
    );
}

#[tokio::test]
async fn test_delete_blob_removes_all_data() {
    let t = S3RegistryTestCase::new();
    let store = t.blob_store();

    let content = b"blob content for delete test";
    let digest = store.create_blob(content).await.unwrap();

    let read_result = store.read_blob(&digest).await;
    assert!(read_result.is_ok(), "Blob should exist after creation");
    assert_eq!(read_result.unwrap(), content);

    store.delete_blob(&digest).await.unwrap();

    let read_after = store.read_blob(&digest).await;
    assert!(
        read_after.is_err(),
        "Blob should not exist after delete_blob"
    );
}

#[tokio::test]
async fn test_delete_upload_cleans_all_artifacts() {
    let t = S3RegistryTestCase::new();
    let store = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create_upload("ns", &uuid).await.unwrap();

    let data = vec![0xCD; 2048];
    store
        .write_upload("ns", &uuid, Box::new(Cursor::new(data)), true)
        .await
        .unwrap();

    let container_prefix = path_builder::upload_container_path("ns", &uuid);
    let (objects_before, _) = store
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        !objects_before.is_empty(),
        "Upload container should have objects before deletion"
    );

    store.delete_upload("ns", &uuid).await.unwrap();

    let (objects_after, _) = store
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        objects_after.is_empty(),
        "All objects under upload container should be removed after delete_upload"
    );
}

#[tokio::test]
async fn test_complete_upload_cleans_upload_container() {
    let t = S3RegistryTestCase::new();
    let store = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create_upload("ns", &uuid).await.unwrap();

    let data = vec![0xEF; 4096];
    store
        .write_upload("ns", &uuid, Box::new(Cursor::new(data)), true)
        .await
        .unwrap();

    let digest = store.complete_upload("ns", &uuid, None).await.unwrap();

    let blob_data = store.read_blob(&digest).await.unwrap();
    assert_eq!(
        blob_data.len(),
        4096,
        "Blob should contain the uploaded data"
    );

    let container_prefix = path_builder::upload_container_path("ns", &uuid);
    let (objects_after, _) = store
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        objects_after.is_empty(),
        "Upload container should be cleaned up after complete_upload"
    );
}
