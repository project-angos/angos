use std::io::Cursor;

use sha2::{Digest as ShaDigest, Sha256};
use uuid::Uuid;

use crate::registry::blob_store::BlobStore;
use crate::registry::blob_store::sha256_ext::Sha256Ext;
use crate::registry::blob_store::tests::{
    test_build_blob_reader_returns_size, test_build_blob_reader_with_offset_returns_full_size,
    test_datastore_blob_operations, test_datastore_list_blobs, test_datastore_list_uploads,
    test_datastore_upload_operations,
};
use crate::registry::tests::S3RegistryTestCase;

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
