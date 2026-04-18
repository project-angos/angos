use std::io::Cursor;

use bytesize::ByteSize;
use sha2::{Digest as ShaDigest, Sha256};
use uuid::Uuid;

use crate::registry::{
    blob_store::{
        self, BlobStore,
        sha256_ext::Sha256Ext,
        tests::{
            test_build_blob_reader_returns_size,
            test_build_blob_reader_with_offset_returns_full_size, test_datastore_blob_operations,
            test_datastore_list_blobs, test_datastore_list_uploads,
            test_datastore_upload_operations,
        },
    },
    data_store, path_builder,
    tests::S3RegistryTestCase,
};

struct UniformTestCase {
    key_prefix: String,
    store: blob_store::s3::Backend,
}

impl UniformTestCase {
    fn new() -> Self {
        let key_prefix = format!("test-uniform-{}", Uuid::new_v4());
        let store = blob_store::s3::Backend::new(&data_store::s3::BackendConfig {
            access_key_id: "root".to_string(),
            secret_key: "roottoor".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: "region".to_string(),
            bucket: "registry".to_string(),
            key_prefix: key_prefix.clone(),
            multipart_part_size: ByteSize::mib(5),
            multipart_uniform_parts: true,
            ..Default::default()
        })
        .unwrap();
        Self { key_prefix, store }
    }
}

impl Drop for UniformTestCase {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let key_prefix = self.key_prefix.clone();
            let data_store = self.store.store.clone();
            handle.spawn(async move {
                if let Err(e) = data_store.delete_prefix(&key_prefix).await {
                    println!("Warning: Failed to clean up UniformTestCase data: {e:?}");
                }
            });
        }
    }
}

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
            .write_upload(
                "ns",
                &uuid,
                Box::new(Cursor::new(chunk.clone())),
                chunk.len() as u64,
                true,
                None,
            )
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
        .write_upload(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
            None,
        )
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
        .write_upload(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
            None,
        )
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
        .write_upload(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
            None,
        )
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

/// Uniform-mode single-part upload: data smaller than the 5 MiB part size
#[tokio::test]
async fn test_uniform_single_part_upload() {
    let t = UniformTestCase::new();
    let store = &t.store;
    let uuid = Uuid::new_v4().to_string();

    let data = vec![0xAA; 1024 * 1024]; // 1 MiB

    let mut expected = Sha256::new();
    expected.update(&data);

    store.create_upload("ns", &uuid).await.unwrap();
    store
        .write_upload(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
            None,
        )
        .await
        .unwrap();

    let digest = store.complete_upload("ns", &uuid, None).await.unwrap();
    assert_eq!(digest, expected.digest());
}

/// Uniform-mode multi-part upload: 3 chunks totalling 12 MiB, requiring multiple S3 parts
#[tokio::test]
async fn test_uniform_multi_part_upload() {
    let t = UniformTestCase::new();
    let store = &t.store;
    let uuid = Uuid::new_v4().to_string();

    // 2 MiB + 4 MiB + 6 MiB = 12 MiB across 3 PATCH requests
    let chunks: Vec<Vec<u8>> = vec![
        vec![0x41; 2 * 1024 * 1024],
        vec![0x42; 4 * 1024 * 1024],
        vec![0x43; 6 * 1024 * 1024],
    ];

    let mut expected = Sha256::new();
    for chunk in &chunks {
        expected.update(chunk);
    }

    store.create_upload("ns", &uuid).await.unwrap();
    for chunk in &chunks {
        store
            .write_upload(
                "ns",
                &uuid,
                Box::new(Cursor::new(chunk.clone())),
                chunk.len() as u64,
                true,
                None,
            )
            .await
            .unwrap();
    }

    let digest = store.complete_upload("ns", &uuid, None).await.unwrap();
    assert_eq!(digest, expected.digest());
}

/// Uniform-mode `complete_upload` removes all staging artifacts from the upload container
#[tokio::test]
async fn test_uniform_complete_cleans_artifacts() {
    let t = UniformTestCase::new();
    let store = &t.store;
    let uuid = Uuid::new_v4().to_string();

    let data = vec![0xBC; 1024]; // well below part size

    store.create_upload("ns", &uuid).await.unwrap();
    store
        .write_upload(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
            None,
        )
        .await
        .unwrap();

    store.complete_upload("ns", &uuid, None).await.unwrap();

    let container_prefix = path_builder::upload_container_path("ns", &uuid);
    let (objects_after, _) = store
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        objects_after.is_empty(),
        "Upload container should be empty after complete_upload in uniform mode"
    );
}

/// Uniform-mode round-trip: uploaded bytes are faithfully preserved through `read_blob`
#[tokio::test]
async fn test_uniform_round_trip_integrity() {
    let t = UniformTestCase::new();
    let store = &t.store;
    let uuid = Uuid::new_v4().to_string();

    // Use two chunks that together exceed 5 MiB so at least one S3 part is flushed,
    // while the tail goes through the staging path — exercises both code branches.
    let chunk_a = vec![0x55; 6 * 1024 * 1024]; // 6 MiB  → flushed as a full part
    let chunk_b = vec![0x66; 512 * 1024]; //       512 KiB → staged as trailing chunk

    let mut expected_content = Vec::with_capacity(chunk_a.len() + chunk_b.len());
    expected_content.extend_from_slice(&chunk_a);
    expected_content.extend_from_slice(&chunk_b);

    store.create_upload("ns", &uuid).await.unwrap();
    for chunk in [&chunk_a, &chunk_b] {
        store
            .write_upload(
                "ns",
                &uuid,
                Box::new(Cursor::new(chunk.clone())),
                chunk.len() as u64,
                true,
                None,
            )
            .await
            .unwrap();
    }

    let digest = store.complete_upload("ns", &uuid, None).await.unwrap();
    let blob_data = store.read_blob(&digest).await.unwrap();
    assert_eq!(
        blob_data, expected_content,
        "Round-tripped blob content must exactly match the original upload"
    );
}
