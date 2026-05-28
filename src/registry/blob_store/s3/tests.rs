use std::io::Cursor;
use std::sync::Arc;

use bytesize::ByteSize;
use sha2::{Digest as ShaDigest, Sha256};
use uuid::Uuid;

use crate::{
    registry::{
        blob_store::{
            self, BlobStore, UploadStore,
            s3::multipart_helpers::{next_part_number, uploaded_size},
            sha256_ext::Sha256Ext,
            tests::{
                test_build_blob_reader_returns_size,
                test_build_blob_reader_with_offset_returns_full_size,
                test_datastore_blob_operations, test_datastore_list_blobs,
                test_datastore_list_uploads, test_datastore_upload_operations,
            },
        },
        path_builder,
        s3_connection::S3ConnectionConfig,
        test_utils::{S3RegistryTestCase, put_blob_direct},
    },
    secret::Secret,
};
use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{ConditionalStore, Etag, ObjectStore, Part, s3::Backend as StorageS3Backend};
use angos_tx_engine::{executor::build_executor, lock::LockStrategy};

#[test]
fn first_part_when_no_parts_uploaded() {
    assert_eq!(next_part_number(0).unwrap(), 1);
}

#[test]
fn second_part_after_one_uploaded() {
    assert_eq!(next_part_number(1).unwrap(), 2);
}

#[test]
fn part_number_increments_with_part_count() {
    assert_eq!(next_part_number(9).unwrap(), 10);
}

#[test]
fn uploaded_size_sums_completed_parts() {
    let parts = vec![
        Part {
            part_number: 1,
            etag: Etag::new("first"),
            size: 5,
        },
        Part {
            part_number: 2,
            etag: Etag::new("second"),
            size: 8,
        },
    ];
    assert_eq!(uploaded_size(&parts), 13);
}

/// `[blob_store.s3]` round-trip: flat TOML deserialises into both the
/// embedded `S3ConnectionConfig` and the `TransportFields` knobs.
#[test]
fn s3_backend_config_toml_round_trip() {
    let toml = r#"
        access_key_id             = "blob-key"
        secret_key                = "blob-secret"
        endpoint                  = "https://blob.s3.example.com"
        bucket                    = "blob-bucket"
        region                    = "us-west-2"
        key_prefix                = "_blobs"
        multipart_part_size       = "50 MiB"
        multipart_uniform_parts   = true
        multipart_copy_threshold  = "5 GiB"
        multipart_copy_chunk_size = "100 MiB"
        multipart_copy_jobs       = 8
    "#;

    let cfg: blob_store::s3::BackendConfig = toml::from_str(toml).expect("deserialize");
    assert_eq!(cfg.connection.access_key_id.expose(), "blob-key");
    assert_eq!(cfg.connection.secret_key.expose(), "blob-secret");
    assert_eq!(cfg.connection.endpoint, "https://blob.s3.example.com");
    assert_eq!(cfg.connection.bucket, "blob-bucket");
    assert_eq!(cfg.connection.region, "us-west-2");
    assert_eq!(cfg.connection.key_prefix, "_blobs");
    assert!(cfg.transport.multipart_uniform_parts);
    assert_eq!(cfg.transport.multipart_copy_jobs, 8);
}

/// Regression for the previous behaviour where `[blob_store.s3]` silently
/// defaulted `region` (and other connection fields) when missing. The
/// documented schema lists every connection field as required.
#[test]
fn s3_backend_config_requires_region() {
    let toml = r#"
        access_key_id = "k"
        secret_key    = "s"
        endpoint      = "http://localhost:9000"
        bucket        = "b"
    "#;
    let err =
        toml::from_str::<blob_store::s3::BackendConfig>(toml).expect_err("region must be required");
    assert!(
        err.to_string().contains("region"),
        "error should mention the missing `region` field, got: {err}"
    );
}

struct UniformTestCase {
    key_prefix: String,
    store: blob_store::s3::Backend,
}

impl UniformTestCase {
    fn new() -> Self {
        let key_prefix = format!("test-uniform-{}", Uuid::new_v4());
        let config = blob_store::s3::BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("root".to_string()),
                secret_key: Secret::new("roottoor".to_string()),
                endpoint: "http://127.0.0.1:9000".to_string(),
                region: "region".to_string(),
                bucket: "registry".to_string(),
                key_prefix: key_prefix.clone(),
            },
            transport: blob_store::s3::TransportFields {
                multipart_part_size: ByteSize::mib(5),
                multipart_uniform_parts: true,
                ..blob_store::s3::TransportFields::default()
            },
        };
        let http = Arc::new(
            S3HttpBackend::new(&config.connection.to_client_config()).expect("s3 http client"),
        );
        let raw_storage = Arc::new(
            StorageS3Backend::builder()
                .client(http)
                .build()
                .expect("s3 storage"),
        );
        let object_store: std::sync::Arc<dyn ObjectStore> = raw_storage.clone();
        let conditional: std::sync::Arc<dyn ConditionalStore> = raw_storage;
        let executor = build_executor(
            object_store,
            Some(conditional),
            LockStrategy::Memory,
            None,
            false,
            false,
        )
        .expect("s3 executor");
        let store = blob_store::s3::Backend::builder()
            .access_key_id(config.connection.access_key_id.clone())
            .secret_key(config.connection.secret_key.clone())
            .endpoint(&config.connection.endpoint)
            .bucket(&config.connection.bucket)
            .region(&config.connection.region)
            .key_prefix(&config.connection.key_prefix)
            .multipart_copy_threshold(config.transport.multipart_copy_threshold)
            .multipart_copy_chunk_size(config.transport.multipart_copy_chunk_size)
            .multipart_copy_jobs(config.transport.multipart_copy_jobs)
            .multipart_part_size(config.transport.multipart_part_size)
            .multipart_uniform_parts(config.transport.multipart_uniform_parts)
            .operation_timeout_secs(config.transport.operation_timeout_secs)
            .operation_attempt_timeout_secs(config.transport.operation_attempt_timeout_secs)
            .max_attempts(config.transport.max_attempts)
            .executor(executor)
            .build()
            .unwrap();
        Self { key_prefix, store }
    }

    async fn cleanup(&self) {
        if let Err(e) = self.store.store.delete_prefix(&self.key_prefix).await {
            println!("Warning: Failed to clean up UniformTestCase data: {e:?}");
        }
    }
}

#[tokio::test]
async fn test_list_uploads() {
    let t = S3RegistryTestCase::new();
    test_datastore_list_uploads(t.blob_store()).await;
    t.cleanup().await;
}

#[tokio::test]
async fn test_list_blobs() {
    let t = S3RegistryTestCase::new();
    test_datastore_list_blobs(t.blob_store()).await;
    t.cleanup().await;
}

#[tokio::test]
async fn test_blob_operations() {
    let t = S3RegistryTestCase::new();
    test_datastore_blob_operations(t.blob_store()).await;
    t.cleanup().await;
}

#[tokio::test]
async fn test_upload_operations() {
    let t = S3RegistryTestCase::new();
    test_datastore_upload_operations(t.blob_store()).await;
    t.cleanup().await;
}

#[tokio::test]
async fn test_blob_reader_returns_size() {
    let t = S3RegistryTestCase::new();
    test_build_blob_reader_returns_size(t.blob_store()).await;
    t.cleanup().await;
}

#[tokio::test]
async fn test_blob_reader_with_offset_returns_full_size() {
    let t = S3RegistryTestCase::new();
    test_build_blob_reader_with_offset_returns_full_size(t.blob_store()).await;
    t.cleanup().await;
}

/// Tests multipart upload with staged chunks and S3 parts produces correct digest
#[tokio::test]
async fn test_multipart_upload_digest() {
    let t = S3RegistryTestCase::new();
    let store: &dyn UploadStore = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create("ns", &uuid).await.unwrap();

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
            .write(
                "ns",
                &uuid,
                Box::new(Cursor::new(chunk.clone())),
                chunk.len() as u64,
                true,
            )
            .await
            .unwrap();
    }

    let digest = store.complete("ns", &uuid, None).await.unwrap();
    assert_eq!(digest, expected.digest());
    t.cleanup().await;
}

#[tokio::test]
async fn test_zero_length_nonuniform_write_keeps_digest_and_size() {
    let t = S3RegistryTestCase::new();
    let store: &dyn UploadStore = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create("ns", &uuid).await.unwrap();

    let data = vec![0xA5; 6 * 1024 * 1024];
    let (digest, size) = store
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data)),
            6 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

    let summary = store.summary("ns", &uuid).await.unwrap();
    assert_eq!(summary.size, size);

    let (same_digest, same_size) = store
        .write("ns", &uuid, Box::new(Cursor::new(Vec::new())), 0, true)
        .await
        .unwrap();

    assert_eq!(same_digest, digest);
    assert_eq!(same_size, size);

    let completed = store.complete("ns", &uuid, Some(&digest)).await.unwrap();
    assert_eq!(completed, digest);
    t.cleanup().await;
}

#[tokio::test]
async fn test_zero_length_uniform_write_keeps_digest_and_size() {
    let t = UniformTestCase::new();
    let store: &dyn UploadStore = &t.store;
    let uuid = Uuid::new_v4().to_string();

    store.create("ns", &uuid).await.unwrap();

    let data = vec![0xA5; 6 * 1024 * 1024];
    let (digest, size) = store
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data)),
            6 * 1024 * 1024,
            true,
        )
        .await
        .unwrap();

    let summary = store.summary("ns", &uuid).await.unwrap();
    assert_eq!(summary.size, size);

    let (same_digest, same_size) = store
        .write("ns", &uuid, Box::new(Cursor::new(Vec::new())), 0, true)
        .await
        .unwrap();

    assert_eq!(same_digest, digest);
    assert_eq!(same_size, size);

    let completed = store.complete("ns", &uuid, Some(&digest)).await.unwrap();
    assert_eq!(completed, digest);
    t.cleanup().await;
}

#[tokio::test]
async fn test_nonuniform_write_rejects_short_body() {
    let t = S3RegistryTestCase::new();
    let store: &dyn UploadStore = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create("ns", &uuid).await.unwrap();

    let result = store
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(vec![0xA5; 1024])),
            6 * 1024 * 1024,
            true,
        )
        .await;

    assert!(matches!(
        result,
        Err(blob_store::Error::UploadBodySize {
            expected: 6_291_456,
            actual: 1024,
        })
    ));

    let _ = store.delete("ns", &uuid).await;
    t.cleanup().await;
}

#[tokio::test]
async fn test_delete_prefix_removes_all_objects() {
    let t = S3RegistryTestCase::new();
    let store: &dyn UploadStore = t.blob_store();
    let uuid = Uuid::new_v4().to_string();

    store.create("ns", &uuid).await.unwrap();

    let data = vec![0xAB; 1024];
    store
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
        )
        .await
        .unwrap();

    let summary = store.summary("ns", &uuid).await;
    assert!(summary.is_ok(), "Upload should exist before deletion");

    store.delete("ns", &uuid).await.unwrap();

    let summary_after = store.summary("ns", &uuid).await;
    assert!(
        summary_after.is_err(),
        "Upload should not exist after delete"
    );
    t.cleanup().await;
}

#[tokio::test]
async fn test_delete_blob_removes_all_data() {
    let t = S3RegistryTestCase::new();
    let backend = t.blob_store();
    let store: &dyn BlobStore = backend;

    let content = b"blob content for delete test";
    let digest = put_blob_direct(&backend.store, content).await;

    let read_result = store.read(&digest).await;
    assert!(read_result.is_ok(), "Blob should exist after creation");
    assert_eq!(read_result.unwrap(), content);

    store.delete(&digest).await.unwrap();

    let read_after = store.read(&digest).await;
    assert!(read_after.is_err(), "Blob should not exist after delete");
    t.cleanup().await;
}

#[tokio::test]
async fn test_delete_upload_cleans_all_artifacts() {
    let t = S3RegistryTestCase::new();
    let backend = t.blob_store();
    let upload: &dyn UploadStore = backend;
    let uuid = Uuid::new_v4().to_string();

    upload.create("ns", &uuid).await.unwrap();

    let data = vec![0xCD; 2048];
    upload
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
        )
        .await
        .unwrap();

    let container_prefix = path_builder::upload_container_path("ns", &uuid);
    let objects_before = backend
        .store
        .list(&container_prefix, 1000, None)
        .await
        .unwrap()
        .items;
    assert!(
        !objects_before.is_empty(),
        "Upload container should have objects before deletion"
    );

    upload.delete("ns", &uuid).await.unwrap();

    let objects_after = backend
        .store
        .list(&container_prefix, 1000, None)
        .await
        .unwrap()
        .items;
    assert!(
        objects_after.is_empty(),
        "All objects under upload container should be removed after delete"
    );
    t.cleanup().await;
}

#[tokio::test]
async fn test_complete_upload_cleans_upload_container() {
    let t = S3RegistryTestCase::new();
    let backend = t.blob_store();
    let blob: &dyn BlobStore = backend;
    let upload: &dyn UploadStore = backend;
    let uuid = Uuid::new_v4().to_string();

    upload.create("ns", &uuid).await.unwrap();

    let data = vec![0xEF; 4096];
    upload
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
        )
        .await
        .unwrap();

    let digest = upload.complete("ns", &uuid, None).await.unwrap();

    let blob_data = blob.read(&digest).await.unwrap();
    assert_eq!(
        blob_data.len(),
        4096,
        "Blob should contain the uploaded data"
    );

    let container_prefix = path_builder::upload_container_path("ns", &uuid);
    let objects_after = backend
        .store
        .list(&container_prefix, 1000, None)
        .await
        .unwrap()
        .items;
    assert!(
        objects_after.is_empty(),
        "Upload container should be cleaned up after complete"
    );
    t.cleanup().await;
}

/// Uniform-mode single-part upload: data smaller than the 5 MiB part size
#[tokio::test]
async fn test_uniform_single_part_upload() {
    let t = UniformTestCase::new();
    let store: &dyn UploadStore = &t.store;
    let uuid = Uuid::new_v4().to_string();

    let data = vec![0xAA; 1024 * 1024]; // 1 MiB

    let mut expected = Sha256::new();
    expected.update(&data);

    store.create("ns", &uuid).await.unwrap();
    store
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
        )
        .await
        .unwrap();

    let digest = store.complete("ns", &uuid, None).await.unwrap();
    assert_eq!(digest, expected.digest());
    t.cleanup().await;
}

/// Uniform-mode multi-part upload: 3 chunks totalling 12 MiB, requiring multiple S3 parts
#[tokio::test]
async fn test_uniform_multi_part_upload() {
    let t = UniformTestCase::new();
    let store: &dyn UploadStore = &t.store;
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

    store.create("ns", &uuid).await.unwrap();
    for chunk in &chunks {
        store
            .write(
                "ns",
                &uuid,
                Box::new(Cursor::new(chunk.clone())),
                chunk.len() as u64,
                true,
            )
            .await
            .unwrap();
    }

    let digest = store.complete("ns", &uuid, None).await.unwrap();
    assert_eq!(digest, expected.digest());
    t.cleanup().await;
}

/// Uniform-mode `complete` removes all staging artifacts from the upload container
#[tokio::test]
async fn test_uniform_complete_cleans_artifacts() {
    let t = UniformTestCase::new();
    let upload: &dyn UploadStore = &t.store;
    let uuid = Uuid::new_v4().to_string();

    let data = vec![0xBC; 1024]; // well below part size

    upload.create("ns", &uuid).await.unwrap();
    upload
        .write(
            "ns",
            &uuid,
            Box::new(Cursor::new(data.clone())),
            data.len() as u64,
            true,
        )
        .await
        .unwrap();

    upload.complete("ns", &uuid, None).await.unwrap();

    let container_prefix = path_builder::upload_container_path("ns", &uuid);
    let objects_after = t
        .store
        .store
        .list(&container_prefix, 1000, None)
        .await
        .unwrap()
        .items;
    assert!(
        objects_after.is_empty(),
        "Upload container should be empty after complete in uniform mode"
    );
    t.cleanup().await;
}

/// Uniform-mode round-trip: uploaded bytes are faithfully preserved through `read`
#[tokio::test]
async fn test_uniform_round_trip_integrity() {
    let t = UniformTestCase::new();
    let blob: &dyn BlobStore = &t.store;
    let upload: &dyn UploadStore = &t.store;
    let uuid = Uuid::new_v4().to_string();

    // Use two chunks that together exceed 5 MiB so at least one S3 part is flushed,
    // while the tail goes through the staging path — exercises both code branches.
    let chunk_a = vec![0x55; 6 * 1024 * 1024]; // 6 MiB  → flushed as a full part
    let chunk_b = vec![0x66; 512 * 1024]; //       512 KiB → staged as trailing chunk

    let mut expected_content = Vec::with_capacity(chunk_a.len() + chunk_b.len());
    expected_content.extend_from_slice(&chunk_a);
    expected_content.extend_from_slice(&chunk_b);

    upload.create("ns", &uuid).await.unwrap();
    for chunk in [&chunk_a, &chunk_b] {
        upload
            .write(
                "ns",
                &uuid,
                Box::new(Cursor::new(chunk.clone())),
                chunk.len() as u64,
                true,
            )
            .await
            .unwrap();
    }

    let digest = upload.complete("ns", &uuid, None).await.unwrap();
    let blob_data = blob.read(&digest).await.unwrap();
    assert_eq!(
        blob_data, expected_content,
        "Round-tripped blob content must exactly match the original upload"
    );
    t.cleanup().await;
}
