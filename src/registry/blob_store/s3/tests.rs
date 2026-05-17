use std::io::Cursor;

use bytesize::ByteSize;
use chrono::Duration;
use sha2::{Digest as ShaDigest, Sha256};
use uuid::Uuid;

use super::{
    UploadedPart,
    multipart_helpers::{next_part_number, uploaded_size},
};
use crate::{
    registry::{
        blob_store::{
            self, MultipartCleanup, OrphanMultipartUpload,
            sha256_ext::Sha256Ext,
            tests::{
                test_build_blob_reader_returns_size,
                test_build_blob_reader_with_offset_returns_full_size,
                test_datastore_blob_operations, test_datastore_list_blobs,
                test_datastore_list_uploads, test_datastore_upload_operations,
            },
        },
        data_store, path_builder,
        test_utils::S3RegistryTestCase,
    },
    secret::Secret,
};

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
        UploadedPart {
            part_number: 1,
            e_tag: "first".to_string(),
            size: 5,
        },
        UploadedPart {
            part_number: 2,
            e_tag: "second".to_string(),
            size: 8,
        },
    ];
    assert_eq!(uploaded_size(&parts), 13);
}

struct UniformTestCase {
    key_prefix: String,
    store: blob_store::s3::Backend,
}

impl UniformTestCase {
    fn new() -> Self {
        let key_prefix = format!("test-uniform-{}", Uuid::new_v4());
        let store = blob_store::s3::Backend::new(&data_store::s3::BackendConfig {
            access_key_id: Secret::new("root".to_string()),
            secret_key: Secret::new("roottoor".to_string()),
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
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::BlobStore;

    let t = S3RegistryTestCase::new();
    let store: &dyn BlobStore = t.blob_store();

    let content = b"blob content for delete test";
    let digest = store.create(content).await.unwrap();

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
    use crate::registry::blob_store::UploadStore;

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
    let (objects_before, _) = backend
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        !objects_before.is_empty(),
        "Upload container should have objects before deletion"
    );

    upload.delete("ns", &uuid).await.unwrap();

    let (objects_after, _) = backend
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        objects_after.is_empty(),
        "All objects under upload container should be removed after delete"
    );
    t.cleanup().await;
}

#[tokio::test]
async fn test_complete_upload_cleans_upload_container() {
    use crate::registry::blob_store::{BlobStore, UploadStore};

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
    let (objects_after, _) = backend
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        objects_after.is_empty(),
        "Upload container should be cleaned up after complete"
    );
    t.cleanup().await;
}

/// Uniform-mode single-part upload: data smaller than the 5 MiB part size
#[tokio::test]
async fn test_uniform_single_part_upload() {
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::UploadStore;

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
    use crate::registry::blob_store::UploadStore;

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
    let (objects_after, _) = t
        .store
        .store
        .list_objects(&container_prefix, 1000, None)
        .await
        .unwrap();
    assert!(
        objects_after.is_empty(),
        "Upload container should be empty after complete in uniform mode"
    );
    t.cleanup().await;
}

/// Uniform-mode round-trip: uploaded bytes are faithfully preserved through `read`
#[tokio::test]
async fn test_uniform_round_trip_integrity() {
    use crate::registry::blob_store::{BlobStore, UploadStore};

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

/// Creates a raw S3 multipart upload without a `startedat` marker, making it a
/// genuine orphan from the registry's perspective.  With a zero timeout every
/// upload satisfies `age >= 0`, so listing must return it and aborting must
/// remove it.
#[tokio::test]
async fn test_cleanup_orphan_multipart_uploads_aborts_old_uploads() {
    let t = S3RegistryTestCase::new();
    let backend = t.blob_store();

    // Build a key that parses correctly as an upload path so the orphan check
    // proceeds past `parse_upload_key` — but deliberately omit the `startedat`
    // object so the registry treats it as a leaked upload.
    let uuid = Uuid::new_v4().to_string();
    let upload_key = path_builder::upload_path("ns", &uuid);

    backend
        .store
        .create_multipart_upload(&upload_key)
        .await
        .unwrap();

    // Verify the upload is visible before cleanup.
    let (uploads_before, _, _) = backend
        .store
        .list_multipart_uploads(None, None, None)
        .await
        .unwrap();
    assert!(
        uploads_before.iter().any(|u| u.key == upload_key),
        "orphan upload should appear in list before cleanup"
    );

    // A zero timeout means every upload, no matter how fresh, satisfies `age >= 0`.
    let orphans: Vec<OrphanMultipartUpload> = backend
        .list_orphan_multipart_uploads(Duration::zero())
        .await
        .unwrap();
    assert!(
        !orphans.is_empty(),
        "at least the orphan upload must appear in the listing"
    );

    for orphan in &orphans {
        backend.abort_orphan_multipart_upload(orphan).await.unwrap();
    }

    // The upload must no longer appear in the listing.
    let (uploads_after, _, _) = backend
        .store
        .list_multipart_uploads(None, None, None)
        .await
        .unwrap();
    assert!(
        !uploads_after.iter().any(|u| u.key == upload_key),
        "orphan upload must be gone from the list after abort"
    );
    t.cleanup().await;
}

/// Listing orphans is pure observation — it must not modify state.  Not calling
/// abort after listing is the structural equivalent of the old dry-run mode.
#[tokio::test]
async fn test_list_orphan_multipart_uploads_does_not_modify_state() {
    let t = S3RegistryTestCase::new();
    let backend = t.blob_store();

    let uuid = Uuid::new_v4().to_string();
    let upload_key = path_builder::upload_path("ns-dry", &uuid);

    backend
        .store
        .create_multipart_upload(&upload_key)
        .await
        .unwrap();

    // List with zero timeout: the just-created upload must appear as an orphan.
    let orphans = backend
        .list_orphan_multipart_uploads(Duration::zero())
        .await
        .unwrap();
    assert!(
        orphans.iter().any(|o| o.key == upload_key),
        "orphan upload must appear in list"
    );

    // Re-list the raw multipart uploads: listing must not have modified state.
    let (uploads_after, _, _) = backend
        .store
        .list_multipart_uploads(None, None, None)
        .await
        .unwrap();
    assert!(
        uploads_after.iter().any(|u| u.key == upload_key),
        "listing orphans must not abort the upload; it must still appear in the listing"
    );

    // Clean up the lingering multipart upload so MinIO stays tidy.
    let (pending, _, _) = backend
        .store
        .list_multipart_uploads(None, None, None)
        .await
        .unwrap();
    for upload in pending.into_iter().filter(|u| u.key == upload_key) {
        let _ = backend
            .store
            .abort_multipart_upload(&upload.key, &upload.upload_id)
            .await;
    }
    t.cleanup().await;
}
