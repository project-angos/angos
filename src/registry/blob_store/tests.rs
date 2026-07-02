use std::io::Cursor;

use chrono::{Duration, Utc};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::*;
use crate::oci::{Algorithm, Digest, Namespace};

pub async fn test_datastore_list_uploads(store: &BlobStore) {
    let namespace = &Namespace::new("test-repo").unwrap();

    let upload_ids = ["upload1", "upload2", "upload3"];
    for id in upload_ids {
        store.create_upload(namespace, id).await.unwrap();

        let content = format!("Content for upload {id}").into_bytes();
        let len = content.len() as u64;
        store
            .write_upload(
                namespace,
                id,
                Box::new(Cursor::new(content)),
                Some(len),
                Algorithm::Sha256,
            )
            .await
            .unwrap();
    }

    let (uploads, _token) = store.list_uploads(namespace, 10, None).await.unwrap();
    assert_eq!(uploads.len(), upload_ids.len());
    for id in upload_ids {
        assert!(uploads.contains(&id.to_string()));
    }

    let (page1, token1) = store.list_uploads(namespace, 2, None).await.unwrap();
    assert_eq!(page1.len(), 2);
    assert!(token1.is_some());

    let (page2, token2) = store.list_uploads(namespace, 2, token1).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert!(token2.is_none());

    let (page1, token1) = store.list_uploads(namespace, 1, None).await.unwrap();
    assert_eq!(page1.len(), 1);
    assert!(token1.is_some());

    let (page2, token2) = store.list_uploads(namespace, 1, token1).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert!(token2.is_some());

    let (page3, token3) = store.list_uploads(namespace, 1, token2).await.unwrap();
    assert_eq!(page3.len(), 1);
    assert!(token3.is_none());

    let upload_to_complete = upload_ids[0];
    let completed_digest =
        Digest::sha256_of_bytes(format!("Content for upload {upload_to_complete}").as_bytes());
    store
        .complete_upload(namespace, upload_to_complete, &completed_digest)
        .await
        .unwrap();

    let (uploads_after_complete, _) = store.list_uploads(namespace, 10, None).await.unwrap();
    assert_eq!(uploads_after_complete.len(), upload_ids.len() - 1);
    assert!(!uploads_after_complete.contains(&upload_to_complete.to_string()));
}

/// Seed the backend with `content` at the canonical blob path for `algorithm`
/// by driving the upload workflow (`create_upload` → `write_upload` →
/// `complete_upload`). Mirrors how production creates blobs.
async fn seed_blob_with(store: &BlobStore, content: &[u8], algorithm: Algorithm) -> Digest {
    let namespace = Namespace::new("test/setup").unwrap();
    let uuid = Uuid::new_v4().to_string();
    store.create_upload(&namespace, &uuid).await.unwrap();
    let len = content.len() as u64;
    store
        .write_upload(
            &namespace,
            &uuid,
            Box::new(Cursor::new(content.to_vec())),
            Some(len),
            algorithm,
        )
        .await
        .unwrap();
    let expected = Digest::from_bytes(algorithm, content);
    store
        .complete_upload(&namespace, &uuid, &expected)
        .await
        .unwrap()
}

async fn seed_blob(store: &BlobStore, content: &[u8]) -> Digest {
    seed_blob_with(store, content, Algorithm::Sha256).await
}

pub async fn test_datastore_blob_operations(store: &BlobStore) {
    let test_content = b"Test blob content";
    let digest = seed_blob(store, test_content).await;

    let retrieved_content = store.read(&digest).await.unwrap();
    assert_eq!(retrieved_content, test_content);

    let size = store.size(&digest).await.unwrap();
    assert_eq!(size, test_content.len() as u64);

    let (mut reader, _) = store.reader(&digest, None).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, test_content);
}

pub async fn test_build_blob_reader_returns_size(store: &BlobStore) {
    let test_content = b"blob reader size test content";
    let digest = seed_blob(store, test_content).await;

    let (mut reader, size) = store.reader(&digest, None).await.unwrap();
    assert_eq!(size, test_content.len() as u64);

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, test_content);
}

#[allow(clippy::cast_possible_truncation)]
pub async fn test_build_blob_reader_with_offset_returns_full_size(store: &BlobStore) {
    let test_content = b"offset blob reader content here";
    let digest = seed_blob(store, test_content).await;
    let offset = 10u64;

    let (mut reader, size) = store.reader(&digest, Some(offset)).await.unwrap();
    assert_eq!(size, test_content.len() as u64);

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, &test_content[offset as usize..]);
}

pub async fn test_datastore_upload_operations(store: &BlobStore) {
    let namespace = &Namespace::new("test-namespace").unwrap();
    let uuid = Uuid::new_v4().to_string();

    let upload_id = store.create_upload(namespace, &uuid).await.unwrap();
    assert_eq!(upload_id, uuid);

    let test_content = b"Test upload content";

    let expected_digest = Digest::sha256_of_bytes(test_content);

    store
        .write_upload(
            namespace,
            &uuid,
            Box::new(Cursor::new(test_content.to_vec())),
            Some(test_content.len() as u64),
            Algorithm::Sha256,
        )
        .await
        .unwrap();

    let summary = store.upload_summary(namespace, &uuid).await.unwrap();
    assert_eq!(summary.size, test_content.len() as u64);
    assert!(Utc::now().signed_duration_since(summary.started_at) < Duration::hours(1));

    let final_digest = store
        .complete_upload(namespace, &uuid, &expected_digest)
        .await
        .unwrap();
    assert_eq!(final_digest, expected_digest);

    let blob_content = store.read(&final_digest).await.unwrap();
    assert_eq!(blob_content, test_content);

    let upload_result = store.upload_summary(namespace, &uuid).await;
    assert!(upload_result.is_err());
}

// Test entry points: run each helper against every backend fixture

use crate::registry::test_utils::backends;

#[tokio::test]
async fn list_uploads() {
    for tc in backends() {
        test_datastore_list_uploads(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}

#[tokio::test]
async fn blob_operations() {
    for tc in backends() {
        test_datastore_blob_operations(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}

#[tokio::test]
async fn blob_reader_returns_size() {
    for tc in backends() {
        test_build_blob_reader_returns_size(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}

#[tokio::test]
async fn blob_reader_with_offset_returns_full_size() {
    for tc in backends() {
        test_build_blob_reader_with_offset_returns_full_size(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}

#[tokio::test]
async fn upload_operations() {
    for tc in backends() {
        test_datastore_upload_operations(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}
