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

/// Drain `list_blobs` one page at a time until the token is `None`, collecting
/// every blob surfaced.
async fn drain_blobs(store: &BlobStore, page_size: u16) -> Vec<Digest> {
    let mut walked = Vec::new();
    let mut marker = None;
    loop {
        let (page, next) = store.list_blobs(page_size, marker).await.unwrap();
        assert!(page.len() <= usize::from(page_size));
        walked.extend(page);
        match next {
            Some(next_marker) => marker = Some(next_marker),
            None => break,
        }
    }
    walked
}

pub async fn test_datastore_list_blobs(store: &BlobStore) {
    let blob_contents = [
        b"aaa_content_1".to_vec(),
        b"bbb_content_2".to_vec(),
        b"ccc_content_3".to_vec(),
    ];

    let mut digests = Vec::new();
    for content in &blob_contents {
        digests.push(seed_blob(store, content).await);
    }

    // A single large page returns every blob with no continuation.
    let (blobs, token) = store.list_blobs(10, None).await.unwrap();
    assert!(token.is_none());
    assert!(blobs.len() >= digests.len());
    for digest in &digests {
        assert!(blobs.contains(digest));
    }

    // Draining one or two at a time surfaces every blob exactly once.
    for page_size in [1, 2] {
        let walked = drain_blobs(store, page_size).await;
        assert_eq!(walked.len(), digests.len());
        for digest in &digests {
            assert!(
                walked.contains(digest),
                "page_size {page_size} missed {digest}"
            );
        }
    }
}

pub async fn test_datastore_list_blobs_across_algorithms(store: &BlobStore) {
    // Blobs of both algorithms live under separate prefixes; the listing must
    // walk across the boundary and surface each exactly once.
    let mut expected = Vec::new();
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        for content in [b"alpha".as_slice(), b"beta".as_slice()] {
            expected.push(seed_blob_with(store, content, algorithm).await);
        }
    }

    let walked = drain_blobs(store, 1).await;
    assert_eq!(walked.len(), expected.len());
    for digest in &expected {
        assert!(walked.contains(digest), "missed {digest}");
    }
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

/// Repeated promotion of identical content converges on one blob. Two
/// independent uploads of the same bytes both complete: the second moves onto
/// the already-present content-addressed path (overwriting identical bytes),
/// yields the same digest with intact content, and both sessions are swept.
/// Covers promotion onto an existing destination plus best-effort session
/// cleanup; single-session crash re-drive is the caller's `size(digest)` gate.
pub async fn test_repeated_promotion_converges(store: &BlobStore) {
    let content = b"idempotent promotion content";
    let first = seed_blob(store, content).await;
    let second = seed_blob(store, content).await;

    assert_eq!(first, second, "identical content must map to one blob");
    assert_eq!(store.read(&first).await.unwrap(), content);

    let namespace = Namespace::new("test/setup").unwrap();
    let (uploads, _) = store.list_uploads(&namespace, 10, None).await.unwrap();
    assert!(
        uploads.is_empty(),
        "promoted sessions must be swept: {uploads:?}"
    );
}

/// `complete_upload` consumes the session's liveness marker, so a second call on
/// the same session returns `UploadNotFound` and leaves the promoted blob
/// intact. A naive S3 re-finalize would overwrite the blob with an empty object,
/// so this guards that the marker is consumed before the multipart-complete.
pub async fn test_complete_upload_fails_on_rerun(store: &BlobStore) {
    let namespace = Namespace::new("test/rerun").unwrap();
    let uuid = Uuid::new_v4().to_string();
    let content = b"one-shot completion";
    store.create_upload(&namespace, &uuid).await.unwrap();
    store
        .write_upload(
            &namespace,
            &uuid,
            Box::new(Cursor::new(content.to_vec())),
            Some(content.len() as u64),
            Algorithm::Sha256,
        )
        .await
        .unwrap();
    let digest = Digest::sha256_of_bytes(content);
    store
        .complete_upload(&namespace, &uuid, &digest)
        .await
        .unwrap();

    let rerun = store.complete_upload(&namespace, &uuid, &digest).await;
    assert!(
        matches!(rerun, Err(Error::UploadNotFound)),
        "re-run of a completed session must fail: {rerun:?}"
    );
    assert_eq!(
        store.read(&digest).await.unwrap(),
        content,
        "blob must stay intact after a rejected re-run"
    );
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
async fn list_blobs() {
    for tc in backends() {
        test_datastore_list_blobs(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}

#[tokio::test]
async fn list_blobs_across_algorithms() {
    for tc in backends() {
        test_datastore_list_blobs_across_algorithms(tc.blob_store().as_ref()).await;
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

#[tokio::test]
async fn repeated_promotion_converges() {
    for tc in backends() {
        test_repeated_promotion_converges(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}

#[tokio::test]
async fn complete_upload_fails_on_rerun() {
    for tc in backends() {
        test_complete_upload_fails_on_rerun(tc.blob_store().as_ref()).await;
        tc.cleanup().await;
    }
}
