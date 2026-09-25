use std::io::Cursor;

use chrono::{Duration, Utc};
use futures_util::TryStreamExt;
use tokio::io::AsyncReadExt;

use angos_oci::{Algorithm, Digest, Namespace, UploadSessionId};
use angos_storage::test_util::frame;

use crate::registry::{
    Error,
    blob_store::{upload_session::HashStart, *},
    keys::{DigestKeys, NamespaceKeys},
    test_utils::{FSRegistryTestCase, RegistryTestCase, for_each_backend},
};

/// Seed `content` at its canonical blob path by driving the whole upload
/// workflow, as production does.
async fn seed_blob_with(store: &BlobStore, content: &[u8], algorithm: Algorithm) -> Digest {
    let namespace = Namespace::new("test/setup").unwrap();
    let session_id = UploadSessionId::generate();
    store
        .create_upload(&namespace, &session_id, None)
        .await
        .unwrap();
    let len = content.len() as u64;
    store
        .write_upload(
            &namespace,
            &session_id,
            Box::new(Cursor::new(content.to_vec())),
            Some(len),
            HashStart::Fresh(algorithm),
            algorithm,
        )
        .await
        .unwrap();
    let expected = Digest::from_bytes(algorithm, content);
    store
        .complete_upload(&namespace, &session_id, &expected, len)
        .await
        .unwrap()
}

async fn seed_blob(store: &BlobStore, content: &[u8]) -> Digest {
    seed_blob_with(store, content, Algorithm::Sha256).await
}

#[tokio::test]
async fn stream_uploads() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let namespace = &Namespace::new("test-repo").unwrap();

        let upload_ids: Vec<UploadSessionId> =
            (0..3).map(|_| UploadSessionId::generate()).collect();
        for id in &upload_ids {
            store.create_upload(namespace, id, None).await.unwrap();

            let content = format!("Content for upload {id}").into_bytes();
            let len = content.len() as u64;
            store
                .write_upload(
                    namespace,
                    id,
                    Box::new(Cursor::new(content)),
                    Some(len),
                    HashStart::Fresh(Algorithm::Sha256),
                    Algorithm::Sha256,
                )
                .await
                .unwrap();
        }

        let uploads: Vec<UploadSessionId> =
            store.stream_uploads(namespace).try_collect().await.unwrap();
        assert_eq!(uploads.len(), upload_ids.len());
        for id in &upload_ids {
            assert!(uploads.contains(id));
        }

        let upload_to_complete = &upload_ids[0];
        let completed_digest =
            Digest::sha256_of_bytes(format!("Content for upload {upload_to_complete}").as_bytes());
        store
            .complete_upload(
                namespace,
                upload_to_complete,
                &completed_digest,
                format!("Content for upload {upload_to_complete}").len() as u64,
            )
            .await
            .unwrap();

        let uploads_after_complete: Vec<UploadSessionId> =
            store.stream_uploads(namespace).try_collect().await.unwrap();
        assert_eq!(uploads_after_complete.len(), upload_ids.len() - 1);
        assert!(!uploads_after_complete.contains(upload_to_complete));
    })
    .await;
}

/// A directory naming no session is scrub's to quarantine, so the sweep that
/// reaps live sessions must not report it as one.
#[tokio::test]
async fn stream_uploads_skips_a_non_session_name() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let namespace = &Namespace::new("test-raw-upload").unwrap();
        let session = UploadSessionId::generate();
        store
            .create_upload(namespace, &session, None)
            .await
            .unwrap();

        let stray = "v2/repositories/test-raw-upload/_uploads/not-a-session/startedat";
        store
            .object_store()
            .put(stray, Bytes::from_static(b"2026-01-01T00:00:00Z"))
            .await
            .unwrap();

        let uploads: Vec<UploadSessionId> =
            store.stream_uploads(namespace).try_collect().await.unwrap();
        assert_eq!(
            uploads,
            vec![session],
            "only the opened session may be reported"
        );
    })
    .await;
}

/// Blobs of the two algorithms live under separate prefixes; the walk must
/// cross the boundary and surface each exactly once.
#[tokio::test]
async fn stream_blobs_across_algorithms() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let mut expected = Vec::new();
        for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
            for content in [b"alpha".as_slice(), b"beta".as_slice()] {
                expected.push(seed_blob_with(store, content, algorithm).await);
            }
        }

        let walked: Vec<Digest> = store.stream_blobs().try_collect().await.unwrap();
        assert_eq!(walked.len(), expected.len());
        for digest in &expected {
            assert!(walked.contains(digest), "missed {digest}");
        }
    })
    .await;
}

#[tokio::test]
async fn blob_operations() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
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
    })
    .await;
}

#[tokio::test]
async fn blob_reader_returns_size() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let test_content = b"blob reader size test content";
        let digest = seed_blob(store, test_content).await;

        let (mut reader, size) = store.reader(&digest, None).await.unwrap();
        assert_eq!(size, test_content.len() as u64);

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, test_content);
    })
    .await;
}

#[tokio::test]
async fn blob_reader_with_offset_returns_full_size() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let test_content = b"offset blob reader content here";
        let digest = seed_blob(store, test_content).await;
        let offset = 10u64;

        let (mut reader, size) = store.reader(&digest, Some(offset)).await.unwrap();
        assert_eq!(size, test_content.len() as u64);

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        let offset = usize::try_from(offset).unwrap();
        assert_eq!(buffer, &test_content[offset..]);
    })
    .await;
}

#[tokio::test]
async fn upload_operations() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let namespace = &Namespace::new("test-namespace").unwrap();
        let session_id = UploadSessionId::generate();

        store
            .create_upload(namespace, &session_id, None)
            .await
            .unwrap();

        let test_content = b"Test upload content";

        let expected_digest = Digest::sha256_of_bytes(test_content);

        store
            .write_upload(
                namespace,
                &session_id,
                Box::new(Cursor::new(test_content.to_vec())),
                Some(test_content.len() as u64),
                HashStart::Fresh(Algorithm::Sha256),
                Algorithm::Sha256,
            )
            .await
            .unwrap();

        let summary = store.upload_summary(namespace, &session_id).await.unwrap();
        assert_eq!(summary.size, test_content.len() as u64);
        assert!(Utc::now().signed_duration_since(summary.started_at) < Duration::hours(1));

        let final_digest = store
            .complete_upload(
                namespace,
                &session_id,
                &expected_digest,
                test_content.len() as u64,
            )
            .await
            .unwrap();
        assert_eq!(final_digest, expected_digest);

        let blob_content = store.read(&final_digest).await.unwrap();
        assert_eq!(blob_content, test_content);

        let upload_result = store.upload_summary(namespace, &session_id).await;
        assert!(upload_result.is_err());
    })
    .await;
}

/// Two independent uploads of the same bytes both complete, the second
/// promoting onto the already-present path, and both sessions are swept.
#[tokio::test]
async fn repeated_promotion_converges() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let content = b"idempotent promotion content";
        let first = seed_blob(store, content).await;
        let second = seed_blob(store, content).await;

        assert_eq!(first, second, "identical content must map to one blob");
        assert_eq!(store.read(&first).await.unwrap(), content);

        let namespace = Namespace::new("test/setup").unwrap();
        let uploads: Vec<UploadSessionId> = store
            .stream_uploads(&namespace)
            .try_collect()
            .await
            .unwrap();
        assert!(
            uploads.is_empty(),
            "promoted sessions must be swept: {uploads:?}"
        );
    })
    .await;
}

/// The session marker must be consumed before the multipart-complete: a naive
/// S3 re-finalize would overwrite the promoted blob with an empty object.
#[tokio::test]
async fn complete_upload_fails_on_rerun() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let namespace = Namespace::new("test/rerun").unwrap();
        let session_id = UploadSessionId::generate();
        let content = b"one-shot completion";
        store
            .create_upload(&namespace, &session_id, None)
            .await
            .unwrap();
        store
            .write_upload(
                &namespace,
                &session_id,
                Box::new(Cursor::new(content.to_vec())),
                Some(content.len() as u64),
                HashStart::Fresh(Algorithm::Sha256),
                Algorithm::Sha256,
            )
            .await
            .unwrap();
        let digest = Digest::sha256_of_bytes(content);
        store
            .complete_upload(&namespace, &session_id, &digest, content.len() as u64)
            .await
            .unwrap();

        let rerun = store
            .complete_upload(&namespace, &session_id, &digest, content.len() as u64)
            .await;
        assert!(
            matches!(rerun, Err(Error::BlobUploadUnknown)),
            "re-run of a completed session must fail: {rerun:?}"
        );
        assert_eq!(
            store.read(&digest).await.unwrap(),
            content,
            "blob must stay intact after a rejected re-run"
        );
    })
    .await;
}

/// FS-only: the assertions are about exact keys under one prefix, which both
/// backends agree on, so this stays independent of a live S3.
/// A chunked upload keeps its whole state in one `session.json`: no legacy
/// keys appear, the checkpoint resumes across chunks, and completion leaves
/// nothing behind.
#[tokio::test]
async fn session_state_is_one_json_record() {
    let tc = FSRegistryTestCase::new();
    let store = tc.blob_store();
    let store = store.as_ref();
    let namespace = &Namespace::new("session-single-record").unwrap();
    let session_id = &UploadSessionId::generate();
    store
        .create_upload(namespace, session_id, None)
        .await
        .unwrap();

    for chunk in ["one", "two", "three", "four"] {
        store
            .append_upload(
                namespace,
                session_id,
                Box::new(Cursor::new(chunk.as_bytes().to_vec())),
                Some(chunk.len() as u64),
            )
            .await
            .unwrap();
    }

    let container = format!("{}/", namespace.upload_container_path(session_id));
    let keys = store
        .object
        .list(&container, 100, None)
        .await
        .unwrap()
        .items;
    assert_eq!(
        keys.iter().filter(|k| k.contains("session.json")).count(),
        1,
        "exactly one session record expected, got: {keys:?}"
    );
    // The whole body's digest verifying at completion is what proves the
    // checkpoint resumed across chunks.
    let content = b"onetwothreefour";
    let digest = Digest::sha256_of_bytes(content);
    store
        .complete_upload(namespace, session_id, &digest, content.len() as u64)
        .await
        .unwrap();
    assert_eq!(store.read(&digest).await.unwrap(), content);
    let leftover = store
        .object
        .list(&container, 100, None)
        .await
        .unwrap()
        .items;
    assert!(
        leftover.is_empty(),
        "completion must leave no session keys: {leftover:?}"
    );
}

#[tokio::test]
async fn a_truncated_blob_reads_as_unknown() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let digest = Digest::sha256_of_bytes(b"blob content");
        store
            .object_store()
            .put(&digest.blob_path(), Bytes::new())
            .await
            .unwrap();

        assert!(matches!(store.size(&digest).await, Err(Error::BlobUnknown)));
        assert!(matches!(store.read(&digest).await, Err(Error::BlobUnknown)));
        assert!(matches!(
            store.reader(&digest, None).await,
            Err(Error::BlobUnknown)
        ));
    })
    .await;
}

#[tokio::test]
async fn the_empty_blob_reads_as_present() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let digest = seed_blob(&store, b"").await;

        assert_eq!(store.size(&digest).await.unwrap(), 0);
        assert!(store.read(&digest).await.unwrap().is_empty());
    })
    .await;
}

/// An append that fails after durably writing bytes leaves the staging object
/// longer than the checkpoint records, and the resume that follows hashes only
/// its own bytes, so the digest matches while the stored bytes do not.
#[tokio::test]
async fn complete_upload_rejects_size_divergence() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let tail = b"orphaned tail".to_vec();
        let namespace = Namespace::new("test/divergence").unwrap();
        let session_id = UploadSessionId::generate();
        let content = b"hashed prefix";
        store
            .create_upload(&namespace, &session_id, None)
            .await
            .unwrap();
        store
            .write_upload(
                &namespace,
                &session_id,
                Box::new(Cursor::new(content.to_vec())),
                Some(content.len() as u64),
                HashStart::Fresh(Algorithm::Sha256),
                Algorithm::Sha256,
            )
            .await
            .unwrap();

        // A tail the session never hashed.
        let upload_key = namespace.upload_path(&session_id);
        store
            .object
            .write_upload(&upload_key, frame(tail.clone()), Some(tail.len() as u64))
            .await
            .unwrap();

        let digest = Digest::sha256_of_bytes(content);
        let result = store
            .complete_upload(&namespace, &session_id, &digest, content.len() as u64)
            .await;

        assert!(
            matches!(result, Err(Error::DigestInvalid)),
            "a staged object longer than the hashed size must not be promoted: {result:?}"
        );
        assert!(
            matches!(store.read(&digest).await, Err(Error::BlobUnknown)),
            "the diverged bytes must never reach the canonical blob path"
        );
    })
    .await;
}

/// A write that durably stores bytes it never checkpoints leaves the staging
/// object longer than the session hashed. The next append must catch the
/// surplus and fail the session closed, never carry it to the promoting PUT.
#[tokio::test]
async fn append_fails_closed_on_size_divergence() {
    for_each_backend(async |tc| {
        let store = tc.blob_store();
        let store = store.as_ref();
        let namespace = &Namespace::new("test/append-divergence").unwrap();
        let session_id = &UploadSessionId::generate();
        store
            .create_upload(namespace, session_id, None)
            .await
            .unwrap();
        store
            .append_upload(
                namespace,
                session_id,
                Box::new(Cursor::new(b"hashed prefix".to_vec())),
                Some(13),
            )
            .await
            .unwrap();

        // A tail the session never hashed, planted straight into the staging object.
        let upload_key = namespace.upload_path(session_id);
        store
            .object
            .write_upload(&upload_key, frame(b"orphaned tail".to_vec()), Some(13))
            .await
            .unwrap();

        let outcome = store
            .append_upload(
                namespace,
                session_id,
                Box::new(Cursor::new(b"more".to_vec())),
                Some(4),
            )
            .await
            .map(|_| ());
        assert!(
            matches!(outcome, Err(Error::DigestInvalid)),
            "an append over an orphaned tail must fail closed: {outcome:?}"
        );
        assert!(
            matches!(
                store.upload_summary(namespace, session_id).await,
                Err(Error::BlobUploadUnknown)
            ),
            "the failed session must be gone"
        );
    })
    .await;
}
