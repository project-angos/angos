//! Integration tests for the S3 backend. These talk to a live `MinIO`
//! instance at `127.0.0.1:9000` — the same convention the rest of the angos
//! workspace uses for S3 integration tests. `MinIO` is expected to be
//! running; tests fail loudly otherwise.

use std::{sync::Arc, time::Duration};

use angos_s3_client::{Backend as S3Backend, BackendConfig as S3Config};
use bytes::Bytes;
use bytesize::ByteSize;
use futures_util::stream;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{
    ByteStream, ConditionalStore, Error, ObjectStore, PresignedStore, SessionState,
    UploadSessionStore, s3::Backend,
};

fn backend() -> Backend {
    backend_with(false, ByteSize::mib(5).as_u64())
}

fn backend_with(uniform_parts: bool, part_size: u64) -> Backend {
    let config = S3Config {
        access_key_id: "root".to_string(),
        secret_key: "roottoor".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "registry".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: format!("storage-s3-tests/{}", Uuid::new_v4()),
        multipart_copy_threshold: ByteSize::mib(5),
        multipart_copy_chunk_size: ByteSize::mib(5),
        multipart_part_size: ByteSize(part_size),
        ..Default::default()
    };
    let client = Arc::new(S3Backend::new(&config).expect("s3 client"));
    Backend::builder()
        .client(client)
        .part_size(part_size)
        .uniform_parts(uniform_parts)
        .build()
        .expect("backend")
}

fn frame(body: Vec<u8>) -> ByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from(body)) }))
}

#[tokio::test]
async fn builder_requires_client() {
    let err = Backend::builder().build().unwrap_err();
    assert!(matches!(err, Error::Backend(msg) if msg.contains("client")));
}

#[tokio::test]
async fn put_then_get_round_trips() {
    let store = backend();
    store
        .put("rt/key", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(store.get("rt/key").await.unwrap(), b"hello");
}

#[tokio::test]
async fn get_missing_key_returns_not_found() {
    let store = backend();
    let key = format!("missing/{}", Uuid::new_v4());
    assert_eq!(store.get(&key).await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn head_reports_size_and_etag() {
    let store = backend();
    store
        .put("hd/k", Bytes::from_static(b"abcdef"))
        .await
        .unwrap();
    let meta = store.head("hd/k").await.unwrap();
    assert_eq!(meta.size, 6);
    assert!(meta.etag.is_some(), "S3 always surfaces an ETag");
}

#[tokio::test]
async fn delete_prefix_clears_subtree() {
    let store = backend();
    let prefix = format!("dp/{}", Uuid::new_v4());
    for k in ["a", "b/c", "b/d"] {
        store
            .put(&format!("{prefix}/{k}"), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    store.delete_prefix(&prefix).await.unwrap();
    let page = store.list(&prefix, 10, None).await.unwrap();
    assert!(
        page.items.is_empty(),
        "prefix must be empty after delete_prefix"
    );
}

#[tokio::test]
async fn get_stream_reports_total_size_not_remaining() {
    let store = backend();
    store
        .put("st/k", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    let (mut body, total) = store.get_stream("st/k", Some(3)).await.unwrap();
    assert_eq!(total, 10);
    let mut buf = Vec::new();
    body.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"3456789");
}

#[tokio::test]
async fn list_children_separates_sub_prefixes_from_objects() {
    let store = backend();
    let prefix = format!("lc/{}", Uuid::new_v4());
    store
        .put(&format!("{prefix}/a"), Bytes::from_static(b"x"))
        .await
        .unwrap();
    store
        .put(&format!("{prefix}/sub/b"), Bytes::from_static(b"y"))
        .await
        .unwrap();

    let page = store.list_children(&prefix, 100, None, None).await.unwrap();
    assert_eq!(page.sub_prefixes, vec!["sub".to_string()]);
    assert_eq!(page.objects, vec!["a".to_string()]);
}

#[tokio::test]
async fn put_if_absent_rejects_existing_key() {
    let store = backend();
    let key = format!("cas/absent/{}", Uuid::new_v4());
    store
        .put_if_absent(&key, Bytes::from_static(b"first"))
        .await
        .unwrap();
    assert_eq!(
        store
            .put_if_absent(&key, Bytes::from_static(b"second"))
            .await
            .unwrap_err(),
        Error::PreconditionFailed
    );
}

#[tokio::test]
async fn put_if_match_rejects_stale_etag() {
    let store = backend();
    let key = format!("cas/match/{}", Uuid::new_v4());
    let etag = store
        .put_if_absent(&key, Bytes::from_static(b"v1"))
        .await
        .unwrap()
        .expect("S3 returns an ETag on create");
    let second = store
        .put_if_match(&key, &etag, Bytes::from_static(b"v2"))
        .await
        .unwrap()
        .expect("S3 returns an ETag on update");
    assert_ne!(etag, second);
    assert_eq!(
        store
            .put_if_match(&key, &etag, Bytes::from_static(b"v3"))
            .await
            .unwrap_err(),
        Error::PreconditionFailed
    );
}

#[tokio::test]
async fn delete_if_match_succeeds_with_current_etag() {
    let store = backend();
    let key = format!("cas/del/{}", Uuid::new_v4());
    let etag = store
        .put_if_absent(&key, Bytes::from_static(b"v"))
        .await
        .unwrap()
        .expect("S3 returns an ETag on create");
    store.delete_if_match(&key, &etag).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap_err(), Error::NotFound);
}

// ─── upload sessions ──────────────────────────────────────────────────────

/// Uniform mode: each `write_upload` emits as many fixed-size parts as fit
/// in the combined (staged + incoming) bytes, then restages the remainder.
#[tokio::test]
async fn upload_session_uniform_round_trip() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/uniform/{}", Uuid::new_v4());

    let chunks: Vec<Vec<u8>> = vec![
        vec![0x41; 2 * 1024 * 1024],
        vec![0x42; 4 * 1024 * 1024],
        vec![0x43; 6 * 1024 * 1024],
    ];
    let mut session = store.create_upload(&key).await.unwrap();
    for chunk in &chunks {
        let len = chunk.len() as u64;
        store
            .write_upload(
                &mut session,
                &format!("{key}.staged"),
                frame(chunk.clone()),
                len,
            )
            .await
            .unwrap();
    }
    let total: u64 = chunks.iter().map(|c| c.len() as u64).sum();
    assert_eq!(session.uploaded_size, total);
    store
        .complete_upload(session, &format!("{key}.staged"))
        .await
        .unwrap();
    let assembled = store.get(&key).await.unwrap();
    assert_eq!(assembled.len() as u64, total);
    let mut expected = Vec::with_capacity(assembled.len());
    for chunk in &chunks {
        expected.extend_from_slice(chunk);
    }
    assert_eq!(assembled, expected);
}

/// Non-uniform mode: a single big write emits one part of the full size.
#[tokio::test]
async fn upload_session_nonuniform_single_part() {
    let store = backend_with(false, 5 * 1024 * 1024);
    let key = format!("up/nonuniform/{}", Uuid::new_v4());
    let data: Vec<u8> = (0..6 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    let mut session = store.create_upload(&key).await.unwrap();
    let len = data.len() as u64;
    store
        .write_upload(
            &mut session,
            &format!("{key}.staged"),
            frame(data.clone()),
            len,
        )
        .await
        .unwrap();
    store
        .complete_upload(session, &format!("{key}.staged"))
        .await
        .unwrap();
    let assembled = store.get(&key).await.unwrap();
    assert_eq!(assembled, data);
}

/// Non-uniform mode flushes at the S3 minimum part size (5 MiB), not at the
/// configured part size (here 50 MiB). A single ~6 MiB write therefore emits
/// one multipart part rather than restaging everything.
#[tokio::test]
async fn upload_session_nonuniform_flushes_at_min_part_size_not_configured_part_size() {
    let store = backend_with(false, 50 * 1024 * 1024);
    let key = format!("up/nonuniform-min/{}", Uuid::new_v4());
    let data: Vec<u8> = (0..6 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();

    let mut session = store.create_upload(&key).await.unwrap();
    let len = data.len() as u64;
    store
        .write_upload(
            &mut session,
            &format!("{key}.staged"),
            frame(data.clone()),
            len,
        )
        .await
        .unwrap();

    match &session.state {
        SessionState::S3 {
            parts,
            staged_size,
            upload_id,
        } => {
            assert_eq!(parts.len(), 1, "the 6 MiB write must emit one part");
            assert_eq!(*staged_size, 0, "nothing should be left staged");
            assert!(upload_id.is_some(), "a multipart session must be open");
        }
        SessionState::Fs => panic!("expected an S3 session state"),
    }

    store
        .complete_upload(session, &format!("{key}.staged"))
        .await
        .unwrap();
    let assembled = store.get(&key).await.unwrap();
    assert_eq!(assembled, data);
}

/// Small upload that never crosses the multipart threshold: `complete_upload`
/// promotes the staging key to the canonical key without ever creating a
/// multipart session.
#[tokio::test]
async fn upload_session_small_upload_takes_singleshot_path() {
    let store = backend_with(false, 5 * 1024 * 1024);
    let key = format!("up/small/{}", Uuid::new_v4());
    let mut session = store.create_upload(&key).await.unwrap();
    store
        .write_upload(
            &mut session,
            &format!("{key}.staged"),
            frame(b"hello".to_vec()),
            5,
        )
        .await
        .unwrap();
    store
        .complete_upload(session, &format!("{key}.staged"))
        .await
        .unwrap();
    assert_eq!(store.get(&key).await.unwrap(), b"hello");
}

/// Zero-byte upload: `complete_upload` puts an empty object at `key`.
#[tokio::test]
async fn upload_session_complete_with_no_writes_creates_empty_object() {
    let store = backend();
    let key = format!("up/empty/{}", Uuid::new_v4());
    let session = store.create_upload(&key).await.unwrap();
    store
        .complete_upload(session, &format!("{key}.staged"))
        .await
        .unwrap();
    assert_eq!(store.get(&key).await.unwrap(), b"");
}

/// Sessions are the handle: serialise, restart, resume.
#[tokio::test]
async fn upload_session_resumes_after_serde_round_trip() {
    let store = backend_with(true, 5 * 1024 * 1024);
    let key = format!("up/resume/{}", Uuid::new_v4());
    let mut session = store.create_upload(&key).await.unwrap();
    let head = vec![0x55; 6 * 1024 * 1024];
    store
        .write_upload(
            &mut session,
            &format!("{key}.staged"),
            frame(head.clone()),
            head.len() as u64,
        )
        .await
        .unwrap();

    let bytes = serde_json::to_vec(&session).unwrap();
    let mut session: crate::UploadSession = serde_json::from_slice(&bytes).unwrap();

    let tail = vec![0x66; 512 * 1024];
    store
        .write_upload(
            &mut session,
            &format!("{key}.staged"),
            frame(tail.clone()),
            tail.len() as u64,
        )
        .await
        .unwrap();
    store
        .complete_upload(session, &format!("{key}.staged"))
        .await
        .unwrap();
    let assembled = store.get(&key).await.unwrap();
    let mut expected = head;
    expected.extend_from_slice(&tail);
    assert_eq!(assembled, expected);
}

/// `abort_pending_uploads` clears every in-flight multipart session at `key`,
/// including those the engine has lost track of.
#[tokio::test]
async fn upload_session_abort_pending_removes_orphans() {
    let store = backend();
    let prefix = format!("up/abort/{}", Uuid::new_v4());
    let key = format!("{prefix}/obj");

    // Manually start two multipart uploads at the same key so we have
    // something to clean up.
    store.client.create_multipart_upload(&key).await.unwrap();
    store.client.create_multipart_upload(&key).await.unwrap();

    store.abort_pending_uploads(&key).await.unwrap();

    let (remaining, _, _) = store
        .client
        .list_multipart_uploads(Some(&key), None, None)
        .await
        .unwrap();
    assert!(remaining.iter().all(|u| u.key != key));
    store.delete_prefix(&prefix).await.unwrap();
}

#[tokio::test]
async fn presign_get_returns_a_url() {
    let store = backend();
    let url = store
        .presign_get("blob/x", Duration::from_mins(1), None)
        .await
        .unwrap();
    assert!(
        url.contains("blob/x") && url.contains("X-Amz-Signature"),
        "expected a SigV4 presigned URL, got: {url}",
    );
}
