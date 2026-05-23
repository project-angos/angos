//! Integration tests for the S3 backend. These talk to a live `MinIO`
//! instance at `127.0.0.1:9000` — the same convention the rest of the angos
//! workspace uses for S3 integration tests. They are skipped automatically
//! when the endpoint is unreachable so a hot `cargo test --workspace` still
//! passes on developer machines without `MinIO` running.

use std::{sync::Arc, time::Duration};

use angos_s3_client::{Backend as S3Backend, BackendConfig as S3Config};
use bytes::Bytes;
use bytesize::ByteSize;
use tokio::{io::AsyncReadExt, sync::mpsc};
use uuid::Uuid;

use crate::{ConditionalStore, Error, MultipartStore, ObjectStore, PresignedStore, s3::Backend};

fn backend() -> Option<Backend> {
    let config = S3Config {
        access_key_id: "root".to_string(),
        secret_key: "roottoor".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "registry".to_string(),
        region: "us-east-1".to_string(),
        key_prefix: format!("storage-s3-tests/{}", Uuid::new_v4()),
        // Keep multipart thresholds tight so the round-trip test can exercise
        // it with a few KiB instead of GiBs.
        multipart_copy_threshold: ByteSize::mib(5),
        multipart_copy_chunk_size: ByteSize::mib(5),
        multipart_part_size: ByteSize::mib(5),
        ..Default::default()
    };
    let client = Arc::new(S3Backend::new(&config).ok()?);
    Backend::builder().client(client).build().ok()
}

/// Probe the configured endpoint by issuing a `HEAD` on a key that almost
/// certainly doesn't exist. Returns `true` when `MinIO` is reachable and
/// responding (either with the object or `NotFound`), `false` when we should
/// skip the test.
async fn minio_reachable(backend: &Backend) -> bool {
    let probe_key = format!("__probe__/{}", Uuid::new_v4());
    matches!(backend.head(&probe_key).await, Ok(_) | Err(Error::NotFound),)
}

macro_rules! require_minio {
    ($store:ident) => {
        let Some($store) = backend() else {
            eprintln!("skipping S3 test: backend builder failed");
            return;
        };
        if !minio_reachable(&$store).await {
            eprintln!("skipping S3 test: MinIO at 127.0.0.1:9000 unreachable");
            return;
        }
    };
}

#[tokio::test]
async fn builder_requires_client() {
    let err = Backend::builder().build().unwrap_err();
    assert!(matches!(err, Error::Backend(msg) if msg.contains("client")));
}

#[tokio::test]
async fn put_then_get_round_trips() {
    require_minio!(store);
    store
        .put("rt/key", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(store.get("rt/key").await.unwrap(), b"hello");
}

#[tokio::test]
async fn get_missing_key_returns_not_found() {
    require_minio!(store);
    let key = format!("missing/{}", Uuid::new_v4());
    assert_eq!(store.get(&key).await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn head_reports_size_and_etag() {
    require_minio!(store);
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
    require_minio!(store);
    let prefix = format!("dp/{}", Uuid::new_v4());
    for k in ["a", "b/c", "b/d"] {
        store
            .put(&format!("{prefix}/{k}"), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    store.delete_prefix(&prefix).await.unwrap();
    let (objects, _) = store
        .list(&prefix, 10, None)
        .await
        .map(|p| (p.items, p.next_token))
        .unwrap();
    assert!(
        objects.is_empty(),
        "prefix must be empty after delete_prefix"
    );
}

#[tokio::test]
async fn get_stream_reports_total_size_not_remaining() {
    require_minio!(store);
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
    require_minio!(store);
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
    require_minio!(store);
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
    require_minio!(store);
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
    // Note: the negative case (bogus ETag must yield PreconditionFailed) is
    // not asserted here — older S3-compatible providers don't enforce
    // `DeleteObject + If-Match`. Capability detection belongs in the consumer
    // layer (see `metadata_store::s3::probe::probe_conditional_capabilities`).
    require_minio!(store);
    let key = format!("cas/del/{}", Uuid::new_v4());
    let etag = store
        .put_if_absent(&key, Bytes::from_static(b"v"))
        .await
        .unwrap()
        .expect("S3 returns an ETag on create");
    store.delete_if_match(&key, &etag).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn multipart_round_trip_assembles_object() {
    require_minio!(store);
    let key = format!("mp/{}", Uuid::new_v4());
    let id = store.create_multipart(&key).await.unwrap();

    // S3 minimum part size is 5 MiB except for the last part.
    let mut data = vec![0u8; 5 * 1024 * 1024];
    for (i, b) in data.iter_mut().enumerate() {
        *b = u8::try_from(i % 251).unwrap_or(0);
    }

    let (tx1, rx1) = mpsc::channel(2);
    tx1.send(Bytes::from(data.clone())).await.unwrap();
    drop(tx1);
    let etag1 = store
        .upload_part_streaming(&key, &id, 1, data.len() as u64, rx1)
        .await
        .unwrap();

    let tail = b"tail";
    let (tx2, rx2) = mpsc::channel(2);
    tx2.send(Bytes::from_static(tail)).await.unwrap();
    drop(tx2);
    let etag2 = store
        .upload_part_streaming(&key, &id, 2, tail.len() as u64, rx2)
        .await
        .unwrap();

    let parts = vec![
        crate::Part {
            part_number: 1,
            etag: etag1,
            size: data.len() as u64,
        },
        crate::Part {
            part_number: 2,
            etag: etag2,
            size: tail.len() as u64,
        },
    ];
    store.complete_multipart(&key, &id, &parts).await.unwrap();

    let fetched = store.get(&key).await.unwrap();
    assert_eq!(fetched.len(), data.len() + tail.len());
    assert_eq!(&fetched[..data.len()], data.as_slice());
    assert_eq!(&fetched[data.len()..], tail);
}

#[tokio::test]
async fn presign_get_returns_a_url() {
    require_minio!(store);
    let url = store
        .presign_get("blob/x", Duration::from_mins(1), None)
        .await
        .unwrap();
    assert!(
        url.contains("blob/x") && url.contains("X-Amz-Signature"),
        "expected a SigV4 presigned URL, got: {url}",
    );
}
