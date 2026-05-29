//! Integration tests for the storage trait surface.
//!
//! Exercises every trait via the in-memory backend so the tests double as a
//! worked example of what a real backend impl looks like, and verify that
//! the traits are object-safe (`Arc<dyn ObjectStore>`) and that supertrait
//! bounds compose (`ConditionalStore: ObjectStore`,
//! `UploadSessionStore: ObjectStore`).

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::stream;

use crate::{
    ConditionalStore, Error, Etag, MemoryObjectStore, ObjectStore, PresignedStore,
    UploadSessionStore,
};

// Memory backend doesn't presign — provide a trivial stub for the test that
// needs to assert object safety of `PresignedStore`.
struct PresignStub;

#[async_trait::async_trait]
impl PresignedStore for PresignStub {
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        _content_type: Option<&str>,
    ) -> Result<String, Error> {
        Ok(format!("mock://{key}?ttl={}", ttl.as_secs()))
    }
}

fn backend() -> Arc<MemoryObjectStore> {
    Arc::new(MemoryObjectStore::new())
}

// =========================================================================
// ObjectStore
// =========================================================================

#[tokio::test]
async fn put_then_get_round_trips() {
    let store: Arc<dyn ObjectStore> = backend();
    store.put("a", Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(store.get("a").await.unwrap(), b"hello");
}

#[tokio::test]
async fn get_missing_key_returns_not_found() {
    let store: Arc<dyn ObjectStore> = backend();
    assert_eq!(store.get("missing").await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn delete_is_idempotent_on_missing_key() {
    let store: Arc<dyn ObjectStore> = backend();
    store.delete("ghost").await.unwrap();
    store.delete("ghost").await.unwrap();
}

#[tokio::test]
async fn delete_prefix_removes_matching_keys_only() {
    let store: Arc<dyn ObjectStore> = backend();
    store.put("a/1", Bytes::from_static(b"x")).await.unwrap();
    store.put("a/2", Bytes::from_static(b"y")).await.unwrap();
    store.put("b/1", Bytes::from_static(b"z")).await.unwrap();
    store.delete_prefix("a/").await.unwrap();
    assert_eq!(store.get("a/1").await.unwrap_err(), Error::NotFound);
    assert_eq!(store.get("a/2").await.unwrap_err(), Error::NotFound);
    assert_eq!(store.get("b/1").await.unwrap(), b"z");
}

#[tokio::test]
async fn head_returns_size_and_etag() {
    let store: Arc<dyn ObjectStore> = backend();
    store.put("k", Bytes::from_static(b"abcdef")).await.unwrap();
    let meta = store.head("k").await.unwrap();
    assert_eq!(meta.size, 6);
    assert!(meta.etag.is_some());
}

#[tokio::test]
async fn get_stream_reports_total_size_not_remaining() {
    use tokio::io::AsyncReadExt;
    let store: Arc<dyn ObjectStore> = backend();
    store
        .put("k", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    let (mut body, total) = store.get_stream("k", Some(3)).await.unwrap();
    assert_eq!(total, 10);
    let mut buf = Vec::new();
    body.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"3456789");
}

#[tokio::test]
async fn list_paginates_in_sorted_order() {
    let store: Arc<dyn ObjectStore> = backend();
    for k in ["dir/a", "dir/c", "dir/b"] {
        store.put(k, Bytes::from_static(b"x")).await.unwrap();
    }
    let page = store.list("dir/", 2, None).await.unwrap();
    assert_eq!(page.items, vec!["a".to_string(), "b".to_string()]);
    let page2 = store.list("dir/", 2, page.next_token).await.unwrap();
    assert_eq!(page2.items, vec!["c".to_string()]);
    assert!(page2.next_token.is_none());
}

#[tokio::test]
async fn list_children_separates_sub_prefixes_from_objects() {
    let store: Arc<dyn ObjectStore> = backend();
    store.put("ns/a", Bytes::from_static(b"x")).await.unwrap();
    store
        .put("ns/sub/b", Bytes::from_static(b"y"))
        .await
        .unwrap();
    store
        .put("ns/sub/c", Bytes::from_static(b"z"))
        .await
        .unwrap();
    let page = store.list_children("ns/", 100, None, None).await.unwrap();
    assert_eq!(page.objects, vec!["a".to_string()]);
    assert_eq!(page.sub_prefixes, vec!["sub/".to_string()]);
}

#[tokio::test]
async fn copy_duplicates_object_under_new_key() {
    let store: Arc<dyn ObjectStore> = backend();
    store
        .put("src", Bytes::from_static(b"payload"))
        .await
        .unwrap();
    store.copy("src", "dst").await.unwrap();
    assert_eq!(store.get("dst").await.unwrap(), b"payload");
    assert_eq!(store.get("src").await.unwrap(), b"payload");
}

// =========================================================================
// ConditionalStore
// =========================================================================

#[tokio::test]
async fn put_if_absent_succeeds_on_fresh_key() {
    let store: Arc<dyn ConditionalStore> = backend();
    let etag = store
        .put_if_absent("k", Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert!(etag.is_some());
}

#[tokio::test]
async fn put_if_absent_rejects_existing_key() {
    let store: Arc<dyn ConditionalStore> = backend();
    store
        .put_if_absent("k", Bytes::from_static(b"v"))
        .await
        .unwrap();
    assert_eq!(
        store
            .put_if_absent("k", Bytes::from_static(b"w"))
            .await
            .unwrap_err(),
        Error::PreconditionFailed
    );
}

#[tokio::test]
async fn put_if_match_succeeds_with_current_etag() {
    let store: Arc<dyn ConditionalStore> = backend();
    let first = store
        .put_if_absent("k", Bytes::from_static(b"v1"))
        .await
        .unwrap()
        .unwrap();
    let second = store
        .put_if_match("k", &first, Bytes::from_static(b"v2"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(store.get("k").await.unwrap(), b"v2");
}

#[tokio::test]
async fn put_if_match_rejects_stale_etag() {
    let store: Arc<dyn ConditionalStore> = backend();
    let etag = store
        .put_if_absent("k", Bytes::from_static(b"v1"))
        .await
        .unwrap()
        .unwrap();
    store
        .put_if_match("k", &etag, Bytes::from_static(b"v2"))
        .await
        .unwrap();
    assert_eq!(
        store
            .put_if_match("k", &etag, Bytes::from_static(b"v3"))
            .await
            .unwrap_err(),
        Error::PreconditionFailed
    );
}

#[tokio::test]
async fn delete_if_match_rejects_stale_etag_but_treats_missing_as_success() {
    let store: Arc<dyn ConditionalStore> = backend();
    let etag = store
        .put_if_absent("k", Bytes::from_static(b"v"))
        .await
        .unwrap()
        .unwrap();
    let stale = Etag::new("\"stale\"");
    assert_eq!(
        store.delete_if_match("k", &stale).await.unwrap_err(),
        Error::PreconditionFailed
    );
    store.delete_if_match("k", &etag).await.unwrap();
    store.delete_if_match("k", &etag).await.unwrap();
}

// =========================================================================
// UploadSessionStore
// =========================================================================

fn one_frame(body: &'static [u8]) -> crate::ByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from_static(body)) }))
}

#[tokio::test]
async fn upload_session_round_trip_creates_object_at_key() {
    let store: Arc<dyn UploadSessionStore> = backend();
    let mut session = store.create_upload("blob").await.unwrap();
    store
        .write_upload(&mut session, "blob.staged", one_frame(b"hello "), 6)
        .await
        .unwrap();
    store
        .write_upload(&mut session, "blob.staged", one_frame(b"world"), 5)
        .await
        .unwrap();
    assert_eq!(session.uploaded_size, 11);
    store.complete_upload(session, "blob.staged").await.unwrap();
    assert_eq!(store.get("blob").await.unwrap(), b"hello world");
}

#[tokio::test]
async fn upload_session_survives_round_trip_through_serde() {
    let store: Arc<dyn UploadSessionStore> = backend();
    let mut session = store.create_upload("blob").await.unwrap();
    store
        .write_upload(&mut session, "blob.staged", one_frame(b"hello "), 6)
        .await
        .unwrap();

    // Persist + reload — the session is the handle.
    let serialised = serde_json::to_string(&session).unwrap();
    let mut session: crate::UploadSession = serde_json::from_str(&serialised).unwrap();

    store
        .write_upload(&mut session, "blob.staged", one_frame(b"world"), 5)
        .await
        .unwrap();
    store.complete_upload(session, "blob.staged").await.unwrap();
    assert_eq!(store.get("blob").await.unwrap(), b"hello world");
}

#[tokio::test]
async fn upload_session_abort_leaves_no_object() {
    let store: Arc<dyn UploadSessionStore> = backend();
    let mut session = store.create_upload("blob").await.unwrap();
    store
        .write_upload(&mut session, "blob.staged", one_frame(b"partial"), 7)
        .await
        .unwrap();
    store.abort_upload(session, "blob.staged").await.unwrap();
    assert_eq!(store.get("blob").await.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn upload_session_complete_with_no_writes_creates_empty_object() {
    let store: Arc<dyn UploadSessionStore> = backend();
    let session = store.create_upload("blob").await.unwrap();
    store.complete_upload(session, "blob.staged").await.unwrap();
    assert_eq!(store.get("blob").await.unwrap(), b"");
}

// =========================================================================
// PresignedStore (object-safety smoke test)
// =========================================================================

#[tokio::test]
async fn presign_get_returns_a_url() {
    let store: Arc<dyn PresignedStore> = Arc::new(PresignStub);
    let url = store
        .presign_get("blob/x", Duration::from_mins(1), None)
        .await
        .unwrap();
    assert!(url.contains("blob/x"));
}
