//! Integration tests for the storage trait surface.
//!
//! Exercises every trait via a minimal in-memory mock backend. The tests
//! double as a worked example of what a real backend impl looks like and
//! verify that the traits are object-safe (`Arc<dyn ObjectStore>`) and that
//! the supertrait bounds compose (`ConditionalStore: ObjectStore`).

use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::{
    BoxedReader, ChildrenPage, ConditionalStore, Error, Etag, MultipartPage, MultipartStore,
    MultipartUpload, ObjectMeta, ObjectStore, Page, Part, PresignedStore, UploadId,
};

/// State for a single in-progress multipart upload in the mock.
#[derive(Default)]
struct MultipartSession {
    parts: HashMap<u32, Bytes>,
    initiated_at: DateTime<Utc>,
}

#[derive(Default)]
struct MockState {
    objects: HashMap<String, (Bytes, Etag)>,
    next_etag: u64,
    multipart: HashMap<(String, UploadId), MultipartSession>,
    next_upload: u64,
}

impl MockState {
    fn new_etag(&mut self) -> Etag {
        self.next_etag += 1;
        Etag::new(format!("\"etag-{}\"", self.next_etag))
    }

    fn new_upload_id(&mut self) -> UploadId {
        self.next_upload += 1;
        UploadId::new(format!("upload-{}", self.next_upload))
    }
}

#[derive(Default)]
struct MockBackend {
    state: Mutex<MockState>,
}

#[async_trait]
impl ObjectStore for MockBackend {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let state = self.state.lock().unwrap();
        state
            .objects
            .get(key)
            .map(|(data, _)| data.to_vec())
            .ok_or(Error::NotFound)
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let data = self.get(key).await?;
        let total = data.len() as u64;
        let start =
            usize::try_from(offset.unwrap_or(0)).map_err(|e| Error::Backend(e.to_string()))?;
        let body: BoxedReader = Box::new(Cursor::new(data[start.min(data.len())..].to_vec()));
        Ok((body, total))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        let etag = state.new_etag();
        state.objects.insert(key.to_string(), (data, etag));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        self.state.lock().unwrap().objects.remove(key);
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        state.objects.retain(|k, _| !k.starts_with(prefix));
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        let state = self.state.lock().unwrap();
        let (data, etag) = state.objects.get(key).ok_or(Error::NotFound)?;
        Ok(ObjectMeta {
            size: data.len() as u64,
            etag: Some(etag.clone()),
            last_modified: None,
        })
    }

    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        let state = self.state.lock().unwrap();
        let mut keys: Vec<String> = state
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        let start = token.as_deref().map_or(0, |t| {
            keys.iter()
                .position(|k| k.as_str() > t)
                .unwrap_or(keys.len())
        });
        let end = (start + n as usize).min(keys.len());
        let items: Vec<String> = keys[start..end].to_vec();
        let next_token = (end < keys.len()).then(|| items.last().cloned().unwrap_or_default());
        Ok(Page { items, next_token })
    }

    async fn list_children(
        &self,
        prefix: &str,
        _n: u16,
        _token: Option<String>,
        _start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        let state = self.state.lock().unwrap();
        let mut sub_prefixes = Vec::new();
        let mut objects = Vec::new();
        for key in state.objects.keys() {
            let Some(rest) = key.strip_prefix(prefix) else {
                continue;
            };
            if let Some(slash) = rest.find('/') {
                let sub = rest[..slash].to_string();
                if !sub_prefixes.contains(&sub) {
                    sub_prefixes.push(sub);
                }
            } else {
                objects.push(rest.to_string());
            }
        }
        sub_prefixes.sort();
        objects.sort();
        Ok(ChildrenPage {
            sub_prefixes,
            objects,
            next_token: None,
        })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error> {
        let data = self.get(source).await?;
        self.put(destination, Bytes::from(data)).await
    }
}

#[async_trait]
impl ConditionalStore for MockBackend {
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), Error> {
        let state = self.state.lock().unwrap();
        let (data, etag) = state.objects.get(key).ok_or(Error::NotFound)?;
        Ok((data.to_vec(), Some(etag.clone())))
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, Error> {
        let mut state = self.state.lock().unwrap();
        if state.objects.contains_key(key) {
            return Err(Error::PreconditionFailed);
        }
        let etag = state.new_etag();
        state.objects.insert(key.to_string(), (data, etag.clone()));
        Ok(Some(etag))
    }

    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, Error> {
        let mut state = self.state.lock().unwrap();
        let current = state.objects.get(key).ok_or(Error::NotFound)?;
        if &current.1 != etag {
            return Err(Error::PreconditionFailed);
        }
        let new_etag = state.new_etag();
        state
            .objects
            .insert(key.to_string(), (data, new_etag.clone()));
        Ok(Some(new_etag))
    }

    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        match state.objects.get(key) {
            Some((_, current)) if current == etag => {
                state.objects.remove(key);
                Ok(())
            }
            Some(_) => Err(Error::PreconditionFailed),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl MultipartStore for MockBackend {
    async fn create_multipart(&self, key: &str) -> Result<UploadId, Error> {
        let mut state = self.state.lock().unwrap();
        let id = state.new_upload_id();
        state.multipart.insert(
            (key.to_string(), id.clone()),
            MultipartSession {
                parts: HashMap::new(),
                initiated_at: Utc::now(),
            },
        );
        Ok(id)
    }

    async fn upload_part_streaming(
        &self,
        key: &str,
        id: &UploadId,
        part_number: u32,
        _content_length: u64,
        mut rx: mpsc::Receiver<Bytes>,
    ) -> Result<Etag, Error> {
        let mut buf = Vec::new();
        while let Some(chunk) = rx.recv().await {
            buf.extend_from_slice(&chunk);
        }
        let mut state = self.state.lock().unwrap();
        let session = state
            .multipart
            .get_mut(&(key.to_string(), id.clone()))
            .ok_or(Error::NotFound)?;
        session.parts.insert(part_number, Bytes::from(buf));
        Ok(state.new_etag())
    }

    async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        id: &UploadId,
        part_number: u32,
        _range: Option<String>,
    ) -> Result<Etag, Error> {
        let data = self.get(source).await?;
        let mut state = self.state.lock().unwrap();
        let session = state
            .multipart
            .get_mut(&(destination.to_string(), id.clone()))
            .ok_or(Error::NotFound)?;
        session.parts.insert(part_number, Bytes::from(data));
        Ok(state.new_etag())
    }

    async fn complete_multipart(
        &self,
        key: &str,
        id: &UploadId,
        parts: &[Part],
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        let session = state
            .multipart
            .remove(&(key.to_string(), id.clone()))
            .ok_or(Error::NotFound)?;
        let mut assembled = Vec::new();
        for part in parts {
            let chunk = session
                .parts
                .get(&part.part_number)
                .ok_or(Error::NotFound)?;
            assembled.extend_from_slice(chunk);
        }
        let etag = state.new_etag();
        state
            .objects
            .insert(key.to_string(), (Bytes::from(assembled), etag));
        Ok(())
    }

    async fn abort_multipart(&self, key: &str, id: &UploadId) -> Result<(), Error> {
        self.state
            .lock()
            .unwrap()
            .multipart
            .remove(&(key.to_string(), id.clone()));
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        _key_marker: Option<&str>,
        _upload_id_marker: Option<&str>,
    ) -> Result<MultipartPage, Error> {
        let state = self.state.lock().unwrap();
        let uploads: Vec<MultipartUpload> = state
            .multipart
            .iter()
            .filter(|((k, _), _)| prefix.is_none_or(|p| k.starts_with(p)))
            .map(|((k, id), session)| MultipartUpload {
                key: k.clone(),
                upload_id: id.clone(),
                initiated_at: session.initiated_at,
            })
            .collect();
        Ok(MultipartPage {
            uploads,
            next_key_marker: None,
            next_upload_id_marker: None,
        })
    }

    async fn list_parts(&self, key: &str, id: &UploadId) -> Result<Vec<Part>, Error> {
        let state = self.state.lock().unwrap();
        let session = state
            .multipart
            .get(&(key.to_string(), id.clone()))
            .ok_or(Error::NotFound)?;
        let mut parts: Vec<Part> = session
            .parts
            .iter()
            .map(|(n, data)| Part {
                part_number: *n,
                etag: Etag::new(format!("\"part-{n}\"")),
                size: data.len() as u64,
            })
            .collect();
        parts.sort_by_key(|p| p.part_number);
        Ok(parts)
    }
}

#[async_trait]
impl PresignedStore for MockBackend {
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        _content_type: Option<&str>,
    ) -> Result<String, Error> {
        Ok(format!("mock://{key}?ttl={}", ttl.as_secs()))
    }
}

fn backend() -> Arc<MockBackend> {
    Arc::new(MockBackend::default())
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
    // Two deletes in a row must both succeed.
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
    assert_eq!(
        total, 10,
        "total size must be reported, not the post-offset remainder"
    );
    let mut buf = Vec::new();
    body.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"3456789");
}

#[tokio::test]
async fn list_paginates_in_sorted_order() {
    let store: Arc<dyn ObjectStore> = backend();
    for k in ["a", "c", "b"] {
        store.put(k, Bytes::from_static(b"x")).await.unwrap();
    }
    let page = store.list("", 2, None).await.unwrap();
    assert_eq!(page.items, vec!["a".to_string(), "b".to_string()]);
    let page2 = store.list("", 2, page.next_token).await.unwrap();
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
    assert_eq!(page.sub_prefixes, vec!["sub".to_string()]);
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
    // Reusing the original etag must now fail.
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
    // Missing object: success.
    store.delete_if_match("k", &etag).await.unwrap();
}

// =========================================================================
// MultipartStore
// =========================================================================

#[tokio::test]
async fn multipart_round_trip_assembles_object_from_parts() {
    let store: Arc<dyn MultipartStore> = backend();
    let id = store.create_multipart("blob").await.unwrap();

    let (tx1, rx1) = mpsc::channel(2);
    tx1.send(Bytes::from_static(b"hello ")).await.unwrap();
    drop(tx1);
    let etag1 = store
        .upload_part_streaming("blob", &id, 1, 6, rx1)
        .await
        .unwrap();

    let (tx2, rx2) = mpsc::channel(2);
    tx2.send(Bytes::from_static(b"world")).await.unwrap();
    drop(tx2);
    let etag2 = store
        .upload_part_streaming("blob", &id, 2, 5, rx2)
        .await
        .unwrap();

    let parts = vec![
        Part {
            part_number: 1,
            etag: etag1,
            size: 6,
        },
        Part {
            part_number: 2,
            etag: etag2,
            size: 5,
        },
    ];
    store.complete_multipart("blob", &id, &parts).await.unwrap();

    assert_eq!(store.get("blob").await.unwrap(), b"hello world");
}

#[tokio::test]
async fn abort_multipart_discards_session() {
    let store: Arc<dyn MultipartStore> = backend();
    let id = store.create_multipart("blob").await.unwrap();
    store.abort_multipart("blob", &id).await.unwrap();
    assert_eq!(
        store.list_parts("blob", &id).await.unwrap_err(),
        Error::NotFound
    );
}

#[tokio::test]
async fn list_multipart_uploads_filters_by_prefix() {
    let store: Arc<dyn MultipartStore> = backend();
    let _id_a = store.create_multipart("repo-a/blob").await.unwrap();
    let _id_b = store.create_multipart("repo-b/blob").await.unwrap();
    let page = store
        .list_multipart_uploads(Some("repo-a/"), None, None)
        .await
        .unwrap();
    assert_eq!(page.uploads.len(), 1);
    assert_eq!(page.uploads[0].key, "repo-a/blob");
}

// =========================================================================
// PresignedStore
// =========================================================================

#[tokio::test]
async fn presign_get_returns_a_url() {
    let store: Arc<dyn PresignedStore> = backend();
    let url = store
        .presign_get("blob/x", Duration::from_mins(1), None)
        .await
        .unwrap();
    assert!(url.contains("blob/x"));
}
