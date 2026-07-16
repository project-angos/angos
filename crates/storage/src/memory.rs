//! In-process memory-backed [`ObjectStore`] and [`ConditionalStore`].
//!
//! Stores objects in a `HashMap<String, (Bytes, Etag)>` guarded by a `Mutex`.
//! Every write generates a fresh monotonic etag so the store can also serve
//! as a [`ConditionalStore`] for CAS-based testing.
//!
//! `get_stream` returns a cursor over an in-memory clone; `copy` clones the
//! entry in-place (with a fresh etag).
//!
//! Test fixture only: no production or configuration path constructs it
//! (`BlobStoreConfig`/`RegistryStorageConfig` are FS or S3, and the
//! in-process job queue runs on the metadata store's engine). It is the fast,
//! deterministic substrate for the tx-engine executor/lock/chaos tests and
//! the conformance suites in `crate::tests`, which hold it to the same trait
//! contract as the real backends.

use std::{
    collections::{BTreeSet, HashMap},
    io::Cursor,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;

use crate::{
    BoxedReader, ByteStream, ChildrenPage, ConditionalStore, Error, Etag, ObjectMeta, ObjectStore,
    Page, object::dir_prefix,
};

/// Inner shared state.
struct Inner {
    data: HashMap<String, (Bytes, Etag)>,
}

impl Inner {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("entry_count", &self.data.len())
            .finish_non_exhaustive()
    }
}

/// In-process [`ObjectStore`] backed by a `HashMap`.
///
/// All clones share the same underlying map. Safe to clone; the `Arc<Mutex<...>>`
/// is shared across clones.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
/// use bytes::Bytes;
/// use angos_storage::{ObjectStore, MemoryObjectStore};
///
/// # #[tokio::main]
/// # async fn main() {
/// let store = Arc::new(MemoryObjectStore::new());
/// store.put("key", Bytes::from("hello")).await.unwrap();
/// let body = store.get("key").await.unwrap();
/// assert_eq!(body, b"hello");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MemoryObjectStore {
    inner: Arc<Mutex<Inner>>,
    counter: Arc<AtomicU64>,
}

impl MemoryObjectStore {
    /// Create a new, empty in-memory object store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn next_etag(&self) -> Etag {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        Etag::new(format!("\"{n}\""))
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.lock()
            .data
            .get(key)
            .map(|(b, _)| b.to_vec())
            .ok_or(Error::NotFound)
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let bytes = self
            .lock()
            .data
            .get(key)
            .map(|(b, _)| b.clone())
            .ok_or(Error::NotFound)?;

        let total = bytes.len() as u64;
        let start = usize::try_from(offset.unwrap_or(0).min(total)).unwrap_or(usize::MAX);
        let slice = bytes.slice(start..);
        let reader: BoxedReader = Box::new(Cursor::new(slice));
        Ok((reader, total))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        let etag = self.next_etag();
        self.lock().data.insert(key.to_string(), (data, etag));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        self.lock().data.remove(key);
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        // Treat `prefix` as a directory boundary (see `dir_prefix`): a
        // non-empty prefix is normalised to `prefix/` so keys that merely share
        // a common string prefix survive (e.g. "jobs/cache" must not delete
        // "jobs/cache_extra/foo"). An empty prefix is a no-op: it must not
        // delete every object.
        let effective_prefix = dir_prefix(prefix);
        if effective_prefix.is_empty() {
            return Ok(());
        }
        let mut guard = self.lock();
        guard
            .data
            .retain(|k, _| !k.starts_with(effective_prefix.as_ref()));
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        self.lock()
            .data
            .get(key)
            .map(|(b, e)| ObjectMeta {
                size: b.len() as u64,
                etag: Some(e.clone()),
                last_modified: None,
            })
            .ok_or(Error::NotFound)
    }

    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        let guard = self.lock();
        let start_after = token.as_deref().unwrap_or("").to_string();

        // Normalise the separator: if the prefix does not end with '/', strip the
        // leading '/' from each suffix so callers get a clean relative name
        // (matching the FS backend, which returns paths relative to the
        // prefix-directory without a leading separator). An empty prefix lists
        // the whole store and every key is already relative to the root.
        let at_boundary = prefix.is_empty() || prefix.ends_with('/');
        let sep_len = usize::from(!at_boundary);

        // Collect and sort all keys matching the prefix, then paginate.
        let mut keys: Vec<String> = guard
            .data
            .keys()
            .filter(|k| {
                k.starts_with(prefix)
                    && k.len() > prefix.len()
                    && (at_boundary || k.as_bytes().get(prefix.len()) == Some(&b'/'))
            })
            // Strip the prefix (and separator) so the result contains only the
            // suffix (filename), matching the FS backend's convention.
            .map(|k| k[prefix.len() + sep_len..].to_string())
            .collect();
        keys.sort_unstable();

        let page_size = n as usize;
        let mut items = Vec::with_capacity(page_size);
        let mut next_token: Option<String> = None;

        let relevant = keys
            .into_iter()
            .filter(|k| k.as_str() > start_after.as_str());

        for key in relevant {
            if items.len() >= page_size {
                next_token = Some(items.last().cloned().unwrap_or_default());
                break;
            }
            items.push(key);
        }

        Ok(Page { items, next_token })
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        let guard = self.lock();
        let skip_before = token
            .as_deref()
            .or(start_after.as_deref())
            .unwrap_or("")
            .to_string();

        let page_size = n as usize;
        let mut sub_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut objects: BTreeSet<String> = BTreeSet::new();

        // Ensure the effective prefix ends with '/' so child names are clean
        // (matching the FS backend convention where prefix acts as a directory).
        let prefix_with_slash;
        let effective_prefix: &str = if prefix.ends_with('/') {
            prefix
        } else {
            prefix_with_slash = format!("{prefix}/");
            &prefix_with_slash
        };

        for key in guard
            .data
            .keys()
            .filter(|k| k.starts_with(effective_prefix))
        {
            let rest = &key[effective_prefix.len()..];
            if let Some(slash) = rest.find('/') {
                // Emit the bare sub-prefix name (no trailing slash) so the
                // memory backend matches the fs and s3 backends and the
                // `ChildrenPage::sub_prefixes` contract. The bare form is used
                // consistently for the `skip_before` comparison and, via the
                // merged page below, for the next continuation token.
                let child = rest[..slash].to_string();
                if child.as_str() > skip_before.as_str() {
                    sub_prefixes.insert(child);
                }
            } else {
                let child = rest.to_string();
                if child.as_str() > skip_before.as_str() {
                    objects.insert(child);
                }
            }
        }

        // Merge and take up to `n` entries.
        let total: Vec<_> = sub_prefixes
            .iter()
            .map(|s| (true, s.clone()))
            .chain(objects.iter().map(|s| (false, s.clone())))
            .take(page_size + 1)
            .collect();

        let truncated = total.len() > page_size;
        let emit_count = if truncated { page_size } else { total.len() };

        let mut result_sub: Vec<String> = Vec::new();
        let mut result_obj: Vec<String> = Vec::new();
        let mut last: Option<String> = None;

        for (is_prefix, name) in total.into_iter().take(emit_count) {
            last = Some(name.clone());
            if is_prefix {
                result_sub.push(name);
            } else {
                result_obj.push(name);
            }
        }

        let next_token = if truncated { last } else { None };

        Ok(ChildrenPage {
            sub_prefixes: result_sub,
            objects: result_obj,
            next_token,
        })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error> {
        let bytes = self
            .lock()
            .data
            .get(source)
            .map(|(b, _)| b.clone())
            .ok_or(Error::NotFound)?;
        let etag = self.next_etag();
        self.lock()
            .data
            .insert(destination.to_string(), (bytes, etag));
        Ok(())
    }

    async fn create_upload(&self, key: &str) -> Result<(), Error> {
        // Truncate any prior content at `key` so a re-`create` starts empty.
        self.put(key, Bytes::new()).await
    }

    async fn write_upload(
        &self,
        key: &str,
        mut body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error> {
        let mut combined = self.get(key).await.unwrap_or_default();
        let current = u64::try_from(combined.len()).map_err(|e| Error::Backend(e.to_string()))?;
        if len == Some(0) {
            return Ok(current);
        }
        let mut buf =
            BytesMut::with_capacity(len.and_then(|l| usize::try_from(l).ok()).unwrap_or(0));
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Backend(e.to_string()))?;
            buf.extend_from_slice(&chunk);
        }
        let actual = u64::try_from(buf.len()).map_err(|e| Error::Backend(e.to_string()))?;
        if let Some(expected) = len
            && actual != expected
        {
            return Err(Error::Backend(format!(
                "memory upload short body: expected {expected} bytes, got {actual}",
            )));
        }
        combined.extend_from_slice(&buf);
        self.put(key, Bytes::from(combined)).await?;
        current
            .checked_add(actual)
            .ok_or_else(|| Error::Backend("upload size overflow".to_string()))
    }

    async fn complete_upload(&self, key: &str) -> Result<(), Error> {
        // Ensure the object exists (an upload completed with no writes still
        // produces an empty object at `key`); otherwise no-op, the data already
        // lives at `key`.
        if self.lock().data.contains_key(key) {
            Ok(())
        } else {
            self.put(key, Bytes::new()).await
        }
    }

    async fn abort_upload(&self, key: &str) -> Result<(), Error> {
        self.delete(key).await
    }

    // `list_multipart_uploads` uses the trait's empty default: the memory
    // backend has no multipart protocol.
}

#[async_trait]
impl ConditionalStore for MemoryObjectStore {
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), Error> {
        self.lock()
            .data
            .get(key)
            .map(|(b, e)| (b.to_vec(), Some(e.clone())))
            .ok_or(Error::NotFound)
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, Error> {
        let etag = self.next_etag();
        let mut guard = self.lock();
        if guard.data.contains_key(key) {
            return Err(Error::PreconditionFailed);
        }
        guard.data.insert(key.to_string(), (data, etag.clone()));
        Ok(Some(etag))
    }

    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, Error> {
        let new_etag = self.next_etag();
        let mut guard = self.lock();
        match guard.data.get(key) {
            Some((_, current)) if current == etag => {
                guard.data.insert(key.to_string(), (data, new_etag.clone()));
                Ok(Some(new_etag))
            }
            Some(_) | None => Err(Error::PreconditionFailed),
        }
    }

    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), Error> {
        let mut guard = self.lock();
        match guard.data.get(key) {
            Some((_, current)) if current == etag => {
                guard.data.remove(key);
                Ok(())
            }
            Some(_) => Err(Error::PreconditionFailed),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::ObjectStore;
    use crate::memory::MemoryObjectStore;

    fn store() -> MemoryObjectStore {
        MemoryObjectStore::new()
    }

    #[tokio::test]
    async fn list_children_paginates_sub_prefixes_with_bare_names() {
        // Regression: sub-prefix names must be bare (no trailing slash) and the
        // bare form must drive both the `start_after` filter and the next-page
        // continuation token so pagination stays correct end-to-end.
        let s = store();
        for name in ["v1", "v2", "v3", "v4"] {
            s.put(&format!("ns/{name}/manifest"), Bytes::from("x"))
                .await
                .unwrap();
        }

        let page1 = s.list_children("ns/", 2, None, None).await.unwrap();
        assert_eq!(page1.sub_prefixes, vec!["v1".to_string(), "v2".to_string()]);
        assert_eq!(page1.next_token.as_deref(), Some("v2"));

        let page2 = s
            .list_children("ns/", 2, page1.next_token, None)
            .await
            .unwrap();
        assert_eq!(page2.sub_prefixes, vec!["v3".to_string(), "v4".to_string()]);
        assert!(page2.next_token.is_none());

        // A caller-supplied `start_after` of a bare sub-prefix name resumes
        // after that name with bare names.
        let after = s
            .list_children("ns/", 10, None, Some("v2".to_string()))
            .await
            .unwrap();
        assert_eq!(after.sub_prefixes, vec!["v3".to_string(), "v4".to_string()]);
        assert!(after.next_token.is_none());
    }

    #[tokio::test]
    async fn head_returns_etag_that_changes_after_put() {
        let s = store();
        s.put("k", Bytes::from("v1")).await.unwrap();
        let e1 = s.head("k").await.unwrap().etag.unwrap();
        s.put("k", Bytes::from("v2")).await.unwrap();
        let e2 = s.head("k").await.unwrap().etag.unwrap();
        assert_ne!(e1, e2, "etag must change on overwrite");
    }
}
