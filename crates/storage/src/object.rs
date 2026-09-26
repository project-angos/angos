use std::{borrow::Cow, pin::Pin};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use tracing::warn;

use crate::{
    BoxedReader,
    error::Error,
    pagination::paginated,
    types::{Children, ChildrenPage, ObjectMeta, Page},
    upload_session::{ByteStream, MultipartUploadPage},
};

/// A lazily-streamed flat enumeration of object keys, one `Result` per key.
/// [`ObjectStore::list_all`] returns this boxed form so it stays object-safe
/// behind `dyn ObjectStore` while backends pick their own concrete stream.
pub type KeyStream<'a> = Pin<Box<dyn Stream<Item = Result<String, Error>> + Send + 'a>>;

/// Normalise `prefix` to a directory boundary for [`ObjectStore::delete_prefix`].
///
/// A non-empty prefix that does not already end with `/` gets one appended so
/// the delete is scoped to the directory `prefix/` and never matches a key that
/// merely shares a string prefix (e.g. `tags/v1` becomes `tags/v1/`, which does
/// not affect `tags/v1-rc/...`). An empty prefix and a prefix that already ends
/// with `/` are returned unchanged.
///
/// Backends share this so their `delete_prefix` semantics agree byte-for-byte.
#[must_use]
pub fn dir_prefix(prefix: &str) -> Cow<'_, str> {
    if prefix.is_empty() || prefix.ends_with('/') {
        Cow::Borrowed(prefix)
    } else {
        Cow::Owned(format!("{prefix}/"))
    }
}

/// Universal object-storage floor.
///
/// Every storage backend implements this. Both FS and S3 can express every
/// operation here: object CRUD, prefix-batch delete, head metadata, two
/// listing modes (flat-recursive and one-level-children, separator hard-coded
/// to `/`), and the keyed, append-only upload primitive (FS appends to a file;
/// S3 drives its native multipart protocol, hidden from consumers).
///
/// # Idempotency
///
/// `delete` and `delete_prefix` are idempotent: deleting a missing key or a
/// prefix with nothing under it counts as success. `get` and `head` on a
/// missing key return [`Error::NotFound`].
///
/// # Uploads
///
/// A single append-only upload primitive that both FS (append-mode file) and
/// S3 (multipart protocol) implement, hiding the S3 multipart wire details
/// (parts, upload IDs, staged remainders) from consumers. Every upload is
/// addressed solely by its `key`; there is no caller-held session value. The
/// caller drives an upload by calling [`ObjectStore::create_upload`] once,
/// [`ObjectStore::write_upload`] per chunk, and finally
/// [`ObjectStore::complete_upload`] or [`ObjectStore::abort_upload`]. After
/// `complete_upload`, the assembled object is visible via the read methods at
/// `key`.
///
/// The S3 backend recovers all multipart state (the in-flight upload id, the
/// committed parts, and the staged sub-part remainder) from S3 itself on every
/// call, so nothing has to be persisted by the caller between calls. The staged
/// remainder lives at a `staged/<offset>` child of the upload key's
/// container: the backend derives that location from `key` alone (it replaces
/// `key`'s
/// final path segment with `staged/<offset>`, where `offset` is the number of
/// bytes already committed to parts). This mirrors the historical
/// `.../_uploads/<uuid>/staged/<offset>` layout so it stays backward
/// compatible.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Read the full object body into memory.
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// Open a streaming reader over the object body, optionally starting at
    /// `offset` bytes. The returned `u64` is the **total** object size (not
    /// the remaining length after `offset`).
    async fn get_stream(&self, key: &str, offset: Option<u64>)
    -> Result<(BoxedReader, u64), Error>;

    /// Write `data` to `key`, replacing any existing object atomically.
    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error>;

    /// Delete `key`. Missing key counts as success.
    async fn delete(&self, key: &str) -> Result<(), Error>;

    /// Delete every object under the directory `prefix`.
    ///
    /// The prefix is treated as a directory boundary: a non-empty prefix is
    /// normalised to a trailing `/`, then every object
    /// whose key sits under that directory is removed. A key that merely shares
    /// a string prefix is never affected: `delete_prefix("tags/v1")` deletes
    /// `tags/v1/...` but leaves `tags/v1-rc/...` untouched.
    ///
    /// A prefix with nothing under it counts as success. An empty prefix is a
    /// no-op (it is never normalised to the store root, so it cannot delete
    /// every object).
    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error>;

    /// Return the object's size and (when available) `ETag` and
    /// last-modified timestamp without reading the body.
    async fn head(&self, key: &str) -> Result<ObjectMeta, Error>;

    /// Whether `key` holds an object: a `head` that reads `NotFound` as `false`.
    async fn exists(&self, key: &str) -> Result<bool, Error> {
        match self.head(key).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Flat-recursive enumeration: returns up to `n` keys under `prefix`,
    /// without grouping by `/`. Pass `token` from the previous call to
    /// resume.
    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        self.list_after(prefix, n, token, None).await
    }

    /// Create the object only when the key is absent. `false` means it
    /// already exists (any content). The check and the write are one atomic
    /// step on every backend: `link(2)` on FS, `If-None-Match: *` on S3. A
    /// provider that cannot honour that atomically must error rather than
    /// overwrite.
    async fn create_if_absent(&self, key: &str, data: Bytes) -> Result<bool, Error>;

    /// Like [`Self::list`], but the enumeration starts strictly after the
    /// relative key `start_after` on a chain's first page (`token` wins when
    /// both are set). This is what serves a `last`-cursor page straight off
    /// the backend's ordered enumeration.
    async fn list_after(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<Page<String>, Error>;

    /// One-level enumeration: returns the immediate sub-prefixes under
    /// `prefix` plus any objects sitting directly at that level (the `/`
    /// separator is hard-coded). `start_after` skips entries up to and
    /// including the given child name; `token` resumes a truncated page.
    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error>;

    /// Flat-recursive enumeration of *every* key under `prefix`, streamed
    /// lazily with no caller-managed continuation token. Keys arrive in no
    /// guaranteed order.
    ///
    /// The default drains [`ObjectStore::list`] pages serially, which both
    /// shipped backends override: S3 walks disjoint key ranges concurrently, so
    /// a whole-store scan (scrub, migration) is bounded by its slowest range
    /// rather than by one continuation-token chain, and FS walks its tree once
    /// instead of re-walking it per page.
    fn list_all<'a>(&'a self, prefix: &'a str) -> KeyStream<'a> {
        Box::pin(paginated(move |token| async move {
            let page = self.list(prefix, 1000, token).await?;
            Ok((page.items, page.next_token))
        }))
    }

    /// Complete one-level enumeration: every immediate child under `prefix`.
    ///
    /// The default drains [`ObjectStore::list_children`] pages serially.
    /// Backends override it with their cheapest complete form: FS reads the
    /// directory once, S3 walks disjoint name ranges concurrently, so callers
    /// needing the full child set use this instead of paging themselves.
    async fn list_all_children(&self, prefix: &str) -> Result<Children, Error> {
        let mut sub_prefixes = Vec::new();
        let mut objects = Vec::new();
        let mut token = None;
        loop {
            let page = self.list_children(prefix, 1000, token, None).await?;
            sub_prefixes.extend(page.sub_prefixes);
            objects.extend(page.objects);
            token = page.next_token;
            if token.is_none() {
                return Ok(Children {
                    sub_prefixes,
                    objects,
                });
            }
        }
    }

    /// Server-side copy from `source` to `destination`, returning the source's
    /// size. Backends choose whether to issue a single-shot copy or a
    /// multipart copy based on source size and backend-specific thresholds.
    async fn copy(&self, source: &str, destination: &str) -> Result<u64, Error>;

    /// Move `source` to `destination`. The default is a size-checked `copy`
    /// then a `delete` of the source, which is correct for every backend.
    /// Backends with a cheaper primitive (notably a same-filesystem `rename`,
    /// which is atomic and never reads the object body into memory) should
    /// override this, so a large staged blob promoted to its canonical
    /// location is moved without buffering the whole object.
    async fn move_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        verified_move(self, source, destination).await
    }

    /// Begin/clear a fresh upload at `key`. Idempotent: discards any leaked
    /// prior in-progress upload at `key` (so re-`create`ing at a reused key
    /// starts clean). Lazy: no backend round-trip is required until the first
    /// write.
    async fn create_upload(&self, key: &str) -> Result<(), Error>;

    /// Append `body` to the upload at `key`, returning the new total uploaded
    /// size. `Some(len)` declares an exact byte count and is validated;
    /// `None` streams the body to EOF (a chunked request with no
    /// `Content-Length`). Append-only.
    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error>;

    /// Finalise the upload at `key`: the assembled object becomes visible at
    /// `key` via the read methods (an empty object when nothing was written).
    /// The caller verifies it, then publishes it with
    /// [`ObjectStore::promote_upload`].
    async fn complete_upload(&self, key: &str) -> Result<(), Error>;

    /// Publish the completed upload at `key` as `destination`, then remove
    /// it. `size` is the length the caller verified, and a backend whose
    /// upload an in-flight write can still grow must publish exactly that
    /// many bytes. The default [`verified_move`] suits a backend whose
    /// completed upload no longer changes.
    async fn promote_upload(&self, key: &str, destination: &str, _size: u64) -> Result<(), Error> {
        verified_move(self, key, destination).await
    }

    /// Discard the upload at `key` and all backend state it owns (in-progress
    /// multipart(s) for `key` on S3, plus any staged remainder). Idempotent
    /// (missing state counts as success).
    async fn abort_upload(&self, key: &str) -> Result<(), Error>;

    /// List in-flight multipart uploads store-wide, one page at a time.
    /// `key_marker`/`upload_id_marker` continue a previous page; pass `None`
    /// for both to start. This is a raw primitive: orphan detection (age
    /// thresholds, live-session checks) is the caller's responsibility.
    ///
    /// Defaults to an empty page: backends without a multipart protocol (FS,
    /// memory) have no in-flight uploads to report.
    async fn list_multipart_uploads(
        &self,
        _key_marker: Option<&str>,
        _upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, Error> {
        Ok(MultipartUploadPage::default())
    }
}

const MOVE_COPY_ATTEMPTS: usize = 3;

/// `copy` then `delete`, redoing the copy until the destination's size matches
/// the source's: some S3 providers report success for a copy that landed an
/// empty object. Fails with the source left in place if no attempt matches.
pub async fn verified_move<S: ObjectStore + ?Sized>(
    store: &S,
    source: &str,
    destination: &str,
) -> Result<(), Error> {
    let (mut expected, mut landed) = (0, 0);
    for attempt in 1..=MOVE_COPY_ATTEMPTS {
        expected = store.copy(source, destination).await?;
        landed = store.head(destination).await?.size;
        if landed == expected {
            return store.delete(source).await;
        }
        warn!(
            "Copy of '{source}' to '{destination}' landed {landed} of {expected} bytes \
             (attempt {attempt}/{MOVE_COPY_ATTEMPTS})"
        );
    }
    Err(Error::Backend(format!(
        "copy of '{source}' to '{destination}' landed {landed} bytes, expected {expected}"
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::{
        Error, ObjectStore, fs,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use super::MOVE_COPY_ATTEMPTS;

    /// Empties `destination` before its first `truncations` size checks, like
    /// a copy that reported success but landed an empty object.
    struct TruncateDestination {
        inner: Arc<dyn ObjectStore>,
        destination: &'static str,
        truncations: usize,
        checks: AtomicUsize,
        copies: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreHook for TruncateDestination {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), Error> {
            match op {
                StoreOp::Copy { .. } => {
                    self.copies.fetch_add(1, Ordering::SeqCst);
                }
                StoreOp::Head { key }
                    if key == self.destination
                        && self.checks.fetch_add(1, Ordering::SeqCst) < self.truncations =>
                {
                    self.inner.put(key, Bytes::new()).await?;
                }
                _ => {}
            }
            Ok(())
        }
    }

    /// The hooked store and its copy counter.
    fn hooked(
        dir: &TempDir,
        truncations: usize,
    ) -> (
        HookedStore<Arc<dyn ObjectStore>, TruncateDestination>,
        Arc<AtomicUsize>,
    ) {
        let inner: Arc<dyn ObjectStore> = Arc::new(fs::Backend::builder(dir.path()).build());
        let copies = Arc::new(AtomicUsize::new(0));
        let hook = TruncateDestination {
            inner: Arc::clone(&inner),
            destination: "mv/dst",
            truncations,
            checks: AtomicUsize::new(0),
            copies: Arc::clone(&copies),
        };
        (HookedStore::new(inner, hook), copies)
    }

    #[tokio::test]
    async fn a_move_redoes_a_copy_that_landed_the_wrong_size() {
        let dir = TempDir::new().unwrap();
        let (store, copies) = hooked(&dir, 1);
        store
            .put("mv/src", Bytes::from_static(b"payload"))
            .await
            .unwrap();

        store.move_object("mv/src", "mv/dst").await.unwrap();

        assert_eq!(store.get("mv/dst").await.unwrap(), b"payload");
        assert_eq!(store.head("mv/src").await.unwrap_err(), Error::NotFound);
        assert_eq!(
            copies.load(Ordering::SeqCst),
            2,
            "the copy must be redone once"
        );
    }

    #[tokio::test]
    async fn a_move_that_never_lands_the_right_size_keeps_its_source() {
        let dir = TempDir::new().unwrap();
        let (store, copies) = hooked(&dir, usize::MAX);
        store
            .put("mv/src", Bytes::from_static(b"payload"))
            .await
            .unwrap();

        let error = store
            .move_object("mv/src", "mv/dst")
            .await
            .expect_err("a move whose copies all land the wrong size must fail");

        assert!(
            matches!(&error, Error::Backend(message) if message.contains("expected 7")),
            "the error must name the expected size, got: {error}"
        );
        assert_eq!(copies.load(Ordering::SeqCst), MOVE_COPY_ATTEMPTS);
        assert_eq!(
            store.get("mv/src").await.unwrap(),
            b"payload",
            "the source must survive to redo the move from"
        );
    }
}
