use std::borrow::Cow;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    BoxedReader,
    error::Error,
    types::{ChildrenPage, ObjectMeta, Page},
    upload_session::{ByteStream, MultipartUploadPage},
};

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
/// remainder lives at a `staged/<offset>` child of the upload key's container —
/// the backend derives that location from `key` alone (it replaces `key`'s
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
    /// normalised to a trailing `/` (see [`dir_prefix`]), then every object
    /// whose key sits under that directory is removed. A key that merely shares
    /// a string prefix is never affected — `delete_prefix("tags/v1")` deletes
    /// `tags/v1/...` but leaves `tags/v1-rc/...` untouched.
    ///
    /// A prefix with nothing under it counts as success. An empty prefix is a
    /// no-op (it is never normalised to the store root, so it cannot delete
    /// every object).
    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error>;

    /// Return the object's size and (when available) `ETag` and
    /// last-modified timestamp without reading the body.
    async fn head(&self, key: &str) -> Result<ObjectMeta, Error>;

    /// Flat-recursive enumeration: returns up to `n` keys under `prefix`,
    /// without grouping by `/`. Pass `token` from the previous call to
    /// resume.
    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
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

    /// Server-side copy from `source` to `destination`. Backends choose
    /// whether to issue a single-shot copy or a multipart copy based on
    /// source size and backend-specific thresholds.
    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error>;

    /// Move `source` to `destination`. The default is a `copy` followed by a
    /// `delete` of the source, which is correct for every backend. Backends
    /// with a cheaper primitive — notably a same-filesystem `rename`, which is
    /// atomic and never reads the object body into memory — should override
    /// this. The transaction engine routes `Mutation::Move` through here, so a
    /// large staged blob promoted to its canonical location is moved without
    /// buffering the whole object.
    async fn move_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        self.copy(source, destination).await?;
        self.delete(source).await
    }

    /// Begin/clear a fresh upload at `key`. Idempotent: discards any leaked
    /// prior in-progress upload at `key` (so re-`create`ing at a reused key
    /// starts clean). Lazy — no backend round-trip is required until the first
    /// write.
    async fn create_upload(&self, key: &str) -> Result<(), Error>;

    /// Append `body` (exactly `len` bytes) to the upload at `key`. Returns the
    /// new total uploaded size. Streamed end-to-end — backends never buffer the
    /// full payload. Append-only.
    async fn write_upload(&self, key: &str, body: ByteStream, len: u64) -> Result<u64, Error>;

    /// Finalise the upload at `key`: the assembled object becomes visible at
    /// `key` via the read methods (an empty object when nothing was written).
    /// The caller is responsible for moving it to its eventual canonical
    /// location.
    async fn complete_upload(&self, key: &str) -> Result<(), Error>;

    /// Discard the upload at `key` and all backend state it owns (in-progress
    /// multipart(s) for `key` on S3, plus any staged remainder). Idempotent
    /// (missing state counts as success).
    async fn abort_upload(&self, key: &str) -> Result<(), Error>;

    /// List in-flight multipart uploads store-wide, one page at a time.
    /// `key_marker`/`upload_id_marker` continue a previous page; pass `None`
    /// for both to start. This is a raw primitive — orphan detection (age
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
