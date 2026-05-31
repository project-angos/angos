use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    BoxedReader,
    error::Error,
    types::{ChildrenPage, ObjectMeta, Page},
    upload_session::{ByteStream, MultipartUploadPage, UploadSession},
};

/// Universal object-storage floor.
///
/// Every storage backend implements this. Both FS and S3 can express every
/// operation here: object CRUD, prefix-batch delete, head metadata, two
/// listing modes (flat-recursive and one-level-children, separator hard-coded
/// to `/`), and the resumable [`UploadSession`] primitive (FS appends to a
/// file; S3 drives its native multipart protocol, hidden from consumers).
///
/// # Idempotency
///
/// `delete` and `delete_prefix` are idempotent: deleting a missing key or
/// empty prefix counts as success. `get` and `head` on a missing key return
/// [`Error::NotFound`].
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

    /// Delete every object whose key starts with `prefix`. Empty prefix
    /// counts as success.
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

    // ── Upload sessions ─────────────────────────────────────────────────────
    //
    // A single resumable, streaming upload primitive that both FS (append-mode
    // file) and S3 (multipart protocol) implement, hiding the S3 multipart wire
    // details (parts, upload IDs) from consumers. Callers store the
    // [`UploadSession`] returned by `create_upload`, pass it back into
    // `write_upload` on every chunk, and finally hand it to `complete_upload`
    // or `abort_upload`. After `complete_upload`, the assembled object is
    // visible via the regular read methods above.

    /// Begin a fresh upload at `key`.
    async fn create_upload(&self, key: &str) -> Result<UploadSession, Error>;

    /// Stream `body` (exactly `len` bytes) into `session`. `staged_dir` is
    /// the directory under which the backend parks per-offset sub-part
    /// remainders (`<staged_dir>/<offset>`, S3 only; FS ignores it). The
    /// session is mutated in place; the caller re-serialises the new state
    /// after the call returns. Bodies are streamed end-to-end — backends
    /// never buffer the full payload.
    async fn write_upload(
        &self,
        session: &mut UploadSession,
        staged_dir: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), Error>;

    /// Finalise the session. After Ok, the assembled object exists at
    /// `session.key` (an empty object when no bytes were written) and is
    /// visible via the regular read methods. The caller is responsible for
    /// moving it to its canonical location. `staged_dir` is the per-offset
    /// staging directory (see [`Self::write_upload`]).
    async fn complete_upload(&self, session: UploadSession, staged_dir: &str) -> Result<(), Error>;

    /// Discard the session and any backend state it owns without producing
    /// an object. `staged_dir` is the per-offset staging directory.
    async fn abort_upload(&self, session: UploadSession, staged_dir: &str) -> Result<(), Error>;

    /// Best-effort: abort any in-flight backend state that may exist for
    /// `key` outside the session-tracking path. On S3 this walks pending
    /// multipart uploads and aborts each one; on FS it is a no-op.
    async fn abort_pending_uploads(&self, key: &str) -> Result<(), Error>;

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

    /// Abort a single in-flight multipart upload identified by `key` and
    /// `upload_id`. Defaults to a no-op for backends without a multipart
    /// protocol.
    async fn abort_multipart_upload(&self, _key: &str, _upload_id: &str) -> Result<(), Error> {
        Ok(())
    }
}
