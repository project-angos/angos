//! Session value types for the resumable, streaming upload primitive on
//! [`ObjectStore`](crate::ObjectStore). The trait methods live on `ObjectStore`
//! itself; this module owns only the values they exchange.
//!
//! A session is a single serialisable [`UploadSession`] value owned by the
//! caller. The backend mutates it in place; the caller persists it (in
//! `blob_store`, alongside an OCI hash context) so a crashed process can
//! resume by handing the same value back. There is no separate handle type:
//! the session *is* the handle.
//!
//! Bodies are streamed end-to-end via [`ByteStream`]; backends never buffer a
//! full upload in memory. Single-shot callers wrap one frame with
//! `stream::once(async move { Ok(bytes) })`; producer/consumer callers wrap
//! a [`tokio::sync::mpsc::Receiver`] with [`channel_stream`].

use std::{io, pin::Pin};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::types::Etag;

/// Boxed byte stream consumed by [`ObjectStore::write_upload`](crate::ObjectStore::write_upload).
///
/// `'static` and `Send`, so it can be shipped across `tokio::spawn` boundaries
/// and stored in handles. The stream yields chunks as `Result<Bytes, io::Error>`
/// — the `io::Error` slot lets readers (file-backed sources, hashing readers)
/// surface I/O failures inline rather than aborting the upload out-of-band.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;

/// Wrap a [`mpsc::Receiver`] of [`Bytes`] as a [`ByteStream`].
#[must_use]
pub fn channel_stream(rx: mpsc::Receiver<Bytes>) -> ByteStream {
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (Ok(chunk), rx))
    }))
}

/// One completed part of an S3 multipart upload. Public only so serde can
/// resolve it through [`UploadSession`]; consumers must treat it as opaque.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub part_number: u32,
    pub etag: Etag,
    pub size: u64,
}

/// Resumable upload session. Carries every piece of state required for the
/// backend to continue the upload after a process crash.
///
/// Sessions are produced by [`ObjectStore::create_upload`](crate::ObjectStore::create_upload),
/// mutated in place by [`ObjectStore::write_upload`](crate::ObjectStore::write_upload), and
/// consumed by [`ObjectStore::complete_upload`](crate::ObjectStore::complete_upload) or
/// [`ObjectStore::abort_upload`](crate::ObjectStore::abort_upload). Persist via the standard
/// `serde` machinery between calls.
///
/// The sub-part remainder's storage key is **not** persisted here: the
/// caller passes it explicitly on every [`Self`]-mutating trait method so
/// the on-disk layout stays under the caller's control.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadSession {
    pub key: String,
    pub uploaded_size: u64,
    pub state: SessionState,
}

/// Backend-tagged inner state. Public so the struct round-trips through
/// JSON; callers should not pattern-match on it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum SessionState {
    /// Append-mode-file state used by the FS backend. Contains no
    /// fields — the staging file at `key` and the `uploaded_size` field
    /// on the parent struct are all that is needed.
    Fs,
    /// S3 multipart state. `upload_id` is `None` until the first flush that
    /// needs a multipart upload opens one lazily; it is then persisted by the
    /// caller after that `write_upload` returns. `parts` is the running list
    /// of completed parts. `staged_size` is the number of bytes currently
    /// stashed at the per-session staging key (the sub-part remainder that has
    /// not been flushed yet).
    S3 {
        upload_id: Option<String>,
        parts: Vec<Part>,
        staged_size: u64,
    },
}

/// An in-flight S3 multipart upload discovered by
/// [`ObjectStore::list_multipart_uploads`](crate::ObjectStore::list_multipart_uploads).
/// Backends without a multipart protocol (FS, memory) never produce these.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingMultipartUpload {
    /// Object key the multipart upload targets.
    pub key: String,
    /// S3 multipart upload identifier, required to abort it.
    pub upload_id: String,
    /// When the multipart upload was initiated, used by callers to decide
    /// whether it has been abandoned.
    pub initiated_at: DateTime<Utc>,
}

/// One page of [`ObjectStore::list_multipart_uploads`](crate::ObjectStore::list_multipart_uploads).
/// When both markers are `None` the listing is exhausted.
#[derive(Clone, Debug, Default)]
pub struct MultipartUploadPage {
    pub uploads: Vec<PendingMultipartUpload>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

// The upload-session methods (`create_upload`, `write_upload`,
// `complete_upload`, `abort_upload`, `abort_pending_uploads`,
// `list_multipart_uploads`, `abort_multipart_upload`) are part of the
// [`ObjectStore`](crate::ObjectStore) trait itself — there is no separate
// upload-store trait. This module owns only the session *value* types those
// methods exchange.
