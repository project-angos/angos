//! Resumable, streaming upload sessions on top of [`ObjectStore`].
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

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{error::Error, object::ObjectStore, types::Etag};

/// Boxed byte stream consumed by [`UploadSessionStore::write_upload`].
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
/// Sessions are produced by [`UploadSessionStore::create_upload`], mutated
/// in place by [`UploadSessionStore::write_upload`], and consumed by
/// [`UploadSessionStore::complete_upload`] or
/// [`UploadSessionStore::abort_upload`]. Persist via the standard `serde`
/// machinery between calls.
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

/// Upload-session extension to [`ObjectStore`].
///
/// Provides a single resumable, streaming upload primitive that both FS
/// (append-mode file) and S3 (multipart protocol) implement, hiding the S3
/// multipart wire protocol (parts and upload IDs) from consumers.
///
/// Consumers store the [`UploadSession`] returned by [`Self::create_upload`]
/// in a durable record (typically alongside their own per-session metadata),
/// pass it back into [`Self::write_upload`] on every chunk, and finally
/// pass it (by value) to [`Self::complete_upload`] or
/// [`Self::abort_upload`]. After [`Self::complete_upload`] returns, the
/// assembled object exists at `session.key` and is visible via the regular
/// [`ObjectStore`] read methods.
#[async_trait]
pub trait UploadSessionStore: ObjectStore {
    /// Begin a fresh upload at `key`.
    async fn create_upload(&self, key: &str) -> Result<UploadSession, Error>;

    /// Stream `body` (exactly `len` bytes) into `session`. `staging_key`
    /// is the storage key the backend uses to park sub-part remainders
    /// (S3 only; FS ignores it). The session is mutated in place; the
    /// caller re-serialises the new state after the call returns. Bodies
    /// are streamed end-to-end — backends never buffer the full payload.
    async fn write_upload(
        &self,
        session: &mut UploadSession,
        staging_key: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), Error>;

    /// Finalise the session. After Ok, the assembled object exists at
    /// `session.key` (an empty object when no bytes were written) and is
    /// visible via the regular `ObjectStore` read methods. The caller is
    /// responsible for moving it to its canonical location.
    async fn complete_upload(&self, session: UploadSession, staging_key: &str)
    -> Result<(), Error>;

    /// Discard the session and any backend state it owns without producing
    /// an object.
    async fn abort_upload(&self, session: UploadSession, staging_key: &str) -> Result<(), Error>;

    /// Best-effort: abort any in-flight backend state that may exist for
    /// `key` outside the session-tracking path. On S3 this walks pending
    /// multipart uploads and aborts each one; on FS it is a no-op.
    async fn abort_pending_uploads(&self, key: &str) -> Result<(), Error>;
}
