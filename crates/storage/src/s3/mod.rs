//! S3-backed implementations of every capability trait.
//!
//! Wraps [`angos_s3_client::Backend`] so consumers get the storage abstraction
//! without depending on the HTTP/S3 layer directly. The wrapper translates
//! `s3_client::Error` and `io::Error` into [`crate::Error`], adapts S3's
//! flat/delimited listing modes to [`Page`](crate::Page) /
//! [`ChildrenPage`](crate::ChildrenPage), and forwards every conditional and
//! presign operation through unchanged.
//!
//! The [`ObjectStore`] impl layers the upload-session primitive on top of the
//! S3 multipart protocol:
//!
//! - `create_upload` is lazy: no `CreateMultipartUpload` round-trip until the
//!   first flush actually needs one. Small uploads (≤ `part_size`) never
//!   open a multipart session at all.
//! - `write_upload` combines any per-session staged remainder with the
//!   incoming stream and emits `UploadPart`s of up to `part_size` bytes
//!   (uniform mode) or one `UploadPart` of all available bytes once they
//!   meet `part_size` (non-uniform mode). Remainder bytes are restaged.
//! - `complete_upload` flushes the remainder as the final part, then
//!   `CompleteMultipartUpload`. Zero-byte uploads close with a single
//!   `PutObject(empty)`; small uploads that never opened a multipart
//!   session promote the staging key directly via `copy_object`.
//! - `abort_pending_uploads` walks `ListMultipartUploads` and aborts every
//!   in-flight session at `key`.
//!
//! Bodies are streamed end-to-end via an mpsc channel handed to
//! `upload_part_streaming` — no part ever sits whole in process memory.

use std::{collections::HashSet, io, sync::Arc, time::Duration};

use angos_s3_client::{Backend as S3Backend, UploadedPart};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::{io::StreamReader, task::AbortOnDropHandle};

use crate::{
    BoxedReader, ByteStream, ChildrenPage, ConditionalStore, Error, Etag, MultipartUploadPage,
    ObjectMeta, ObjectStore, Page, Part, PendingMultipartUpload, PresignedStore, SessionState,
    UploadSession, channel_stream, object::dir_prefix,
};

pub const DEFAULT_PART_SIZE: u64 = 5 * 1024 * 1024;

const FRAME_SIZE: usize = 1024 * 1024;
const FRAME_BUFFER_CAPACITY: usize = 8;
/// S3 protocol floor for non-final multipart parts: a part must be at least
/// 5 MiB before it can be flushed as an `UploadPart`.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Builder for [`Backend`].
pub struct Builder {
    client: Option<Arc<S3Backend>>,
    part_size: u64,
    uniform_parts: bool,
}

impl Builder {
    fn new() -> Self {
        Self {
            client: None,
            part_size: DEFAULT_PART_SIZE,
            uniform_parts: false,
        }
    }

    /// The underlying S3 HTTP client. Construct it with
    /// `angos_s3_client::Backend::new(&config)` and pass it in here.
    #[must_use]
    pub fn client(mut self, client: Arc<S3Backend>) -> Self {
        self.client = Some(client);
        self
    }

    /// Target part size for upload sessions (uniform mode) or minimum part
    /// size before flushing (non-uniform mode). Defaults to 5 MiB — the
    /// S3 minimum.
    #[must_use]
    pub fn part_size(mut self, size: u64) -> Self {
        self.part_size = size;
        self
    }

    /// `true` = uniform mode: each `write_upload` call emits as many parts
    /// of exactly `part_size` bytes as fit, restaging the remainder.
    /// `false` = non-uniform mode: each call emits at most one part of the
    /// full available size, flushing only once the combined pending +
    /// incoming bytes meet `part_size`. Defaults to non-uniform.
    #[must_use]
    pub fn uniform_parts(mut self, on: bool) -> Self {
        self.uniform_parts = on;
        self
    }

    /// # Errors
    /// Returns [`Error::Backend`] when [`client`](Self::client) was never
    /// called.
    pub fn build(self) -> Result<Backend, Error> {
        let client = self
            .client
            .ok_or_else(|| Error::Backend("s3::Backend requires a client".to_string()))?;
        Ok(Backend {
            client,
            part_size: self.part_size,
            uniform_parts: self.uniform_parts,
        })
    }
}

/// S3 [`ObjectStore`] (+ conditional, presign) implementation.
#[derive(Clone, Debug)]
pub struct Backend {
    pub client: Arc<S3Backend>,
    pub part_size: u64,
    pub uniform_parts: bool,
}

impl Backend {
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
    }
}

#[async_trait]
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        Ok(self.client.read(key).await?)
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let result = self.client.get_object(key, offset).await?;
        // S3 returns the content-length of the (possibly ranged) response;
        // the trait contract requires the **total** object size.
        let total = result.content_length + offset.unwrap_or(0);
        Ok((result.body, total))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        Ok(self.client.put_object(key, data).await?)
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        // S3 DELETE on a missing key returns 204, mapped to Ok by the client.
        Ok(self.client.delete(key).await?)
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        // Normalise to a directory boundary (see `dir_prefix`) so the underlying
        // raw list-prefix delete cannot wipe keys that merely share a string
        // prefix (e.g. "tags/v1" must not delete "tags/v1-rc/..."). An empty
        // prefix is a no-op — it must not delete every object under the bucket.
        let effective_prefix = dir_prefix(prefix);
        if effective_prefix.is_empty() {
            return Ok(());
        }
        Ok(self.client.delete_prefix(effective_prefix.as_ref()).await?)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        let (size, etag, last_modified) = self.client.head_object(key).await?;
        Ok(ObjectMeta {
            size,
            etag: etag.map(Etag::new),
            last_modified,
        })
    }

    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        let (items, next_token) = self.client.list_objects(prefix, n, token).await?;
        Ok(Page { items, next_token })
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        let (sub_prefixes, objects, next_token) = self
            .client
            .list_prefixes(prefix, "/", n, token, start_after)
            .await?;
        Ok(ChildrenPage {
            sub_prefixes,
            objects,
            next_token,
        })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error> {
        Ok(self.client.copy_object(source, destination).await?)
    }

    // ── Upload sessions ─────────────────────────────────────────────────────

    async fn create_upload(&self, key: &str) -> Result<UploadSession, Error> {
        Ok(UploadSession {
            key: key.to_string(),
            uploaded_size: 0,
            state: SessionState::S3 {
                upload_id: None,
                parts: Vec::new(),
                staged_size: 0,
            },
        })
    }

    async fn write_upload(
        &self,
        session: &mut UploadSession,
        staged_dir: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), Error> {
        if len == 0 {
            return Ok(());
        }
        let staged_size = s3_state(&session.state)?.2;

        // The current remainder (if any) sits at `<staged_dir>/<offset>` where
        // `offset` is the session's `uploaded_size` — the value at the time the
        // previous call staged it, before this chunk is counted.
        let read_offset = session.uploaded_size;
        let read_key = staged_key(staged_dir, read_offset);
        let staged_bytes = load_staged(&self.client, &read_key, staged_size).await?;
        let staged_len =
            u64::try_from(staged_bytes.len()).map_err(|e| Error::Backend(e.to_string()))?;

        let combined = chain_staged_with_body(staged_bytes, body);
        let mut reader = StreamReader::new(combined);

        let available = staged_len + len;

        // Non-uniform mode flushes once the combined bytes reach the
        // operator-configured `part_size`, but never below the S3 5 MiB floor
        // (a smaller non-final `UploadPart` would be rejected).
        let nonuniform_threshold = self.part_size.max(MIN_PART_SIZE);
        let (parts_to_emit, emit_size, restaged) = if self.uniform_parts {
            let part_size = self.part_size;
            let full = available / part_size;
            (full, part_size, available - full * part_size)
        } else if available >= nonuniform_threshold {
            (1u64, available, 0u64)
        } else {
            (0u64, 0u64, available)
        };

        for _ in 0..parts_to_emit {
            let part_number = {
                let (_, parts, _) = s3_state(&session.state)?;
                next_part_number(parts)?
            };
            let upload_id =
                ensure_upload_id(&self.client, &mut session.state, &session.key).await?;
            let etag = stream_part(
                &self.client,
                &session.key,
                &upload_id,
                part_number,
                emit_size,
                &mut reader,
            )
            .await?;
            let (_, parts, _) = s3_state_mut(&mut session.state)?;
            parts.push(Part {
                part_number,
                etag: Etag::new(etag),
                size: emit_size,
            });
        }

        // The new remainder is staged at the post-write offset; the previous
        // staged file (at `read_offset`) is removed once superseded, so at most
        // one staged file exists per session.
        let write_offset = read_offset
            .checked_add(len)
            .ok_or_else(|| Error::Backend("session size overflow".to_string()))?;
        let (_, _, staged_size_field) = s3_state_mut(&mut session.state)?;
        if restaged > 0 {
            let mut remainder = Vec::with_capacity(
                usize::try_from(restaged).map_err(|e| Error::Backend(e.to_string()))?,
            );
            (&mut reader)
                .take(restaged)
                .read_to_end(&mut remainder)
                .await?;
            let actual =
                u64::try_from(remainder.len()).map_err(|e| Error::Backend(e.to_string()))?;
            if actual != restaged {
                return Err(Error::Backend(format!(
                    "short read while restaging: expected {restaged}, got {actual}",
                )));
            }
            let write_key = staged_key(staged_dir, write_offset);
            self.client
                .put_object(&write_key, Bytes::from(remainder))
                .await?;
            if staged_len > 0 {
                let _ = self.client.delete(&read_key).await;
            }
            *staged_size_field = restaged;
        } else if staged_len > 0 {
            let _ = self.client.delete(&read_key).await;
            *staged_size_field = 0;
        }

        session.uploaded_size = write_offset;
        Ok(())
    }

    async fn complete_upload(
        &self,
        mut session: UploadSession,
        staged_dir: &str,
    ) -> Result<(), Error> {
        let (upload_id_opt, _, staged_size) = s3_state(&session.state)?;
        let upload_id_opt = upload_id_opt.clone();
        // The final remainder sits at the last write offset == `uploaded_size`.
        let read_key = staged_key(staged_dir, session.uploaded_size);

        match (upload_id_opt, staged_size) {
            (None, 0) => {
                self.client.put_object(&session.key, Bytes::new()).await?;
            }
            (None, _) => {
                // Small upload — promote the staged remainder to the canonical
                // key and clean up.
                self.client.copy_object(&read_key, &session.key).await?;
                let _ = self.client.delete(&read_key).await;
            }
            (Some(upload_id), staged) => {
                if staged > 0 {
                    let part_number = {
                        let (_, parts, _) = s3_state(&session.state)?;
                        next_part_number(parts)?
                    };
                    let data = self.client.read(&read_key).await?;
                    let len =
                        u64::try_from(data.len()).map_err(|e| Error::Backend(e.to_string()))?;
                    let body = Bytes::from(data);
                    let body_stream: ByteStream = Box::pin(stream::once(async move { Ok(body) }));
                    let etag = self
                        .client
                        .upload_part_streaming(
                            &session.key,
                            &upload_id,
                            part_number,
                            len,
                            body_stream,
                        )
                        .await?;
                    let (_, parts, staged_field) = s3_state_mut(&mut session.state)?;
                    parts.push(Part {
                        part_number,
                        etag: Etag::new(etag),
                        size: len,
                    });
                    *staged_field = 0;
                    let _ = self.client.delete(&read_key).await;
                }

                let (_, parts, _) = s3_state(&session.state)?;
                let view: Vec<UploadedPart> = parts
                    .iter()
                    .map(|p| UploadedPart {
                        part_number: p.part_number,
                        e_tag: p.etag.as_str().to_string(),
                        size: p.size,
                    })
                    .collect();
                self.client
                    .complete_multipart_upload(&session.key, &upload_id, &view)
                    .await?;
            }
        }

        Ok(())
    }

    async fn abort_upload(&self, session: UploadSession, staged_dir: &str) -> Result<(), Error> {
        let (upload_id_opt, _, _) = s3_state(&session.state)?;
        if let Some(id) = upload_id_opt {
            let _ = self.client.abort_multipart_upload(&session.key, id).await;
        }
        let read_key = staged_key(staged_dir, session.uploaded_size);
        let _ = self.client.delete(&read_key).await;
        Ok(())
    }

    async fn abort_pending_uploads(&self, key: &str) -> Result<(), Error> {
        // Track aborted upload-ids and stop once a listing turns up nothing we
        // have not already aborted. This keeps the sweep bounded even if S3's
        // eventually-consistent listing keeps returning an upload we just
        // aborted, instead of spinning forever.
        let mut aborted: HashSet<String> = HashSet::new();
        loop {
            let (uploads, _, _) = self
                .client
                .list_multipart_uploads(Some(key), None, None)
                .await?;
            let pending: Vec<_> = uploads
                .into_iter()
                .filter(|u| u.key == key && !aborted.contains(&u.upload_id))
                .collect();
            if pending.is_empty() {
                break;
            }
            for u in pending {
                self.client
                    .abort_multipart_upload(&u.key, &u.upload_id)
                    .await?;
                aborted.insert(u.upload_id);
            }
        }
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, Error> {
        let (uploads, next_key_marker, next_upload_id_marker) = self
            .client
            .list_multipart_uploads(None, key_marker, upload_id_marker)
            .await?;
        Ok(MultipartUploadPage {
            uploads: uploads
                .into_iter()
                .map(|u| PendingMultipartUpload {
                    key: u.key,
                    upload_id: u.upload_id,
                    initiated_at: u.initiated_at,
                })
                .collect(),
            next_key_marker,
            next_upload_id_marker,
        })
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<(), Error> {
        self.client
            .abort_multipart_upload(key, upload_id)
            .await
            .map_err(Error::from)
    }
}

#[async_trait]
impl ConditionalStore for Backend {
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), Error> {
        let (body, etag) = self.client.read_with_etag(key).await?;
        Ok((body, etag.map(Etag::new)))
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, Option<Etag>, Option<DateTime<Utc>>), Error> {
        let (body, etag, last_modified) = self.client.read_with_metadata(key).await?;
        Ok((body, etag.map(Etag::new), last_modified))
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, Error> {
        let etag = self.client.put_object_if_not_exists(key, data).await?;
        Ok(etag.map(Etag::new))
    }

    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, Error> {
        let etag = self
            .client
            .put_object_if_match(key, etag.as_str(), data)
            .await?;
        Ok(etag.map(Etag::new))
    }

    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), Error> {
        Ok(self.client.delete_if_match(key, etag.as_str()).await?)
    }
}

#[async_trait]
impl PresignedStore for Backend {
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        content_type: Option<&str>,
    ) -> Result<String, Error> {
        Ok(self
            .client
            .generate_presigned_url(key, ttl, content_type)
            .await?)
    }
}

fn s3_state(state: &SessionState) -> Result<(&Option<String>, &[Part], u64), Error> {
    match state {
        SessionState::S3 {
            upload_id,
            parts,
            staged_size,
        } => Ok((upload_id, parts.as_slice(), *staged_size)),
        SessionState::Fs => Err(Error::Backend(
            "upload session is not an S3 session".to_string(),
        )),
    }
}

fn s3_state_mut(
    state: &mut SessionState,
) -> Result<(&mut Option<String>, &mut Vec<Part>, &mut u64), Error> {
    match state {
        SessionState::S3 {
            upload_id,
            parts,
            staged_size,
        } => Ok((upload_id, parts, staged_size)),
        SessionState::Fs => Err(Error::Backend(
            "upload session is not an S3 session".to_string(),
        )),
    }
}

fn next_part_number(parts: &[Part]) -> Result<u32, Error> {
    u32::try_from(parts.len() + 1).map_err(|e| Error::Backend(e.to_string()))
}

/// Storage key for the sub-part remainder staged at `offset` bytes under
/// `staged_dir` (the `<staged_dir>/<offset>` layout). `offset` is the
/// session's `uploaded_size` at the moment the remainder was written, so the
/// next call recomputes the same key for read-back.
fn staged_key(staged_dir: &str, offset: u64) -> String {
    format!("{staged_dir}/{offset}")
}

async fn load_staged(client: &S3Backend, staging: &str, expected: u64) -> Result<Vec<u8>, Error> {
    if expected == 0 {
        return Ok(Vec::new());
    }
    match client.read(staging).await {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(Error::Backend(e.to_string())),
    }
}

fn chain_staged_with_body(staged: Vec<u8>, body: ByteStream) -> ByteStream {
    if staged.is_empty() {
        return body;
    }
    let head = stream::once(async move { Ok(Bytes::from(staged)) });
    Box::pin(head.chain(body))
}

async fn ensure_upload_id(
    client: &S3Backend,
    state: &mut SessionState,
    key: &str,
) -> Result<String, Error> {
    let (upload_id, _, _) = s3_state_mut(state)?;
    if let Some(id) = upload_id {
        return Ok(id.clone());
    }
    let new_id = client.create_multipart_upload(key).await?;
    *upload_id = Some(new_id.clone());
    Ok(new_id)
}

async fn stream_part<R>(
    client: &S3Backend,
    key: &str,
    upload_id: &str,
    part_number: u32,
    size: u64,
    source: &mut R,
) -> Result<String, Error>
where
    R: AsyncRead + Unpin + Send,
{
    let (tx, rx) = mpsc::channel::<Bytes>(FRAME_BUFFER_CAPACITY);
    let body = channel_stream(rx);

    let upload_handle = AbortOnDropHandle::new(tokio::spawn({
        let client = client.clone();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        async move {
            client
                .upload_part_streaming(&key, &upload_id, part_number, size, body)
                .await
        }
    }));

    let mut sent: u64 = 0;
    let mut buf = BytesMut::with_capacity(FRAME_SIZE);
    while sent < size {
        let want = (size - sent).min(FRAME_SIZE as u64);
        buf.clear();
        let n = {
            let mut limited = source.take(want);
            limited.read_buf(&mut buf).await?
        };
        if n == 0 {
            return Err(Error::Backend(format!(
                "short read for part {part_number}: expected {size}, got {sent}",
            )));
        }
        sent = sent
            .checked_add(u64::try_from(n).map_err(|e| Error::Backend(e.to_string()))?)
            .ok_or_else(|| Error::Backend("part size overflow".to_string()))?;
        if tx.send(buf.split().freeze()).await.is_err() {
            return match upload_handle.await {
                Ok(Ok(_)) => Err(Error::Backend(
                    "upload task stopped before receiving full part body".to_string(),
                )),
                Ok(Err(e)) => Err(e.into()),
                Err(e) => Err(Error::Backend(e.to_string())),
            };
        }
    }
    drop(tx);
    match upload_handle.await {
        Ok(Ok(etag)) => Ok(etag),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => Err(Error::Backend(e.to_string())),
    }
}

#[cfg(test)]
mod tests;
