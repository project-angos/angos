//! S3-backed implementations of every capability trait.
//!
//! Wraps [`angos_s3_client::Backend`] so consumers get the storage abstraction
//! without depending on the HTTP/S3 layer directly. The wrapper translates
//! `s3_client::Error` and `io::Error` into [`crate::Error`], adapts S3's
//! flat/delimited listing modes to [`Page`](crate::Page) /
//! [`ChildrenPage`](crate::ChildrenPage), and forwards every conditional and
//! presign operation through unchanged.
//!
//! The [`ObjectStore`] upload methods layer the keyed, append-only upload
//! primitive on top of the S3 multipart protocol. They are *keyless*: every
//! call recovers the upload's state (the in-flight upload id, the committed
//! parts, the staged sub-part remainder) from S3 itself, so nothing has to be
//! persisted by the caller between calls.
//!
//! - `create_upload` clears any leaked prior upload at `key` (`abort_upload`
//!   semantics) and returns. It is lazy: no `CreateMultipartUpload` round-trip
//!   happens until the first flush actually needs one. Small uploads
//!   (≤ `part_size`) never open a multipart upload at all.
//! - `write_upload` recovers the upload id (searching `ListMultipartUploads`
//!   for `key`) and the committed parts (`ListParts`) to compute the next part
//!   number and the committed byte offset, HEADs the staged remainder at that
//!   offset, combines it with the incoming stream, and emits `UploadPart`s of
//!   up to `part_size` bytes (uniform mode) or one `UploadPart` of all
//!   available bytes once they meet `part_size` (non-uniform mode). The new
//!   remainder is restaged at the new offset and the superseded staged object
//!   deleted. Returns the new total size (`sum(parts) + remainder`).
//! - `complete_upload` recovers the same state, then: with no upload id and no
//!   remainder it `PutObject(empty)`s `key`; with no upload id but a remainder
//!   it `copy_object`s the staged remainder to `key`; otherwise it flushes the
//!   final remainder as the last part and `CompleteMultipartUpload`s, then
//!   cleans up the staged objects.
//! - `abort_upload` searches every in-flight multipart upload at `key` and
//!   aborts each, then deletes any staged remainder under the key's staging
//!   container. Idempotent.
//!
//! The staged remainder lives at a `staged/<offset>` child of the upload key's
//! container, derived from `key` alone by replacing the key's final path
//! segment with `staged/<offset>` (so `.../<uuid>/data` stages at
//! `.../<uuid>/staged/<offset>`), matching the historical layout.
//!
//! Bodies are streamed end-to-end via an mpsc channel handed to
//! `upload_part_streaming`: no part ever sits whole in process memory.

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
    ObjectMeta, ObjectStore, Page, PendingMultipartUpload, PresignedStore, channel_stream,
    object::dir_prefix,
};

pub const DEFAULT_PART_SIZE: u64 = 5 * 1024 * 1024;

const FRAME_SIZE: usize = 1024 * 1024;
const FRAME_BUFFER_CAPACITY: usize = 8;
/// S3 protocol floor for non-final multipart parts: a part must be at least
/// 5 MiB before it can be flushed as an `UploadPart`.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Builder for [`Backend`]. The S3 HTTP client is required and supplied to
/// [`Backend::builder`]; `part_size` and `uniform_parts` are optional fluent
/// setters.
pub struct Builder {
    client: Arc<S3Backend>,
    part_size: u64,
    uniform_parts: bool,
}

impl Builder {
    fn new(client: Arc<S3Backend>) -> Self {
        Self {
            client,
            part_size: DEFAULT_PART_SIZE,
            uniform_parts: false,
        }
    }

    /// Target part size for upload sessions (uniform mode) or minimum part
    /// size before flushing (non-uniform mode). Defaults to 5 MiB, the
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

    /// Consume the builder and produce the [`Backend`].
    #[must_use]
    pub fn build(self) -> Backend {
        Backend {
            client: self.client,
            part_size: self.part_size,
            uniform_parts: self.uniform_parts,
        }
    }
}

/// S3 [`ObjectStore`] (+ upload, conditional, presign) implementation.
#[derive(Clone, Debug)]
pub struct Backend {
    pub client: Arc<S3Backend>,
    pub part_size: u64,
    pub uniform_parts: bool,
}

impl Backend {
    /// Return a builder wrapping the underlying S3 HTTP `client` (construct it
    /// with `angos_s3_client::Backend::new(&config)`). Other settings are
    /// optional fluent setters on the returned builder.
    #[must_use]
    pub fn builder(client: Arc<S3Backend>) -> Builder {
        Builder::new(client)
    }

    /// Recover an upload's in-flight state from S3: the open multipart upload id
    /// (if any) and the list of committed parts. State is read fresh on every
    /// call; nothing is persisted by the caller.
    async fn recover_upload(&self, key: &str) -> Result<RecoveredUpload, Error> {
        let upload_id = self.search_upload_id(key).await?;
        let parts = match &upload_id {
            Some(id) => self.client.list_parts(key, id).await?,
            None => Vec::new(),
        };
        Ok(RecoveredUpload { upload_id, parts })
    }

    /// Search the in-flight multipart uploads for the one whose key is exactly
    /// `key`, paging through `ListMultipartUploads` until found or exhausted.
    async fn search_upload_id(&self, key: &str) -> Result<Option<String>, Error> {
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        loop {
            let (uploads, next_key, next_upload_id) = self
                .client
                .list_multipart_uploads(
                    Some(key),
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                )
                .await?;
            for upload in uploads {
                if upload.key == key {
                    return Ok(Some(upload.upload_id));
                }
            }
            if next_key.is_none() {
                return Ok(None);
            }
            key_marker = next_key;
            upload_id_marker = next_upload_id;
        }
    }

    /// HEAD the staged remainder at `offset` for its size, returning 0 when no
    /// staged object exists there.
    async fn staged_size(&self, key: &str, offset: u64) -> Result<u64, Error> {
        match self.client.object_size(&staged_key(key, offset)).await {
            Ok(size) => Ok(size),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(Error::Backend(e.to_string())),
        }
    }
}

/// In-flight multipart state recovered from S3 by [`Backend::recover_upload`].
struct RecoveredUpload {
    upload_id: Option<String>,
    parts: Vec<UploadedPart>,
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
        // prefix is a no-op: it must not delete every object under the bucket.
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

    async fn create_upload(&self, key: &str) -> Result<(), Error> {
        // Clear any leaked prior in-progress upload at this key so a re-`create`
        // at a reused key starts clean. Lazy otherwise: no multipart opened.
        self.abort_upload(key).await
    }

    async fn write_upload(&self, key: &str, body: ByteStream, len: u64) -> Result<u64, Error> {
        // Recover the in-flight state from S3: the upload id (if a multipart is
        // already open), the committed parts, and from them the committed byte
        // offset and the next part number.
        let recovered = self.recover_upload(key).await?;
        let mut upload_id = recovered.upload_id;
        let mut parts = recovered.parts;
        let committed_size = parts.iter().map(|p| p.size).sum::<u64>();

        if len == 0 {
            // Nothing to append; total is whatever is already committed plus the
            // staged remainder at the committed offset.
            let staged_len = self.staged_size(key, committed_size).await?;
            return committed_size
                .checked_add(staged_len)
                .ok_or_else(|| Error::Backend("upload size overflow".to_string()));
        }

        // The current remainder (if any) sits at `staged/<committed_size>`. HEAD
        // it for its size, then read it back to combine with the incoming body.
        let read_key = staged_key(key, committed_size);
        let staged_len = self.staged_size(key, committed_size).await?;
        let staged_bytes = load_staged(&self.client, &read_key, staged_len).await?;
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
            let part_number = next_part_number(&parts)?;
            // Open the multipart lazily, only when a part actually needs
            // flushing.
            let id = ensure_upload_id(&self.client, &mut upload_id, key).await?;
            let etag =
                stream_part(&self.client, key, &id, part_number, emit_size, &mut reader).await?;
            parts.push(UploadedPart {
                part_number,
                e_tag: etag,
                size: emit_size,
            });
        }

        // The new remainder is staged at the new committed offset; the previous
        // staged file (at `committed_size`) is removed once superseded, so at
        // most one staged file exists per upload.
        let new_committed = parts.iter().map(|p| p.size).sum::<u64>();
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
            let write_key = staged_key(key, new_committed);
            self.client
                .put_object(&write_key, Bytes::from(remainder))
                .await?;
            if staged_len > 0 && write_key != read_key {
                let _ = self.client.delete(&read_key).await;
            }
        } else if staged_len > 0 {
            let _ = self.client.delete(&read_key).await;
        }

        new_committed
            .checked_add(restaged)
            .ok_or_else(|| Error::Backend("upload size overflow".to_string()))
    }

    async fn complete_upload(&self, key: &str) -> Result<(), Error> {
        let recovered = self.recover_upload(key).await?;
        let upload_id = recovered.upload_id;
        let mut parts = recovered.parts;
        let committed_size = parts.iter().map(|p| p.size).sum::<u64>();

        // The final remainder sits at the committed offset.
        let read_key = staged_key(key, committed_size);
        let staged_len = self.staged_size(key, committed_size).await?;

        match (upload_id, staged_len) {
            (None, 0) => {
                self.client.put_object(key, Bytes::new()).await?;
            }
            (None, _) => {
                // Small upload: promote the staged remainder to the canonical
                // key and clean up.
                self.client.copy_object(&read_key, key).await?;
                let _ = self.client.delete(&read_key).await;
            }
            (Some(upload_id), staged) => {
                if staged > 0 {
                    let part_number = next_part_number(&parts)?;
                    let data = self.client.read(&read_key).await?;
                    let part_len =
                        u64::try_from(data.len()).map_err(|e| Error::Backend(e.to_string()))?;
                    let body = Bytes::from(data);
                    let body_stream: ByteStream = Box::pin(stream::once(async move { Ok(body) }));
                    let etag = self
                        .client
                        .upload_part_streaming(key, &upload_id, part_number, part_len, body_stream)
                        .await?;
                    parts.push(UploadedPart {
                        part_number,
                        e_tag: etag,
                        size: part_len,
                    });
                }

                self.client
                    .complete_multipart_upload(key, &upload_id, &parts)
                    .await?;
                let _ = self.client.delete(&read_key).await;
            }
        }

        Ok(())
    }

    async fn abort_upload(&self, key: &str) -> Result<(), Error> {
        // Abort every in-flight multipart upload at `key`. Track aborted
        // upload-ids and stop once a listing turns up nothing new, so the sweep
        // stays bounded even under S3's eventually-consistent listing.
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
        // Delete any staged remainder objects under the key's staging container.
        let _ = self.client.delete_prefix(&staged_container(key)).await;
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

fn next_part_number(parts: &[UploadedPart]) -> Result<u32, Error> {
    u32::try_from(parts.len() + 1).map_err(|e| Error::Backend(e.to_string()))
}

/// The staging container for the upload at `key`: the key's final path segment
/// (e.g. `data`) replaced with `staged`. So `.../<uuid>/data` stages under
/// `.../<uuid>/staged`, matching the historical layout. A key with no `/`
/// stages under a sibling `staged` directory at the store root.
fn staged_container(key: &str) -> String {
    match key.rfind('/') {
        Some(idx) => format!("{}/staged", &key[..idx]),
        None => "staged".to_string(),
    }
}

/// Storage key for the sub-part remainder staged at `offset` bytes. `offset` is
/// the committed byte count (`sum(parts)`) at the moment the remainder is
/// written, so the next call recomputes the same key for read-back from the
/// recovered parts list.
fn staged_key(key: &str, offset: u64) -> String {
    format!("{}/{offset}", staged_container(key))
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
    upload_id: &mut Option<String>,
    key: &str,
) -> Result<String, Error> {
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
