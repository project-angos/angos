//! S3 [`UploadSessionStore`] implementation.
//!
//! Layers the upload-session primitive on top of the S3 multipart protocol:
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

use std::{collections::HashSet, io};

use angos_s3_client::{Backend as S3Backend, UploadedPart};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::{io::StreamReader, task::AbortOnDropHandle};

use crate::{
    ByteStream, Error, Etag, Part, SessionState, UploadSession, UploadSessionStore, channel_stream,
    s3::Backend,
};

const FRAME_SIZE: usize = 1024 * 1024;
const FRAME_BUFFER_CAPACITY: usize = 8;
/// S3 protocol floor for non-final multipart parts: a part must be at least
/// 5 MiB before it can be flushed as an `UploadPart`.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

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

#[async_trait]
impl UploadSessionStore for Backend {
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
        staging_key: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), Error> {
        if len == 0 {
            return Ok(());
        }
        let staged_size = s3_state(&session.state)?.2;

        let staging = staging_key.to_string();
        let staged_bytes = load_staged(&self.client, &staging, staged_size).await?;
        let staged_len =
            u64::try_from(staged_bytes.len()).map_err(|e| Error::Backend(e.to_string()))?;

        let combined = chain_staged_with_body(staged_bytes, body);
        let mut reader = StreamReader::new(combined);

        let available = staged_len + len;

        let (parts_to_emit, emit_size, restaged) = if self.uniform_parts {
            let part_size = self.part_size;
            let full = available / part_size;
            (full, part_size, available - full * part_size)
        } else if available >= MIN_PART_SIZE {
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

        // Persist or drop the staging key based on what's left.
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
            self.client
                .put_object(&staging, Bytes::from(remainder))
                .await?;
            *staged_size_field = restaged;
        } else if staged_len > 0 {
            let _ = self.client.delete(&staging).await;
            *staged_size_field = 0;
        }

        session.uploaded_size = session
            .uploaded_size
            .checked_add(len)
            .ok_or_else(|| Error::Backend("session size overflow".to_string()))?;
        Ok(())
    }

    async fn complete_upload(
        &self,
        mut session: UploadSession,
        staging_key: &str,
    ) -> Result<(), Error> {
        let (upload_id_opt, _, staged_size) = s3_state(&session.state)?;
        let upload_id_opt = upload_id_opt.clone();
        let staging = staging_key.to_string();

        match (upload_id_opt, staged_size) {
            (None, 0) => {
                self.client.put_object(&session.key, Bytes::new()).await?;
            }
            (None, _) => {
                // Small upload — promote the staging key to the canonical key
                // and clean up.
                self.client.copy_object(&staging, &session.key).await?;
                let _ = self.client.delete(&staging).await;
            }
            (Some(upload_id), staged) => {
                if staged > 0 {
                    let part_number = {
                        let (_, parts, _) = s3_state(&session.state)?;
                        next_part_number(parts)?
                    };
                    let data = self.client.read(&staging).await?;
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
                    let _ = self.client.delete(&staging).await;
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

    async fn abort_upload(&self, session: UploadSession, staging_key: &str) -> Result<(), Error> {
        let (upload_id_opt, _, _) = s3_state(&session.state)?;
        if let Some(id) = upload_id_opt {
            let _ = self.client.abort_multipart_upload(&session.key, id).await;
        }
        let _ = self.client.delete(staging_key).await;
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
