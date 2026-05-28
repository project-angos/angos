use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use futures_util::stream;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{instrument, warn};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            Error,
            hashing_reader::HashingReader,
            s3::{
                Backend, FRAME_BUFFER_CAPACITY, FRAME_SIZE, MIN_PART_SIZE,
                multipart_helpers::{
                    CompletionPlan, UploadMode, next_part_number,
                    uploaded_size as total_uploaded_size,
                },
            },
        },
        path_builder,
    },
};
use angos_storage::{
    Error as StorageError, Etag, MultipartStore, ObjectStore, Part, UploadId, channel_stream,
};

struct FlushContext<'a> {
    upload_path: &'a str,
    upload_id: &'a str,
    part_number: u32,
    pending_path: &'a str,
    pending_size: u64,
    uploaded_size: u64,
    available: u64,
}

struct BufferContext<'a> {
    pending_path: &'a str,
    uploaded_size: u64,
    pending_size: u64,
}

type StreamingUploadHandle = AbortOnDropHandle<Result<Etag, StorageError>>;

impl Backend {
    pub async fn resolve_nonuniform_state(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<(String, Vec<Part>, u64, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        let (id, parts) = match self.read_session(name, uuid).await {
            Ok(record) => match record.multipart_upload_id {
                Some(id) => (id, record.parts),
                None => self.ensure_multipart_upload(&upload_path).await?,
            },
            Err(Error::UploadNotFound) => self.ensure_multipart_upload(&upload_path).await?,
            Err(e) => return Err(e),
        };
        let uploaded = total_uploaded_size(&parts);
        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending = self.store.head(&pending_path).await.map_or(0, |m| m.size);
        Ok((id, parts, uploaded, pending))
    }

    async fn load_pending_bytes(&self, pending_path: &str, pending_size: u64) -> Vec<u8> {
        if pending_size > 0 {
            self.store.get(pending_path).await.unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    fn spawn_nonuniform_upload_part(
        &self,
        ctx: &FlushContext<'_>,
    ) -> (mpsc::Sender<Bytes>, StreamingUploadHandle) {
        let (tx, rx) = mpsc::channel::<Bytes>(FRAME_BUFFER_CAPACITY);

        let store = self.store.clone();
        let upload_path = ctx.upload_path.to_string();
        let upload_id_str = ctx.upload_id.to_string();
        let part_number = ctx.part_number;
        let available = ctx.available;
        let upload_handle = AbortOnDropHandle::new(tokio::spawn(async move {
            let id = UploadId::new(&upload_id_str);
            store
                .upload_part(
                    &upload_path,
                    &id,
                    part_number,
                    available,
                    channel_stream(rx),
                )
                .await
        }));

        (tx, upload_handle)
    }

    async fn flush_pending_as_part(
        &self,
        name: &str,
        uuid: &str,
        ctx: FlushContext<'_>,
        pending_bytes: Vec<u8>,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    ) -> Result<(Digest, u64), Error> {
        let hasher = self.load_hasher(name, uuid, ctx.uploaded_size).await?;

        let combined: Box<dyn AsyncRead + Unpin + Send + Sync> = if pending_bytes.is_empty() {
            Box::new(stream)
        } else {
            Box::new(Cursor::new(pending_bytes).chain(stream))
        };

        let mut hashing_reader = HashingReader::with_hasher(combined, hasher);
        let (tx, upload_handle) = self.spawn_nonuniform_upload_part(&ctx);

        let mut buf = BytesMut::with_capacity(FRAME_SIZE);
        let mut sent = 0;
        loop {
            buf.clear();
            let n = hashing_reader
                .read_buf(&mut buf)
                .await
                .map_err(|e| Error::UploadBodyRead(e.to_string()))?;
            if n == 0 {
                break;
            }
            let new_sent = sent + u64::try_from(n)?;
            if new_sent > ctx.available {
                warn!(
                    "upload body size mismatch for '{name}/{uuid}': expected {} bytes, read {new_sent}",
                    ctx.available,
                );
                return Err(Error::UploadBodySize {
                    expected: ctx.available,
                    actual: new_sent,
                });
            }
            sent = new_sent;
            if tx.send(buf.split().freeze()).await.is_err() {
                return match upload_handle.await {
                    Ok(Ok(_etag)) => Err(Error::StorageBackend(
                        "upload task stopped before receiving complete body".to_string(),
                    )),
                    Ok(Err(error)) => Err(error.into()),
                    Err(join_error) => Err(self
                        .handle_upload_task_failure(
                            UploadMode::Nonuniform,
                            name,
                            uuid,
                            ctx.upload_path,
                            ctx.upload_id,
                            join_error,
                        )
                        .await),
                };
            }
        }
        drop(tx);
        if sent != ctx.available {
            warn!(
                "upload body size mismatch for '{name}/{uuid}': expected {} bytes, read {sent}",
                ctx.available,
            );
            return Err(Error::UploadBodySize {
                expected: ctx.available,
                actual: sent,
            });
        }

        // Storage errors keep the multipart session retryable; task failure leaves state unknown.
        match upload_handle.await {
            Ok(Ok(_etag)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(join_error) => {
                return Err(self
                    .handle_upload_task_failure(
                        UploadMode::Nonuniform,
                        name,
                        uuid,
                        ctx.upload_path,
                        ctx.upload_id,
                        join_error,
                    )
                    .await);
            }
        }

        if ctx.pending_size > 0 {
            self.store.delete(ctx.pending_path).await?;
        }

        let new_size = ctx.uploaded_size + ctx.available;
        self.save_hasher(name, uuid, new_size, hashing_reader.serialized_state())
            .await?;

        let digest = hashing_reader.digest();
        Ok((digest, new_size))
    }

    async fn buffer_pending(
        &self,
        name: &str,
        uuid: &str,
        ctx: BufferContext<'_>,
        pending_bytes: Vec<u8>,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    ) -> Result<(Digest, u64), Error> {
        let logical_offset = ctx.uploaded_size + ctx.pending_size;
        let hasher = self.load_hasher(name, uuid, logical_offset).await?;

        let mut buf = pending_bytes;
        let original_len = buf.len();
        let mut hashing_reader = HashingReader::with_hasher(stream, hasher);
        hashing_reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| Error::UploadBodyRead(e.to_string()))?;

        let new_logical = ctx.uploaded_size + u64::try_from(buf.len())?;
        if buf.len() == original_len {
            let digest = hashing_reader.digest();
            return Ok((digest, new_logical));
        }

        self.store.put(ctx.pending_path, Bytes::from(buf)).await?;

        self.save_hasher(name, uuid, new_logical, hashing_reader.serialized_state())
            .await?;

        let digest = hashing_reader.digest();
        Ok((digest, new_logical))
    }

    #[instrument(skip(self, stream))]
    pub async fn write_upload_nonuniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
    ) -> Result<(Digest, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);

        let (upload_id, part_list, uploaded_size, pending_size) =
            self.resolve_nonuniform_state(name, uuid).await?;

        let next_part_number = next_part_number(part_list.len())?;

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let available = pending_size + content_length;
        let pending_bytes = self.load_pending_bytes(&pending_path, pending_size).await;

        if should_flush_pending(pending_size, content_length, MIN_PART_SIZE) {
            self.flush_pending_as_part(
                name,
                uuid,
                FlushContext {
                    upload_path: &upload_path,
                    upload_id: &upload_id,
                    part_number: next_part_number,
                    pending_path: &pending_path,
                    pending_size,
                    uploaded_size,
                    available,
                },
                pending_bytes,
                stream,
            )
            .await
        } else {
            self.buffer_pending(
                name,
                uuid,
                BufferContext {
                    pending_path: &pending_path,
                    uploaded_size,
                    pending_size,
                },
                pending_bytes,
                stream,
            )
            .await
        }
    }

    /// Prepare the nonuniform upload for the transactional commit step.
    ///
    /// Flushes any pending buffered data as a final multipart part, then
    /// completes the S3 multipart upload so the assembled object lands at
    /// `upload_path`. Returns a [`CompletionPlan`] that tells
    /// `complete_via_session` which `Mutation` to use in the engine
    /// transaction (always `Move` for the nonuniform path).
    ///
    /// The upload-container cleanup is **not** performed here; it happens
    /// after the transaction commits in `complete_via_session`.
    #[instrument(skip(self))]
    pub async fn prepare_complete_upload_nonuniform(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<CompletionPlan, Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        let session_upload = match self.read_session(name, uuid).await {
            Ok(record) => record.multipart_upload_id.map(|id| (id, record.parts)),
            Err(Error::UploadNotFound) => None,
            Err(e) => return Err(e),
        };
        let (upload_id, mut parts) = if let Some(pair) = session_upload {
            pair
        } else {
            let (id, parts) = self.discover_multipart_upload(&upload_path).await?;
            (id.ok_or(Error::UploadNotFound)?, parts)
        };
        let mut uploaded_size = total_uploaded_size(&parts);

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending_size = self.store.head(&pending_path).await.map_or(0, |m| m.size);
        if pending_size > 0 {
            let pending_data = self.store.get(&pending_path).await?;
            let next_part = next_part_number(parts.len())?;
            let id = UploadId::new(&upload_id);
            let data = Bytes::from(pending_data);
            let len = u64::try_from(data.len())?;
            let etag = self
                .store
                .upload_part(
                    &upload_path,
                    &id,
                    next_part,
                    len,
                    Box::pin(stream::once(async move { Ok(data) })),
                )
                .await?;
            parts.push(Part {
                part_number: next_part,
                etag,
                size: pending_size,
            });
            uploaded_size += pending_size;
        }

        let digest = self
            .resolve_upload_digest(name, uuid, uploaded_size, digest)
            .await?;

        self.complete_multipart_upload(&upload_path, &upload_id, &parts)
            .await?;

        Ok(CompletionPlan::Move {
            src: upload_path,
            digest,
        })
    }
}

/// Returns `true` when the combined pending and incoming bytes meet or exceed
/// `min_part_size`, meaning the data can be flushed immediately as a multipart
/// part rather than accumulated in the pending buffer.
fn should_flush_pending(pending_size: u64, content_length: u64, min_part_size: u64) -> bool {
    pending_size + content_length >= min_part_size
}

#[cfg(test)]
mod tests {
    use super::should_flush_pending;

    const MIN: u64 = 5 * 1024 * 1024;

    // --- should_flush_pending ---

    #[test]
    fn flushes_when_combined_meets_min_part_size() {
        assert!(should_flush_pending(0, MIN, MIN));
    }

    #[test]
    fn flushes_when_combined_exceeds_min_part_size() {
        assert!(should_flush_pending(1, MIN, MIN));
    }

    #[test]
    fn buffers_when_combined_is_below_min_part_size() {
        assert!(!should_flush_pending(0, MIN - 1, MIN));
    }

    #[test]
    fn flushes_when_pending_alone_meets_min_part_size() {
        assert!(should_flush_pending(MIN, 0, MIN));
    }

    #[test]
    fn buffers_empty_content_with_zero_pending() {
        assert!(!should_flush_pending(0, 0, MIN));
    }

    #[test]
    fn flushes_when_pending_and_content_together_reach_threshold() {
        let half = MIN / 2;
        assert!(should_flush_pending(half, MIN - half, MIN));
    }
}
