use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
    task::JoinError,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, instrument, warn};

use super::{Backend, FRAME_SIZE, MIN_PART_SIZE, S3UploadState, UploadedPart};
use crate::{
    oci::Digest,
    registry::{
        blob_store::{Error, hashing_reader::HashingReader, sha256_ext::Sha256Ext},
        path_builder,
    },
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

impl Backend {
    pub async fn resolve_nonuniform_state(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<(String, Vec<UploadedPart>, u64, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        if let Some(S3UploadState {
            multipart_upload_id: Some(id),
            parts,
            pending_size,
            ..
        }) = self.retrieve_cached_upload_state(name, uuid).await
        {
            let uploaded: u64 = parts.iter().map(|p| p.size).sum();
            return Ok((id, parts, uploaded, pending_size));
        }
        let id = if let Some(id) = self.get_or_search_upload_id(&upload_path).await? {
            id
        } else {
            let id = self.store.create_multipart_upload(&upload_path).await?;
            self.cache_upload_id(&upload_path, &id).await;
            id
        };
        let parts = self.store.list_parts(&upload_path, &id).await?;
        let uploaded: u64 = parts.iter().map(|p| p.size).sum();
        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending = self.store.object_size(&pending_path).await.unwrap_or(0);
        Ok((id, parts, uploaded, pending))
    }

    async fn load_pending_bytes(&self, pending_path: &str, pending_size: u64) -> Vec<u8> {
        if pending_size > 0 {
            self.store
                .get_object_body(pending_path, None)
                .await
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    async fn handle_nonuniform_upload_task_failure(
        &self,
        name: &str,
        uuid: &str,
        ctx: &FlushContext<'_>,
        join_error: JoinError,
    ) -> Error {
        let kind = if join_error.is_panic() {
            "panicked"
        } else if join_error.is_cancelled() {
            "was cancelled"
        } else {
            "failed unexpectedly"
        };
        error!(
            "nonuniform upload task {kind} for '{name}/{uuid}'; aborting multipart upload {upload_id}: {join_error}",
            upload_id = ctx.upload_id,
        );
        if let Err(abort_err) = self
            .store
            .abort_multipart_upload(ctx.upload_path, ctx.upload_id)
            .await
        {
            warn!("abort_multipart_upload failed during cleanup of '{name}/{uuid}': {abort_err}");
        }
        self.evict_upload_id(ctx.upload_path).await;
        self.evict_upload_state(name, uuid).await;
        Error::StorageBackend(format!("upload task {kind}: {join_error}"))
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

        let (tx, rx) = mpsc::channel::<Bytes>(32);

        let store = self.store.clone();
        let upload_path_owned = ctx.upload_path.to_string();
        let upload_id_owned = ctx.upload_id.to_string();
        let upload_handle = AbortOnDropHandle::new(tokio::spawn(async move {
            store
                .upload_part_streaming(
                    &upload_path_owned,
                    &upload_id_owned,
                    ctx.part_number,
                    ctx.available,
                    rx,
                )
                .await
        }));

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
                return Err(Error::StorageBackend(
                    "upload task failed to receive data".to_string(),
                ));
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
                    .handle_nonuniform_upload_task_failure(name, uuid, &ctx, join_error)
                    .await);
            }
        }

        if ctx.pending_size > 0 {
            self.store.delete_object(ctx.pending_path).await?;
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

        self.store
            .put_object(ctx.pending_path, Bytes::from(buf))
            .await?;

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

    #[instrument(skip(self))]
    pub async fn get_upload_state_nonuniform(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<S3UploadState, Error> {
        if let Some(cached) = self.retrieve_cached_upload_state(name, uuid).await {
            return Ok(cached);
        }

        let upload_path = path_builder::upload_path(name, uuid);

        let (multipart_upload_id, parts) =
            if let Some(upload_id) = self.get_or_search_upload_id(&upload_path).await? {
                let parts = self.store.list_parts(&upload_path, &upload_id).await?;
                (Some(upload_id), parts)
            } else {
                (None, Vec::new())
            };

        let uploaded: u64 = parts.iter().map(|p| p.size).sum();

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending_size = self.store.object_size(&pending_path).await.unwrap_or(0);

        let state = S3UploadState {
            size: uploaded + pending_size,
            multipart_upload_id,
            parts,
            pending_size,
        };
        self.cache_upload_state(name, uuid, &state).await;
        Ok(state)
    }

    #[instrument(skip(self))]
    pub async fn complete_upload_nonuniform(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        let upload_id = self
            .get_or_search_upload_id(&upload_path)
            .await?
            .ok_or(Error::UploadNotFound)?;

        let mut parts = if let Some(cached) = self.retrieve_cached_upload_state(name, uuid).await {
            cached.parts
        } else {
            self.store.list_parts(&upload_path, &upload_id).await?
        };
        let mut uploaded_size: u64 = parts.iter().map(|p| p.size).sum();

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending_size = self.store.object_size(&pending_path).await.unwrap_or(0);
        if pending_size > 0 {
            let pending_data = self.store.get_object_body(&pending_path, None).await?;
            let next_part = next_part_number(parts.len())?;
            let e_tag = self
                .store
                .upload_part(
                    &upload_path,
                    &upload_id,
                    next_part,
                    Bytes::from(pending_data),
                )
                .await?;
            parts.push(UploadedPart {
                part_number: next_part,
                e_tag,
                size: pending_size,
            });
            uploaded_size += pending_size;
        }

        let digest = digest
            .cloned()
            .unwrap_or(self.load_hasher(name, uuid, uploaded_size).await?.digest());

        self.store
            .complete_multipart_upload(&upload_path, &upload_id, &parts)
            .await?;

        self.evict_upload_id(&upload_path).await;
        self.evict_upload_state(name, uuid).await;

        let blob_path = path_builder::blob_path(&digest);
        self.store.copy_object(&upload_path, &blob_path).await?;

        let container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&container).await?;

        Ok(digest)
    }
}

/// Returns `true` when the combined pending and incoming bytes meet or exceed
/// `min_part_size`, meaning the data can be flushed immediately as a multipart
/// part rather than accumulated in the pending buffer.
fn should_flush_pending(pending_size: u64, content_length: u64, min_part_size: u64) -> bool {
    pending_size + content_length >= min_part_size
}

/// Returns the 1-based S3 part number for the next part to upload, given the
/// number of parts already completed. Errors only if `completed_parts + 1`
/// overflows `u32` — practically unreachable since S3 caps part counts at
/// `10_000`, but propagated cleanly to keep the call path panic-free.
fn next_part_number(completed_parts: usize) -> Result<u32, Error> {
    Ok(u32::try_from(completed_parts + 1)?)
}

#[cfg(test)]
mod tests {
    use super::{next_part_number, should_flush_pending};

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

    // --- next_part_number ---

    #[test]
    fn first_part_when_no_parts_uploaded() {
        assert_eq!(next_part_number(0).unwrap(), 1);
    }

    #[test]
    fn second_part_after_one_uploaded() {
        assert_eq!(next_part_number(1).unwrap(), 2);
    }

    #[test]
    fn part_number_increments_with_part_count() {
        assert_eq!(next_part_number(9).unwrap(), 10);
    }
}
