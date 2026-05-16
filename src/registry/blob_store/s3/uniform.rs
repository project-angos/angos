use std::io::{self, Cursor, ErrorKind};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use sha2::Sha256;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, instrument, warn};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            Error, UploadSummary,
            hashing_reader::HashingReader,
            s3::{Backend, FRAME_BUFFER_CAPACITY, FRAME_SIZE, S3UploadState, UploadedPart},
            sha256_ext::Sha256Ext,
        },
        path_builder,
    },
};

struct ChunkContext<'a> {
    upload_path: &'a str,
    upload_id: &'a str,
    part_number: u32,
    size: u64,
}

impl Backend {
    #[instrument(skip(self, chunk))]
    pub async fn store_staged_chunk(
        &self,
        namespace: &str,
        upload_id: &str,
        chunk: Bytes,
        offset: u64,
    ) -> Result<(), Error> {
        let key = path_builder::upload_staged_container_path(namespace, upload_id, offset);
        self.store.put_object(&key, chunk).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn load_staged_chunk(
        &self,
        namespace: &str,
        upload_id: &str,
        offset: u64,
    ) -> Result<Vec<u8>, Error> {
        let key = path_builder::upload_staged_container_path(namespace, upload_id, offset);
        match self.store.get_object_body(&key, None).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn save_hasher(
        &self,
        name: &str,
        uuid: &str,
        offset: u64,
        state: Vec<u8>,
    ) -> Result<(), Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        self.store.put_object(&hash_state_path, state).await?;
        Ok(())
    }

    pub async fn load_hasher(&self, name: &str, uuid: &str, offset: u64) -> Result<Sha256, Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        let state = self.store.get_object_body(&hash_state_path, None).await?;
        Sha256::from_state(&state)
    }

    async fn resolve_uniform_state(
        &self,
        name: &str,
        uuid: &str,
        upload_path: &str,
        append: bool,
    ) -> Result<(Option<String>, Vec<UploadedPart>), Error> {
        if append {
            if let Some(state) = self.retrieve_cached_upload_state(name, uuid).await {
                return Ok((state.multipart_upload_id, state.parts));
            }
            let Some(id) = self.get_or_search_upload_id(upload_path).await? else {
                return Ok((None, Vec::new()));
            };
            let parts = self.store.list_parts(upload_path, &id).await?;
            Ok((Some(id), parts))
        } else {
            self.evict_upload_id(upload_path).await;
            self.store.abort_pending_uploads(upload_path).await?;
            Ok((None, Vec::new()))
        }
    }

    async fn stream_part(
        &self,
        name: &str,
        uuid: &str,
        ctx: ChunkContext<'_>,
        reader: &mut HashingReader<impl AsyncRead + Unpin + Send + Sync>,
    ) -> Result<(), Error> {
        let (tx, rx) = mpsc::channel::<Bytes>(FRAME_BUFFER_CAPACITY);

        let store = self.store.clone();
        let upload_path = ctx.upload_path.to_string();
        let upload_id = ctx.upload_id.to_string();
        let upload_handle = AbortOnDropHandle::new(tokio::spawn(async move {
            store
                .upload_part_streaming(&upload_path, &upload_id, ctx.part_number, ctx.size, rx)
                .await
        }));

        let mut part_reader = reader.take(ctx.size);
        let mut sent = 0;
        let mut buf = BytesMut::with_capacity(FRAME_SIZE);
        loop {
            buf.clear();
            let n = part_reader.read_buf(&mut buf).await?;
            if n == 0 {
                break;
            }
            sent += u64::try_from(n)?;
            if tx.send(buf.split().freeze()).await.is_err() {
                return match upload_handle.await {
                    Ok(Ok(_etag)) => Err(Error::StorageBackend(
                        "upload task stopped before receiving complete body".to_string(),
                    )),
                    Ok(Err(error)) => Err(error.into()),
                    Err(join_error) => {
                        let kind = if join_error.is_panic() {
                            "panicked"
                        } else if join_error.is_cancelled() {
                            "was cancelled"
                        } else {
                            "failed unexpectedly"
                        };
                        error!(
                            "uniform upload task {kind} for '{name}/{uuid}'; aborting multipart upload {upload_id}: {join_error}",
                            upload_id = ctx.upload_id,
                        );
                        if let Err(abort_err) = self
                            .store
                            .abort_multipart_upload(ctx.upload_path, ctx.upload_id)
                            .await
                        {
                            warn!(
                                "abort_multipart_upload failed during cleanup of '{name}/{uuid}': {abort_err}"
                            );
                        }
                        self.evict_upload_id(ctx.upload_path).await;
                        self.evict_upload_state(name, uuid).await;
                        Err(Error::StorageBackend(format!(
                            "upload task {kind}: {join_error}"
                        )))
                    }
                };
            }
        }

        if sent != ctx.size {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!("expected {} bytes for multipart part, got {sent}", ctx.size),
            )
            .into());
        }

        drop(tx);
        match upload_handle.await {
            Ok(Ok(_etag)) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(join_error) => {
                let kind = if join_error.is_panic() {
                    "panicked"
                } else if join_error.is_cancelled() {
                    "was cancelled"
                } else {
                    "failed unexpectedly"
                };
                error!(
                    "uniform upload task {kind} for '{name}/{uuid}'; aborting multipart upload {upload_id}: {join_error}",
                    upload_id = ctx.upload_id,
                );
                if let Err(abort_err) = self
                    .store
                    .abort_multipart_upload(ctx.upload_path, ctx.upload_id)
                    .await
                {
                    warn!(
                        "abort_multipart_upload failed during cleanup of '{name}/{uuid}': {abort_err}"
                    );
                }
                self.evict_upload_id(ctx.upload_path).await;
                self.evict_upload_state(name, uuid).await;
                Err(Error::StorageBackend(format!(
                    "upload task {kind}: {join_error}"
                )))
            }
        }
    }

    #[instrument(skip(self, stream))]
    pub async fn write_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        if append && content_length == 0 {
            let state = self.get_upload_state_uniform(name, uuid).await?;
            let digest = self.load_hasher(name, uuid, state.size).await?.digest();
            return Ok((digest, state.size));
        }

        let upload_path = path_builder::upload_path(name, uuid);

        let (mut upload_id, part_list) = self
            .resolve_uniform_state(name, uuid, &upload_path, append)
            .await?;

        let mut uploaded_size: u64 = part_list.iter().map(|p| p.size).sum();
        let mut uploaded_parts = next_part_number(part_list.len())?;

        let staged = self.load_staged_chunk(name, uuid, uploaded_size).await?;
        let staged_len = u64::try_from(staged.len())?;
        let hasher = self.load_hasher(name, uuid, uploaded_size).await?;

        let reader = Cursor::new(staged).chain(stream);
        let mut reader = HashingReader::with_hasher(reader, hasher);

        let mut total_size = uploaded_size;
        let available = staged_len + content_length;
        let full_parts = available / self.multipart_part_size;
        let remainder = available % self.multipart_part_size;

        for _ in 0..full_parts {
            let upload_id = if let Some(id) = &upload_id {
                id
            } else {
                let id = self.store.create_multipart_upload(&upload_path).await?;
                self.cache_upload_id(&upload_path, &id).await;
                upload_id.insert(id)
            };
            self.stream_part(
                name,
                uuid,
                ChunkContext {
                    upload_path: &upload_path,
                    upload_id,
                    part_number: uploaded_parts,
                    size: self.multipart_part_size,
                },
                &mut reader,
            )
            .await?;
            uploaded_parts += 1;
            uploaded_size += self.multipart_part_size;
            total_size += self.multipart_part_size;
            if append {
                self.save_hasher(name, uuid, total_size, reader.serialized_state())
                    .await?;
            }
        }

        if remainder > 0 {
            let mut staged = Vec::with_capacity(usize::try_from(remainder)?);
            let mut remainder_reader = (&mut reader).take(remainder);
            remainder_reader.read_to_end(&mut staged).await?;
            if u64::try_from(staged.len())? != remainder {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "expected {remainder} bytes for staged multipart remainder, got {}",
                        staged.len()
                    ),
                )
                .into());
            }
            self.store_staged_chunk(name, uuid, Bytes::from(staged), uploaded_size)
                .await?;
            total_size += remainder;
        }

        if !append || remainder > 0 {
            self.save_hasher(name, uuid, total_size, reader.serialized_state())
                .await?;
        }

        let digest = reader.digest();
        Ok((digest, total_size))
    }

    #[instrument(skip(self))]
    pub async fn get_upload_state_uniform(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<S3UploadState, Error> {
        if let Some(cached) = self.retrieve_cached_upload_state(name, uuid).await {
            return Ok(cached);
        }

        let key = path_builder::upload_path(name, uuid);

        let (multipart_upload_id, parts) =
            if let Some(upload_id) = self.get_or_search_upload_id(&key).await? {
                let parts = self.store.list_parts(&key, &upload_id).await?;
                (Some(upload_id), parts)
            } else {
                (None, Vec::new())
            };

        let mut size: u64 = parts.iter().map(|p| p.size).sum();

        let staged_path = path_builder::upload_staged_container_path(name, uuid, size);
        if let Ok(staged_size) = self.store.object_size(&staged_path).await {
            size += staged_size;
        }

        let state = S3UploadState {
            size,
            multipart_upload_id,
            parts,
            pending_size: 0,
        };
        self.cache_upload_state(name, uuid, &state).await;
        Ok(state)
    }

    #[instrument(skip(self))]
    pub async fn complete_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let key = path_builder::upload_path(name, uuid);

        let cached = self.retrieve_cached_upload_state(name, uuid).await;
        let (upload_id, mut parts) = match cached {
            Some(S3UploadState {
                multipart_upload_id: Some(id),
                parts,
                ..
            }) => (Some(id), parts),
            _ => match self.get_or_search_upload_id(&key).await? {
                Some(id) => {
                    let parts = self.store.list_parts(&key, &id).await?;
                    (Some(id), parts)
                }
                None => (None, Vec::new()),
            },
        };
        let mut size: u64 = parts.iter().map(|p| p.size).sum();

        let source_key = path_builder::upload_staged_container_path(name, uuid, size);

        if upload_id.is_none() {
            match self.store.object_size(&source_key).await {
                Ok(staged_size) => {
                    size += staged_size;
                    let digest = digest
                        .cloned()
                        .unwrap_or(self.load_hasher(name, uuid, size).await?.digest());
                    let blob_path = path_builder::blob_path(&digest);
                    self.store.copy_object(&source_key, &blob_path).await?;
                    self.evict_upload_state(name, uuid).await;
                    let key = path_builder::upload_container_path(name, uuid);
                    self.store.delete_prefix(&key).await?;
                    return Ok(digest);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound && size == 0 => {
                    let digest = digest
                        .cloned()
                        .unwrap_or(self.load_hasher(name, uuid, size).await?.digest());
                    let blob_path = path_builder::blob_path(&digest);
                    self.store.put_object(&blob_path, Bytes::new()).await?;
                    self.evict_upload_state(name, uuid).await;
                    let key = path_builder::upload_container_path(name, uuid);
                    self.store.delete_prefix(&key).await?;
                    return Ok(digest);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Error::UploadNotFound),
                Err(e) => return Err(e.into()),
            }
        }

        let upload_id = upload_id.expect("upload_id checked above");

        if let Ok(staged_size) = self.store.object_size(&source_key).await {
            let part_number = next_part_number(parts.len())?;
            let e_tag = self
                .store
                .upload_part_copy(&source_key, &key, &upload_id, part_number, None)
                .await?;
            parts.push(UploadedPart {
                part_number,
                e_tag,
                size: staged_size,
            });
            size += staged_size;
        }

        let digest = digest.cloned();
        let digest = digest.unwrap_or(self.load_hasher(name, uuid, size).await?.digest());

        self.store
            .complete_multipart_upload(&key, &upload_id, &parts)
            .await?;

        self.evict_upload_id(&key).await;
        self.evict_upload_state(name, uuid).await;

        let blob_path = path_builder::blob_path(&digest);
        self.store.copy_object(&key, &blob_path).await?;

        let key = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&key).await?;

        Ok(digest)
    }

    pub async fn build_upload_summary(
        &self,
        name: &str,
        uuid: &str,
        size: u64,
    ) -> Result<UploadSummary, Error> {
        let date_path = path_builder::upload_start_date_path(name, uuid);
        let date_bytes = self.store.get_object_body(&date_path, None).await?;
        let date_str = String::from_utf8_lossy(&date_bytes);
        let started_at = DateTime::parse_from_rfc3339(&date_str)
            .unwrap_or_else(|_| Utc::now().fixed_offset())
            .with_timezone(&Utc);
        Ok(UploadSummary { size, started_at })
    }
}

/// Returns the 1-based part number for the next part to upload, given the
/// number of parts already completed.  S3 part numbers start at 1.
fn next_part_number(completed_parts: usize) -> Result<u32, Error> {
    Ok(u32::try_from(completed_parts + 1)?)
}

#[cfg(test)]
mod tests {
    use crate::registry::blob_store::s3::uniform::next_part_number;

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
