use std::io::{self, Cursor};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::instrument;

use super::{Backend, S3UploadState, UploadedPart, chunked_reader::ChunkedReader};
use crate::{
    oci::Digest,
    registry::{
        blob_store::{Error, UploadSummary, hashing_reader::HashingReader, sha256_ext::Sha256Ext},
        path_builder,
    },
};

struct ChunkContext<'a> {
    upload_path: &'a str,
    upload_id: &'a str,
    uploaded_size: u64,
    append: bool,
}

struct ChunkOutcome {
    chunk_len: u64,
    flushed: bool,
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
    ) -> Result<(String, Vec<UploadedPart>), Error> {
        if append {
            if let Some(S3UploadState {
                multipart_upload_id: Some(id),
                parts,
                ..
            }) = self.retrieve_cached_upload_state(name, uuid).await
            {
                return Ok((id, parts));
            }
            let id = if let Some(id) = self.get_or_search_upload_id(upload_path).await? {
                id
            } else {
                let id = self.store.create_multipart_upload(upload_path).await?;
                self.cache_upload_id(upload_path, &id).await;
                id
            };
            let parts = self.store.list_parts(upload_path, &id).await?;
            Ok((id, parts))
        } else {
            self.evict_upload_id(upload_path).await;
            self.store.abort_pending_uploads(upload_path).await?;
            let id = self.store.create_multipart_upload(upload_path).await?;
            self.cache_upload_id(upload_path, &id).await;
            Ok((id, Vec::new()))
        }
    }

    async fn process_chunk(
        &self,
        name: &str,
        uuid: &str,
        ctx: ChunkContext<'_>,
        uploaded_parts: &mut u32,
        chunk: Bytes,
        reader: &mut ChunkedReader<HashingReader<impl AsyncRead + Unpin + Send + Sync>>,
    ) -> Result<ChunkOutcome, Error> {
        let chunk_len = u64::try_from(chunk.len())?;
        let flush = should_flush(chunk_len, self.multipart_part_size);

        if flush {
            self.store
                .upload_part(ctx.upload_path, ctx.upload_id, *uploaded_parts, chunk)
                .await?;
            *uploaded_parts += 1;
        } else {
            reader.mark_finished();
            self.store_staged_chunk(name, uuid, chunk, ctx.uploaded_size)
                .await?;
        }

        if ctx.append {
            self.save_hasher(
                name,
                uuid,
                ctx.uploaded_size + chunk_len,
                reader.serialized_state(),
            )
            .await?;
        }

        Ok(ChunkOutcome {
            chunk_len,
            flushed: flush,
        })
    }

    #[instrument(skip(self, stream))]
    pub async fn write_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);

        let (upload_id, part_list) = self
            .resolve_uniform_state(name, uuid, &upload_path, append)
            .await?;

        let mut uploaded_size: u64 = part_list.iter().map(|p| p.size).sum();
        let mut uploaded_parts = next_part_number(part_list.len())?;

        let staged = self.load_staged_chunk(name, uuid, uploaded_size).await?;
        let hasher = self.load_hasher(name, uuid, uploaded_size).await?;

        let reader = Cursor::new(staged).chain(stream);
        let reader = HashingReader::with_hasher(reader, hasher);
        let mut reader = ChunkedReader::new(reader, self.multipart_part_size);

        let mut total_size = uploaded_size;
        while let Some(mut chunk_reader) = reader.next_chunk() {
            let mut buf = Vec::with_capacity(usize::try_from(self.multipart_part_size)?);
            chunk_reader.read_to_end(&mut buf).await?;
            let chunk = Bytes::from(buf);
            let outcome = self
                .process_chunk(
                    name,
                    uuid,
                    ChunkContext {
                        upload_path: &upload_path,
                        upload_id: &upload_id,
                        uploaded_size,
                        append,
                    },
                    &mut uploaded_parts,
                    chunk,
                    &mut reader,
                )
                .await?;
            total_size = uploaded_size + outcome.chunk_len;
            if outcome.flushed {
                uploaded_size += outcome.chunk_len;
            }
        }

        if !append {
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

        let Ok(Some(upload_id)) = self.get_or_search_upload_id(&key).await else {
            return Err(Error::UploadNotFound);
        };

        let mut parts = if let Some(cached) = self.retrieve_cached_upload_state(name, uuid).await {
            cached.parts
        } else {
            self.store.list_parts(&key, &upload_id).await?
        };
        let mut size: u64 = parts.iter().map(|p| p.size).sum();

        let source_key = path_builder::upload_staged_container_path(name, uuid, size);

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

/// Returns `true` when `chunk_len` is large enough to flush as a multipart part.
/// Smaller chunks must be staged so they can accumulate across PATCH requests
/// before reaching the S3 minimum part size.
fn should_flush(chunk_len: u64, multipart_part_size: u64) -> bool {
    chunk_len >= multipart_part_size
}

/// Returns the 1-based part number for the next part to upload, given the
/// number of parts already completed.  S3 part numbers start at 1.
fn next_part_number(completed_parts: usize) -> Result<u32, Error> {
    Ok(u32::try_from(completed_parts + 1)?)
}

#[cfg(test)]
mod tests {
    use super::{next_part_number, should_flush};

    // --- should_flush ---

    #[test]
    fn flushes_when_chunk_meets_part_size() {
        assert!(should_flush(5 * 1024 * 1024, 5 * 1024 * 1024));
    }

    #[test]
    fn flushes_when_chunk_exceeds_part_size() {
        assert!(should_flush(5 * 1024 * 1024 + 1, 5 * 1024 * 1024));
    }

    #[test]
    fn stages_when_chunk_below_part_size() {
        assert!(!should_flush(5 * 1024 * 1024 - 1, 5 * 1024 * 1024));
    }

    #[test]
    fn stages_empty_chunk() {
        assert!(!should_flush(0, 5 * 1024 * 1024));
    }

    #[test]
    fn flushes_when_part_size_is_one_and_chunk_is_nonempty() {
        assert!(should_flush(1, 1));
    }

    #[test]
    fn stages_when_part_size_is_one_and_chunk_is_empty() {
        assert!(!should_flush(0, 1));
    }

    #[test]
    fn flushes_when_chunk_is_max_u64() {
        assert!(should_flush(u64::MAX, 5 * 1024 * 1024));
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
