use std::io::{self, Cursor};

use aws_sdk_s3::types::CompletedPart;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::instrument;

use super::{Backend, S3UploadState, chunked_reader::ChunkedReader};
use crate::{
    oci::Digest,
    registry::{
        blob_store::{Error, UploadSummary, hashing_reader::HashingReader, sha256_ext::Sha256Ext},
        path_builder,
    },
};

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

    #[instrument(skip(self, stream))]
    pub async fn write_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);

        let (upload_id, part_list) = if append {
            if let Some(S3UploadState {
                multipart_upload_id: Some(id),
                parts,
                ..
            }) = self.retrieve_cached_upload_state(name, uuid).await
            {
                (id, parts)
            } else {
                let id = if let Some(id) = self.get_or_search_upload_id(&upload_path).await? {
                    id
                } else {
                    let id = self.store.create_multipart_upload(&upload_path).await?;
                    self.cache_upload_id(&upload_path, &id).await;
                    id
                };
                let parts = self.store.list_parts(&upload_path, &id).await?;
                (id, parts)
            }
        } else {
            self.evict_upload_id(&upload_path).await;
            self.store.abort_pending_uploads(&upload_path).await?;
            let id = self.store.create_multipart_upload(&upload_path).await?;
            self.cache_upload_id(&upload_path, &id).await;
            (id, Vec::new())
        };

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let mut uploaded_size: u64 = part_list.iter().map(|p| p.size as u64).sum();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let mut uploaded_parts = (part_list.len() + 1) as i32;

        let chunk = self.load_staged_chunk(name, uuid, uploaded_size).await?;
        let hasher = self.load_hasher(name, uuid, uploaded_size).await?;

        let reader = Cursor::new(chunk).chain(stream);
        let reader = HashingReader::with_hasher(reader, hasher);
        let mut reader = ChunkedReader::new(reader, self.multipart_part_size as u64);

        let mut total_size = uploaded_size;
        while let Some(mut chunk_reader) = reader.next_chunk() {
            let mut chunk = Vec::with_capacity(self.multipart_part_size);
            chunk_reader.read_to_end(&mut chunk).await?;
            let chunk = Bytes::from(chunk);
            let chunk_len = chunk.len() as u64;

            if chunk.len() < self.multipart_part_size {
                reader.mark_finished();
                self.store_staged_chunk(name, uuid, chunk, uploaded_size)
                    .await?;
                total_size = uploaded_size + chunk_len;
            } else {
                self.store
                    .upload_part(&upload_path, &upload_id, uploaded_parts, chunk)
                    .await?;
                uploaded_parts += 1;
                total_size = uploaded_size + chunk_len;
            }

            if append {
                self.save_hasher(
                    name,
                    uuid,
                    uploaded_size + chunk_len,
                    reader.serialized_state(),
                )
                .await?;
            }

            if chunk_len >= self.multipart_part_size as u64 {
                uploaded_size += chunk_len;
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

        #[allow(clippy::cast_sign_loss)]
        let mut size: u64 = parts.iter().map(|p| p.size as u64).sum();

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

        let part_list = if let Some(cached) = self.retrieve_cached_upload_state(name, uuid).await {
            cached.parts
        } else {
            self.store.list_parts(&key, &upload_id).await?
        };
        let mut size = part_list
            .iter()
            .map(|p| u64::try_from(p.size).unwrap_or(0))
            .sum::<u64>();

        let source_key = path_builder::upload_staged_container_path(name, uuid, size);

        let mut parts = Vec::new();
        for part in part_list {
            parts.push(
                CompletedPart::builder()
                    .part_number(part.part_number)
                    .e_tag(part.e_tag)
                    .build(),
            );
        }

        if let Ok(staged_size) = self.store.object_size(&source_key).await {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let part_number = (parts.len() + 1) as i32;
            let e_tag = self
                .store
                .upload_part_copy(&source_key, &key, &upload_id, part_number, None)
                .await?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build(),
            );
            size += staged_size;
        }

        let digest = digest.cloned();
        let digest = digest.unwrap_or(self.load_hasher(name, uuid, size).await?.digest());

        self.store
            .complete_multipart_upload(&key, &upload_id, parts)
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
