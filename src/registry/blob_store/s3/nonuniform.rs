use std::io::Cursor;

use aws_sdk_s3::{primitives::ByteStream, types::CompletedPart};
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tracing::instrument;

use super::{
    Backend, FRAME_SIZE, MIN_PART_SIZE, S3UploadState, UploadedPart, channel_body::ChannelBody,
};
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
    part_number: i32,
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
            #[allow(clippy::cast_sign_loss)]
            let uploaded: u64 = parts.iter().map(|p| p.size as u64).sum();
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
        #[allow(clippy::cast_sign_loss)]
        let uploaded: u64 = parts.iter().map(|p| p.size as u64).sum();
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
        let body = ByteStream::from_body_1_x(ChannelBody { rx });

        let store = self.store.clone();
        let upload_path_owned = ctx.upload_path.to_string();
        let upload_id_owned = ctx.upload_id.to_string();
        let upload_handle = tokio::spawn(async move {
            store
                .upload_part_streaming(
                    &upload_path_owned,
                    &upload_id_owned,
                    ctx.part_number,
                    ctx.available,
                    body,
                )
                .await
        });

        let mut buf = BytesMut::with_capacity(FRAME_SIZE);
        loop {
            buf.clear();
            let n = hashing_reader.read_buf(&mut buf).await?;
            if n == 0 {
                break;
            }
            if tx.send(buf.split().freeze()).await.is_err() {
                return Err(Error::StorageBackend(
                    "upload task failed to receive data".to_string(),
                ));
            }
        }
        drop(tx);
        upload_handle
            .await
            .map_err(|e| Error::StorageBackend(e.to_string()))??;

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
        let mut hashing_reader = HashingReader::with_hasher(stream, hasher);
        hashing_reader.read_to_end(&mut buf).await?;

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let new_logical = ctx.uploaded_size + buf.len() as u64;
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

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let next_part_number = (part_list.len() + 1) as i32;

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let available = pending_size + content_length;
        let pending_bytes = self.load_pending_bytes(&pending_path, pending_size).await;

        if available >= MIN_PART_SIZE {
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

        #[allow(clippy::cast_sign_loss)]
        let uploaded: u64 = parts.iter().map(|p| p.size as u64).sum();

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

        let part_list = if let Some(cached) = self.retrieve_cached_upload_state(name, uuid).await {
            cached.parts
        } else {
            self.store.list_parts(&upload_path, &upload_id).await?
        };
        #[allow(clippy::cast_sign_loss)]
        let mut uploaded_size: u64 = part_list.iter().map(|p| p.size as u64).sum();

        let mut parts: Vec<CompletedPart> = part_list
            .into_iter()
            .map(|p| {
                CompletedPart::builder()
                    .part_number(p.part_number)
                    .e_tag(p.e_tag)
                    .build()
            })
            .collect();

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending_size = self.store.object_size(&pending_path).await.unwrap_or(0);
        if pending_size > 0 {
            let pending_data = self.store.get_object_body(&pending_path, None).await?;
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let next_part = (parts.len() + 1) as i32;
            let e_tag = self
                .store
                .upload_part(
                    &upload_path,
                    &upload_id,
                    next_part,
                    Bytes::from(pending_data),
                )
                .await?;
            parts.push(
                CompletedPart::builder()
                    .part_number(next_part)
                    .e_tag(e_tag)
                    .build(),
            );
            uploaded_size += pending_size;
        }

        let digest = digest
            .cloned()
            .unwrap_or(self.load_hasher(name, uuid, uploaded_size).await?.digest());

        self.store
            .complete_multipart_upload(&upload_path, &upload_id, parts)
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
