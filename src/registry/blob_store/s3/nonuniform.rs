use std::io::Cursor;

use aws_sdk_s3::{primitives::ByteStream, types::CompletedPart};
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tracing::instrument;

use super::{Backend, FRAME_SIZE, MIN_PART_SIZE, channel_body::ChannelBody};
use crate::{
    oci::Digest,
    registry::{
        blob_store::{Error, UploadState, hashing_reader::HashingReader, sha256_ext::Sha256Ext},
        path_builder,
    },
};

impl Backend {
    pub async fn resolve_nonuniform_state(
        &self,
        name: &str,
        uuid: &str,
        state: Option<UploadState>,
    ) -> Result<(String, Vec<(i32, String, i64)>, u64, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        if let Some(UploadState {
            multipart_upload_id: Some(id),
            parts,
            pending_size,
            ..
        }) = state
        {
            #[allow(clippy::cast_sign_loss)]
            let uploaded: u64 = parts.iter().map(|(_, _, sz)| *sz as u64).sum();
            return Ok((id, parts, uploaded, pending_size));
        }
        if let Some(UploadState {
            multipart_upload_id: Some(id),
            parts,
            pending_size,
            ..
        }) = self.retrieve_cached_upload_state(name, uuid).await
        {
            #[allow(clippy::cast_sign_loss)]
            let uploaded: u64 = parts.iter().map(|(_, _, sz)| *sz as u64).sum();
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
        let uploaded: u64 = parts.iter().map(|(_, _, sz)| *sz as u64).sum();
        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending = self.store.object_size(&pending_path).await.unwrap_or(0);
        Ok((id, parts, uploaded, pending))
    }

    #[instrument(skip(self, stream))]
    pub async fn write_upload_nonuniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        state: Option<UploadState>,
    ) -> Result<(Digest, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);

        let (upload_id, part_list, uploaded_size, pending_size) =
            self.resolve_nonuniform_state(name, uuid, state).await?;

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let next_part_number = (part_list.len() + 1) as i32;

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let available = pending_size + content_length;

        if available >= MIN_PART_SIZE {
            let hasher = self.load_hasher(name, uuid, uploaded_size).await?;

            let pending_bytes = if pending_size > 0 {
                self.store
                    .get_object_body(&pending_path, None)
                    .await
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            let combined: Box<dyn AsyncRead + Unpin + Send + Sync> = if pending_bytes.is_empty() {
                Box::new(stream)
            } else {
                Box::new(Cursor::new(pending_bytes).chain(stream))
            };

            let mut hashing_reader = HashingReader::with_hasher(combined, hasher);

            let (tx, rx) = mpsc::channel::<Bytes>(32);
            let body = ByteStream::from_body_1_x(ChannelBody { rx });

            let store = self.store.clone();
            let upload_path_clone = upload_path.clone();
            let upload_id_clone = upload_id.clone();
            let upload_handle = tokio::spawn(async move {
                store
                    .upload_part_streaming(
                        &upload_path_clone,
                        &upload_id_clone,
                        next_part_number,
                        available,
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

            if pending_size > 0 {
                self.store.delete_object(&pending_path).await?;
            }

            let new_size = uploaded_size + available;
            self.save_hasher(name, uuid, new_size, hashing_reader.serialized_state())
                .await?;

            let digest = hashing_reader.digest();
            Ok((digest, new_size))
        } else {
            let logical_offset = uploaded_size + pending_size;
            let hasher = self.load_hasher(name, uuid, logical_offset).await?;

            let pending_bytes = if pending_size > 0 {
                self.store
                    .get_object_body(&pending_path, None)
                    .await
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            let mut buf = pending_bytes;
            let mut hashing_reader = HashingReader::with_hasher(stream, hasher);
            hashing_reader.read_to_end(&mut buf).await?;

            let new_logical = uploaded_size + buf.len() as u64;
            self.store
                .put_object(&pending_path, Bytes::from(buf))
                .await?;

            self.save_hasher(name, uuid, new_logical, hashing_reader.serialized_state())
                .await?;

            let digest = hashing_reader.digest();
            Ok((digest, new_logical))
        }
    }

    #[instrument(skip(self))]
    pub async fn get_upload_state_nonuniform(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<UploadState, Error> {
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
        let uploaded: u64 = parts.iter().map(|(_, _, sz)| *sz as u64).sum();

        let pending_path = path_builder::upload_patch_pending_path(name, uuid);
        let pending_size = self.store.object_size(&pending_path).await.unwrap_or(0);

        let state = UploadState {
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
        let mut uploaded_size: u64 = part_list.iter().map(|(_, _, size)| *size as u64).sum();

        let mut parts: Vec<CompletedPart> = part_list
            .into_iter()
            .map(|(part_num, e_tag, _)| {
                CompletedPart::builder()
                    .part_number(part_num)
                    .e_tag(e_tag)
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
