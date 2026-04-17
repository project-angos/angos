mod channel_body;
mod chunked_reader;
#[cfg(test)]
pub mod tests;

use std::{
    fmt::{self, Debug, Formatter},
    io::{self, Cursor},
    sync::Arc,
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use bytes::{Bytes, BytesMut};
use channel_body::ChannelBody;
use chrono::{DateTime, Duration, Utc};
use chunked_reader::ChunkedReader;
use sha2::{Digest as ShaDigestTrait, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tracing::{debug, info, instrument};

use crate::{
    cache::{Cache, CacheExt},
    oci::Digest,
    registry::{
        blob_store::{
            BlobStore, BoxedReader, Error, MultipartCleanup, UploadState,
            hashing_reader::HashingReader, sha256_ext::Sha256Ext,
        },
        data_store, pagination, path_builder,
    },
};

const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
const FRAME_SIZE: usize = 256 * 1024;

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    multipart_part_size: usize,
    uniform_parts: bool,
    cache: Option<Arc<dyn Cache>>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &data_store::s3::BackendConfig) -> Result<Self, Error> {
        info!("Using S3 blob-store backend");
        #[allow(clippy::cast_possible_truncation)]
        let multipart_part_size = config.multipart_part_size.as_u64() as usize;
        let store = data_store::s3::Backend::new(config)?;

        Ok(Self {
            store,
            multipart_part_size,
            uniform_parts: config.multipart_uniform_parts,
            cache: None,
        })
    }

    pub fn with_cache(mut self, cache: Arc<dyn Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    // ---- Multipart upload ID cache helpers ----

    fn upload_id_cache_key(path: &str) -> String {
        format!("upload_id:{path}")
    }

    async fn get_or_search_upload_id(&self, path: &str) -> Result<Option<String>, Error> {
        if let Some(cache) = &self.cache
            && let Ok(Some(id)) = cache.retrieve_value(&Self::upload_id_cache_key(path)).await
        {
            return Ok(Some(id));
        }
        let id = self.store.search_multipart_upload_id(path).await?;
        if let Some(ref upload_id) = id {
            self.cache_upload_id(path, upload_id).await;
        }
        Ok(id)
    }

    async fn cache_upload_id(&self, path: &str, upload_id: &str) {
        if let Some(cache) = &self.cache {
            let _ = cache
                .store_value(&Self::upload_id_cache_key(path), upload_id, 3600)
                .await;
        }
    }

    async fn evict_upload_id(&self, path: &str) {
        if let Some(cache) = &self.cache {
            let _ = cache.delete_value(&Self::upload_id_cache_key(path)).await;
        }
    }

    // ---- Upload state cache helpers ----

    fn upload_state_cache_key(namespace: &str, uuid: &str) -> String {
        format!("upload_state:{namespace}:{uuid}")
    }

    async fn cache_upload_state(&self, namespace: &str, uuid: &str, state: &UploadState) {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            let _ = cache.store(&key, state, 3600).await;
        }
    }

    async fn retrieve_cached_upload_state(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Option<UploadState> {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            if let Ok(Some(state)) = cache.retrieve::<UploadState>(&key).await {
                return Some(state);
            }
        }
        None
    }

    async fn evict_upload_state(&self, namespace: &str, uuid: &str) {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            let _ = cache.delete_value(&key).await;
        }
    }

    // ---- Uniform mode helpers ----

    #[instrument(skip(self, chunk))]
    async fn store_staged_chunk(
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
    async fn load_staged_chunk(
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

    // ---- Shared hasher helpers ----

    async fn save_hasher(
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

    async fn load_hasher(&self, name: &str, uuid: &str, offset: u64) -> Result<Sha256, Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        let state = self.store.get_object_body(&hash_state_path, None).await?;
        Sha256::from_state(&state)
    }

    // ---- Uniform mode write/complete/size/summary ----

    #[instrument(skip(self, stream))]
    async fn write_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        append: bool,
        state: Option<UploadState>,
    ) -> Result<(Digest, u64), Error> {
        let upload_path = path_builder::upload_path(name, uuid);

        let (upload_id, part_list) = if append {
            if let Some(UploadState {
                multipart_upload_id: Some(id),
                parts,
                ..
            }) = state
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
        let mut uploaded_size: u64 = part_list.iter().map(|(_, _, size)| *size as u64).sum();
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
    async fn get_upload_state_uniform(&self, name: &str, uuid: &str) -> Result<UploadState, Error> {
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
        let mut size: u64 = parts
            .iter()
            .map(|(_, _, part_size)| *part_size as u64)
            .sum();

        let staged_path = path_builder::upload_staged_container_path(name, uuid, size);
        if let Ok(staged_size) = self.store.object_size(&staged_path).await {
            size += staged_size;
        }

        let state = UploadState {
            size,
            multipart_upload_id,
            parts,
            pending_size: 0,
        };
        self.cache_upload_state(name, uuid, &state).await;
        Ok(state)
    }

    #[instrument(skip(self))]
    async fn complete_upload_uniform(
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
            .map(|(_, _, s)| u64::try_from(*s).unwrap_or(0))
            .sum::<u64>();

        let source_key = path_builder::upload_staged_container_path(name, uuid, size);

        let mut parts = Vec::new();
        for (part_num, e_tag, _) in part_list {
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_num)
                    .e_tag(e_tag)
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
                aws_sdk_s3::types::CompletedPart::builder()
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

    // ---- Non-uniform mode write/complete/size/summary ----

    async fn resolve_nonuniform_state(
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
    async fn write_upload_nonuniform(
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
    async fn get_upload_state_nonuniform(
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
    async fn complete_upload_nonuniform(
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

        let mut parts: Vec<aws_sdk_s3::types::CompletedPart> = part_list
            .into_iter()
            .map(|(part_num, e_tag, _)| {
                aws_sdk_s3::types::CompletedPart::builder()
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
                aws_sdk_s3::types::CompletedPart::builder()
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

    async fn build_upload_summary(
        &self,
        name: &str,
        uuid: &str,
        state: &UploadState,
    ) -> Result<(Digest, u64, DateTime<Utc>), Error> {
        let size = state.size;
        let digest = self.load_hasher(name, uuid, size).await?.digest();
        let date_path = path_builder::upload_start_date_path(name, uuid);
        let date_bytes = self.store.get_object_body(&date_path, None).await?;
        let date_str = String::from_utf8_lossy(&date_bytes);
        let start_date = DateTime::parse_from_rfc3339(&date_str)
            .unwrap_or_else(|_| Utc::now().fixed_offset())
            .with_timezone(&Utc);
        Ok((digest, size, start_date))
    }
}

#[async_trait]
impl BlobStore for Backend {
    #[instrument(skip(self))]
    async fn list_blobs(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        debug!("Fetching {n} blob(s) with continuation token: {continuation_token:?}");
        let algorithm = "sha256";
        let path = path_builder::blobs_root_dir();
        let blob_prefix = format!("{path}/{algorithm}/");

        let mut all_blobs = Vec::new();
        let mut list_continuation_token = None;

        loop {
            let (objects, next_token) = self
                .store
                .list_objects(&blob_prefix, 1000, list_continuation_token)
                .await?;

            for key in objects {
                if !key.ends_with("/data") {
                    continue;
                }

                let key_without_data = &key[..key.len() - 5];
                if let Some(slash_pos) = key_without_data.rfind('/') {
                    let digest = &key_without_data[slash_pos + 1..];
                    all_blobs.push(Digest::Sha256(digest.into()));
                }
            }

            list_continuation_token = next_token;
            if list_continuation_token.is_none() {
                break;
            }
        }

        Ok(pagination::paginate_sorted(
            &all_blobs,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self))]
    async fn list_uploads(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        debug!(
            "Fetching {n} upload(s) for namespace '{namespace}' with continuation token: {continuation_token:?}"
        );
        let uploads_dir = path_builder::uploads_root_dir(namespace);

        let (prefixes, _, next_continuation_token) = self
            .store
            .list_prefixes(&uploads_dir, "/", i32::from(n), continuation_token, None)
            .await?;

        Ok((prefixes, next_continuation_token))
    }

    #[instrument(skip(self))]
    async fn create_upload(&self, name: &str, uuid: &str) -> Result<String, Error> {
        let date_path = path_builder::upload_start_date_path(name, uuid);
        self.store
            .put_object(&date_path, Utc::now().to_rfc3339())
            .await?;

        let state = Sha256::new().serialized_state();
        self.save_hasher(name, uuid, 0, state).await?;

        Ok(uuid.to_string())
    }

    #[instrument(skip(self, stream))]
    async fn write_upload(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        append: bool,
        state: Option<UploadState>,
    ) -> Result<(Digest, u64), Error> {
        let result = if self.uniform_parts {
            self.write_upload_uniform(name, uuid, stream, append, state)
                .await
        } else {
            self.write_upload_nonuniform(name, uuid, stream, content_length, state)
                .await
        };
        if result.is_ok() {
            self.evict_upload_state(name, uuid).await;
        }
        result
    }

    #[instrument(skip(self))]
    async fn get_upload_state(&self, name: &str, uuid: &str) -> Result<UploadState, Error> {
        if self.uniform_parts {
            self.get_upload_state_uniform(name, uuid).await
        } else {
            self.get_upload_state_nonuniform(name, uuid).await
        }
    }

    #[instrument(skip(self))]
    async fn read_upload_summary(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<(Digest, u64, DateTime<Utc>), Error> {
        let state = if self.uniform_parts {
            self.get_upload_state_uniform(name, uuid).await?
        } else {
            self.get_upload_state_nonuniform(name, uuid).await?
        };
        self.build_upload_summary(name, uuid, &state).await
    }

    #[instrument(skip(self))]
    async fn complete_upload(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        if self.uniform_parts {
            self.complete_upload_uniform(name, uuid, digest).await
        } else {
            self.complete_upload_nonuniform(name, uuid, digest).await
        }
    }

    #[instrument(skip(self))]
    async fn delete_upload(&self, name: &str, uuid: &str) -> Result<(), Error> {
        let upload_path = path_builder::upload_path(name, uuid);
        self.evict_upload_id(&upload_path).await;
        self.evict_upload_state(name, uuid).await;
        self.store.abort_pending_uploads(&upload_path).await?;

        let upload_path = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&upload_path).await?;
        Ok(())
    }

    #[instrument(skip(self, content))]
    async fn create_blob(&self, content: &[u8]) -> Result<Digest, Error> {
        let mut hasher = Sha256::new();
        hasher.update(content);
        let digest = hasher.digest();

        let blob_path = path_builder::blob_path(&digest);
        self.store
            .put_object(&blob_path, Bytes::copy_from_slice(content))
            .await?;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn read_blob(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        let path = path_builder::blob_path(digest);
        Ok(self.store.get_object_body(&path, None).await?)
    }

    #[instrument(skip(self))]
    async fn get_blob_size(&self, digest: &Digest) -> Result<u64, Error> {
        let path = path_builder::blob_path(digest);
        match self.store.object_size(&path).await {
            Ok(size) => Ok(size),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn build_blob_reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let path = path_builder::blob_path(digest);
        let res = self
            .store
            .get_object(&path, start_offset)
            .await
            .map_err(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    Error::BlobNotFound
                } else {
                    e.into()
                }
            })?;

        let remaining: u64 = res
            .content_length
            .unwrap_or_default()
            .try_into()
            .unwrap_or(0);
        let total_size = remaining + start_offset.unwrap_or(0);

        Ok((Box::new(res.body.into_async_read()), total_size))
    }

    #[instrument(skip(self))]
    async fn get_blob_url(
        &self,
        digest: &Digest,
        content_type: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let path = path_builder::blob_path(digest);
        let url = self
            .store
            .generate_presigned_url(&path, StdDuration::from_secs(1800), content_type)
            .await?;
        Ok(Some(url))
    }

    #[instrument(skip(self))]
    async fn delete_blob(&self, digest: &Digest) -> Result<(), Error> {
        let path = path_builder::blob_container_dir(digest);
        self.store.delete_prefix(&path).await?;
        Ok(())
    }
}

fn parse_upload_key(key: &str) -> Option<(String, String)> {
    let key = key.strip_prefix("v2/repositories/")?;
    let key = key.strip_suffix("/data")?;
    let (namespace, uuid) = key.rsplit_once("/_uploads/")?;
    Some((namespace.to_string(), uuid.to_string()))
}

#[async_trait]
impl MultipartCleanup for Backend {
    #[instrument(skip(self))]
    async fn cleanup_orphan_multipart_uploads(
        &self,
        timeout: Duration,
        dry_run: bool,
    ) -> Result<usize, Error> {
        let mut count = 0;
        let now = Utc::now();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        loop {
            let (uploads, next_key, next_upload_id) = self
                .store
                .list_multipart_uploads(None, key_marker.as_deref(), upload_id_marker.as_deref())
                .await?;

            for (key, upload_id, initiated) in uploads {
                if now.signed_duration_since(initiated) < timeout {
                    continue;
                }

                let Some((namespace, uuid)) = parse_upload_key(&key) else {
                    continue;
                };

                let startedat_path = path_builder::upload_start_date_path(&namespace, &uuid);
                if self.store.object_size(&startedat_path).await.is_ok() {
                    continue;
                }

                if dry_run {
                    info!("DRY RUN: would abort orphan multipart upload {key}");
                } else {
                    info!("Aborting orphan multipart upload {key}");
                    self.store.abort_multipart_upload(&key, &upload_id).await?;
                }
                count += 1;
            }

            if next_key.is_none() {
                break;
            }
            key_marker = next_key;
            upload_id_marker = next_upload_id;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_parse_upload_key_valid() {
        let result = parse_upload_key("v2/repositories/my-repo/_uploads/abc-123-def/data");
        assert_eq!(
            result,
            Some(("my-repo".to_string(), "abc-123-def".to_string()))
        );
    }

    #[test]
    fn test_parse_upload_key_nested_namespace() {
        let result = parse_upload_key("v2/repositories/org/project/image/_uploads/uuid-here/data");
        assert_eq!(
            result,
            Some(("org/project/image".to_string(), "uuid-here".to_string()))
        );
    }

    #[test]
    fn test_parse_upload_key_invalid_prefix() {
        let result = parse_upload_key("invalid/prefix/_uploads/uuid/data");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_upload_key_invalid_suffix() {
        let result = parse_upload_key("v2/repositories/repo/_uploads/uuid/staged");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_upload_key_missing_uploads() {
        let result = parse_upload_key("v2/repositories/repo/blobs/sha256/abc/data");
        assert_eq!(result, None);
    }
}
