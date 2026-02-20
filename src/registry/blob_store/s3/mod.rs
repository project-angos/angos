mod chunked_reader;
#[cfg(test)]
pub mod tests;

use std::fmt::{Debug, Formatter};
use std::io::Cursor;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use chunked_reader::ChunkedReader;
use sha2::{Digest as ShaDigestTrait, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, info, instrument};

use crate::oci::Digest;
use crate::registry::blob_store::hashing_reader::HashingReader;
use crate::registry::blob_store::sha256_ext::Sha256Ext;
use crate::registry::blob_store::{BlobStore, BoxedReader, Error, MultipartCleanup};
use crate::registry::{data_store, pagination, path_builder};

#[derive(Clone)]
pub struct Backend {
    pub store: data_store::s3::Backend,
    multipart_part_size: usize,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

impl Backend {
    pub fn new(config: &data_store::s3::BackendConfig) -> Result<Self, Error> {
        info!("Using S3 blob-store backend");
        #[allow(clippy::cast_possible_truncation)]
        let multipart_part_size = config.multipart_part_size.as_u64() as usize; // XXX: abstraction leaks
        let store = data_store::s3::Backend::new(config)?;

        Ok(Self {
            store,
            multipart_part_size,
        })
    }

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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

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

        // For consistent pagination, we need to get all blobs matching our criteria first
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
        append: bool,
    ) -> Result<(), Error> {
        let upload_path = path_builder::upload_path(name, uuid);

        let upload_id = if append {
            match self.store.search_multipart_upload_id(&upload_path).await? {
                Some(id) => id,
                None => self.store.create_multipart_upload(&upload_path).await?,
            }
        } else {
            self.store.abort_pending_uploads(&upload_path).await?;
            self.store.create_multipart_upload(&upload_path).await?
        };

        let part_list = self.store.list_parts(&upload_path, &upload_id).await?;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let mut uploaded_size: u64 = part_list.iter().map(|(_, _, size)| *size as u64).sum();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let mut uploaded_parts = (part_list.len() + 1) as i32;

        // NOTE: if the part is not big enough (at least 5M, as per the S3 protocol),
        // we store it in as a staging blob.
        // First, we load the staged chunk if any and append the new data
        let chunk = self.load_staged_chunk(name, uuid, uploaded_size).await?;
        let hasher = self.load_hasher(name, uuid, uploaded_size).await?;

        let reader = Cursor::new(chunk).chain(stream);
        let reader = HashingReader::with_hasher(reader, hasher);
        let mut reader = ChunkedReader::new(reader, self.multipart_part_size as u64);

        while let Some(mut chunk_reader) = reader.next_chunk() {
            let mut chunk = Vec::with_capacity(self.multipart_part_size);
            chunk_reader.read_to_end(&mut chunk).await?;
            let chunk = Bytes::from(chunk);
            let chunk_len = chunk.len() as u64;

            // Last chunk for this write(), store remaining data in staging
            if chunk.len() < self.multipart_part_size {
                reader.mark_finished();
                self.store_staged_chunk(name, uuid, chunk, uploaded_size)
                    .await?;
            } else {
                self.store
                    .upload_part(&upload_path, &upload_id, uploaded_parts, chunk)
                    .await?;
                uploaded_parts += 1;
            }

            self.save_hasher(
                name,
                uuid,
                uploaded_size + chunk_len,
                reader.serialized_state(),
            )
            .await?;

            if chunk_len >= self.multipart_part_size as u64 {
                uploaded_size += chunk_len;
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn read_upload_summary(
        &self,
        name: &str,
        uuid: &str,
    ) -> Result<(Digest, u64, DateTime<Utc>), Error> {
        let key = path_builder::upload_path(name, uuid);

        let mut size = 0u64;
        if let Some(upload_id) = self.store.search_multipart_upload_id(&key).await? {
            #[allow(clippy::cast_sign_loss)]
            for (_, _, part_size) in self.store.list_parts(&key, &upload_id).await? {
                size += part_size as u64;
            }
        }

        let staged_path = path_builder::upload_staged_container_path(name, uuid, size);
        if let Ok(staged_size) = self.store.object_size(&staged_path).await {
            size += staged_size;
        }

        let digest = self.load_hasher(name, uuid, size).await?.digest();

        let date_path = path_builder::upload_start_date_path(name, uuid);
        let date_bytes = self.store.get_object_body(&date_path, None).await?;
        let date_str = String::from_utf8_lossy(&date_bytes);
        let start_date = DateTime::parse_from_rfc3339(&date_str)
            .unwrap_or_else(|_| Utc::now().fixed_offset())
            .with_timezone(&Utc);

        Ok((digest, size, start_date))
    }

    #[instrument(skip(self))]
    async fn complete_upload(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let key = path_builder::upload_path(name, uuid);

        let Ok(Some(upload_id)) = self.store.search_multipart_upload_id(&key).await else {
            return Err(Error::UploadNotFound);
        };

        let part_list = self.store.list_parts(&key, &upload_id).await?;
        let mut size = part_list
            .iter()
            .map(|(_, _, s)| u64::try_from(*s).unwrap_or(0))
            .sum::<u64>();

        let source_key = path_builder::upload_staged_container_path(name, uuid, size);

        // Build parts list for completion
        let mut parts = Vec::new();
        for (part_num, e_tag, _) in part_list {
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(part_num)
                    .e_tag(e_tag)
                    .build(),
            );
        }

        // Add staged data as final part if it exists
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

        let blob_path = path_builder::blob_path(&digest);
        self.store.copy_object(&key, &blob_path).await?;

        // NOTE: in case of error, remaining parts will be deleted by the scrub job
        let key = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&key).await?;

        Ok(digest)
    }

    #[instrument(skip(self))]
    async fn delete_upload(&self, name: &str, uuid: &str) -> Result<(), Error> {
        let upload_path = path_builder::upload_path(name, uuid);
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn build_blob_reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<BoxedReader, Error> {
        let path = path_builder::blob_path(digest);
        let res = self
            .store
            .get_object(&path, start_offset)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::BlobNotFound
                } else {
                    e.into()
                }
            })?;

        Ok(Box::new(res.body.into_async_read()))
    }

    #[instrument(skip(self))]
    async fn get_blob_url(&self, digest: &Digest) -> Result<Option<String>, Error> {
        let path = path_builder::blob_path(digest);
        let url = self
            .store
            .generate_presigned_url(&path, std::time::Duration::from_secs(1800)) // TODO: make configurable
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
