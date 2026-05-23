//! S3-backed implementations of every capability trait.
//!
//! Wraps [`angos_s3_client::Backend`] so consumers get the storage abstraction
//! without depending on the HTTP/S3 layer directly. The wrapper translates
//! `s3_client::Error` and `io::Error` into [`crate::Error`], adapts S3's
//! flat/delimited listing modes to [`Page`](crate::Page) /
//! [`ChildrenPage`](crate::ChildrenPage), and forwards every multipart and
//! presign operation through unchanged.

use std::{sync::Arc, time::Duration};

use angos_s3_client::{Backend as S3Backend, UploadedPart};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
    BoxedReader, ChildrenPage, ConditionalStore, Error, Etag, MultipartPage, MultipartStore,
    MultipartUpload, ObjectMeta, ObjectStore, Page, Part, PresignedStore, UploadId,
};

/// Builder for [`Backend`].
pub struct Builder {
    client: Option<Arc<S3Backend>>,
}

impl Builder {
    fn new() -> Self {
        Self { client: None }
    }

    /// The underlying S3 HTTP client. Construct it with
    /// `angos_s3_client::Backend::new(&config)` and pass it in here.
    #[must_use]
    pub fn client(mut self, client: Arc<S3Backend>) -> Self {
        self.client = Some(client);
        self
    }

    /// # Errors
    /// Returns [`Error::Backend`] when [`client`](Self::client) was never
    /// called.
    pub fn build(self) -> Result<Backend, Error> {
        let client = self
            .client
            .ok_or_else(|| Error::Backend("s3::Backend requires a client".to_string()))?;
        Ok(Backend { client })
    }
}

/// S3 [`ObjectStore`] (+ conditional, multipart, presign) implementation.
#[derive(Clone, Debug)]
pub struct Backend {
    client: Arc<S3Backend>,
}

impl Backend {
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
    }
}

fn part_from_uploaded(part: UploadedPart) -> Part {
    Part {
        part_number: part.part_number,
        etag: Etag::new(part.e_tag),
        size: part.size,
    }
}

fn uploaded_from_part(part: &Part) -> UploadedPart {
    UploadedPart {
        part_number: part.part_number,
        e_tag: part.etag.as_str().to_string(),
        size: part.size,
    }
}

#[async_trait]
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        Ok(self.client.read(key).await?)
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let result = self.client.get_object(key, offset).await?;
        // S3 returns the content-length of the (possibly ranged) response;
        // the trait contract requires the **total** object size.
        let total = result.content_length + offset.unwrap_or(0);
        Ok((result.body, total))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        Ok(self.client.put_object(key, data).await?)
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        // S3 DELETE on a missing key returns 204, mapped to Ok by the client.
        Ok(self.client.delete(key).await?)
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        Ok(self.client.delete_prefix(prefix).await?)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        let (size, etag, last_modified) = self.client.head_object(key).await?;
        Ok(ObjectMeta {
            size,
            etag: etag.map(Etag::new),
            last_modified,
        })
    }

    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        let (items, next_token) = self.client.list_objects(prefix, n, token).await?;
        Ok(Page { items, next_token })
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        let (sub_prefixes, objects, next_token) = self
            .client
            .list_prefixes(prefix, "/", n, token, start_after)
            .await?;
        Ok(ChildrenPage {
            sub_prefixes,
            objects,
            next_token,
        })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error> {
        Ok(self.client.copy_object(source, destination).await?)
    }
}

#[async_trait]
impl ConditionalStore for Backend {
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), Error> {
        let (body, etag) = self.client.read_with_etag(key).await?;
        Ok((body, etag.map(Etag::new)))
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, Error> {
        let etag = self.client.put_object_if_not_exists(key, data).await?;
        Ok(etag.map(Etag::new))
    }

    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, Error> {
        let etag = self
            .client
            .put_object_if_match(key, etag.as_str(), data)
            .await?;
        Ok(etag.map(Etag::new))
    }

    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), Error> {
        Ok(self.client.delete_if_match(key, etag.as_str()).await?)
    }
}

#[async_trait]
impl MultipartStore for Backend {
    async fn create_multipart(&self, key: &str) -> Result<UploadId, Error> {
        let id = self.client.create_multipart_upload(key).await?;
        Ok(UploadId::new(id))
    }

    async fn upload_part_streaming(
        &self,
        key: &str,
        id: &UploadId,
        part_number: u32,
        content_length: u64,
        rx: mpsc::Receiver<Bytes>,
    ) -> Result<Etag, Error> {
        let etag = self
            .client
            .upload_part_streaming(key, id.as_str(), part_number, content_length, rx)
            .await?;
        Ok(Etag::new(etag))
    }

    async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        id: &UploadId,
        part_number: u32,
        range: Option<String>,
    ) -> Result<Etag, Error> {
        let etag = self
            .client
            .upload_part_copy(source, destination, id.as_str(), part_number, range)
            .await?;
        Ok(Etag::new(etag))
    }

    async fn complete_multipart(
        &self,
        key: &str,
        id: &UploadId,
        parts: &[Part],
    ) -> Result<(), Error> {
        let uploaded: Vec<UploadedPart> = parts.iter().map(uploaded_from_part).collect();
        Ok(self
            .client
            .complete_multipart_upload(key, id.as_str(), &uploaded)
            .await?)
    }

    async fn abort_multipart(&self, key: &str, id: &UploadId) -> Result<(), Error> {
        Ok(self.client.abort_multipart_upload(key, id.as_str()).await?)
    }

    async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartPage, Error> {
        let (uploads, next_key_marker, next_upload_id_marker) = self
            .client
            .list_multipart_uploads(prefix, key_marker, upload_id_marker)
            .await?;
        let uploads = uploads
            .into_iter()
            .map(|u| MultipartUpload {
                key: u.key,
                upload_id: UploadId::new(u.upload_id),
                initiated_at: u.initiated_at,
            })
            .collect();
        Ok(MultipartPage {
            uploads,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    async fn list_parts(&self, key: &str, id: &UploadId) -> Result<Vec<Part>, Error> {
        let parts = self.client.list_parts(key, id.as_str()).await?;
        Ok(parts.into_iter().map(part_from_uploaded).collect())
    }
}

#[async_trait]
impl PresignedStore for Backend {
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        content_type: Option<&str>,
    ) -> Result<String, Error> {
        Ok(self
            .client
            .generate_presigned_url(key, ttl, content_type)
            .await?)
    }
}

#[cfg(test)]
mod tests;
