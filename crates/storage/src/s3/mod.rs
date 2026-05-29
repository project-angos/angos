//! S3-backed implementations of every capability trait.
//!
//! Wraps [`angos_s3_client::Backend`] so consumers get the storage abstraction
//! without depending on the HTTP/S3 layer directly. The wrapper translates
//! `s3_client::Error` and `io::Error` into [`crate::Error`], adapts S3's
//! flat/delimited listing modes to [`Page`](crate::Page) /
//! [`ChildrenPage`](crate::ChildrenPage), and forwards every conditional and
//! presign operation through unchanged.
//!
//! The [`UploadSessionStore`](crate::UploadSessionStore) implementation lives
//! in [`upload_session`].

mod upload_session;

use std::{sync::Arc, time::Duration};

use angos_s3_client::Backend as S3Backend;
use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    BoxedReader, ChildrenPage, ConditionalStore, Error, Etag, ObjectMeta, ObjectStore, Page,
    PresignedStore,
};

pub const DEFAULT_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Builder for [`Backend`].
pub struct Builder {
    client: Option<Arc<S3Backend>>,
    part_size: u64,
    uniform_parts: bool,
}

impl Builder {
    fn new() -> Self {
        Self {
            client: None,
            part_size: DEFAULT_PART_SIZE,
            uniform_parts: false,
        }
    }

    /// The underlying S3 HTTP client. Construct it with
    /// `angos_s3_client::Backend::new(&config)` and pass it in here.
    #[must_use]
    pub fn client(mut self, client: Arc<S3Backend>) -> Self {
        self.client = Some(client);
        self
    }

    /// Target part size for upload sessions (uniform mode) or minimum part
    /// size before flushing (non-uniform mode). Defaults to 5 MiB — the
    /// S3 minimum.
    #[must_use]
    pub fn part_size(mut self, size: u64) -> Self {
        self.part_size = size;
        self
    }

    /// `true` = uniform mode: each `write_upload` call emits as many parts
    /// of exactly `part_size` bytes as fit, restaging the remainder.
    /// `false` = non-uniform mode: each call emits at most one part of the
    /// full available size, flushing only once the combined pending +
    /// incoming bytes meet `part_size`. Defaults to non-uniform.
    #[must_use]
    pub fn uniform_parts(mut self, on: bool) -> Self {
        self.uniform_parts = on;
        self
    }

    /// # Errors
    /// Returns [`Error::Backend`] when [`client`](Self::client) was never
    /// called.
    pub fn build(self) -> Result<Backend, Error> {
        let client = self
            .client
            .ok_or_else(|| Error::Backend("s3::Backend requires a client".to_string()))?;
        Ok(Backend {
            client,
            part_size: self.part_size,
            uniform_parts: self.uniform_parts,
        })
    }
}

/// S3 [`ObjectStore`] (+ conditional, upload-session, presign) implementation.
#[derive(Clone, Debug)]
pub struct Backend {
    pub client: Arc<S3Backend>,
    pub part_size: u64,
    pub uniform_parts: bool,
}

impl Backend {
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
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
