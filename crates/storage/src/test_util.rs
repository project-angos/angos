//! Fault-injection fixtures, exported to other crates by the `test-util`
//! feature.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;

use crate::{
    BoxedReader, ByteStream, ChildrenPage, Error, MultipartUploadPage, ObjectMeta, ObjectStore,
    Page,
};

/// Single-frame [`ByteStream`] over `body`.
#[must_use]
pub fn frame(body: &[u8]) -> ByteStream {
    let bytes = Bytes::copy_from_slice(body);
    Box::pin(stream::once(async move { Ok(bytes) }))
}

/// The call a [`HookedStore`] is about to delegate.
#[derive(Debug)]
pub enum StoreOp<'a> {
    Get {
        key: &'a str,
    },
    GetStream {
        key: &'a str,
    },
    Put {
        key: &'a str,
    },
    Delete {
        key: &'a str,
    },
    DeletePrefix {
        prefix: &'a str,
    },
    Head {
        key: &'a str,
    },
    List {
        prefix: &'a str,
    },
    ListChildren {
        prefix: &'a str,
    },
    Copy {
        source: &'a str,
        destination: &'a str,
    },
    WriteUpload {
        key: &'a str,
    },
    CompleteUpload {
        key: &'a str,
    },
    AbortUpload {
        key: &'a str,
    },
    ListMultipartUploads,
}

/// Runs before every delegated call; an error short-circuits it.
#[async_trait]
pub trait StoreHook: Send + Sync {
    async fn before(&self, op: StoreOp<'_>) -> Result<(), Error>;
}

/// Consults a [`StoreHook`] before delegating. `move_object` and
/// `promote_upload` keep the trait defaults, so hooks see their copy and
/// delete.
pub struct HookedStore<H> {
    inner: Arc<dyn ObjectStore>,
    hook: H,
}

impl<H> HookedStore<H> {
    pub fn new(inner: Arc<dyn ObjectStore>, hook: H) -> Self {
        Self { inner, hook }
    }
}

#[async_trait]
impl<H: StoreHook> ObjectStore for HookedStore<H> {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.hook.before(StoreOp::Get { key }).await?;
        self.inner.get(key).await
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        self.hook.before(StoreOp::GetStream { key }).await?;
        self.inner.get_stream(key, offset).await
    }

    async fn create_if_absent(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        self.hook.before(StoreOp::Put { key }).await?;
        self.inner.create_if_absent(key, data).await
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        self.hook.before(StoreOp::Put { key }).await?;
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        self.hook.before(StoreOp::Delete { key }).await?;
        self.inner.delete(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        self.hook.before(StoreOp::DeletePrefix { prefix }).await?;
        self.inner.delete_prefix(prefix).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        self.hook.before(StoreOp::Head { key }).await?;
        self.inner.head(key).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<Page<String>, Error> {
        self.hook.before(StoreOp::List { prefix }).await?;
        self.inner.list_after(prefix, n, token, start_after).await
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        self.hook.before(StoreOp::ListChildren { prefix }).await?;
        self.inner
            .list_children(prefix, n, token, start_after)
            .await
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<u64, Error> {
        self.hook
            .before(StoreOp::Copy {
                source,
                destination,
            })
            .await?;
        self.inner.copy(source, destination).await
    }

    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error> {
        self.hook.before(StoreOp::WriteUpload { key }).await?;
        self.inner.write_upload(key, body, len).await
    }

    async fn complete_upload(&self, key: &str) -> Result<(), Error> {
        self.hook.before(StoreOp::CompleteUpload { key }).await?;
        self.inner.complete_upload(key).await
    }

    async fn abort_upload(&self, key: &str) -> Result<(), Error> {
        self.hook.before(StoreOp::AbortUpload { key }).await?;
        self.inner.abort_upload(key).await
    }

    async fn list_multipart_uploads(
        &self,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, Error> {
        self.hook.before(StoreOp::ListMultipartUploads).await?;
        self.inner
            .list_multipart_uploads(key_marker, upload_id_marker)
            .await
    }
}
