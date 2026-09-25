//! Shared test fixtures for storage-backed tests.
//!
//! [`HookedStore`] replaces the hand-rolled delegating store decorators the
//! workspace's tests used for fault injection: it forwards every
//! [`ObjectStore`] method to the wrapped store after consulting a
//! [`StoreHook`], so a test only writes the hook logic (crash on the N-th
//! write, gate a specific key, fail a named key) instead of seventeen
//! delegation methods.
//!
//! Enabled for this crate's own tests and, through the `test-util` feature,
//! for downstream crates' dev-dependencies.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;

use crate::{
    BoxedReader, ByteStream, ChildrenPage, Error, MultipartUploadPage, ObjectMeta, ObjectStore,
    Page,
};

/// Single-frame [`ByteStream`] over `body`.
pub fn frame(body: impl Into<Bytes>) -> ByteStream {
    let bytes = body.into();
    Box::pin(stream::once(async move { Ok(bytes) }))
}

/// One storage call about to be delegated by a [`HookedStore`], with the
/// arguments a hook can meaningfully inspect.
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
        data: &'a Bytes,
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
    CreateUpload {
        key: &'a str,
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

/// Interception point run before every delegated call of a [`HookedStore`].
///
/// Returning an error short-circuits the call without touching the wrapped
/// store; returning `Ok(())` delegates. Hooks hold their own state (atomic
/// counters, gates, fault scripts) and may clone the wrapped store to apply
/// side effects themselves before failing, which models lost-acknowledgement
/// faults.
#[async_trait]
pub trait StoreHook: Send + Sync {
    /// Decide the fate of `op`.
    ///
    /// # Errors
    /// Any error returned here is surfaced to the caller as the outcome of
    /// the intercepted call.
    async fn before(&self, op: StoreOp<'_>) -> Result<(), Error>;
}

/// A store decorator that consults a [`StoreHook`] before delegating to the
/// wrapped store.
///
/// Implemented for an `Arc<dyn ObjectStore>` inner; coerce the wrapped store
/// accordingly at construction. `move_object` is intentionally left on the
/// trait default (copy plus delete), so hooks observe moves through their
/// constituent writes.
#[derive(Debug)]
pub struct HookedStore<S, H> {
    inner: S,
    hook: H,
}

impl<S, H> HookedStore<S, H> {
    pub fn new(inner: S, hook: H) -> Self {
        Self { inner, hook }
    }
}

macro_rules! delegate_object_store {
    ($inner:ty) => {
        #[async_trait]
        impl<H: StoreHook> ObjectStore for HookedStore<$inner, H> {
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
                self.hook.before(StoreOp::Put { key, data: &data }).await?;
                self.inner.create_if_absent(key, data).await
            }

            async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
                self.hook.before(StoreOp::Put { key, data: &data }).await?;
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

            async fn list(
                &self,
                prefix: &str,
                n: u16,
                token: Option<String>,
            ) -> Result<Page<String>, Error> {
                self.hook.before(StoreOp::List { prefix }).await?;
                self.inner.list(prefix, n, token).await
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

            async fn create_upload(&self, key: &str) -> Result<(), Error> {
                self.hook.before(StoreOp::CreateUpload { key }).await?;
                self.inner.create_upload(key).await
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
    };
}

delegate_object_store!(Arc<dyn ObjectStore>);
