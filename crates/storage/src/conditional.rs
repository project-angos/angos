use async_trait::async_trait;
use bytes::Bytes;

use crate::{error::Error, object::ObjectStore, types::Etag};

/// Compare-and-swap extension to [`ObjectStore`].
///
/// Backends that implement this advertise atomic create-if-absent,
/// update-if-unchanged, and delete-if-unchanged. The S3 backend implements it
/// via HTTP conditional headers (`If-None-Match: *`, `If-Match: <etag>`); FS
/// backends in the current plan do not implement it (consumers fall back to
/// a `LockBackend` acquire around an unconditional `put`).
///
/// All conditional methods return [`Error::PreconditionFailed`] when the
/// condition is not met, which the caller distinguishes from a real backend
/// error to drive CAS retry loops.
#[async_trait]
pub trait ConditionalStore: ObjectStore {
    /// Read the object body together with its `ETag`. The `ETag` is `None`
    /// when the backend does not surface one for this object.
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), Error>;

    /// Atomic create-if-absent. Returns the new `ETag` on success (when the
    /// backend surfaces one) or [`Error::PreconditionFailed`] if `key`
    /// already exists. `Ok(None)` means the write succeeded but the backend
    /// returned no `ETag`.
    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, Error>;

    /// Atomic update-if-unchanged. Returns the new `ETag` on success, or
    /// [`Error::PreconditionFailed`] when the stored object's `ETag` no
    /// longer matches `etag`. `Ok(None)` means the write succeeded but the
    /// backend returned no `ETag`.
    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, Error>;

    /// Atomic delete-if-unchanged. Returns [`Error::PreconditionFailed`]
    /// when the stored object's `ETag` no longer matches `etag`. Missing
    /// objects count as success (consistent with [`ObjectStore::delete`]).
    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), Error>;
}
