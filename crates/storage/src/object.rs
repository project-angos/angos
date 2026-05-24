use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    BoxedReader,
    error::Error,
    types::{ChildrenPage, ObjectMeta, Page},
};

/// Universal object-storage floor.
///
/// Every storage backend implements this. Both FS and S3 can express every
/// operation here: object CRUD, prefix-batch delete, head metadata, and two
/// listing modes (flat-recursive and one-level-children, separator hard-coded
/// to `/`).
///
/// # Idempotency
///
/// `delete` and `delete_prefix` are idempotent: deleting a missing key or
/// empty prefix counts as success. `get` and `head` on a missing key return
/// [`Error::NotFound`].
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Read the full object body into memory.
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// Open a streaming reader over the object body, optionally starting at
    /// `offset` bytes. The returned `u64` is the **total** object size (not
    /// the remaining length after `offset`).
    async fn get_stream(&self, key: &str, offset: Option<u64>)
    -> Result<(BoxedReader, u64), Error>;

    /// Write `data` to `key`, replacing any existing object atomically.
    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error>;

    /// Delete `key`. Missing key counts as success.
    async fn delete(&self, key: &str) -> Result<(), Error>;

    /// Delete every object whose key starts with `prefix`. Empty prefix
    /// counts as success.
    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error>;

    /// Return the object's size and (when available) `ETag` and
    /// last-modified timestamp without reading the body.
    async fn head(&self, key: &str) -> Result<ObjectMeta, Error>;

    /// Flat-recursive enumeration: returns up to `n` keys under `prefix`,
    /// without grouping by `/`. Pass `token` from the previous call to
    /// resume.
    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error>;

    /// One-level enumeration: returns the immediate sub-prefixes under
    /// `prefix` plus any objects sitting directly at that level (the `/`
    /// separator is hard-coded). `start_after` skips entries up to and
    /// including the given child name; `token` resumes a truncated page.
    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error>;

    /// Server-side copy from `source` to `destination`. Backends choose
    /// whether to issue a single-shot copy or a multipart copy based on
    /// source size and backend-specific thresholds.
    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error>;
}
