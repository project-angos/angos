use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use tracing::warn;

use crate::BoxedReader;
use crate::error::Error;
use crate::pagination::paginated;
use crate::types::{Children, ChildrenPage, ObjectMeta, Page};
use crate::upload_session::{ByteStream, MultipartUploadPage};

/// Boxed so [`ObjectStore`] stays object-safe.
pub type KeyStream<'a> = Pin<Box<dyn Stream<Item = Result<String, Error>> + Send + 'a>>;

/// The storage floor every backend implements. Deletes are idempotent; `get`
/// and `head` on a missing key return [`Error::NotFound`]. An upload is
/// addressed by its key alone: `write_upload` per chunk, then
/// `complete_upload` or `abort_upload`.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// The returned `u64` is the **total** object size, not what remains after
    /// `offset`.
    async fn get_stream(&self, key: &str, offset: Option<u64>)
    -> Result<(BoxedReader, u64), Error>;

    /// Replaces any existing object atomically.
    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error>;

    async fn delete(&self, key: &str) -> Result<(), Error>;

    /// Deletes everything under the directory `prefix`: `tags/v1` never touches
    /// `tags/v1-rc/...`. An empty prefix is a no-op, never the whole store.
    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error>;

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error>;

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        match self.head(key).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Up to `n` keys under `prefix`, recursively.
    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        self.list_after(prefix, n, token, None).await
    }

    /// `false` when the key already exists. Atomic on every backend (`link(2)`
    /// on FS, `If-None-Match: *` on S3); a provider that cannot honour that
    /// must error rather than overwrite.
    async fn create_if_absent(&self, key: &str, data: Bytes) -> Result<bool, Error>;

    /// [`Self::list`] starting strictly after `start_after`; `token` wins when
    /// both are set.
    async fn list_after(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<Page<String>, Error>;

    /// The immediate children of `prefix`, strictly after the child name
    /// `start_after`.
    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error>;

    /// Every key under `prefix` in ascending order.
    fn list_in_order<'a>(&'a self, prefix: &'a str) -> KeyStream<'a> {
        Box::pin(paginated(move |token| self.list(prefix, 1000, token)))
    }

    /// Every key under `prefix`, in no guaranteed order, so S3 can walk key
    /// ranges concurrently.
    fn list_all<'a>(&'a self, prefix: &'a str) -> KeyStream<'a> {
        self.list_in_order(prefix)
    }

    /// Every immediate child of `prefix`.
    async fn list_all_children(&self, prefix: &str) -> Result<Children, Error> {
        let mut sub_prefixes = Vec::new();
        let mut objects = Vec::new();
        let mut token = None;
        loop {
            let page = self.list_children(prefix, 1000, token, None).await?;
            sub_prefixes.extend(page.sub_prefixes);
            objects.extend(page.objects);
            token = page.next_token;
            if token.is_none() {
                return Ok(Children {
                    sub_prefixes,
                    objects,
                });
            }
        }
    }

    /// Server-side copy, returning the source's size.
    async fn copy(&self, source: &str, destination: &str) -> Result<u64, Error>;

    /// Defaults to a size-checked copy then delete.
    async fn move_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        verified_move(self, source, destination).await
    }

    /// Appends `body`, returning the new total size. `Some(len)` is an exact,
    /// validated length; `None` streams to EOF.
    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error>;

    /// Makes the assembled object readable at `key`, empty when nothing was
    /// written.
    async fn complete_upload(&self, key: &str) -> Result<(), Error>;

    /// Publishes the completed upload as `destination`. A backend whose upload
    /// an in-flight write can still grow must publish exactly the verified
    /// `size` bytes.
    async fn promote_upload(&self, key: &str, destination: &str, _size: u64) -> Result<(), Error> {
        verified_move(self, key, destination).await
    }

    /// Discards the upload and all backend state it owns. Idempotent.
    async fn abort_upload(&self, key: &str) -> Result<(), Error>;

    /// In-flight multipart uploads store-wide; empty on backends without them.
    async fn list_multipart_uploads(
        &self,
        _key_marker: Option<&str>,
        _upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, Error> {
        Ok(MultipartUploadPage::default())
    }
}

const MOVE_COPY_ATTEMPTS: usize = 3;

/// `copy` then `delete`, redoing the copy until the destination's size matches
/// the source's: some S3 providers report success for a copy that landed an
/// empty object. Fails with the source left in place if no attempt matches.
pub async fn verified_move<S: ObjectStore + ?Sized>(
    store: &S,
    source: &str,
    destination: &str,
) -> Result<(), Error> {
    let (mut expected, mut landed) = (0, 0);
    for attempt in 1..=MOVE_COPY_ATTEMPTS {
        expected = store.copy(source, destination).await?;
        landed = store.head(destination).await?.size;
        if landed == expected {
            return store.delete(source).await;
        }
        warn!(
            "Copy of '{source}' to '{destination}' landed {landed} of {expected} bytes \
             (attempt {attempt}/{MOVE_COPY_ATTEMPTS})"
        );
    }
    Err(Error::Backend(format!(
        "copy of '{source}' to '{destination}' landed {landed} bytes, expected {expected}"
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::test_util::{HookedStore, StoreHook, StoreOp};
    use crate::{Error, ObjectStore, fs};

    use super::MOVE_COPY_ATTEMPTS;

    /// Empties `destination` before its first `truncations` size checks, like
    /// a copy that reported success but landed an empty object.
    struct TruncateDestination {
        inner: Arc<dyn ObjectStore>,
        destination: &'static str,
        truncations: usize,
        checks: AtomicUsize,
        copies: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreHook for TruncateDestination {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), Error> {
            match op {
                StoreOp::Copy { .. } => {
                    self.copies.fetch_add(1, Ordering::SeqCst);
                }
                StoreOp::Head { key }
                    if key == self.destination
                        && self.checks.fetch_add(1, Ordering::SeqCst) < self.truncations =>
                {
                    self.inner.put(key, Bytes::new()).await?;
                }
                _ => {}
            }
            Ok(())
        }
    }

    fn hooked(
        dir: &TempDir,
        truncations: usize,
    ) -> (HookedStore<TruncateDestination>, Arc<AtomicUsize>) {
        let inner: Arc<dyn ObjectStore> = Arc::new(fs::Backend::new(dir.path(), false));
        let copies = Arc::new(AtomicUsize::new(0));
        let hook = TruncateDestination {
            inner: Arc::clone(&inner),
            destination: "mv/dst",
            truncations,
            checks: AtomicUsize::new(0),
            copies: Arc::clone(&copies),
        };
        (HookedStore::new(inner, hook), copies)
    }

    #[tokio::test]
    async fn a_move_redoes_a_copy_that_landed_the_wrong_size() {
        let dir = TempDir::new().unwrap();
        let (store, copies) = hooked(&dir, 1);
        store
            .put("mv/src", Bytes::from_static(b"payload"))
            .await
            .unwrap();

        store.move_object("mv/src", "mv/dst").await.unwrap();

        assert_eq!(store.get("mv/dst").await.unwrap(), b"payload");
        assert_eq!(store.head("mv/src").await.unwrap_err(), Error::NotFound);
        assert_eq!(
            copies.load(Ordering::SeqCst),
            2,
            "the copy must be redone once"
        );
    }

    #[tokio::test]
    async fn a_move_that_never_lands_the_right_size_keeps_its_source() {
        let dir = TempDir::new().unwrap();
        let (store, copies) = hooked(&dir, usize::MAX);
        store
            .put("mv/src", Bytes::from_static(b"payload"))
            .await
            .unwrap();

        let error = store
            .move_object("mv/src", "mv/dst")
            .await
            .expect_err("a move whose copies all land the wrong size must fail");

        assert!(
            matches!(&error, Error::Backend(message) if message.contains("expected 7")),
            "the error must name the expected size, got: {error}"
        );
        assert_eq!(copies.load(Ordering::SeqCst), MOVE_COPY_ATTEMPTS);
        assert_eq!(
            store.get("mv/src").await.unwrap(),
            b"payload",
            "the source must survive to redo the move from"
        );
    }
}
