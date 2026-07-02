//! Pins that the CAS executor verifies its Prepare-phase read set concurrently:
//! every read's `get_with_etag` blocks on a barrier sized to the read count, so
//! the transaction can only complete when all reads are in flight at once. A
//! sequential Prepare would stall on the first read and trip the test timeout.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::{sync::Barrier, time::timeout};

use angos_storage::{
    ByteStream, ChildrenPage, ConditionalStore, Error as StorageError, Etag, MemoryObjectStore,
    MultipartUploadPage, ObjectMeta, ObjectStore, Page,
};
use angos_tx_engine::{
    executor::TransactionExecutor,
    transaction::{Mutation, Transaction},
};

mod common;

/// Delegates everything to an inner [`MemoryObjectStore`] but holds each
/// `get_with_etag` at a [`Barrier`] until every expected read has arrived.
struct BarrierReadStore {
    inner: Arc<MemoryObjectStore>,
    barrier: Barrier,
}

impl std::fmt::Debug for BarrierReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierReadStore").finish_non_exhaustive()
    }
}

#[async_trait]
impl ObjectStore for BarrierReadStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get(key).await
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(angos_storage::BoxedReader, u64), StorageError> {
        self.inner.get_stream(key, offset).await
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
        self.inner.delete_prefix(prefix).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, StorageError> {
        self.inner.list(prefix, n, token).await
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, StorageError> {
        self.inner
            .list_children(prefix, n, token, start_after)
            .await
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
        self.inner.copy(source, destination).await
    }

    async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
        self.inner.create_upload(key).await
    }

    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, StorageError> {
        self.inner.write_upload(key, body, len).await
    }

    async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
        self.inner.complete_upload(key).await
    }

    async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
        self.inner.abort_upload(key).await
    }

    async fn list_multipart_uploads(
        &self,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, StorageError> {
        self.inner
            .list_multipart_uploads(key_marker, upload_id_marker)
            .await
    }
}

#[async_trait]
impl ConditionalStore for BarrierReadStore {
    async fn get_with_etag(&self, key: &str) -> Result<(Vec<u8>, Option<Etag>), StorageError> {
        // Only releases once every Prepare read has arrived: overlap required.
        self.barrier.wait().await;
        self.inner.get_with_etag(key).await
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<Option<Etag>, StorageError> {
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        etag: &Etag,
        data: Bytes,
    ) -> Result<Option<Etag>, StorageError> {
        self.inner.put_if_match(key, etag, data).await
    }

    async fn delete_if_match(&self, key: &str, etag: &Etag) -> Result<(), StorageError> {
        self.inner.delete_if_match(key, etag).await
    }
}

#[tokio::test]
async fn cas_prepare_verifies_reads_concurrently() {
    let inner = Arc::new(MemoryObjectStore::new());
    let read_keys: Vec<String> = (0..4).map(|i| format!("reads/k{i}")).collect();
    for key in &read_keys {
        inner
            .put(key, Bytes::from_static(b"body"))
            .await
            .expect("plant read key");
    }

    let store = Arc::new(BarrierReadStore {
        inner: inner.clone(),
        barrier: Barrier::new(read_keys.len()),
    });
    let executor = common::cas_executor(store, common::memory_lock());

    let mut builder = Transaction::builder();
    for key in &read_keys {
        builder = builder.read(key.clone(), Bytes::from_static(b"body"));
    }
    let tx = builder
        .mutation(Mutation::Put {
            key: "out/result".to_string(),
            body: Bytes::from_static(b"done"),
            expected: None,
        })
        .build();

    timeout(Duration::from_secs(5), executor.execute(tx))
        .await
        .expect("Prepare reads must all be in flight at once to pass the barrier")
        .expect("transaction applies");

    let result = inner.get("out/result").await.expect("mutation applied");
    assert_eq!(result, b"done");
}
