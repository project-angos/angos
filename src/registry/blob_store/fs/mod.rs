#[cfg(test)]
pub mod tests;

mod session;

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use serde::Deserialize;
use tracing::instrument;

use angos_tx_engine::{executor::TransactionExecutor, transaction::Mutation};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{BlobStore, BoxedReader, Error, UploadStore, UploadSummary},
        pagination, path_builder,
    },
};
use angos_storage::{Error as StorageError, ObjectStore, fs::Backend as StorageFsBackend};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    #[serde(default)]
    pub sync_to_disk: bool,
}

#[derive(Clone)]
pub struct Backend {
    pub store: StorageFsBackend,
    executor: Arc<dyn TransactionExecutor>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish_non_exhaustive()
    }
}

impl Backend {
    pub fn builder() -> BackendBuilder {
        BackendBuilder::default()
    }

    /// Return the transaction executor shared across all session operations.
    pub fn executor(&self) -> &Arc<dyn TransactionExecutor> {
        &self.executor
    }
}

#[derive(Default)]
pub struct BackendBuilder {
    root_dir: Option<String>,
    sync_to_disk: Option<bool>,
    executor: Option<Arc<dyn TransactionExecutor>>,
}

impl BackendBuilder {
    #[must_use]
    pub fn root_dir(mut self, root_dir: impl Into<String>) -> Self {
        self.root_dir = Some(root_dir.into());
        self
    }

    #[must_use]
    pub fn sync_to_disk(mut self, sync_to_disk: bool) -> Self {
        self.sync_to_disk = Some(sync_to_disk);
        self
    }

    #[must_use]
    pub fn executor(mut self, executor: Arc<dyn TransactionExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let root_dir = self.root_dir.ok_or_else(|| {
            Error::StorageBackend("FS blob-store builder requires root_dir".into())
        })?;
        let sync_to_disk = self.sync_to_disk.unwrap_or(false);
        let executor = self.executor.ok_or_else(|| {
            Error::StorageBackend("FS blob-store builder requires an executor".into())
        })?;
        let store = StorageFsBackend::builder()
            .root_dir(&root_dir)
            .sync_to_disk(sync_to_disk)
            .build()
            .map_err(|e| Error::StorageBackend(e.to_string()))?;
        Ok(Backend { store, executor })
    }
}

#[async_trait]
impl BlobStore for Backend {
    #[instrument(skip(self))]
    async fn list(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error> {
        let sha256_prefix = format!("{}/sha256", path_builder::blobs_root_dir());

        let (shards, _) = self.store.list_all_children(&sha256_prefix).await?;

        let mut digests = Vec::new();
        for shard in shards {
            let shard_path = format!("{sha256_prefix}/{shard}");
            let (digest_names, _) = self.store.list_all_children(&shard_path).await?;
            for digest_name in digest_names {
                digests.push(Digest::Sha256(digest_name.into()));
            }
        }

        Ok(pagination::paginate(
            &digests,
            n,
            continuation_token.as_deref(),
        ))
    }

    #[instrument(skip(self))]
    async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error> {
        match self.store.get(&path_builder::blob_path(digest)).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn size(&self, digest: &Digest) -> Result<u64, Error> {
        match self.store.head(&path_builder::blob_path(digest)).await {
            Ok(meta) => Ok(meta.size),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        match self
            .store
            .get_stream(&path_builder::blob_path(digest), start_offset)
            .await
        {
            Ok((reader, total)) => Ok((reader, total)),
            Err(StorageError::NotFound) => Err(Error::BlobNotFound),
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self))]
    async fn delete(&self, digest: &Digest) -> Result<(), Error> {
        let container = path_builder::blob_container_dir(digest);
        self.store.delete_prefix(&container).await?;
        // Tidy up the two-character shard parent (`<algo>/<prefix>`); stop at
        // the `blobs/<algo>` level which is shared across all blobs.
        self.store.prune_empty_ancestors(&container, 2).await;
        Ok(())
    }
}

#[async_trait]
impl UploadStore for Backend {
    #[instrument(skip(self))]
    async fn list(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        self.list_via_session(namespace, n, continuation_token)
            .await
    }

    #[instrument(skip(self))]
    async fn create(&self, name: &str, uuid: &str) -> Result<String, Error> {
        self.create_via_session(name, uuid).await
    }

    #[instrument(skip(self, stream))]
    async fn write(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>,
        _content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        self.write_via_session(name, uuid, stream, append).await
    }

    #[instrument(skip(self))]
    async fn summary(&self, name: &str, uuid: &str) -> Result<UploadSummary, Error> {
        self.summary_via_session(name, uuid).await
    }

    #[instrument(skip(self))]
    async fn complete(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        self.complete_via_session(name, uuid, digest).await
    }

    #[instrument(skip(self))]
    async fn finalize_mutations(
        &self,
        name: &str,
        uuid: &str,
        expected_digest: Option<&Digest>,
    ) -> Result<(Digest, Vec<Mutation>), Error> {
        self.finalize_mutations_via_session(name, uuid, expected_digest)
            .await
    }

    #[instrument(skip(self))]
    async fn delete(&self, name: &str, uuid: &str) -> Result<(), Error> {
        self.delete_via_session(name, uuid).await
    }
}
