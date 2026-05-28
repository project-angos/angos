//! Upload-session operations for the filesystem blob-store.
//!
//! Upload sessions are represented as JSON records at
//! `upload-sessions/<namespace>/<uuid>.json` (the "session record"). The
//! `complete` step submits a single `Transaction` to the backend's shared
//! `TransactionExecutor` so the rename from upload staging to blob canonical
//! path is atomic with the session-record deletion.
//!
//! The session record is the sole source of truth for upload metadata.

use std::io::SeekFrom;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncSeekExt;
use tracing::instrument;
use uuid::Uuid;

use angos_tx_engine::transaction::{Mutation, Transaction};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            Error, UploadSummary, fs::Backend, hashing_reader::HashingReader,
            sha256_ext::Sha256Ext, upload_session::UploadSessionRecord,
        },
        pagination, path_builder,
    },
};
use angos_storage::{Error as StorageError, ObjectStore};

impl Backend {
    // ── session-record helpers ───────────────────────────────────────────

    async fn read_session(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Result<UploadSessionRecord, Error> {
        let key = path_builder::upload_session_path(namespace, uuid);
        match self.store.get(&key).await {
            Ok(data) => serde_json::from_slice(&data).map_err(Error::from),
            Err(StorageError::NotFound) => Err(Error::UploadNotFound),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_session(&self, record: &UploadSessionRecord) -> Result<(), Error> {
        let key = path_builder::upload_session_path(&record.namespace, &record.session_id);
        let body = serde_json::to_vec(record).map_err(Error::from)?;
        self.store.put(&key, Bytes::from(body)).await?;
        Ok(())
    }

    // ── UploadStore session methods ──────────────────────────────────────

    /// List upload sessions for `namespace`.
    #[instrument(skip(self))]
    pub async fn list_via_session(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let prefix = path_builder::upload_sessions_namespace_prefix(namespace);
        let page = self.store.list(&prefix, 1000, None).await?;

        // Items returned by `list` are already relative to `prefix`, so each
        // item is just `<uuid>.json` — strip only the `.json` suffix.
        let mut uuids: Vec<String> = page
            .items
            .iter()
            .filter_map(|key| key.strip_suffix(".json").map(ToOwned::to_owned))
            .collect();
        uuids.sort();

        Ok(pagination::paginate(
            &uuids,
            n,
            continuation_token.as_deref(),
        ))
    }

    /// Create a fresh upload session and return its UUID.
    ///
    /// Also creates the upload-staging file (empty) so `write_via_session` can
    /// open it with `open_for_write`.
    #[instrument(skip(self))]
    pub async fn create_via_session(&self, namespace: &str, uuid: &str) -> Result<String, Error> {
        // Create the empty staging file so `open_for_write` can seek into it.
        let upload_key = path_builder::upload_path(namespace, uuid);
        self.store.put(&upload_key, Bytes::new()).await?;

        let initial_state = Sha256::new().serialized_state();
        let hash_context = BASE64.encode(&initial_state);

        let record = UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.to_string(),
            owner: Uuid::new_v4().to_string(),
            started_at: Utc::now(),
            size: 0,
            hash_context,
            multipart_upload_id: None,
            parts: Vec::new(),
        };
        self.write_session(&record).await?;
        Ok(uuid.to_string())
    }

    /// Append bytes to the staging file and update the session record.
    #[instrument(skip(self, stream))]
    pub async fn write_via_session(
        &self,
        namespace: &str,
        uuid: &str,
        stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        let mut record = self.read_session(namespace, uuid).await?;

        let upload_key = path_builder::upload_path(namespace, uuid);
        let (mut file, upload_size) = self
            .store
            .open_for_write(&upload_key, append)
            .await
            .map_err(|e| match e {
                StorageError::NotFound => Error::UploadNotFound,
                other => Error::from(other),
            })?;

        file.seek(SeekFrom::Start(upload_size)).await?;

        let state_bytes = BASE64
            .decode(&record.hash_context)
            .map_err(|e| Error::HashSerialization(e.to_string()))?;
        let hasher = Sha256::from_state(&state_bytes)?;

        let mut reader = HashingReader::with_hasher(stream, hasher);

        let written = tokio::io::copy(&mut reader, &mut file).await?;
        let total_size = upload_size + written;
        let digest = reader.digest();

        // Refresh heartbeat and persist updated state.
        record.started_at = Utc::now();
        record.size = total_size;
        record.hash_context = BASE64.encode(reader.serialized_state());
        self.write_session(&record).await?;

        Ok((digest, total_size))
    }

    /// Read size and start time from the session record.
    #[instrument(skip(self))]
    pub async fn summary_via_session(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Result<UploadSummary, Error> {
        let record = self.read_session(namespace, uuid).await?;
        Ok(UploadSummary {
            size: record.size,
            started_at: record.started_at,
        })
    }

    /// Compute the canonical digest of the staged bytes and return the
    /// mutations needed to atomically promote them to `blob-data/<digest>`
    /// and delete the session record. Used by callers (e.g. the cache job
    /// handler) that merge these mutations into a larger transaction.
    #[instrument(skip(self))]
    pub async fn finalize_mutations_via_session(
        &self,
        namespace: &str,
        uuid: &str,
        expected_digest: Option<&Digest>,
    ) -> Result<(Digest, Vec<Mutation>), Error> {
        let record = self.read_session(namespace, uuid).await?;

        let digest = if let Some(d) = expected_digest {
            d.clone()
        } else {
            let state_bytes = BASE64
                .decode(&record.hash_context)
                .map_err(|e| Error::HashSerialization(e.to_string()))?;
            Sha256::from_state(&state_bytes)?.digest()
        };

        let upload_key = path_builder::upload_path(namespace, uuid);
        let blob_key = path_builder::blob_path(&digest);
        let session_key = path_builder::upload_session_path(namespace, uuid);

        Ok((
            digest,
            vec![
                Mutation::Move {
                    src: upload_key,
                    dst: blob_key,
                },
                Mutation::Delete {
                    key: session_key,
                    expected: None,
                },
            ],
        ))
    }

    /// Copy the upload staging file to its canonical blob path and delete the
    /// session record + staging container via a single engine transaction.
    #[instrument(skip(self))]
    pub async fn complete_via_session(
        &self,
        namespace: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let (digest, mutations) = self
            .finalize_mutations_via_session(namespace, uuid, digest)
            .await?;
        let mut builder = Transaction::builder();
        for m in mutations {
            builder = builder.mutation(m);
        }

        self.executor()
            .execute(builder.build())
            .await
            .map_err(|e| Error::StorageBackend(e.to_string()))?;

        // Best-effort cleanup of empty container directories (not in the
        // transaction because prune_empty_ancestors is FS-specific).
        let container = path_builder::upload_container_path(namespace, uuid);
        self.store.prune_empty_ancestors(&container, 2).await;

        Ok(digest)
    }

    /// Delete the session record and staging container.
    #[instrument(skip(self))]
    pub async fn delete_via_session(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        let session_key = path_builder::upload_session_path(namespace, uuid);
        // Delete session record — ignore NotFound (idempotent).
        match self.store.delete(&session_key).await {
            Ok(()) | Err(StorageError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }

        let container = path_builder::upload_container_path(namespace, uuid);
        self.store.delete_prefix(&container).await?;
        self.store.prune_empty_ancestors(&container, 2).await;
        Ok(())
    }
}
