//! Durable upload-session record and the orchestration that drives it.
//!
//! A JSON record at `upload-sessions/<namespace>/<uuid>.json` is the single
//! source of truth for upload metadata that must survive a process crash.
//! Backend-specific upload state lives inside `session` (an opaque
//! [`UploadSession`] handed back by the storage backend on every call);
//! `blob_store` only carries the OCI-domain fields the storage layer must
//! not know about (hash context, OCI namespace, owner instance id).
//!
//! Path naming, hashing, OCI digest validation, and the engine transaction
//! that promotes the assembled object to `blob-data/<digest>` live here.
//! Backend-specific upload mechanics (FS append, S3 multipart) are
//! encapsulated inside [`UploadSessionStore`]; this module never sees them.
//!
//! `complete` is two-phase:
//! 1. [`UploadSessionStore::complete_upload`] runs (S3 multipart-complete
//!    on S3; no-op on FS) so the assembled object lands at `upload_path`.
//! 2. A single engine `Transaction` atomically moves the assembled object
//!    to its canonical blob path and deletes the session record.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncRead;
use tracing::instrument;
use uuid::Uuid;

use angos_tx_engine::{StorageError, UploadSession, transaction::Mutation};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            BlobStore, Error, UploadSummary,
            hashing_reader::{HashingReader, hashing_stream},
            sha256_ext::Sha256Ext,
        },
        pagination, path_builder,
    },
};

/// A durable upload-session record persisted at
/// `upload-sessions/<namespace>/<uuid>.json`. The `session_id` equals the
/// upload `uuid` so the existing `(namespace, uuid)` addressing maps 1:1
/// without introducing a new ID space.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionRecord {
    /// Equals the upload UUID passed to `BlobStore::create_upload`.
    pub session_id: String,
    /// OCI namespace owning this upload.
    pub namespace: String,
    /// Instance identifier of the process that created the session.
    pub owner: String,
    /// Wall-clock time at which the session was created (and refreshed on
    /// each `write` call so `scrub`'s `UploadChecker` uses the latest
    /// activity time rather than creation time alone).
    pub started_at: DateTime<Utc>,
    /// Serialised SHA-256 state, base64-encoded (raw bytes from
    /// `Sha256Ext::serialized_state`). Persisted so the hash computation can
    /// resume after a crash without re-reading the uploaded bytes.
    pub hash_context: String,
    /// Opaque backend-managed upload state. Mutated in place by every
    /// `UploadSessionStore::write_upload` call and re-serialised here.
    pub session: UploadSession,
}

impl BlobStore {
    pub async fn read_session(
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

    #[instrument(skip(self))]
    pub async fn list_uploads(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let prefix = path_builder::upload_sessions_namespace_prefix(namespace);
        let page = self.store.list(&prefix, 1000, None).await?;

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

    #[instrument(skip(self))]
    pub async fn create_upload(&self, namespace: &str, uuid: &str) -> Result<String, Error> {
        let upload_path = path_builder::upload_path(namespace, uuid);
        // Clear any leaked multipart session at this key from a previous run.
        self.store.abort_pending_uploads(&upload_path).await?;

        let session = self.store.create_upload(&upload_path).await?;
        let hash_context = BASE64.encode(Sha256::new().serialized_state());

        let record = UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.to_string(),
            owner: Uuid::new_v4().to_string(),
            started_at: Utc::now(),
            hash_context,
            session,
        };
        self.write_session(&record).await?;
        Ok(uuid.to_string())
    }

    #[instrument(skip(self, stream))]
    pub async fn write_upload(
        &self,
        namespace: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
    ) -> Result<(Digest, u64), Error> {
        let mut record = self.read_session(namespace, uuid).await?;

        if content_length == 0 {
            let state_bytes = BASE64
                .decode(&record.hash_context)
                .map_err(|e| Error::HashSerialization(e.to_string()))?;
            let digest = Sha256::from_state(&state_bytes)?.digest();
            return Ok((digest, record.session.uploaded_size));
        }

        let state_bytes = BASE64
            .decode(&record.hash_context)
            .map_err(|e| Error::HashSerialization(e.to_string()))?;
        let hasher = Sha256::from_state(&state_bytes)?;
        let hashing_reader = HashingReader::with_hasher(stream, hasher);
        let (body_stream, finish) = hashing_stream(hashing_reader, content_length);

        let staging_key = path_builder::upload_patch_pending_path(namespace, uuid);
        let write_result = self
            .store
            .write_upload(
                &mut record.session,
                &staging_key,
                body_stream,
                content_length,
            )
            .await;
        let final_state = finish
            .await
            .map_err(|e| Error::StorageBackend(e.to_string()))?;
        // Hash-task errors (typically UploadBodySize) win over the storage
        // error they triggered.
        let final_state = match (write_result, final_state) {
            (Ok(()), Ok(state)) => state,
            (_, Err(e)) => return Err(e),
            (Err(e), Ok(_)) => return Err(e.into()),
        };

        record.hash_context = BASE64.encode(&final_state.serialized);
        record.started_at = Utc::now();
        self.write_session(&record).await?;

        Ok((final_state.digest, record.session.uploaded_size))
    }

    #[instrument(skip(self))]
    pub async fn upload_summary(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Result<UploadSummary, Error> {
        let record = self.read_session(namespace, uuid).await?;
        Ok(UploadSummary {
            size: record.session.uploaded_size,
            started_at: record.started_at,
        })
    }

    /// Compute the canonical digest, run the backend completion step, and
    /// return the engine mutations that atomically promote the staged
    /// bytes to `blob-data/<digest>` and delete the session record.
    #[instrument(skip(self))]
    pub async fn finalize_upload_mutations(
        &self,
        namespace: &str,
        uuid: &str,
        expected_digest: Option<&Digest>,
    ) -> Result<(Digest, Vec<Mutation>), Error> {
        let record = self.read_session(namespace, uuid).await?;
        let upload_key = record.session.key.clone();
        let session_key = path_builder::upload_session_path(namespace, uuid);

        let digest = resolve_digest(&record, expected_digest)?;
        let staging_key = path_builder::upload_patch_pending_path(namespace, uuid);
        self.store
            .complete_upload(record.session, &staging_key)
            .await?;
        let blob_key = path_builder::blob_path(&digest);

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

    /// Finish the upload and atomically relocate the data to the canonical
    /// blob path while deleting the session record.
    ///
    /// Delegates the two-phase finalization (backend completion + the
    /// promoting `Move`+`Delete` transaction) to the engine's
    /// [`Store::finalize_upload`](angos_tx_engine::store::Store::finalize_upload).
    #[instrument(skip(self))]
    pub async fn complete_upload(
        &self,
        namespace: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let record = self.read_session(namespace, uuid).await?;
        let final_digest = resolve_digest(&record, digest)?;
        let staging_key = path_builder::upload_patch_pending_path(namespace, uuid);
        let session_key = path_builder::upload_session_path(namespace, uuid);
        let blob_key = path_builder::blob_path(&final_digest);

        self.store
            .finalize_upload(record.session, &staging_key, &blob_key, &session_key)
            .await
            .map_err(|e| Error::StorageBackend(e.to_string()))?;

        // Best-effort cleanup of staging artifacts; not in the transaction
        // because failure here does not affect correctness.
        let container = path_builder::upload_container_path(namespace, uuid);
        let _ = self.store.delete_prefix(&container).await;
        self.store.prune_empty_ancestors(&container, 2).await;
        Ok(final_digest)
    }

    /// Abort the upload and delete the session record plus any staged
    /// bytes. Idempotent.
    #[instrument(skip(self))]
    pub async fn delete_upload(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        let staging_key = path_builder::upload_patch_pending_path(namespace, uuid);
        if let Ok(record) = self.read_session(namespace, uuid).await {
            let _ = self.store.abort_upload(record.session, &staging_key).await;
        }
        let upload_path = path_builder::upload_path(namespace, uuid);
        let _ = self.store.abort_pending_uploads(&upload_path).await;

        let session_key = path_builder::upload_session_path(namespace, uuid);
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

fn resolve_digest(
    record: &UploadSessionRecord,
    expected: Option<&Digest>,
) -> Result<Digest, Error> {
    if let Some(d) = expected {
        return Ok(d.clone());
    }
    let state_bytes = BASE64
        .decode(&record.hash_context)
        .map_err(|e| Error::HashSerialization(e.to_string()))?;
    Ok(Sha256::from_state(&state_bytes)?.digest())
}
