//! Durable upload-session state and the orchestration that drives it.
//!
//! Upload metadata that must survive a process crash is persisted as a set of
//! transparent files under the per-upload container
//! `v2/repositories/<namespace>/_uploads/<uuid>/`:
//!
//! - `startedat` — RFC3339 timestamp of the last activity, used by `scrub` for
//!   age-based orphan detection.
//! - `hashstates/sha256/<offset>` — the serialised SHA-256 hasher state after
//!   consuming the upload's bytes up to `<offset>`, so the hash computation can
//!   resume after a crash without re-reading the uploaded bytes.
//! - `session` — the opaque backend-managed [`UploadSession`] (JSON), carrying
//!   the S3 multipart upload id and parts list so an in-flight multipart upload
//!   survives a restart.
//! - `data` — the assembled upload bytes (FS append target / S3 multipart key).
//! - `staged/<offset>` — S3-only multipart sub-part remainder, one file per
//!   offset, superseded as the upload advances.
//!
//! Backend-specific upload mechanics (FS append, S3 multipart) are
//! encapsulated inside the storage backend's upload-session methods; this
//! module never sees them.
//! Session metadata is reconstructed by reading the per-file artifacts through
//! the engine [`Store`](angos_tx_engine::store::Store), never from a single
//! JSON blob.
//!
//! `complete` is two-phase:
//! 1. [`Store::complete_upload`](angos_tx_engine::store::Store::complete_upload)
//!    runs (S3 multipart-complete on S3; no-op on FS) so the assembled object
//!    lands at `upload_path`.
//! 2. A single engine `Transaction` atomically moves the assembled object to
//!    its canonical blob path and deletes the per-file session artifacts.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncRead;
use tracing::instrument;

use angos_tx_engine::{
    StorageError, UploadSession,
    transaction::{Mutation, Transaction},
};

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

/// Hash algorithm under which the serialised hasher state is checkpointed
/// (`hashstates/<HASH_ALGORITHM>/<offset>`).
const HASH_ALGORITHM: &str = "sha256";

/// In-memory reconstruction of an upload session, assembled from the
/// per-file artifacts under the upload container. The `session_id` equals the
/// upload `uuid` so the existing `(namespace, uuid)` addressing maps 1:1
/// without introducing a new ID space.
#[derive(Debug, Clone)]
pub struct UploadSessionRecord {
    /// Equals the upload UUID passed to `BlobStore::create_upload`.
    pub session_id: String,
    /// OCI namespace owning this upload.
    pub namespace: String,
    /// Wall-clock time of the last activity, read from the `startedat` file
    /// (refreshed on each `write` call so `scrub`'s `UploadChecker` uses the
    /// latest activity time rather than creation time alone).
    pub started_at: DateTime<Utc>,
    /// Raw serialised SHA-256 hasher state read from the highest-offset
    /// `hashstates/sha256/<offset>` checkpoint. Resumes the hash computation
    /// after a crash without re-reading the uploaded bytes.
    pub hash_context: Vec<u8>,
    /// Opaque backend-managed upload state, read from the `session` file.
    /// Mutated in place by every `write_upload` call and re-serialised back to
    /// that file.
    pub session: UploadSession,
}

impl BlobStore {
    pub async fn read_session(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Result<UploadSessionRecord, Error> {
        let session_key = path_builder::upload_session_state_path(namespace, uuid);
        let session = match self.store.get(&session_key).await {
            Ok(data) => serde_json::from_slice::<UploadSession>(&data).map_err(Error::from)?,
            Err(StorageError::NotFound) => return Err(Error::UploadNotFound),
            Err(e) => return Err(e.into()),
        };

        let started_at = self.read_start_date(namespace, uuid).await?;
        let hash_context = self.read_hash_context(namespace, uuid).await?;

        Ok(UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.to_string(),
            started_at,
            hash_context,
            session,
        })
    }

    /// Persist the opaque backend session, the activity timestamp, and the
    /// hasher-state checkpoint for `record` to their respective per-file
    /// artifacts under the upload container.
    async fn write_session(&self, record: &UploadSessionRecord) -> Result<(), Error> {
        let namespace = &record.namespace;
        let uuid = &record.session_id;

        let session_key = path_builder::upload_session_state_path(namespace, uuid);
        let body = serde_json::to_vec(&record.session).map_err(Error::from)?;
        self.store.put(&session_key, Bytes::from(body)).await?;

        self.write_start_date(namespace, uuid, record.started_at)
            .await?;
        self.write_hash_context(
            namespace,
            uuid,
            record.session.uploaded_size,
            &record.hash_context,
        )
        .await?;
        Ok(())
    }

    /// Read the RFC3339 `startedat` file and parse it as a UTC timestamp.
    async fn read_start_date(&self, namespace: &str, uuid: &str) -> Result<DateTime<Utc>, Error> {
        let key = path_builder::upload_start_date_path(namespace, uuid);
        let data = match self.store.get(&key).await {
            Ok(data) => data,
            Err(StorageError::NotFound) => return Err(Error::UploadNotFound),
            Err(e) => return Err(e.into()),
        };
        let text = String::from_utf8(data)?;
        Ok(DateTime::parse_from_rfc3339(text.trim())?.with_timezone(&Utc))
    }

    /// Write the RFC3339 `startedat` file.
    async fn write_start_date(
        &self,
        namespace: &str,
        uuid: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), Error> {
        let key = path_builder::upload_start_date_path(namespace, uuid);
        let body = started_at.to_rfc3339();
        self.store.put(&key, Bytes::from(body)).await?;
        Ok(())
    }

    /// Read the highest-offset `hashstates/sha256/<offset>` checkpoint. The
    /// offset is the cumulative number of bytes hashed, so the maximum offset
    /// is the most recent state.
    async fn read_hash_context(&self, namespace: &str, uuid: &str) -> Result<Vec<u8>, Error> {
        let dir = format!(
            "{}/",
            path_builder::upload_hash_context_dir(namespace, uuid, HASH_ALGORITHM)
        );
        let mut highest: Option<u64> = None;
        let mut token = None;
        loop {
            let page = self.store.list(&dir, 1000, token).await?;
            for key in &page.items {
                // `list` yields prefix-relative keys, so the trailing path
                // component is the checkpoint offset (cumulative bytes hashed).
                let Some(offset) = key.rsplit('/').next().and_then(|s| s.parse::<u64>().ok())
                else {
                    continue;
                };
                if highest.is_none_or(|best| offset > best) {
                    highest = Some(offset);
                }
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        let Some(offset) = highest else {
            return Err(Error::UploadNotFound);
        };
        let key = path_builder::upload_hash_context_path(namespace, uuid, HASH_ALGORITHM, offset);
        match self.store.get(&key).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => Err(Error::UploadNotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Write the serialised hasher `state` as the `hashstates/sha256/<offset>`
    /// checkpoint, where `offset` is the cumulative number of bytes hashed.
    async fn write_hash_context(
        &self,
        namespace: &str,
        uuid: &str,
        offset: u64,
        state: &[u8],
    ) -> Result<(), Error> {
        let key = path_builder::upload_hash_context_path(namespace, uuid, HASH_ALGORITHM, offset);
        self.store.put(&key, Bytes::copy_from_slice(state)).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn list_uploads(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
        let root = format!("{}/", path_builder::uploads_root_dir(namespace));
        let mut uuids: Vec<String> = Vec::new();
        let mut token = None;
        loop {
            let page = self.store.list_children(&root, 1000, token, None).await?;
            // Sub-prefix names are bare per the `ChildrenPage` contract, so the
            // upload UUIDs can be taken directly.
            uuids.extend(page.sub_prefixes);
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
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
        let hash_context = Sha256::new().serialized_state();

        let record = UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.to_string(),
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
            let digest = Sha256::from_state(&record.hash_context)?.digest();
            return Ok((digest, record.session.uploaded_size));
        }

        let hasher = Sha256::from_state(&record.hash_context)?;
        let hashing_reader = HashingReader::with_hasher(stream, hasher);
        let (body_stream, finish) = hashing_stream(hashing_reader, content_length);

        let staged_dir = path_builder::upload_staged_dir(namespace, uuid);
        let write_result = self
            .store
            .write_upload(
                &mut record.session,
                &staged_dir,
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

        record.hash_context = final_state.serialized;
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
    /// bytes to `blob-data/<digest>` and delete the per-file session artifacts.
    #[instrument(skip(self))]
    pub async fn finalize_upload_mutations(
        &self,
        namespace: &str,
        uuid: &str,
        expected_digest: Option<&Digest>,
    ) -> Result<(Digest, Vec<Mutation>), Error> {
        let record = self.read_session(namespace, uuid).await?;
        let upload_key = record.session.key.clone();

        let digest = resolve_digest(&record, expected_digest)?;
        let staged_dir = path_builder::upload_staged_dir(namespace, uuid);
        self.store
            .complete_upload(record.session, &staged_dir)
            .await?;
        let blob_key = path_builder::blob_path(&digest);

        let mut mutations = vec![Mutation::Move {
            src: upload_key,
            dst: blob_key,
        }];
        for key in session_record_keys(namespace, uuid) {
            mutations.push(Mutation::Delete {
                key,
                expected: None,
            });
        }

        Ok((digest, mutations))
    }

    /// Finish the upload and atomically relocate the data to the canonical
    /// blob path while deleting the per-file session artifacts.
    ///
    /// The two-phase finalization is composed here in the registry, not the
    /// engine: [`Self::finalize_upload_mutations`] runs the backend completion
    /// step (an engine *primitive*) and returns the promoting `Move` + record
    /// `Delete` mutations, which this method commits in a single engine
    /// [`Transaction`] via [`Store::execute`](angos_tx_engine::store::Store::execute).
    #[instrument(skip(self))]
    pub async fn complete_upload(
        &self,
        namespace: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let (final_digest, mutations) = self
            .finalize_upload_mutations(namespace, uuid, digest)
            .await?;

        let mut builder = Transaction::builder();
        for mutation in mutations {
            builder = builder.mutation(mutation);
        }
        self.store
            .execute(builder.build())
            .await
            .map_err(|e| Error::StorageBackend(e.to_string()))?;

        // Best-effort cleanup of staging artifacts; not in the transaction
        // because failure here does not affect correctness.
        let container = path_builder::upload_container_path(namespace, uuid);
        let _ = self.store.delete_prefix(&container).await;
        Ok(final_digest)
    }

    /// Abort the upload and delete the per-file session artifacts plus any
    /// staged bytes. Idempotent.
    #[instrument(skip(self))]
    pub async fn delete_upload(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        let staged_dir = path_builder::upload_staged_dir(namespace, uuid);
        if let Ok(record) = self.read_session(namespace, uuid).await {
            let _ = self.store.abort_upload(record.session, &staged_dir).await;
        }
        let upload_path = path_builder::upload_path(namespace, uuid);
        let _ = self.store.abort_pending_uploads(&upload_path).await;

        let container = path_builder::upload_container_path(namespace, uuid);
        self.store.delete_prefix(&container).await?;
        Ok(())
    }
}

/// The per-file session artifacts deleted atomically by the finalization
/// transaction. The bulk staging artifacts (`data`, `staged/`, the
/// `hashstates/` tree) are swept best-effort afterwards via `delete_prefix`;
/// only the metadata files that mark the session as live are removed in the
/// transaction.
fn session_record_keys(namespace: &str, uuid: &str) -> Vec<String> {
    vec![
        path_builder::upload_session_state_path(namespace, uuid),
        path_builder::upload_start_date_path(namespace, uuid),
    ]
}

fn resolve_digest(
    record: &UploadSessionRecord,
    expected: Option<&Digest>,
) -> Result<Digest, Error> {
    if let Some(d) = expected {
        return Ok(d.clone());
    }
    Ok(Sha256::from_state(&record.hash_context)?.digest())
}
