//! Upload-session operations for the S3 blob-store.
//!
//! Upload sessions are backed by a JSON record at
//! `upload-sessions/<namespace>/<uuid>.json`. The S3 multipart-id is persisted
//! in the record so it survives a process crash.
//!
//! The byte-streaming machinery in `uniform.rs` / `nonuniform.rs` is reused
//! unchanged: those helpers accept `(namespace, uuid, ...)` and read/write the
//! existing upload-container files. Only the session metadata (start time, hash
//! state, multipart-id, parts) is lifted into the durable record.
//!
//! The `complete` step follows the same two-phase pattern as the FS backend:
//! 1. Any S3-protocol multipart-complete runs first (outside the transaction)
//!    to assemble the object at `upload_path`.
//! 2. A single engine `Transaction` then atomically moves the assembled object
//!    to its canonical blob path and deletes the session record.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest as _, Sha256};
use tracing::instrument;
use uuid::Uuid;

use angos_tx_engine::transaction::{Mutation, Transaction};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            Error, UploadSummary,
            s3::{
                Backend,
                multipart_helpers::{CompletionPlan, uploaded_size},
            },
            sha256_ext::Sha256Ext,
            upload_session::UploadSessionRecord,
        },
        pagination, path_builder,
    },
};
use angos_storage::{Error as StorageError, MultipartStore, ObjectStore, UploadId};

impl Backend {
    // ── session-record helpers ───────────────────────────────────────────

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

    // ── UploadStore session methods ──────────────────────────────────────

    /// List upload sessions under `upload-sessions/<namespace>/`.
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

    /// Initiate a new upload session.
    ///
    /// For S3, the multipart upload is created immediately so the
    /// `multipart_upload_id` is captured in the session record before any
    /// bytes are written. This ensures that a crash after `create` but before
    /// the first `write` call leaves a recoverable session record containing
    /// the multipart-id.
    #[instrument(skip(self))]
    pub async fn create_via_session(&self, namespace: &str, uuid: &str) -> Result<String, Error> {
        let upload_path = path_builder::upload_path(namespace, uuid);

        // Initiate the S3 multipart upload and persist the ID immediately.
        let multipart_upload_id = self
            .store
            .create_multipart(&upload_path)
            .await?
            .into_inner();

        let initial_state = Sha256::new().serialized_state();
        let hash_context = BASE64.encode(&initial_state);

        // The uniform/nonuniform streaming helpers read the hasher state from the
        // legacy `hashstates/sha256/<offset>` path via `load_hasher`. Write the
        // initial state at offset 0 so the first `write` call can load it.
        self.save_hasher(namespace, uuid, 0, initial_state.clone())
            .await?;

        let record = UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.to_string(),
            owner: Uuid::new_v4().to_string(),
            started_at: Utc::now(),
            size: 0,
            hash_context,
            multipart_upload_id: Some(multipart_upload_id),
            parts: Vec::new(),
        };
        self.write_session(&record).await?;
        Ok(uuid.to_string())
    }

    /// Stream bytes through the existing uniform/nonuniform machinery and
    /// update the session record with refreshed metadata.
    #[instrument(skip(self, stream))]
    pub async fn write_via_session(
        &self,
        namespace: &str,
        uuid: &str,
        stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        // Delegate to the existing streaming code unchanged.
        let result = if self.uniform_parts {
            self.write_upload_uniform(namespace, uuid, stream, content_length, append)
                .await
        } else {
            self.write_upload_nonuniform(namespace, uuid, stream, content_length)
                .await
        };

        if result.is_ok() {
            // Re-read the updated session state from S3 to capture parts/id,
            // then update the durable record with the fresh hash state and size.
            let upload_path = path_builder::upload_path(namespace, uuid);
            let (multipart_upload_id, parts) = self.discover_multipart_upload(&upload_path).await?;

            // Reload the hasher state from the hash-context file that the
            // streaming code updates.
            let total_size = uploaded_size(&parts);
            let hash_state = self.load_hasher(namespace, uuid, total_size).await;

            let mut record = self.read_session(namespace, uuid).await?;
            record.started_at = Utc::now();
            record.size = if let Ok((_, sz)) = &result {
                *sz
            } else {
                record.size
            };
            record.multipart_upload_id = multipart_upload_id;
            record.parts = parts;
            if let Ok(hasher) = hash_state {
                record.hash_context = BASE64.encode(hasher.serialized_state());
            }
            self.write_session(&record).await?;
        }

        result
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

    /// Run the S3-protocol multipart-complete (the non-retractable side-effect)
    /// and return the engine mutations needed to atomically promote the
    /// assembled object to its canonical blob path and delete the session
    /// record. The caller embeds the mutations in a larger transaction.
    ///
    /// **Side-effects already taken** when this returns: the S3 multipart has
    /// been finalised (or an empty-upload was detected). A crash between this
    /// call and the engine transaction's apply leaves an assembled object at
    /// `upload_path` plus a stale session record; scrub's `UploadChecker`
    /// reclaims both.
    #[instrument(skip(self))]
    pub async fn finalize_mutations_via_session(
        &self,
        namespace: &str,
        uuid: &str,
        expected_digest: Option<&Digest>,
    ) -> Result<(Digest, Vec<Mutation>), Error> {
        // Confirm the session exists without deserializing its contents.
        let session_key = path_builder::upload_session_path(namespace, uuid);
        match self.store.head(&session_key).await {
            Ok(_) => {}
            Err(StorageError::NotFound) => return Err(Error::UploadNotFound),
            Err(e) => return Err(e.into()),
        }

        // Run the S3-protocol multipart-complete (if needed) so the assembled
        // object lands at `upload_path`. This step is idempotent on S3.
        let plan = if self.uniform_parts {
            self.prepare_complete_upload_uniform(namespace, uuid, expected_digest)
                .await?
        } else {
            self.prepare_complete_upload_nonuniform(namespace, uuid, expected_digest)
                .await?
        };

        let (blob_mutation, final_digest) = match plan {
            CompletionPlan::Move { src, digest } => {
                let blob_key = path_builder::blob_path(&digest);
                (Mutation::Move { src, dst: blob_key }, digest)
            }
            CompletionPlan::PutEmpty { digest } => {
                let blob_key = path_builder::blob_path(&digest);
                (
                    Mutation::Put {
                        key: blob_key,
                        body: Bytes::new(),
                        expected: None,
                    },
                    digest,
                )
            }
        };

        Ok((
            final_digest,
            vec![
                blob_mutation,
                Mutation::Delete {
                    key: session_key,
                    expected: None,
                },
            ],
        ))
    }

    /// Finish the multipart upload and atomically relocate the data to the
    /// canonical blob path while deleting the session record.
    ///
    /// Two-phase execution:
    /// 1. The S3-protocol multipart-complete assembles the object at
    ///    `upload_path` (outside the transaction).
    /// 2. A single engine `Transaction` atomically moves the assembled object
    ///    to `blob_path(digest)` and deletes the session record.
    ///
    /// Upload-container cleanup (staging artifacts) runs after the transaction
    /// commits and is best-effort.
    #[instrument(skip(self))]
    pub async fn complete_via_session(
        &self,
        namespace: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        let (final_digest, mutations) = self
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

        // Best-effort cleanup of staging artifacts; not in the transaction
        // because failure here does not affect correctness — the scrub pass
        // will reap any orphaned containers.
        let _ = self.delete_upload_container(namespace, uuid).await;

        Ok(final_digest)
    }

    /// Abort the S3 multipart upload and delete the session record plus any
    /// staged bytes. Idempotent.
    #[instrument(skip(self))]
    pub async fn delete_via_session(&self, namespace: &str, uuid: &str) -> Result<(), Error> {
        // Read the session to get the multipart-id (may be absent if crashed
        // before the first `write`).
        if let Ok(record) = self.read_session(namespace, uuid).await
            && let Some(upload_id) = &record.multipart_upload_id
        {
            let upload_path = path_builder::upload_path(namespace, uuid);
            let id = UploadId::new(upload_id);
            // Best-effort abort — ignore errors so the rest of the cleanup
            // still proceeds.
            let _ = self.store.abort_multipart(&upload_path, &id).await;
        }

        // Delete the durable session record.
        let session_key = path_builder::upload_session_path(namespace, uuid);
        match self.store.delete(&session_key).await {
            Ok(()) | Err(StorageError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }

        // Delete staging container.
        let upload_container = path_builder::upload_container_path(namespace, uuid);
        self.store.delete_prefix(&upload_container).await?;
        Ok(())
    }
}
