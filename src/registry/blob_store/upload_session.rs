//! Durable upload progress and the orchestration that drives it.
//!
//! Everything an upload must survive a crash with lives under the container
//! `v2/repositories/<namespace>/_uploads/<session_id>/`: the `session.json`
//! record (last activity, committed offset, hasher checkpoint), the assembled
//! `data` bytes, and the S3-only `staged/<offset>` remainder.
//!
//! The backend owns the upload mechanics behind its keyed `ObjectStore`
//! methods, so nothing about them is persisted here.

use std::io::Cursor;

use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tracing::{instrument, warn};

use angos_oci::{Algorithm, Digest, Namespace, UploadSessionId};
use angos_storage::{Error as StorageError, paginated};

use crate::registry::{
    Error,
    blob_store::{
        BlobStore, UploadSummary,
        hashing_reader::{HashingReader, hashing_stream},
        resumable_hasher::{HashState, Hasher},
    },
    keys::{DigestKeys, NamespaceKeys, REPOS_ROOT},
    pagination,
};

/// Bytes peeked from a chunked (`None`) body to tell an empty finalize from
/// one carrying data.
const PEEK_FRAME_SIZE: usize = 8 * 1024;

/// How an append seeds its hasher.
#[derive(Debug)]
pub enum HashStart {
    /// Rebuild every supported algorithm from the checkpoint, for a chunked
    /// upload whose target algorithm was unknown during PATCH.
    Resume,
    /// Start one algorithm fresh, for a monolithic PUT that knows its target
    /// up front and has no checkpointed bytes.
    Fresh(Algorithm),
}

/// The durable `session.json`: an upload's progress as of its last write.
#[derive(Serialize, Deserialize)]
pub struct SessionFile {
    /// Refreshed on each write so prune's upload sweep ages sessions on
    /// activity rather than creation.
    pub last_activity: DateTime<Utc>,
    /// Bytes written and hashed so far.
    pub committed_offset: u64,
    /// Base64 of the hasher checkpoint at `committed_offset`, so a crash does
    /// not force re-reading those bytes.
    pub hash_state: String,
}

impl SessionFile {
    /// Decode `session.json`, checkpoint included. Any other shape reads as
    /// [`Error::Corrupt`], which prune's upload sweep treats as a session that
    /// can never complete.
    pub fn decode(raw: &[u8]) -> Result<Self, Error> {
        let file: Self = serde_json::from_slice(raw)
            .map_err(|e| Error::Corrupt(format!("upload session record: {e}")))?;
        file.hash_context()?;
        Ok(file)
    }

    /// The hasher checkpoint at `committed_offset`.
    fn hash_context(&self) -> Result<Vec<u8>, Error> {
        BASE64_STANDARD
            .decode(&self.hash_state)
            .map_err(|e| Error::Corrupt(format!("upload session hash state: {e}")))
    }
}

impl BlobStore {
    pub async fn read_session(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
    ) -> Result<SessionFile, Error> {
        let key = namespace.upload_session_path(session_id);
        match self.object.get(&key).await {
            Ok(raw) => SessionFile::decode(&raw),
            Err(StorageError::NotFound) => Err(Error::BlobUploadUnknown),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist `record` as one atomic `session.json` put.
    async fn write_session(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
        record: &SessionFile,
    ) -> Result<(), Error> {
        let key = namespace.upload_session_path(session_id);
        self.object
            .put(&key, Bytes::from(serde_json::to_vec(record)?))
            .await?;
        Ok(())
    }

    /// Streams every in-flight upload UUID in `namespace` lazily, unsorted;
    /// at most one listing page is buffered.
    pub fn stream_uploads(
        &self,
        namespace: &Namespace,
    ) -> impl Stream<Item = Result<UploadSessionId, Error>> + Send + '_ {
        let root = format!("{}/", namespace.uploads_root_dir());
        paginated(move |token| {
            let root = root.clone();
            async move {
                let page = self.object.list_children(&root, 1000, token, None).await?;
                // A directory naming no session is scrub's to quarantine.
                let sessions = page
                    .sub_prefixes
                    .iter()
                    .filter_map(|name| name.parse().ok())
                    .collect();
                Ok((sessions, page.next_token))
            }
        })
    }

    /// Every namespace with an upload session, unpaginated and unsorted;
    /// `scope` restricts the walk to one repository's subtree. It must walk
    /// this store, since the metadata catalog keys namespaces off `_manifests`
    /// and would miss an upload-only namespace on a split backend.
    #[instrument(skip(self))]
    pub async fn collect_upload_namespaces(
        &self,
        scope: Option<&str>,
    ) -> Result<Vec<Namespace>, Error> {
        // The scoped walk keeps the repository segment in the names it
        // returns, so the prefix carries it back into each namespace.
        let (root, prefix) = match scope {
            Some(repository) => (
                format!("{REPOS_ROOT}/{repository}"),
                format!("{repository}/"),
            ),
            None => (REPOS_ROOT.to_string(), String::new()),
        };

        pagination::collect_namespaces_with_marker(
            &root,
            &prefix,
            "_uploads",
            self.namespace_walk_concurrency.get(),
            |path| async move {
                let sub_prefixes = self.object.list_all_children(&path).await?.sub_prefixes;
                Ok(sub_prefixes)
            },
        )
        .await
    }

    /// Opens an upload session. `algorithm` is the one the client said it
    /// would close with, letting the session checkpoint that hash alone;
    /// without it every supported algorithm must be kept, since the digest is
    /// only known at the closing `PUT`.
    #[instrument(skip(self))]
    pub async fn create_upload(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
        algorithm: Option<Algorithm>,
    ) -> Result<(), Error> {
        let upload_path = namespace.upload_path(session_id);
        // Also clears any leaked prior multipart and staged remainder.
        self.object.create_upload(&upload_path).await?;

        let hasher = match algorithm {
            Some(algorithm) => Hasher::for_algorithm(algorithm),
            None => Hasher::new(),
        };
        let record = SessionFile {
            last_activity: Utc::now(),
            committed_offset: 0,
            hash_state: BASE64_STANDARD.encode(hasher.state().to_bytes()?),
        };
        self.write_session(namespace, session_id, &record).await
    }

    /// Append the final chunk and return its digest under `algorithm` plus the
    /// total size. Resuming the checkpoint lets an upload whose algorithm was
    /// unknown during PATCH close under any supported one; a fresh start hashes
    /// a monolithic body under its one known algorithm.
    #[instrument(skip(self, stream))]
    pub async fn write_upload(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
        start: HashStart,
        algorithm: Algorithm,
    ) -> Result<(Digest, u64), Error> {
        let (hasher, size) = self
            .append(namespace, session_id, stream, content_length, start)
            .await?;
        Ok((hasher.digest(algorithm)?, size))
    }

    /// Append a chunk without finalizing, returning the live hasher and the
    /// new total. PATCH discards the hasher; the PUT finalizes the digest.
    #[instrument(skip(self, stream))]
    pub async fn append_upload(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
    ) -> Result<(Hasher, u64), Error> {
        self.append(
            namespace,
            session_id,
            stream,
            content_length,
            HashStart::Resume,
        )
        .await
    }

    /// Append `stream` to the session, persisting the updated hash state and
    /// size, and return the live hasher plus the new total.
    async fn append(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
        mut stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
        start: HashStart,
    ) -> Result<(Hasher, u64), Error> {
        let mut record = self.read_session(namespace, session_id).await?;
        let hasher = match start {
            HashStart::Resume => HashState::from_bytes(&record.hash_context()?)?.into_hasher()?,
            HashStart::Fresh(algorithm) => Hasher::for_algorithm(algorithm),
        };

        if content_length == Some(0) {
            return Ok((hasher, record.committed_offset));
        }

        // A chunked finalize with an empty body must short-circuit like the
        // `Some(0)` branch rather than round-trip for zero bytes, so peek one
        // frame and chain it back ahead of the stream when it carries data.
        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = if content_length.is_none() {
            let mut peek = BytesMut::with_capacity(PEEK_FRAME_SIZE);
            stream
                .read_buf(&mut peek)
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            if peek.is_empty() {
                return Ok((hasher, record.committed_offset));
            }
            Box::new(Cursor::new(peek.freeze()).chain(stream))
        } else {
            stream
        };

        let hashing_reader = HashingReader::new(stream, hasher);
        let (body_stream, finish) = hashing_stream(hashing_reader, content_length);

        let upload_path = namespace.upload_path(session_id);
        let write_result = self
            .object
            .write_upload(&upload_path, body_stream, content_length)
            .await;
        let hash_result = finish.await.map_err(|e| Error::Internal(e.to_string()))?;
        // A hash-task error wins over the storage error it triggered.
        let (backend_total, hasher, written) = match (write_result, hash_result) {
            (Ok(size), Ok((hasher, written))) => (size, hasher, written),
            (_, Err(e)) => return Err(e),
            (Err(e), Ok(_)) => return Err(e.into()),
        };

        // The staged object must be exactly what this session hashed: the
        // checkpoint's bytes plus this call's. A surplus is an orphaned tail a
        // write left behind after durably storing bytes it never checkpointed,
        // so the digest would vouch for content the stored object does not
        // hold. Fail the session closed here rather than promote it at the PUT.
        let hashed_total = record
            .committed_offset
            .checked_add(written)
            .ok_or_else(|| Error::Internal("upload size overflow".to_string()))?;
        if backend_total != hashed_total {
            warn!(
                "Upload session staged {backend_total} bytes but hashed {hashed_total}, failing closed"
            );
            let container = namespace.upload_container_path(session_id);
            let _ = self.object.delete_prefix(&container).await;
            return Err(Error::DigestInvalid);
        }

        record.hash_state = BASE64_STANDARD.encode(hasher.state().to_bytes()?);
        record.committed_offset = hashed_total;
        record.last_activity = Utc::now();
        self.write_session(namespace, session_id, &record).await?;

        Ok((hasher, hashed_total))
    }

    #[instrument(skip(self))]
    pub async fn upload_summary(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
    ) -> Result<UploadSummary, Error> {
        let record = self.read_session(namespace, session_id).await?;
        Ok(UploadSummary {
            size: record.committed_offset,
            started_at: record.last_activity,
        })
    }

    /// Finish the upload and promote the assembled data to its canonical blob
    /// path.
    ///
    /// The session record is consumed up front, so a re-run returns
    /// [`Error::BlobUploadUnknown`] instead of re-finalizing an already
    /// completed upload, which on S3 would overwrite the blob with an empty
    /// object.
    ///
    /// The assembled object must be exactly `hashed_size` long, or its bytes
    /// do not hash to `digest` and it is rejected rather than promoted.
    #[instrument(skip(self))]
    pub async fn complete_upload(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
        digest: &Digest,
        hashed_size: u64,
    ) -> Result<Digest, Error> {
        // The record's existence alone answers liveness, so a HEAD suffices.
        let session_key = namespace.upload_session_path(session_id);
        if !self.object.exists(&session_key).await? {
            return Err(Error::BlobUploadUnknown);
        }
        self.object.delete(&session_key).await?;

        let upload_key = namespace.upload_path(session_id);
        self.object.complete_upload(&upload_key).await?;

        // An append that fails after durably writing bytes leaves staged data
        // the checkpoint never recorded, and the resume that follows hashes
        // only its own bytes, so only the size can show the divergence.
        let staged_size = self.object.head(&upload_key).await?.size;
        if staged_size != hashed_size {
            warn!("Staged {staged_size} bytes but hashed {hashed_size}, refusing to promote");
            let container = namespace.upload_container_path(session_id);
            let _ = self.object.delete_prefix(&container).await;
            return Err(Error::DigestInvalid);
        }

        let blob_key = digest.blob_path();
        self.object.move_object(&upload_key, &blob_key).await?;

        // Best-effort sweep; scrub reclaims whatever is left.
        let container = namespace.upload_container_path(session_id);
        let _ = self.object.delete_prefix(&container).await;

        Ok(digest.clone())
    }

    /// Abort the upload and delete the session artifacts plus any staged
    /// bytes. Idempotent.
    #[instrument(skip(self))]
    pub async fn delete_upload(
        &self,
        namespace: &Namespace,
        session_id: &UploadSessionId,
    ) -> Result<(), Error> {
        let upload_path = namespace.upload_path(session_id);
        // Discards the backend state the upload owns.
        let _ = self.object.abort_upload(&upload_path).await;

        let container = namespace.upload_container_path(session_id);
        self.object.delete_prefix(&container).await?;
        Ok(())
    }
}
