//! Durable upload progress and the orchestration that drives it.
//!
//! Upload metadata that must survive a process crash is persisted as a set of
//! transparent files under the per-upload container
//! `v2/repositories/<namespace>/_uploads/<uuid>/`:
//!
//! - `startedat`: RFC3339 timestamp of the last activity, used by `scrub` for
//!   age-based orphan detection.
//! - `hashstates/<offset>`: serialised hasher state for every supported
//!   digest algorithm, checkpointed together after consuming the upload's bytes
//!   up to `<offset>`, so hashing resumes after a crash without re-reading them.
//!   The highest checkpoint offset also records how many bytes were consumed, so
//!   the upload's size is recovered from it on resume.
//! - `data`: the assembled upload bytes (FS append target / S3 multipart key).
//! - `staged/<offset>`: S3-only multipart sub-part remainder, one file per
//!   offset, superseded as the upload advances.
//!
//! Backend-specific upload mechanics (FS append, S3 multipart) are encapsulated
//! inside the storage backend's keyed [`ObjectStore`] methods; there is no
//! persisted session value, so the S3 backend recovers its multipart state from
//! S3 on each call and the upload is addressed purely by its `data` key. Upload
//! progress (size, hash) is the blob store's concern, reconstructed by reading
//! the per-file artifacts.
//!
//! `complete` promotes the upload under the caller's `blob-data:{digest}` lock:
//! 1. The `startedat` liveness marker is deleted, consuming the session so a
//!    re-run fails (`UploadNotFound`) instead of re-finalizing.
//! 2. The object store's `complete_upload` runs (S3 multipart-complete; no-op
//!    finalize on FS) so the assembled object lands at `upload_path`.
//! 3. The assembled object is moved to its content-addressed blob path, then the
//!    remaining staging artifacts are swept best-effort.

use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::{TryStreamExt, stream::Stream};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    try_join,
};
use tracing::instrument;

use angos_storage::paginated;
use angos_tx_engine::StorageError;

use crate::{
    oci::{Algorithm, Digest, Namespace},
    registry::{
        Error,
        blob_store::{
            BlobStore, UploadSummary,
            hashing_reader::{HashingReader, hashing_stream},
            resumable_hasher::{HashState, Hasher},
        },
        pagination, path_builder,
    },
};

/// Bytes peeked from a chunked (`None`) body to tell an empty finalize from one
/// carrying data, before deciding whether to short-circuit or stream.
const PEEK_FRAME_SIZE: usize = 8 * 1024;

/// How an append seeds its hasher.
enum HashStart {
    /// Rebuild every supported algorithm from the persisted checkpoint, for a
    /// chunked upload whose target algorithm was unknown during PATCH.
    Resume,
    /// Start a single algorithm fresh, for a monolithic PUT whose algorithm is
    /// known up front and which has no prior checkpointed bytes, so the other
    /// algorithms are never computed.
    Fresh(Algorithm),
}

/// In-memory reconstruction of an upload's progress, assembled from the
/// per-file artifacts under the upload container. The `session_id` equals the
/// upload `uuid` so the existing `(namespace, uuid)` addressing maps 1:1
/// without introducing a new ID space.
#[derive(Debug, Clone)]
pub struct UploadSessionRecord {
    /// Equals the upload UUID passed to `BlobStore::create_upload`.
    pub session_id: String,
    /// OCI namespace owning this upload.
    pub namespace: Namespace,
    /// Wall-clock time of the last activity, read from the `startedat` file
    /// (refreshed on each `write` call so `scrub`'s `UploadChecker` uses the
    /// latest activity time rather than creation time alone).
    pub started_at: DateTime<Utc>,
    /// Serialised hasher-state checkpoint for every supported digest algorithm,
    /// read from the highest-offset `hashstates/<offset>` file. Resumes
    /// the hash computation after a crash without re-reading the uploaded bytes.
    pub hash_context: Vec<u8>,
    /// Number of bytes consumed so far, recovered from the highest hasher-state
    /// checkpoint offset (the cumulative bytes hashed equals the bytes written).
    pub uploaded_size: u64,
}

impl BlobStore {
    pub async fn read_session(
        &self,
        namespace: &Namespace,
        uuid: &str,
    ) -> Result<UploadSessionRecord, Error> {
        // The start-date read and the hash-context read (itself a LIST + GET)
        // are independent, so issue them concurrently to save a serial S3
        // round-trip on every session read (every PATCH/finalize).
        let (started_at, (hash_context, uploaded_size)) = try_join!(
            self.read_start_date(namespace, uuid),
            self.read_hash_context(namespace, uuid),
        )?;

        Ok(UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.clone(),
            started_at,
            hash_context,
            uploaded_size,
        })
    }

    /// Persist the activity timestamp and the hasher-state checkpoint for
    /// `record` to their respective per-file artifacts under the upload
    /// container.
    async fn write_session(&self, record: &UploadSessionRecord) -> Result<(), Error> {
        let namespace = &record.namespace;
        let uuid = &record.session_id;

        // The two artifacts live at distinct keys and do not depend on each
        // other, so persist them concurrently to save a serial S3 round-trip on
        // every session write.
        try_join!(
            self.write_start_date(namespace, uuid, record.started_at),
            self.write_hash_context(namespace, uuid, record.uploaded_size, &record.hash_context),
        )?;
        Ok(())
    }

    /// Read the RFC3339 `startedat` file and parse it as a UTC timestamp.
    async fn read_start_date(
        &self,
        namespace: &Namespace,
        uuid: &str,
    ) -> Result<DateTime<Utc>, Error> {
        let key = path_builder::upload_start_date_path(namespace, uuid);
        let data = match self.object.get(&key).await {
            Ok(data) => data,
            Err(StorageError::NotFound) => return Err(Error::BlobUploadUnknown),
            Err(e) => return Err(e.into()),
        };
        let text = String::from_utf8(data)?;
        Ok(DateTime::parse_from_rfc3339(text.trim())?.with_timezone(&Utc))
    }

    /// Write the RFC3339 `startedat` file.
    async fn write_start_date(
        &self,
        namespace: &Namespace,
        uuid: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), Error> {
        let key = path_builder::upload_start_date_path(namespace, uuid);
        let body = started_at.to_rfc3339();
        self.object.put(&key, Bytes::from(body)).await?;
        Ok(())
    }

    /// Read the highest-offset `hashstates/<offset>` checkpoint. The
    /// offset is the cumulative number of bytes hashed, so the maximum offset
    /// is both the most recent hasher state and the bytes consumed so far.
    async fn read_hash_context(
        &self,
        namespace: &Namespace,
        uuid: &str,
    ) -> Result<(Vec<u8>, u64), Error> {
        let dir = format!(
            "{}/",
            path_builder::upload_hash_context_dir(namespace, uuid)
        );
        let dir = &dir;
        let highest: Option<u64> = paginated(move |token| async move {
            let page = self.object.list(dir, 1000, token).await?;
            Ok::<_, Error>((page.items, page.next_token))
        })
        .try_fold(None, |best: Option<u64>, key| async move {
            // `list` yields prefix-relative keys, so the trailing path
            // component is the checkpoint offset (cumulative bytes hashed).
            let offset = key.rsplit('/').next().and_then(|s| s.parse::<u64>().ok());
            Ok(best.max(offset))
        })
        .await?;

        let Some(offset) = highest else {
            return Err(Error::BlobUploadUnknown);
        };
        let key = path_builder::upload_hash_context_path(namespace, uuid, offset);
        match self.object.get(&key).await {
            Ok(data) => Ok((data, offset)),
            Err(StorageError::NotFound) => Err(Error::BlobUploadUnknown),
            Err(e) => Err(e.into()),
        }
    }

    /// Write the serialised hasher `state` as the `hashstates/<offset>`
    /// checkpoint, where `offset` is the cumulative number of bytes hashed.
    async fn write_hash_context(
        &self,
        namespace: &Namespace,
        uuid: &str,
        offset: u64,
        state: &[u8],
    ) -> Result<(), Error> {
        let key = path_builder::upload_hash_context_path(namespace, uuid, offset);
        self.object.put(&key, Bytes::copy_from_slice(state)).await?;
        Ok(())
    }

    /// Streams every in-flight upload UUID in `namespace` lazily, unsorted;
    /// at most one listing page is buffered.
    pub fn stream_uploads(
        &self,
        namespace: &Namespace,
    ) -> impl Stream<Item = Result<String, Error>> + Send + '_ {
        let root = format!("{}/", path_builder::uploads_root_dir(namespace));
        paginated(move |token| {
            let root = root.clone();
            async move {
                let page = self.object.list_children(&root, 1000, token, None).await?;
                // Sub-prefix names are bare per the `ChildrenPage` contract, so
                // the upload UUIDs can be taken directly.
                Ok((page.sub_prefixes, page.next_token))
            }
        })
    }

    /// Walks the `_uploads`-keyed tree in a single concurrent walk and returns
    /// every namespace with an upload session, unpaginated and unsorted. `scope`
    /// restricts the walk to one repository's subtree; `None` walks the whole
    /// store. Upload sessions live on the blob store, so discovery walks this
    /// store: the metadata catalog keys namespaces off `_manifests` and cannot
    /// see an upload-only namespace when the two stores are separate backends.
    #[instrument(skip(self))]
    pub async fn collect_upload_namespaces(
        &self,
        scope: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let (root, prefix) = path_builder::namespace_walk_root(scope);

        pagination::collect_namespaces_with_marker(
            &root,
            &prefix,
            "_uploads",
            pagination::NAMESPACE_WALK_CONCURRENCY,
            |path| async move {
                let (sub_prefixes, _) = self.object.list_all_children(&path).await?;
                Ok(sub_prefixes)
            },
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn create_upload(&self, namespace: &Namespace, uuid: &str) -> Result<String, Error> {
        let upload_path = path_builder::upload_path(namespace, uuid);
        // Begin/clear a fresh upload at the data key (clears any leaked prior
        // multipart and staged remainder).
        self.object.create_upload(&upload_path).await?;

        let hash_context = Hasher::new().state().to_bytes()?;
        let record = UploadSessionRecord {
            session_id: uuid.to_string(),
            namespace: namespace.clone(),
            started_at: Utc::now(),
            hash_context,
            uploaded_size: 0,
        };
        self.write_session(&record).await?;
        Ok(uuid.to_string())
    }

    /// Append the final chunk of a chunked upload and return its digest under
    /// `algorithm` (whose value fixes the canonical blob path) plus the total
    /// size. Resumes the both-algorithm checkpoint, so an upload whose algorithm
    /// was unknown during PATCH can be finalized under any supported algorithm.
    #[instrument(skip(self, stream))]
    pub async fn write_upload(
        &self,
        namespace: &Namespace,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
        algorithm: Algorithm,
    ) -> Result<(Digest, u64), Error> {
        let (hasher, size) = self
            .append(namespace, uuid, stream, content_length, HashStart::Resume)
            .await?;
        Ok((hasher.digest(algorithm)?, size))
    }

    /// Write a single-shot (monolithic) upload whose `algorithm` is known up
    /// front and which has no prior chunked writes, hashing only the target so
    /// the other supported algorithms are never computed. Returns the digest and
    /// total size.
    #[instrument(skip(self, stream))]
    pub async fn write_monolithic_upload(
        &self,
        namespace: &Namespace,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
        algorithm: Algorithm,
    ) -> Result<(Digest, u64), Error> {
        let (hasher, size) = self
            .append(
                namespace,
                uuid,
                stream,
                content_length,
                HashStart::Fresh(algorithm),
            )
            .await?;
        Ok((hasher.digest(algorithm)?, size))
    }

    /// Append a chunk to a chunked upload without finalizing, resuming the
    /// both-algorithm checkpoint, and return the live hasher plus the new total.
    /// PATCH discards the hasher; the digest is finalized at the PUT.
    #[instrument(skip(self, stream))]
    pub async fn append_upload(
        &self,
        namespace: &Namespace,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
    ) -> Result<(Hasher, u64), Error> {
        self.append(namespace, uuid, stream, content_length, HashStart::Resume)
            .await
    }

    /// Append `stream` to the session, persisting the updated hash state and
    /// size, and return the live hasher fed by the full body so far plus the new
    /// total. `start` selects whether the hasher resumes every algorithm from
    /// the checkpoint or starts a single algorithm fresh.
    async fn append(
        &self,
        namespace: &Namespace,
        uuid: &str,
        mut stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: Option<u64>,
        start: HashStart,
    ) -> Result<(Hasher, u64), Error> {
        let mut record = self.read_session(namespace, uuid).await?;
        let hasher = match start {
            HashStart::Resume => HashState::from_bytes(&record.hash_context)?.into_hasher()?,
            HashStart::Fresh(algorithm) => Hasher::for_algorithm(algorithm),
        };

        if content_length == Some(0) {
            return Ok((hasher, record.uploaded_size));
        }

        // A chunked finalize (`None`) with an empty body must short-circuit like
        // the `Some(0)` branch instead of doing a backend round-trip for zero
        // bytes. Peek one frame: on immediate EOF return the seeded hasher; on
        // data, chain the peeked bytes back ahead of the remaining stream so the
        // hashing reader sees the full body (mirrors the backend's staged-remainder
        // chaining).
        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = if content_length.is_none() {
            let mut peek = BytesMut::with_capacity(PEEK_FRAME_SIZE);
            stream
                .read_buf(&mut peek)
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            if peek.is_empty() {
                return Ok((hasher, record.uploaded_size));
            }
            Box::new(Cursor::new(peek.freeze()).chain(stream))
        } else {
            stream
        };

        let hashing_reader = HashingReader::new(stream, hasher);
        let (body_stream, finish) = hashing_stream(hashing_reader, content_length);

        let upload_path = path_builder::upload_path(namespace, uuid);
        let write_result = self
            .object
            .write_upload(&upload_path, body_stream, content_length)
            .await;
        let hash_result = finish.await.map_err(|e| Error::Internal(e.to_string()))?;
        // Hash-task errors (typically UploadBodySize) win over the storage
        // error they triggered.
        let (hasher, new_size) = match (write_result, hash_result) {
            (Ok(size), Ok(hasher)) => (hasher, size),
            (_, Err(e)) => return Err(e),
            (Err(e), Ok(_)) => return Err(e.into()),
        };

        record.hash_context = hasher.state().to_bytes()?;
        record.uploaded_size = new_size;
        record.started_at = Utc::now();
        self.write_session(&record).await?;

        Ok((hasher, new_size))
    }

    #[instrument(skip(self))]
    pub async fn upload_summary(
        &self,
        namespace: &Namespace,
        uuid: &str,
    ) -> Result<UploadSummary, Error> {
        let record = self.read_session(namespace, uuid).await?;
        Ok(UploadSummary {
            size: record.uploaded_size,
            started_at: record.started_at,
        })
    }

    /// Finish the upload and promote the assembled data to its canonical blob
    /// path.
    ///
    /// The session's `startedat` liveness marker is deleted up front, consuming
    /// the session so a re-run returns [`Error::BlobUploadUnknown`] rather than
    /// re-finalizing an already-completed upload (on S3 a naive re-finalize
    /// overwrites the blob with an empty object). The caller holds the
    /// `blob-data:{digest}` lock and skips this when the blob already exists, so
    /// a crash after promotion is short-circuited; a crash after the marker is
    /// consumed but before promotion makes the client re-push, and scrub
    /// reclaims the leftover session dir.
    #[instrument(skip(self))]
    pub async fn complete_upload(
        &self,
        namespace: &Namespace,
        uuid: &str,
        digest: &Digest,
    ) -> Result<Digest, Error> {
        // Confirm the session is live, then consume its liveness marker so any
        // re-run fails at the check above instead of re-finalizing.
        self.read_session(namespace, uuid).await?;
        let started_at = path_builder::upload_start_date_path(namespace, uuid);
        self.object.delete(&started_at).await?;

        let upload_key = path_builder::upload_path(namespace, uuid);
        self.object.complete_upload(&upload_key).await?;

        let blob_key = path_builder::blob_path(digest);
        self.object.move_object(&upload_key, &blob_key).await?;

        // Sweep the remaining staging artifacts best-effort; scrub reclaims any leftover.
        let container = path_builder::upload_container_path(namespace, uuid);
        let _ = self.object.delete_prefix(&container).await;

        Ok(digest.clone())
    }

    /// Abort the upload and delete the per-file session artifacts plus any
    /// staged bytes. Idempotent.
    #[instrument(skip(self))]
    pub async fn delete_upload(&self, namespace: &Namespace, uuid: &str) -> Result<(), Error> {
        let upload_path = path_builder::upload_path(namespace, uuid);
        // Discard the upload and all backend state it owns (in-progress
        // multipart(s) and any staged remainder on S3; the staging file on FS).
        let _ = self.object.abort_upload(&upload_path).await;

        let container = path_builder::upload_container_path(namespace, uuid);
        self.object.delete_prefix(&container).await?;
        Ok(())
    }
}
