use sha2::{Digest as ShaDigest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{instrument, warn};
use uuid::Uuid;

use crate::{
    event_webhook::event::{Event, EventActor, EventKind},
    oci::{Digest, Namespace},
    registry::{
        DOCKER_UPLOAD_UUID, Error, HeaderMap, Registry, ResponseHeaders,
        blob_ownership::BlobOwnership, blob_store,
    },
};

/// Caps the namespaces CEL-evaluated for a from-less mount, bounding an
/// attacker-influenceable fan-out. Candidates beyond the cap fall back to a
/// normal upload session, so no access is over-granted.
const MAX_FROM_LESS_MOUNT_CANDIDATES: usize = 32;

/// Default cap on a blob's cumulative uploaded size, mirroring
/// [`manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES`](crate::registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES).
pub const DEFAULT_MAX_BLOB_SIZE_BYTES: u64 = 100 * 1024 * 1024 * 1024;

pub enum StartUploadResponse {
    ExistingBlob { headers: HeaderMap },
    Session { headers: HeaderMap },
}

/// An OCI cross-repository blob mount request
/// (`POST /v2/<ns>/blobs/uploads/?mount=<digest>[&from=<repo>]`).
/// An unsatisfiable mount falls back to a normal upload session rather than
/// failing, per the distribution spec.
#[derive(Debug)]
pub struct BlobMount {
    pub digest: Digest,
    pub from: Option<Namespace>,
}

pub struct GetUploadResponse {
    pub headers: HeaderMap,
}

pub struct PatchUploadResponse {
    pub headers: HeaderMap,
}

pub struct CompleteUploadResponse {
    pub headers: HeaderMap,
    pub events: Vec<Event>,
}

/// Headers for a completed-blob response (used by `StartUpload` when the digest
/// already exists, and by `CompleteUpload` when the upload finishes).
fn blob_location_headers(namespace: &Namespace, digest: &Digest) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/{digest}"))
        .docker_content_digest(digest)
        .into_inner()
}

fn upload_session_headers(namespace: &Namespace, session_uuid: &str) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/uploads/{session_uuid}"))
        .range("0-0")
        .with(DOCKER_UPLOAD_UUID, session_uuid)
        .into_inner()
}

fn upload_status_headers(namespace: &Namespace, session_id: &str, range_max: u64) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/uploads/{session_id}"))
        .range(format!("0-{range_max}"))
        .with(DOCKER_UPLOAD_UUID, session_id)
        .into_inner()
}

fn patch_upload_headers(namespace: &Namespace, session_id: &str, range_max: u64) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/uploads/{session_id}"))
        .range(format!("0-{range_max}"))
        .with(DOCKER_UPLOAD_UUID, session_id)
        .content_length(0)
        .into_inner()
}

async fn hash_upload_stream<S>(mut stream: S, content_length: Option<u64>) -> Result<Digest, Error>
where
    S: AsyncRead + Unpin,
{
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut read = 0_u64;

    loop {
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .map_err(|e| blob_store::Error::UploadBodyRead(e.to_string()))?;
        if bytes_read == 0 {
            break;
        }

        let bytes_read_u64 = u64::try_from(bytes_read).map_err(blob_store::Error::from)?;
        read = read
            .checked_add(bytes_read_u64)
            .ok_or(blob_store::Error::UploadBodySize {
                expected: content_length.unwrap_or(read),
                actual: u64::MAX,
            })?;
        if let Some(expected) = content_length
            && read > expected
        {
            return Err(blob_store::Error::UploadBodySize {
                expected,
                actual: read,
            }
            .into());
        }

        hasher.update(&buffer[..bytes_read]);
    }

    if let Some(expected) = content_length
        && read != expected
    {
        return Err(blob_store::Error::UploadBodySize {
            expected,
            actual: read,
        }
        .into());
    }

    Ok(Digest::from_sha256(hasher))
}

impl Registry {
    async fn complete_existing_upload<S>(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        content_length: Option<u64>,
        stream: &mut S,
    ) -> Result<bool, Error>
    where
        S: AsyncRead + Unpin,
    {
        let session = self.acquire_blob_data_lock(digest).await?;
        let result = async {
            if self.blob_store.size(digest).await.is_err() {
                return Ok(false);
            }

            let upload_digest = hash_upload_stream(stream, content_length).await?;
            if &upload_digest != digest {
                warn!("Expected digest '{digest}', got '{upload_digest}'");
                return Err(Error::DigestInvalid);
            }

            BlobOwnership::new(self.metadata_store.as_ref())
                .grant(namespace, digest)
                .await?;

            Ok(true)
        }
        .await;
        session.release().await;
        result
    }

    async fn finish_completed_upload(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        session_key: &str,
        digest: &Digest,
    ) -> CompleteUploadResponse {
        if let Err(error) = self.blob_store.delete_upload(namespace, session_key).await {
            warn!("Failed to delete completed upload state: {error}");
        }

        let repository = self.repository_name_for(namespace);
        let event = Event::new(EventKind::BlobPush, namespace.to_string(), repository)
            .digest(Some(digest.to_string()))
            .actor(actor);

        CompleteUploadResponse {
            headers: blob_location_headers(namespace, digest),
            events: vec![event],
        }
    }

    /// Grants `namespace` a reference to `mount.digest`, re-checked against the
    /// authorized `source`, returning `Ok(None)` when the source no longer
    /// holds the blob or its bytes are gone. Check and grant run under the
    /// `blob-data:{digest}` lock so a concurrent `delete_blob` cannot leave a
    /// dangling reference.
    async fn try_cross_repo_mount(
        &self,
        namespace: &Namespace,
        mount: &BlobMount,
        source: &Namespace,
    ) -> Result<Option<HeaderMap>, Error> {
        let session = self.acquire_blob_data_lock(&mount.digest).await?;
        let result = async {
            if self.blob_store.size(&mount.digest).await.is_err()
                || !BlobOwnership::new(self.metadata_store.as_ref())
                    .can_read(source, &mount.digest)
                    .await?
            {
                return Ok(None);
            }

            BlobOwnership::new(self.metadata_store.as_ref())
                .grant(namespace, &mount.digest)
                .await?;
            Ok(Some(blob_location_headers(namespace, &mount.digest)))
        }
        .await;
        session.release().await;
        result
    }

    /// Source namespaces whose read policy must permit the caller: `[from]`
    /// when set and owning the blob, else every namespace referencing it.
    /// Empty when the mount cannot be satisfied.
    pub async fn mount_source_candidates(
        &self,
        mount: &BlobMount,
    ) -> Result<Vec<Namespace>, Error> {
        if self.blob_store.size(&mount.digest).await.is_err() {
            return Ok(Vec::new());
        }

        if let Some(from) = &mount.from {
            let readable = BlobOwnership::new(self.metadata_store.as_ref())
                .can_read(from, &mount.digest)
                .await?;
            return Ok(if readable {
                vec![from.clone()]
            } else {
                Vec::new()
            });
        }

        let mut candidates = BlobOwnership::new(self.metadata_store.as_ref())
            .referencing_namespaces(&mount.digest)
            .await?;
        // Sort before truncating so the kept candidates are deterministic.
        candidates.sort();
        candidates.truncate(MAX_FROM_LESS_MOUNT_CANDIDATES);
        Ok(candidates)
    }

    /// Opens a fresh resumable upload session and returns its `202` headers.
    async fn open_upload_session(
        &self,
        namespace: &Namespace,
    ) -> Result<StartUploadResponse, Error> {
        let session_uuid = Uuid::new_v4().to_string();
        self.blob_store
            .create_upload(namespace, &session_uuid)
            .await?;

        Ok(StartUploadResponse::Session {
            headers: upload_session_headers(namespace, &session_uuid),
        })
    }

    /// Starts a blob upload: `ExistingBlob` (201) when the namespace already
    /// owns `digest`, otherwise a new session (202).
    #[instrument]
    pub async fn start_upload(
        &self,
        namespace: &Namespace,
        digest: Option<Digest>,
    ) -> Result<StartUploadResponse, Error> {
        if let Some(digest) = digest
            && self.blob_store.size(&digest).await.is_ok()
            && BlobOwnership::new(self.metadata_store.as_ref())
                .can_read(namespace, &digest)
                .await?
        {
            return Ok(StartUploadResponse::ExistingBlob {
                headers: blob_location_headers(namespace, &digest),
            });
        }

        self.open_upload_session(namespace).await
    }

    /// Starts a cross-repository blob mount from the authorized `source`,
    /// falling back to an ordinary upload session when the mount cannot be
    /// satisfied. A satisfied mount returns a `blob.push` event (the session
    /// fallback returns none: its eventual upload completion emits one), so a
    /// mounted blob is as visible to webhook consumers as an uploaded one.
    #[instrument(skip(actor))]
    pub async fn mount_blob(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        mount: &BlobMount,
        source: &Namespace,
    ) -> Result<(StartUploadResponse, Vec<Event>), Error> {
        if let Some(headers) = self.try_cross_repo_mount(namespace, mount, source).await? {
            let repository = self.repository_name_for(namespace);
            let event = Event::new(EventKind::BlobPush, namespace.to_string(), repository)
                .digest(Some(mount.digest.to_string()))
                .actor(actor);
            return Ok((StartUploadResponse::ExistingBlob { headers }, vec![event]));
        }

        Ok((self.open_upload_session(namespace).await?, Vec::new()))
    }

    /// Early-reject a known-length body whose declared length would push the
    /// session's cumulative size past `max_blob_size_bytes`, aborting the
    /// session first so its staged bytes are reclaimed.
    async fn reject_oversized_known_length(
        &self,
        namespace: &Namespace,
        session_key: &str,
        committed: u64,
        content_length: Option<u64>,
    ) -> Result<(), Error> {
        let limit = self.max_blob_size_bytes;
        if let Some(len) = content_length
            && committed.checked_add(len).is_none_or(|total| total > limit)
        {
            self.abort_upload_quietly(namespace, session_key).await;
            return Err(Error::BlobBodyTooLarge {
                limit: usize::try_from(limit).unwrap_or(usize::MAX),
            });
        }
        Ok(())
    }

    /// Bound a chunked (`None` content-length) body to `remaining + 1` bytes so
    /// it can never grow the session past `max_blob_size_bytes` without the
    /// extra byte tripping the overflow check after the write. `remaining` is
    /// the headroom left before the cap; a `Some(_)` content-length is passed
    /// through unbounded because [`Self::reject_oversized_known_length`] already
    /// vetted it.
    fn bound_blob_stream<S>(
        &self,
        committed: u64,
        content_length: Option<u64>,
        stream: S,
    ) -> tokio::io::Take<S>
    where
        S: AsyncRead + Unpin,
    {
        if content_length.is_some() {
            // A vetted known length never trips the guard; cap at exactly the
            // limit so a deceptive Content-Length cannot smuggle extra bytes.
            return stream.take(self.max_blob_size_bytes.saturating_add(1));
        }
        let remaining = self.max_blob_size_bytes.saturating_sub(committed);
        stream.take(remaining.saturating_add(1))
    }

    /// After a write, reject (and abort) when the session's cumulative size has
    /// exceeded `max_blob_size_bytes`, i.e. the chunked guard byte was consumed.
    async fn reject_if_oversized(
        &self,
        namespace: &Namespace,
        session_key: &str,
        new_total: u64,
    ) -> Result<(), Error> {
        let limit = self.max_blob_size_bytes;
        if new_total > limit {
            self.abort_upload_quietly(namespace, session_key).await;
            return Err(Error::BlobBodyTooLarge {
                limit: usize::try_from(limit).unwrap_or(usize::MAX),
            });
        }
        Ok(())
    }

    /// Best-effort abort of an upload session whose body breached the size cap;
    /// a cleanup failure is logged, not surfaced, since the caller already has a
    /// terminal error to return.
    async fn abort_upload_quietly(&self, namespace: &Namespace, session_key: &str) {
        if let Err(error) = self.blob_store.delete_upload(namespace, session_key).await {
            warn!("Failed to abort oversized upload session: {error}");
        }
    }

    #[instrument(skip(stream))]
    pub async fn patch_upload<S>(
        &self,
        namespace: &Namespace,
        session_id: Uuid,
        start_offset: Option<u64>,
        content_length: Option<u64>,
        stream: S,
    ) -> Result<PatchUploadResponse, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let session_key = session_id.to_string();

        let summary = self
            .blob_store
            .upload_summary(namespace, &session_key)
            .await?;

        if let Some(offset) = start_offset
            && offset != summary.size
        {
            return Err(Error::RangeNotSatisfiable);
        }

        self.reject_oversized_known_length(namespace, &session_key, summary.size, content_length)
            .await?;

        let bounded = self.bound_blob_stream(summary.size, content_length, stream);
        let (_, size) = self
            .blob_store
            .write_upload(namespace, &session_key, Box::new(bounded), content_length)
            .await?;

        self.reject_if_oversized(namespace, &session_key, size)
            .await?;

        let range_max = size.saturating_sub(1);

        Ok(PatchUploadResponse {
            headers: patch_upload_headers(namespace, &session_key, range_max),
        })
    }

    #[instrument(skip(stream, actor))]
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_upload<S>(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        session_id: Uuid,
        digest: &Digest,
        start_offset: Option<u64>,
        content_length: Option<u64>,
        stream: S,
    ) -> Result<CompleteUploadResponse, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let session_key = session_id.to_string();

        let committed = match self
            .blob_store
            .upload_summary(namespace, &session_key)
            .await
        {
            Ok(summary) => summary.size,
            Err(blob_store::Error::UploadNotFound) => 0,
            Err(e) => return Err(e.into()),
        };
        let has_prior_writes = committed > 0;

        // A final-chunk PUT carrying a Content-Range must resume from the
        // committed offset; a gap or rewind is an out-of-order chunk (416), not
        // a digest mismatch.
        if let Some(offset) = start_offset
            && offset != committed
        {
            return Err(Error::RangeNotSatisfiable);
        }

        self.reject_oversized_known_length(namespace, &session_key, committed, content_length)
            .await?;

        // Bound the final chunk so a chunked (`None` content-length) PUT that
        // carries the whole body cannot grow the session past the cap.
        let mut stream = self.bound_blob_stream(committed, content_length, stream);
        if !has_prior_writes
            && self
                .complete_existing_upload(namespace, digest, content_length, &mut stream)
                .await?
        {
            return Ok(self
                .finish_completed_upload(actor, namespace, &session_key, digest)
                .await);
        }

        let (upload_digest, new_total) = self
            .blob_store
            .write_upload(namespace, &session_key, Box::new(stream), content_length)
            .await?;

        self.reject_if_oversized(namespace, &session_key, new_total)
            .await?;

        if &upload_digest != digest {
            warn!("Expected digest '{digest}', got '{upload_digest}'");
            return Err(Error::DigestInvalid);
        }

        let session = self.acquire_blob_data_lock(digest).await?;
        let result = async {
            match self.blob_store.size(digest).await {
                Ok(_) => {}
                Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                    self.blob_store
                        .complete_upload(namespace, &session_key, Some(digest))
                        .await?;
                }
                Err(error) => return Err(Error::from(error)),
            }

            BlobOwnership::new(self.metadata_store.as_ref())
                .grant(namespace, digest)
                .await
        }
        .await;
        session.release().await;
        result?;

        Ok(self
            .finish_completed_upload(actor, namespace, &session_key, digest)
            .await)
    }

    #[instrument]
    pub async fn delete_upload(
        &self,
        namespace: &Namespace,
        session_id: Uuid,
    ) -> Result<(), Error> {
        let uuid = session_id.to_string();
        self.blob_store.delete_upload(namespace, &uuid).await?;

        Ok(())
    }

    #[instrument]
    pub async fn get_upload_status(
        &self,
        namespace: &Namespace,
        session_id: Uuid,
    ) -> Result<GetUploadResponse, Error> {
        let uuid = session_id.to_string();
        let summary = self.blob_store.upload_summary(namespace, &uuid).await?;

        let range_max = summary.size.saturating_sub(1);

        Ok(GetUploadResponse {
            headers: upload_status_headers(namespace, &uuid, range_max),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, str::FromStr, sync::Arc};

    use async_trait::async_trait;
    use bytes::Bytes;
    use hyper::{
        Request,
        header::{LOCATION, RANGE},
    };
    use uuid::Uuid;

    use angos_storage::{
        BoxedReader as StorageBoxedReader, ByteStream, ChildrenPage, Error as StorageError,
        MultipartUploadPage, ObjectMeta, ObjectStore, Page,
    };
    use angos_tx_engine::store::Store;

    use crate::{
        auth::Authorizer,
        cache,
        event_webhook::event::EventKind,
        identity::ClientIdentity,
        oci::{Digest, Namespace},
        registry::{
            BlobMount, DOCKER_CONTENT_DIGEST, DOCKER_UPLOAD_UUID, Error, Registry, RegistryConfig,
            StartUploadResponse,
            blob_ownership::BlobOwnership,
            blob_store::BlobStore,
            metadata_store::LinkKind,
            path_builder,
            repository_resolver::RepositoryResolver,
            test_utils::{
                FSRegistryTestCase, RegistryTestCase, backends, create_test_registry,
                create_test_repositories, put_blob_direct,
            },
        },
        test_fixtures::configuration::load_config,
    };

    /// Which storage operation the [`FailingStorage`] wrapper turns into a
    /// hard error. Everything else delegates to the inner backend untouched.
    #[derive(Clone, Copy)]
    enum FailOp {
        /// Fail the session-record delete that backs `delete_upload` cleanup.
        Delete,
        /// Fail `complete_upload`: must never run on the existing-blob path.
        CompleteUpload,
        /// Fail `write_upload`: must never run on the monolithic existing-blob
        /// path.
        WriteUpload,
    }

    /// Delegating storage wrapper that injects a single failure at the storage
    /// seam so upload fast-paths can be proven to avoid a given op. Wraps an
    /// [`ObjectStore`] (the CRUD floor plus the upload-session methods), so it
    /// stands in for the whole storage seam of the [`Store`] façade.
    struct FailingStorage {
        inner: Arc<dyn ObjectStore>,
        fail: FailOp,
    }

    fn fail(message: &str) -> StorageError {
        StorageError::Backend(message.to_string())
    }

    #[async_trait]
    impl ObjectStore for FailingStorage {
        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.inner.get(key).await
        }

        async fn get_stream(
            &self,
            key: &str,
            offset: Option<u64>,
        ) -> Result<(StorageBoxedReader, u64), StorageError> {
            self.inner.get_stream(key, offset).await
        }

        async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }

        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            if matches!(self.fail, FailOp::Delete) {
                return Err(fail("delete failed"));
            }
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
            if matches!(self.fail, FailOp::WriteUpload) {
                return Err(fail(
                    "write should not be called for monolithic existing blob upload",
                ));
            }
            self.inner.write_upload(key, body, len).await
        }

        async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
            if matches!(self.fail, FailOp::CompleteUpload) {
                return Err(fail("complete should not be called for existing blob data"));
            }
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

    /// Rebuild `inner` with its storage seam wrapped so `fail` errors out,
    /// reusing the same upload backend and executor.
    fn failing_blob_store(inner: &Arc<BlobStore>, fail: FailOp) -> Arc<BlobStore> {
        let object = inner.store.object_store().clone();
        let failing = Arc::new(FailingStorage {
            inner: object,
            fail,
        });
        let facade = Arc::new(Store::builder(failing, inner.store.executor().clone()).build());
        Arc::new(BlobStore::new(facade))
    }

    #[tokio::test]
    async fn test_start_upload() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test upload content";

            let response = registry.start_upload(namespace, None).await.unwrap();
            match response {
                StartUploadResponse::Session { headers } => {
                    assert!(
                        headers[LOCATION.as_str()]
                            .starts_with(&format!("/v2/{namespace}/blobs/uploads/"))
                    );
                    assert!(!headers[DOCKER_UPLOAD_UUID].is_empty());
                }
                StartUploadResponse::ExistingBlob { .. } => panic!("Expected Session response"),
            }

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            let response = registry
                .start_upload(namespace, Some(digest.clone()))
                .await
                .unwrap();
            match response {
                StartUploadResponse::Session { headers } => {
                    assert!(
                        headers[LOCATION.as_str()]
                            .starts_with(&format!("/v2/{namespace}/blobs/uploads/"))
                    );
                    assert!(!headers[DOCKER_UPLOAD_UUID].is_empty());
                }
                StartUploadResponse::ExistingBlob { .. } => {
                    panic!("Expected unowned blob to start a new session")
                }
            }

            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(namespace, &digest)
                .await
                .unwrap();

            let response = registry
                .start_upload(namespace, Some(digest.clone()))
                .await
                .unwrap();
            match response {
                StartUploadResponse::ExistingBlob { headers } => {
                    assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
                    assert_eq!(
                        headers[LOCATION.as_str()],
                        format!("/v2/{namespace}/blobs/{digest}")
                    );
                }
                StartUploadResponse::Session { .. } => panic!("Expected Existing response"),
            }
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_grants_and_returns_existing() {
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();
            let content = b"cross-repo mountable blob";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(source, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: Some(source.clone()),
            };
            let (response, _events) = registry
                .mount_blob(None, target, &mount, source)
                .await
                .unwrap();

            match response {
                StartUploadResponse::ExistingBlob { headers } => {
                    assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
                    assert_eq!(
                        headers[LOCATION.as_str()],
                        format!("/v2/{target}/blobs/{digest}")
                    );
                }
                StartUploadResponse::Session { .. } => {
                    panic!("Expected a mounted existing-blob response")
                }
            }

            assert!(
                BlobOwnership::new(registry.metadata_store.as_ref())
                    .can_read(target, &digest)
                    .await
                    .unwrap(),
                "mount must grant the target namespace a reference"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_emits_blob_push_event() {
        // A satisfied mount must emit blob.push so webhook consumers see the
        // mounted blob, just as a completed upload does.
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();

            let digest = put_blob_direct(registry.metadata_store.store(), b"mountable").await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(source, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: Some(source.clone()),
            };
            let (response, events) = registry
                .mount_blob(None, target, &mount, source)
                .await
                .unwrap();

            assert!(matches!(response, StartUploadResponse::ExistingBlob { .. }));
            assert_eq!(events.len(), 1, "a satisfied mount must emit one event");
            assert_eq!(events[0].kind, EventKind::BlobPush);
            assert_eq!(events[0].namespace, target.to_string());
            assert_eq!(
                events[0].digest.as_deref(),
                Some(digest.to_string().as_str())
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_fallback_emits_no_event() {
        // The session fallback grants nothing yet; its eventual upload emits the
        // event, so the mount itself must not.
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();

            // Present but unowned by `source` -> the mount falls back to a session.
            let digest = put_blob_direct(registry.metadata_store.store(), b"not owned").await;
            let mount = BlobMount {
                digest,
                from: Some(source.clone()),
            };
            let (response, events) = registry
                .mount_blob(None, target, &mount, source)
                .await
                .unwrap();

            assert!(matches!(response, StartUploadResponse::Session { .. }));
            assert!(events.is_empty(), "a fallback session must emit no event");
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_falls_back_when_source_lacks_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();
            let content = b"blob present but not owned by source";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;

            let mount = BlobMount {
                digest: digest.clone(),
                from: Some(source.clone()),
            };
            let (response, _events) = registry
                .mount_blob(None, target, &mount, source)
                .await
                .unwrap();

            match response {
                StartUploadResponse::Session { headers } => {
                    assert!(
                        headers[LOCATION.as_str()]
                            .starts_with(&format!("/v2/{target}/blobs/uploads/"))
                    );
                }
                StartUploadResponse::ExistingBlob { .. } => {
                    panic!("Expected a fall-back session when the source does not own the blob")
                }
            }

            assert!(
                !BlobOwnership::new(registry.metadata_store.as_ref())
                    .can_read(target, &digest)
                    .await
                    .unwrap(),
                "a failed mount must not grant the target namespace a reference"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_falls_back_when_blob_absent() {
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();

            let absent = Digest::from_str(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();
            let mount = BlobMount {
                digest: absent,
                from: Some(source.clone()),
            };
            let (response, _events) = registry
                .mount_blob(None, target, &mount, source)
                .await
                .unwrap();

            assert!(
                matches!(response, StartUploadResponse::Session { .. }),
                "an absent blob must fall back to a normal upload session"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_automatic_grants_referenced_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let owner = &Namespace::new("test-repo/owner").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();
            let content = b"automatically discoverable blob";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(owner, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: None,
            };
            let (response, _events) = registry
                .mount_blob(None, target, &mount, owner)
                .await
                .unwrap();

            match response {
                StartUploadResponse::ExistingBlob { headers } => {
                    assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
                }
                StartUploadResponse::Session { .. } => {
                    panic!("automatic discovery must mount a referenced blob")
                }
            }
            assert!(
                BlobOwnership::new(registry.metadata_store.as_ref())
                    .can_read(target, &digest)
                    .await
                    .unwrap(),
                "automatic mount must grant the target namespace a reference"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_automatic_falls_back_for_unreferenced_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let target = &Namespace::new("test-repo/target").unwrap();
            let source = &Namespace::new("test-repo/source").unwrap();
            let content = b"orphan blob present but unreferenced";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;

            let mount = BlobMount {
                digest: digest.clone(),
                from: None,
            };
            let (response, _events) = registry
                .mount_blob(None, target, &mount, source)
                .await
                .unwrap();

            assert!(
                matches!(response, StartUploadResponse::Session { .. }),
                "an unreferenced (orphan) blob must not be auto-mounted"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_mount_blob_grants_only_from_the_authorized_source() {
        for test_case in backends() {
            let registry = test_case.registry();
            let owner = &Namespace::new("test-repo/owner").unwrap();
            let authorized = &Namespace::new("test-repo/authorized").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();
            let content = b"held by owner, not by the authorized source";

            // Guards the authorize-then-grant TOCTOU: the grant is conditioned
            // on the authorized source, not on `owner`.
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(owner, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: None,
            };
            let (response, _events) = registry
                .mount_blob(None, target, &mount, authorized)
                .await
                .unwrap();

            assert!(
                matches!(response, StartUploadResponse::Session { .. }),
                "mount must fall back when the authorized source does not hold the blob"
            );
            assert!(
                !BlobOwnership::new(registry.metadata_store.as_ref())
                    .can_read(target, &digest)
                    .await
                    .unwrap(),
                "target must not be granted a reference from an unauthorized source"
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn mount_source_candidates_resolves_from_and_discovery() {
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let other = &Namespace::new("test-repo/other").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();
            let content = b"candidate resolution blob";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            let ownership = BlobOwnership::new(registry.metadata_store.as_ref());
            ownership.grant(source, &digest).await.unwrap();
            ownership.grant(other, &digest).await.unwrap();

            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: digest.clone(),
                    from: Some(source.clone()),
                })
                .await
                .unwrap();
            assert_eq!(candidates, vec![source.clone()]);

            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: digest.clone(),
                    from: Some(target.clone()),
                })
                .await
                .unwrap();
            assert!(candidates.is_empty());

            // Lexicographic order guards the sort-before-truncate determinism.
            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: digest.clone(),
                    from: None,
                })
                .await
                .unwrap();
            assert_eq!(candidates, vec![other.clone(), source.clone()]);

            let absent = Digest::from_str(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();
            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: absent,
                    from: None,
                })
                .await
                .unwrap();
            assert!(candidates.is_empty());

            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn authorize_mount_source_requires_read_on_the_source() {
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let content = b"mount authorization blob";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(source, &digest)
                .await
                .unwrap();

            let config = load_config(
                r#"
                [global.access_policy]
                default = "deny"
                rules = ["identity.id == 'reader'"]

                [repository."test-repo".access_policy]
                default = "allow"
            "#,
            );
            let cache = cache::Config::Memory.to_backend().unwrap();
            let authorizer = Authorizer::new(&config, &cache).unwrap();

            let (parts, ()) = Request::builder()
                .uri("/v2/")
                .body(())
                .unwrap()
                .into_parts();
            let mut reader = ClientIdentity::new(None);
            reader.id = Some("reader".to_string());
            let stranger = ClientIdentity::new(None);

            for from in [Some(source.clone()), None] {
                let mount = BlobMount {
                    digest: digest.clone(),
                    from,
                };
                assert!(
                    authorizer
                        .authorize_mount_source(&mount, &reader, &parts, registry)
                        .await
                        .unwrap()
                        .is_some(),
                    "a caller permitted to read the source must be allowed to mount"
                );
                assert!(
                    authorizer
                        .authorize_mount_source(&mount, &stranger, &parts, registry)
                        .await
                        .unwrap()
                        .is_none(),
                    "a caller denied read on the source must not be allowed to mount"
                );
            }

            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_patch_upload() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test patch content";
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream = Cursor::new(content);
            let response = registry
                .patch_upload(
                    namespace,
                    session_id,
                    None,
                    Some(content.len() as u64),
                    stream,
                )
                .await
                .unwrap();
            assert_eq!(
                response.headers[RANGE.as_str()],
                format!("0-{}", content.len() - 1)
            );

            let additional_content = b" additional";
            let stream = Cursor::new(additional_content);
            let response = registry
                .patch_upload(
                    namespace,
                    session_id,
                    Some(content.len() as u64),
                    Some(additional_content.len() as u64),
                    stream,
                )
                .await
                .unwrap();
            assert_eq!(
                response.headers[RANGE.as_str()],
                format!("0-{}", content.len() + additional_content.len() - 1)
            );

            let summary = registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(
                summary.size,
                (content.len() + additional_content.len()) as u64
            );
            test_case.cleanup().await;
        }
    }

    // A chunked request (`Transfer-Encoding: chunked`, no `Content-Length`) is
    // authorized with `content_length = None`: the body streams to EOF and the
    // blob is stored with the digest derived from the bytes actually read. This
    // is the `docker push` path.
    #[tokio::test]
    async fn patch_upload_without_content_length_streams_to_eof() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"chunked upload with no declared length";
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            registry
                .patch_upload(namespace, session_id, None, None, Cursor::new(content))
                .await
                .expect("a chunked PATCH (no Content-Length) must be accepted");

            let expected_digest = Digest::from_bytes(content);
            registry
                .complete_upload(
                    None,
                    namespace,
                    session_id,
                    &expected_digest,
                    None,
                    None,
                    Cursor::new(Vec::new()),
                )
                .await
                .expect("completing a chunked upload must succeed");

            assert_eq!(
                registry.blob_store.read(&expected_digest).await.unwrap(),
                content
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_complete_upload() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test complete content";
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream = Cursor::new(content);
            registry
                .patch_upload(
                    namespace,
                    session_id,
                    None,
                    Some(content.len() as u64),
                    stream,
                )
                .await
                .unwrap();

            let expected_digest = Digest::from_bytes(content);

            let empty_stream = Cursor::new(Vec::new());
            let response = registry
                .complete_upload(
                    None,
                    namespace,
                    session_id,
                    &expected_digest,
                    None,
                    Some(0),
                    empty_stream,
                )
                .await
                .unwrap();

            assert_eq!(
                response.headers[DOCKER_CONTENT_DIGEST],
                expected_digest.to_string()
            );
            assert_eq!(response.events.len(), 1);
            assert!(matches!(response.events[0].kind, EventKind::BlobPush));

            let stored_content = registry.blob_store.read(&expected_digest).await.unwrap();
            assert_eq!(stored_content, content);

            let blob_index = registry
                .metadata_store
                .read_blob_index(&expected_digest)
                .await
                .unwrap();
            let namespace_links = blob_index.namespace.get(namespace.as_ref()).unwrap();
            assert!(namespace_links.contains(&LinkKind::Blob(expected_digest.clone())));

            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_complete_upload_succeeds_when_cleanup_delete_fails() {
        let test_case = FSRegistryTestCase::new();
        let registry = create_test_registry(
            failing_blob_store(&RegistryTestCase::blob_store(&test_case), FailOp::Delete),
            test_case.metadata_store(),
        );
        let namespace = &Namespace::new("test-repo").unwrap();
        let content = b"test complete content despite cleanup failure";
        let session_id = Uuid::new_v4();

        registry
            .blob_store
            .create_upload(namespace, &session_id.to_string())
            .await
            .unwrap();

        registry
            .patch_upload(
                namespace,
                session_id,
                None,
                Some(content.len() as u64),
                Cursor::new(content),
            )
            .await
            .unwrap();

        let expected_digest = Digest::from_bytes(content);
        let response = registry
            .complete_upload(
                None,
                namespace,
                session_id,
                &expected_digest,
                None,
                Some(0),
                Cursor::new(Vec::new()),
            )
            .await
            .unwrap();

        assert_eq!(
            response.headers[DOCKER_CONTENT_DIGEST],
            expected_digest.to_string()
        );
        assert_eq!(
            registry.blob_store.read(&expected_digest).await.unwrap(),
            content
        );
    }

    #[tokio::test]
    async fn test_complete_upload_reuses_existing_blob_data() {
        let test_case = FSRegistryTestCase::new();
        let registry = create_test_registry(
            failing_blob_store(
                &RegistryTestCase::blob_store(&test_case),
                FailOp::CompleteUpload,
            ),
            test_case.metadata_store(),
        );
        let first_namespace = &Namespace::new("test-repo/first").unwrap();
        let second_namespace = &Namespace::new("test-repo/second").unwrap();
        let content = b"shared upload content";
        let digest = put_blob_direct(registry.metadata_store.store(), content).await;

        BlobOwnership::new(registry.metadata_store.as_ref())
            .grant(first_namespace, &digest)
            .await
            .unwrap();

        let session_id = Uuid::new_v4();
        registry
            .blob_store
            .create_upload(second_namespace, &session_id.to_string())
            .await
            .unwrap();
        registry
            .patch_upload(
                second_namespace,
                session_id,
                None,
                Some(content.len() as u64),
                Cursor::new(content),
            )
            .await
            .unwrap();

        registry
            .complete_upload(
                None,
                second_namespace,
                session_id,
                &digest,
                None,
                Some(0),
                Cursor::new(Vec::new()),
            )
            .await
            .unwrap();

        assert_eq!(registry.blob_store.read(&digest).await.unwrap(), content);
        assert!(
            BlobOwnership::new(registry.metadata_store.as_ref())
                .can_read(second_namespace, &digest)
                .await
                .unwrap()
        );
        assert!(
            registry
                .blob_store
                .upload_summary(second_namespace, &session_id.to_string())
                .await
                .is_err()
        );

        test_case.cleanup().await;
    }

    #[tokio::test]
    async fn test_complete_upload_hashes_existing_blob_without_upload_storage_write() {
        let test_case = FSRegistryTestCase::new();
        let registry = create_test_registry(
            failing_blob_store(
                &RegistryTestCase::blob_store(&test_case),
                FailOp::WriteUpload,
            ),
            test_case.metadata_store(),
        );
        let first_namespace = &Namespace::new("test-repo/first").unwrap();
        let second_namespace = &Namespace::new("test-repo/second").unwrap();
        let content = b"shared monolithic upload content";
        let digest = put_blob_direct(registry.metadata_store.store(), content).await;

        BlobOwnership::new(registry.metadata_store.as_ref())
            .grant(first_namespace, &digest)
            .await
            .unwrap();

        let session_id = Uuid::new_v4();
        registry
            .blob_store
            .create_upload(second_namespace, &session_id.to_string())
            .await
            .unwrap();

        registry
            .complete_upload(
                None,
                second_namespace,
                session_id,
                &digest,
                None,
                Some(content.len() as u64),
                Cursor::new(content),
            )
            .await
            .unwrap();

        assert!(
            BlobOwnership::new(registry.metadata_store.as_ref())
                .can_read(second_namespace, &digest)
                .await
                .unwrap()
        );
        assert!(
            registry
                .blob_store
                .upload_summary(second_namespace, &session_id.to_string())
                .await
                .is_err()
        );

        test_case.cleanup().await;
    }

    #[tokio::test]
    async fn test_delete_upload() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            assert!(
                registry
                    .blob_store
                    .upload_summary(namespace, &session_id.to_string())
                    .await
                    .is_ok()
            );

            registry.delete_upload(namespace, session_id).await.unwrap();

            assert!(
                registry
                    .blob_store
                    .upload_summary(namespace, &session_id.to_string())
                    .await
                    .is_err()
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_get_upload_status() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test range content";
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let response = registry
                .get_upload_status(namespace, session_id)
                .await
                .unwrap();
            assert_eq!(response.headers[RANGE.as_str()], "0-0");

            let stream = Cursor::new(content);
            registry
                .patch_upload(
                    namespace,
                    session_id,
                    None,
                    Some(content.len() as u64),
                    stream,
                )
                .await
                .unwrap();

            let response = registry
                .get_upload_status(namespace, session_id)
                .await
                .unwrap();
            assert_eq!(
                response.headers[RANGE.as_str()],
                format!("0-{}", content.len() - 1)
            );
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_patch_upload_offset_validation_still_works() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream = Cursor::new(b"some data".to_vec());
            registry
                .patch_upload(namespace, session_id, None, Some(9), stream)
                .await
                .unwrap();

            let stream = Cursor::new(b"more data".to_vec());
            let result = registry
                .patch_upload(namespace, session_id, Some(0), Some(9), stream)
                .await;

            assert!(matches!(result, Err(Error::RangeNotSatisfiable)));
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_complete_upload_digest_mismatch_still_rejected() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let session_id = Uuid::new_v4();

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream = Cursor::new(b"test content".to_vec());
            registry
                .patch_upload(namespace, session_id, None, Some(12), stream)
                .await
                .unwrap();

            let wrong_digest = Digest::from_str(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

            let empty_stream = Cursor::new(Vec::new());
            let result = registry
                .complete_upload(
                    None,
                    namespace,
                    session_id,
                    &wrong_digest,
                    None,
                    Some(0),
                    empty_stream,
                )
                .await;

            assert!(matches!(result, Err(Error::DigestInvalid)));
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_write_upload_returns_digest_and_size() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let session_id = Uuid::new_v4();
            let content = b"hello world upload";

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync> =
                Box::new(Cursor::new(content.to_vec()));
            let (digest, size) = registry
                .blob_store
                .write_upload(
                    namespace,
                    &session_id.to_string(),
                    stream,
                    Some(content.len() as u64),
                )
                .await
                .unwrap();

            assert_eq!(size, content.len() as u64);
            assert_eq!(digest, Digest::from_bytes(content));

            let summary = registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();

            assert_eq!(size, summary.size);
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_upload_summary_size_accumulates() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let session_id = Uuid::new_v4();
            let content = b"size check content";

            registry
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let summary = registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(summary.size, 0);

            let stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync> =
                Box::new(Cursor::new(content.to_vec()));
            registry
                .blob_store
                .write_upload(
                    namespace,
                    &session_id.to_string(),
                    stream,
                    Some(content.len() as u64),
                )
                .await
                .unwrap();

            let summary = registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(summary.size, content.len() as u64);
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn test_complete_upload_with_corrupted_hash_state_returns_error() {
        let test_case = FSRegistryTestCase::new();
        let registry = test_case.registry();
        let namespace = &Namespace::new("test-repo").unwrap();
        let content = b"test content that should not be lost";
        let session_id = Uuid::new_v4();

        registry
            .blob_store
            .create_upload(namespace, &session_id.to_string())
            .await
            .unwrap();

        let stream = Cursor::new(content);
        registry
            .patch_upload(
                namespace,
                session_id,
                None,
                Some(content.len() as u64),
                stream,
            )
            .await
            .unwrap();

        let summary = registry
            .blob_store
            .upload_summary(namespace, &session_id.to_string())
            .await
            .unwrap();

        assert_eq!(summary.size, content.len() as u64);

        // Corrupt every `hashstates/sha256/<offset>` checkpoint so that
        // `complete_upload` cannot reconstruct the final digest from the
        // persisted hasher state.
        let hashstates_dir =
            test_case
                .temp_dir()
                .path()
                .join(path_builder::upload_hash_context_dir(
                    namespace,
                    &session_id.to_string(),
                    "sha256",
                ));
        for entry in std::fs::read_dir(&hashstates_dir).unwrap() {
            let checkpoint = entry.unwrap().path();
            std::fs::write(&checkpoint, b"not-a-valid-hasher-state").unwrap();
        }

        let empty_stream = Cursor::new(Vec::new());
        let result = registry
            .complete_upload(
                None,
                namespace,
                session_id,
                &Digest::from_bytes(content),
                None,
                Some(0),
                empty_stream,
            )
            .await;

        assert!(
            result.is_err(),
            "complete_upload should return error when hash state is corrupted"
        );

        let upload_path = path_builder::upload_path(namespace, &session_id.to_string());
        let upload_file_path = test_case.temp_dir().path().join(&upload_path);
        assert!(
            upload_file_path.exists(),
            "upload data should NOT be deleted when hash state is corrupted"
        );

        let preserved_content = std::fs::read(&upload_file_path).unwrap();
        assert_eq!(
            preserved_content, content,
            "original upload content should be preserved"
        );
    }

    /// Build a registry over an `FSRegistryTestCase`'s stores but with a tiny
    /// `max_blob_size_bytes`, so the blob-size cap can be exercised end-to-end.
    fn tiny_blob_cap_registry(
        test_case: &FSRegistryTestCase,
        max_blob_size_bytes: u64,
    ) -> Registry {
        let resolver = Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let config = RegistryConfig::default().max_blob_size_bytes(max_blob_size_bytes);
        Registry::new(
            test_case.blob_store(),
            test_case.metadata_store(),
            resolver,
            config,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn patch_upload_known_length_over_cap_is_rejected_and_aborts_session() {
        let test_case = FSRegistryTestCase::new();
        let registry = tiny_blob_cap_registry(&test_case, 8);
        let namespace = &Namespace::new("test-repo").unwrap();
        let session_id = Uuid::new_v4();

        registry
            .blob_store
            .create_upload(namespace, &session_id.to_string())
            .await
            .unwrap();

        let content = b"way past the eight byte cap";
        let result = registry
            .patch_upload(
                namespace,
                session_id,
                None,
                Some(content.len() as u64),
                Cursor::new(content.to_vec()),
            )
            .await;

        assert!(
            matches!(result, Err(Error::BlobBodyTooLarge { limit: 8 })),
            "a known-length body over the cap must be rejected with BlobBodyTooLarge"
        );
        assert!(
            registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .is_err(),
            "the oversized upload session must be aborted, not committed"
        );
    }

    #[tokio::test]
    async fn patch_upload_chunked_over_cap_is_rejected_and_aborts_session() {
        let test_case = FSRegistryTestCase::new();
        let registry = tiny_blob_cap_registry(&test_case, 8);
        let namespace = &Namespace::new("test-repo").unwrap();
        let session_id = Uuid::new_v4();

        registry
            .blob_store
            .create_upload(namespace, &session_id.to_string())
            .await
            .unwrap();

        // No Content-Length (chunked): the body must be bounded mid-stream and
        // the overflow detected after the write.
        let content = b"way past the eight byte cap";
        let result = registry
            .patch_upload(
                namespace,
                session_id,
                None,
                None,
                Cursor::new(content.to_vec()),
            )
            .await;

        assert!(
            matches!(result, Err(Error::BlobBodyTooLarge { limit: 8 })),
            "a chunked body over the cap must be rejected with BlobBodyTooLarge"
        );
        assert!(
            registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .is_err(),
            "the oversized chunked upload session must be aborted, not committed"
        );
    }

    #[tokio::test]
    async fn complete_upload_chunked_over_cap_is_rejected_and_aborts_session() {
        let test_case = FSRegistryTestCase::new();
        let registry = tiny_blob_cap_registry(&test_case, 8);
        let namespace = &Namespace::new("test-repo").unwrap();
        let session_id = Uuid::new_v4();

        registry
            .blob_store
            .create_upload(namespace, &session_id.to_string())
            .await
            .unwrap();

        // A single chunked PUT carrying the whole body must also be bounded.
        let content = b"single chunked PUT over the cap";
        let digest = Digest::from_bytes(content);
        let result = registry
            .complete_upload(
                None,
                namespace,
                session_id,
                &digest,
                None,
                None,
                Cursor::new(content.to_vec()),
            )
            .await;

        assert!(
            matches!(result, Err(Error::BlobBodyTooLarge { limit: 8 })),
            "a chunked PUT over the cap must be rejected with BlobBodyTooLarge"
        );
        assert!(
            registry
                .blob_store
                .upload_summary(namespace, &session_id.to_string())
                .await
                .is_err(),
            "the oversized chunked PUT session must be aborted, not committed"
        );
    }

    #[tokio::test]
    async fn patch_upload_at_cap_is_accepted() {
        let test_case = FSRegistryTestCase::new();
        let registry = tiny_blob_cap_registry(&test_case, 8);
        let namespace = &Namespace::new("test-repo").unwrap();
        let session_id = Uuid::new_v4();

        registry
            .blob_store
            .create_upload(namespace, &session_id.to_string())
            .await
            .unwrap();

        // Exactly at the cap, via both the known-length and chunked paths.
        registry
            .patch_upload(
                namespace,
                session_id,
                None,
                Some(4),
                Cursor::new(b"abcd".to_vec()),
            )
            .await
            .expect("a body within the cap must be accepted");
        registry
            .patch_upload(
                namespace,
                session_id,
                Some(4),
                None,
                Cursor::new(b"efgh".to_vec()),
            )
            .await
            .expect("a chunked body that fills the cap exactly must be accepted");

        let summary = registry
            .blob_store
            .upload_summary(namespace, &session_id.to_string())
            .await
            .unwrap();
        assert_eq!(
            summary.size, 8,
            "cumulative size must reach the cap exactly"
        );
    }
}
