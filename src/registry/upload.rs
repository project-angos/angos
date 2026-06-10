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
    util::sha256::finalize_digest,
};

/// Maximum number of candidate namespaces CEL-evaluated during a from-less
/// mount authorization. Referrers of a popular blob could number in the
/// thousands; without a cap, a single public mount request would trigger one
/// CEL evaluation per candidate — an attacker-influenceable fan-out. This cap
/// bounds only that per-candidate authorization loop. The full referrer set is
/// still read from the blob index and materialized into a Vec first; that Vec is
/// then sorted and the cap is a `truncate` applied afterwards (so the kept
/// candidates are the lexicographically-smallest `MAX_FROM_LESS_MOUNT_CANDIDATES`,
/// deterministic across requests), so the cap does not bound that read or
/// allocation. The cap is fail-safe: if the caller holds access only to a
/// namespace beyond this limit the mount is simply not granted and the client
/// falls back to a normal upload session (202), which re-authorizes
/// independently. No access is over-granted.
const MAX_FROM_LESS_MOUNT_CANDIDATES: usize = 32;

pub enum StartUploadResponse {
    ExistingBlob { headers: HeaderMap },
    Session { headers: HeaderMap },
}

/// An OCI cross-repository blob mount request
/// (`POST /v2/<ns>/blobs/uploads/?mount=<digest>[&from=<repo>]`).
///
/// `digest` is the blob to mount into the target namespace. `from` is the source
/// repository the client claims already holds it; it is optional:
/// - **With `from`** the mount succeeds only when the blob is present AND readable
///   from that repository.
/// - **Without `from`** (automatic content discovery) the mount succeeds when the
///   blob is present AND already referenced by *any* namespace.
///
/// Either way, a mount that cannot be satisfied falls back to a normal upload
/// session rather than failing, per the distribution spec.
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

async fn hash_upload_stream<S>(mut stream: S, content_length: u64) -> Result<Digest, Error>
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
                expected: content_length,
                actual: u64::MAX,
            })?;
        if read > content_length {
            return Err(blob_store::Error::UploadBodySize {
                expected: content_length,
                actual: read,
            }
            .into());
        }

        hasher.update(&buffer[..bytes_read]);
    }

    if read != content_length {
        return Err(blob_store::Error::UploadBodySize {
            expected: content_length,
            actual: read,
        }
        .into());
    }

    Ok(finalize_digest(hasher))
}

impl Registry {
    async fn complete_existing_upload<S>(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        content_length: u64,
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

    /// Attempts an OCI cross-repository blob mount, granting `namespace` a
    /// reference to `mount.digest` drawn from `source`.
    ///
    /// `source` is the namespace the caller was *authorized* to read the blob
    /// from (resolved by the authorization gate from
    /// [`mount_source_candidates`](Self::mount_source_candidates)). The grant
    /// proceeds only when that same source still references the blob and its
    /// bytes still exist — re-checked here rather than against an independently
    /// re-derived candidate set, so concurrent index churn between authorization
    /// and grant cannot mount from a source the caller was never authorized
    /// against. On success the completed-blob headers are returned (the caller
    /// answers `201 Created` with no body transfer).
    ///
    /// Returns `Ok(None)` when the source no longer holds the blob (or its bytes
    /// are gone), so the caller falls back to opening a normal upload session
    /// (`202 Accepted`) — the spec requires a mount that cannot be satisfied to
    /// degrade to an ordinary upload rather than fail.
    ///
    /// The entire size-check / `can_read` / `grant` sequence runs under the
    /// `blob-data:{digest}` coarse lock, serializing it against a concurrent
    /// `delete_blob` reclaim. Without the lock a delete could see no remaining
    /// namespace references and reclaim the blob bytes between the size check
    /// and the grant, leaving the target with a dangling reference.
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

    /// Source namespaces a cross-repo mount of `mount.digest` would draw the blob
    /// from — the namespaces whose read policy must permit the caller before the
    /// mount may grant a reference. Empty when the blob is absent locally or no
    /// eligible namespace holds it, in which case the caller falls back to an
    /// ordinary upload session.
    ///
    /// - **`from` set** — `[from]` when that repository owns the blob, else empty.
    /// - **`from` unset** (automatic discovery) — every namespace that references
    ///   the blob; the caller need only be authorized to read one of them.
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
        // Sort before truncating so the kept candidates are deterministic — the
        // lexicographically-smallest MAX_FROM_LESS_MOUNT_CANDIDATES — rather than an
        // arbitrary HashMap-ordered subset that varies per request and could let a
        // given caller's mount authorize on one request and not another.
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

    /// Starts an ordinary blob upload.
    ///
    /// When `digest` names a blob the namespace already owns, returns it with no
    /// transfer (`ExistingBlob` → `201`); otherwise opens a session (`202`).
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

    /// Starts a cross-repository blob mount, granting `namespace` a reference to
    /// `mount.digest` drawn from `source` (the namespace the caller was
    /// authorized to read the blob from).
    ///
    /// When the mount can be satisfied, grants the namespace a reference with no
    /// transfer (`ExistingBlob` → `201`); otherwise falls back to opening an
    /// ordinary upload session (`202`) — a mount that cannot be satisfied must
    /// degrade to an upload rather than fail.
    #[instrument]
    pub async fn mount_blob(
        &self,
        namespace: &Namespace,
        mount: &BlobMount,
        source: &Namespace,
    ) -> Result<StartUploadResponse, Error> {
        if let Some(headers) = self.try_cross_repo_mount(namespace, mount, source).await? {
            return Ok(StartUploadResponse::ExistingBlob { headers });
        }

        self.open_upload_session(namespace).await
    }

    #[instrument(skip(stream))]
    pub async fn patch_upload<S>(
        &self,
        namespace: &Namespace,
        session_id: Uuid,
        start_offset: Option<u64>,
        content_length: u64,
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

        let (_, size) = self
            .blob_store
            .write_upload(namespace, &session_key, Box::new(stream), content_length)
            .await?;

        let range_max = size.saturating_sub(1);

        Ok(PatchUploadResponse {
            headers: patch_upload_headers(namespace, &session_key, range_max),
        })
    }

    #[instrument(skip(stream, actor))]
    pub async fn complete_upload<S>(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        session_id: Uuid,
        digest: &Digest,
        content_length: u64,
        stream: S,
    ) -> Result<CompleteUploadResponse, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let session_key = session_id.to_string();

        let has_prior_writes = match self
            .blob_store
            .upload_summary(namespace, &session_key)
            .await
        {
            Ok(summary) => summary.size > 0,
            Err(blob_store::Error::UploadNotFound) => false,
            Err(e) => return Err(e.into()),
        };

        let mut stream = stream;
        if !has_prior_writes
            && self
                .complete_existing_upload(namespace, digest, content_length, &mut stream)
                .await?
        {
            return Ok(self
                .finish_completed_upload(actor, namespace, &session_key, digest)
                .await);
        }

        let (upload_digest, _) = self
            .blob_store
            .write_upload(namespace, &session_key, Box::new(stream), content_length)
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
            BlobMount, DOCKER_CONTENT_DIGEST, DOCKER_UPLOAD_UUID, Error, StartUploadResponse,
            blob_ownership::BlobOwnership,
            blob_store::BlobStore,
            metadata_store::link_kind::LinkKind,
            path_builder,
            test_utils::{
                FSRegistryTestCase, RegistryTestCase, backends, create_test_registry,
                put_blob_direct,
            },
        },
        test_fixtures::configuration::load_config,
        util::sha256,
    };

    /// Which storage operation the [`FailingStorage`] wrapper turns into a
    /// hard error. Everything else delegates to the inner backend untouched.
    #[derive(Clone, Copy)]
    enum FailOp {
        /// Fail the session-record delete that backs `delete_upload` cleanup.
        Delete,
        /// Fail `complete_upload` — must never run on the existing-blob path.
        CompleteUpload,
        /// Fail `write_upload` — must never run on the monolithic existing-blob
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
            len: u64,
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
        let facade = Arc::new(
            Store::builder()
                .object(failing)
                .executor(inner.store.executor().clone())
                .build()
                .expect("failing store façade"),
        );
        Arc::new(
            BlobStore::builder()
                .store(facade)
                .build()
                .expect("failing blob store"),
        )
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

            // The blob exists and is owned by `source` (but not yet by `target`).
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(source, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: Some(source.clone()),
            };
            let response = registry.mount_blob(target, &mount, source).await.unwrap();

            // 201-equivalent: the mount granted `target` a reference with no
            // transfer, so the existing-blob headers come back.
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

            // The mount must have actually granted `target` ownership.
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
    async fn test_mount_blob_falls_back_when_source_lacks_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let source = &Namespace::new("test-repo/source").unwrap();
            let target = &Namespace::new("test-repo/target").unwrap();
            let content = b"blob present but not owned by source";

            // The blob bytes exist, but `source` was never granted a reference, so
            // the mount cannot be satisfied and must fall back to a session.
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;

            let mount = BlobMount {
                digest: digest.clone(),
                from: Some(source.clone()),
            };
            let response = registry.mount_blob(target, &mount, source).await.unwrap();

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

            // The failed mount must NOT have granted `target` anything.
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

            // The blob does not exist at all -> fall back to a session.
            let absent = Digest::from_str(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();
            let mount = BlobMount {
                digest: absent,
                from: Some(source.clone()),
            };
            let response = registry.mount_blob(target, &mount, source).await.unwrap();

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

            // The blob is referenced by some namespace (owner), so a from-less
            // mount can discover it automatically.
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(owner, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: None,
            };
            let response = registry.mount_blob(target, &mount, owner).await.unwrap();

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

            // The blob bytes exist but no namespace references them, so even the
            // authorized source does not hold it and the mount falls back to a
            // session.
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;

            let mount = BlobMount {
                digest: digest.clone(),
                from: None,
            };
            let response = registry.mount_blob(target, &mount, source).await.unwrap();

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

            // `owner` holds the blob; the source the caller was authorized against
            // (`authorized`) does NOT. The grant is conditioned on the authorized
            // source, so the mount must fall back rather than grant from `owner`
            // (closes the authorize-then-grant TOCTOU).
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(owner, &digest)
                .await
                .unwrap();

            let mount = BlobMount {
                digest: digest.clone(),
                from: None,
            };
            let response = registry
                .mount_blob(target, &mount, authorized)
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

            // `from` set and owning the blob -> exactly that namespace.
            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: digest.clone(),
                    from: Some(source.clone()),
                })
                .await
                .unwrap();
            assert_eq!(candidates, vec![source.clone()]);

            // `from` set but NOT owning the blob -> no candidate.
            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: digest.clone(),
                    from: Some(target.clone()),
                })
                .await
                .unwrap();
            assert!(candidates.is_empty());

            // from-less discovery -> every namespace that references the blob.
            // Production sorts before truncating, so the output is already in
            // lexicographic order ("test-repo/other" < "test-repo/source");
            // assert that directly (no in-test sort) to guard the determinism.
            let candidates = registry
                .mount_source_candidates(&BlobMount {
                    digest: digest.clone(),
                    from: None,
                })
                .await
                .unwrap();
            assert_eq!(candidates, vec![other.clone(), source.clone()]);

            // Absent blob -> no candidates.
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

    // A cross-repo mount must not grant the caller a blob they cannot read from
    // the source: authorization is checked against a namespace that holds the
    // blob, for both an explicit `from` and from-less automatic discovery.
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

            // Only identity id "reader" may read; everyone else is denied.
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
                .patch_upload(namespace, session_id, None, content.len() as u64, stream)
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
                    additional_content.len() as u64,
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
                .patch_upload(namespace, session_id, None, content.len() as u64, stream)
                .await
                .unwrap();

            let expected_digest = sha256::digest(content);

            let empty_stream = Cursor::new(Vec::new());
            let response = registry
                .complete_upload(
                    None,
                    namespace,
                    session_id,
                    &expected_digest,
                    0,
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
                content.len() as u64,
                Cursor::new(content),
            )
            .await
            .unwrap();

        let expected_digest = sha256::digest(content);
        let response = registry
            .complete_upload(
                None,
                namespace,
                session_id,
                &expected_digest,
                0,
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
                content.len() as u64,
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
                0,
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
                content.len() as u64,
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
                .patch_upload(namespace, session_id, None, content.len() as u64, stream)
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
                .patch_upload(namespace, session_id, None, 9, stream)
                .await
                .unwrap();

            let stream = Cursor::new(b"more data".to_vec());
            let result = registry
                .patch_upload(namespace, session_id, Some(0), 9, stream)
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
                .patch_upload(namespace, session_id, None, 12, stream)
                .await
                .unwrap();

            let wrong_digest = Digest::from_str(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

            let empty_stream = Cursor::new(Vec::new());
            let result = registry
                .complete_upload(None, namespace, session_id, &wrong_digest, 0, empty_stream)
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
                    content.len() as u64,
                )
                .await
                .unwrap();

            assert_eq!(size, content.len() as u64);
            assert_eq!(digest, sha256::digest(content));

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
                    content.len() as u64,
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
            .patch_upload(namespace, session_id, None, content.len() as u64, stream)
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
                &sha256::digest(content),
                0,
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
}
