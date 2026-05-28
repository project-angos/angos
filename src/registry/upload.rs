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

pub enum StartUploadResponse {
    ExistingBlob { headers: HeaderMap },
    Session { headers: HeaderMap },
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

    async fn finish_completed_upload(
        &self,
        actor: Option<EventActor>,
        namespace: &Namespace,
        session_key: &str,
        digest: &Digest,
    ) -> CompleteUploadResponse {
        if let Err(error) = self.upload_store.delete(namespace, session_key).await {
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

        let session_uuid = Uuid::new_v4().to_string();
        self.upload_store.create(namespace, &session_uuid).await?;

        Ok(StartUploadResponse::Session {
            headers: upload_session_headers(namespace, &session_uuid),
        })
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

        let summary = self.upload_store.summary(namespace, &session_key).await?;

        if let Some(offset) = start_offset
            && offset != summary.size
        {
            return Err(Error::RangeNotSatisfiable);
        }

        let (_, size) = self
            .upload_store
            .write(
                namespace,
                &session_key,
                Box::new(stream),
                content_length,
                true,
            )
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

        let append = match self.upload_store.summary(namespace, &session_key).await {
            Ok(summary) => summary.size > 0,
            Err(blob_store::Error::UploadNotFound) => false,
            Err(e) => return Err(e.into()),
        };

        let mut stream = stream;
        if !append
            && self
                .complete_existing_upload(namespace, digest, content_length, &mut stream)
                .await?
        {
            return Ok(self
                .finish_completed_upload(actor, namespace, &session_key, digest)
                .await);
        }

        let (upload_digest, _) = self
            .upload_store
            .write(
                namespace,
                &session_key,
                Box::new(stream),
                content_length,
                append,
            )
            .await?;

        if &upload_digest != digest {
            warn!("Expected digest '{digest}', got '{upload_digest}'");
            return Err(Error::DigestInvalid);
        }

        match self.blob_store.size(digest).await {
            Ok(_) => {}
            Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
                self.upload_store
                    .complete(namespace, &session_key, Some(digest))
                    .await?;
            }
            Err(error) => return Err(Error::from(error)),
        }

        BlobOwnership::new(self.metadata_store.as_ref())
            .grant(namespace, digest)
            .await?;

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
        self.upload_store.delete(namespace, &uuid).await?;

        Ok(())
    }

    #[instrument]
    pub async fn get_upload_status(
        &self,
        namespace: &Namespace,
        session_id: Uuid,
    ) -> Result<GetUploadResponse, Error> {
        let uuid = session_id.to_string();
        let summary = self.upload_store.summary(namespace, &uuid).await?;

        let range_max = summary.size.saturating_sub(1);

        Ok(GetUploadResponse {
            headers: upload_status_headers(namespace, &uuid, range_max),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use async_trait::async_trait;
    use hyper::header::{LOCATION, RANGE};
    use uuid::Uuid;

    use angos_tx_engine::transaction::Mutation;

    use crate::{
        event_webhook::event::EventKind,
        oci::{Digest, Namespace},
        registry::{
            DOCKER_CONTENT_DIGEST, DOCKER_UPLOAD_UUID, Error, StartUploadResponse,
            blob_ownership::BlobOwnership,
            blob_store,
            blob_store::{BoxedReader, UploadStore, UploadSummary},
            metadata_store::link_kind::LinkKind,
            path_builder,
            test_utils::{
                FSRegistryTestCase, RegistryTestCase, backends, create_test_registry,
                put_blob_direct,
            },
        },
        util::sha256,
    };

    struct DeleteFailingUploadStore {
        inner: Arc<dyn UploadStore>,
    }

    struct CompleteFailingUploadStore {
        inner: Arc<dyn UploadStore>,
    }

    struct WriteFailingUploadStore {
        inner: Arc<dyn UploadStore>,
    }

    #[async_trait]
    impl UploadStore for DeleteFailingUploadStore {
        async fn list(
            &self,
            namespace: &str,
            n: u16,
            continuation_token: Option<String>,
        ) -> Result<(Vec<String>, Option<String>), blob_store::Error> {
            self.inner.list(namespace, n, continuation_token).await
        }

        async fn create(&self, namespace: &str, uuid: &str) -> Result<String, blob_store::Error> {
            self.inner.create(namespace, uuid).await
        }

        async fn write(
            &self,
            namespace: &str,
            uuid: &str,
            stream: BoxedReader,
            content_length: u64,
            append: bool,
        ) -> Result<(Digest, u64), blob_store::Error> {
            self.inner
                .write(namespace, uuid, stream, content_length, append)
                .await
        }

        async fn summary(
            &self,
            namespace: &str,
            uuid: &str,
        ) -> Result<UploadSummary, blob_store::Error> {
            self.inner.summary(namespace, uuid).await
        }

        async fn complete(
            &self,
            namespace: &str,
            uuid: &str,
            digest: Option<&Digest>,
        ) -> Result<Digest, blob_store::Error> {
            self.inner.complete(namespace, uuid, digest).await
        }

        async fn finalize_mutations(
            &self,
            namespace: &str,
            uuid: &str,
            expected_digest: Option<&Digest>,
        ) -> Result<(Digest, Vec<Mutation>), blob_store::Error> {
            self.inner
                .finalize_mutations(namespace, uuid, expected_digest)
                .await
        }

        async fn delete(&self, _namespace: &str, _uuid: &str) -> Result<(), blob_store::Error> {
            Err(blob_store::Error::StorageBackend(
                "delete failed".to_string(),
            ))
        }
    }

    #[async_trait]
    impl UploadStore for CompleteFailingUploadStore {
        async fn list(
            &self,
            namespace: &str,
            n: u16,
            continuation_token: Option<String>,
        ) -> Result<(Vec<String>, Option<String>), blob_store::Error> {
            self.inner.list(namespace, n, continuation_token).await
        }

        async fn create(&self, namespace: &str, uuid: &str) -> Result<String, blob_store::Error> {
            self.inner.create(namespace, uuid).await
        }

        async fn write(
            &self,
            namespace: &str,
            uuid: &str,
            stream: BoxedReader,
            content_length: u64,
            append: bool,
        ) -> Result<(Digest, u64), blob_store::Error> {
            self.inner
                .write(namespace, uuid, stream, content_length, append)
                .await
        }

        async fn summary(
            &self,
            namespace: &str,
            uuid: &str,
        ) -> Result<UploadSummary, blob_store::Error> {
            self.inner.summary(namespace, uuid).await
        }

        async fn complete(
            &self,
            _namespace: &str,
            _uuid: &str,
            _digest: Option<&Digest>,
        ) -> Result<Digest, blob_store::Error> {
            Err(blob_store::Error::StorageBackend(
                "complete should not be called for existing blob data".to_string(),
            ))
        }

        async fn finalize_mutations(
            &self,
            _namespace: &str,
            _uuid: &str,
            _expected_digest: Option<&Digest>,
        ) -> Result<(Digest, Vec<Mutation>), blob_store::Error> {
            Err(blob_store::Error::StorageBackend(
                "finalize_mutations should not be called for existing blob data".to_string(),
            ))
        }

        async fn delete(&self, namespace: &str, uuid: &str) -> Result<(), blob_store::Error> {
            self.inner.delete(namespace, uuid).await
        }
    }

    #[async_trait]
    impl UploadStore for WriteFailingUploadStore {
        async fn list(
            &self,
            namespace: &str,
            n: u16,
            continuation_token: Option<String>,
        ) -> Result<(Vec<String>, Option<String>), blob_store::Error> {
            self.inner.list(namespace, n, continuation_token).await
        }

        async fn create(&self, namespace: &str, uuid: &str) -> Result<String, blob_store::Error> {
            self.inner.create(namespace, uuid).await
        }

        async fn write(
            &self,
            _namespace: &str,
            _uuid: &str,
            _stream: BoxedReader,
            _content_length: u64,
            _append: bool,
        ) -> Result<(Digest, u64), blob_store::Error> {
            Err(blob_store::Error::StorageBackend(
                "write should not be called for monolithic existing blob upload".to_string(),
            ))
        }

        async fn summary(
            &self,
            namespace: &str,
            uuid: &str,
        ) -> Result<UploadSummary, blob_store::Error> {
            self.inner.summary(namespace, uuid).await
        }

        async fn complete(
            &self,
            _namespace: &str,
            _uuid: &str,
            _digest: Option<&Digest>,
        ) -> Result<Digest, blob_store::Error> {
            Err(blob_store::Error::StorageBackend(
                "complete should not be called for monolithic existing blob upload".to_string(),
            ))
        }

        async fn finalize_mutations(
            &self,
            _namespace: &str,
            _uuid: &str,
            _expected_digest: Option<&Digest>,
        ) -> Result<(Digest, Vec<Mutation>), blob_store::Error> {
            Err(blob_store::Error::StorageBackend(
                "finalize_mutations should not be called for monolithic existing blob upload"
                    .to_string(),
            ))
        }

        async fn delete(&self, namespace: &str, uuid: &str) -> Result<(), blob_store::Error> {
            self.inner.delete(namespace, uuid).await
        }
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
    async fn test_patch_upload() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test patch content";
            let session_id = Uuid::new_v4();

            registry
                .upload_store
                .create(namespace, &session_id.to_string())
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
                .upload_store
                .summary(namespace, &session_id.to_string())
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
                .upload_store
                .create(namespace, &session_id.to_string())
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
            RegistryTestCase::blob_store(&test_case),
            Arc::new(DeleteFailingUploadStore {
                inner: test_case.upload_store(),
            }),
            None,
            test_case.metadata_store(),
        );
        let namespace = &Namespace::new("test-repo").unwrap();
        let content = b"test complete content despite cleanup failure";
        let session_id = Uuid::new_v4();

        registry
            .upload_store
            .create(namespace, &session_id.to_string())
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
            RegistryTestCase::blob_store(&test_case),
            Arc::new(CompleteFailingUploadStore {
                inner: test_case.upload_store(),
            }),
            None,
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
            .upload_store
            .create(second_namespace, &session_id.to_string())
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
                .upload_store
                .summary(second_namespace, &session_id.to_string())
                .await
                .is_err()
        );

        test_case.cleanup().await;
    }

    #[tokio::test]
    async fn test_complete_upload_hashes_existing_blob_without_upload_storage_write() {
        let test_case = FSRegistryTestCase::new();
        let registry = create_test_registry(
            RegistryTestCase::blob_store(&test_case),
            Arc::new(WriteFailingUploadStore {
                inner: test_case.upload_store(),
            }),
            None,
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
            .upload_store
            .create(second_namespace, &session_id.to_string())
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
                .upload_store
                .summary(second_namespace, &session_id.to_string())
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
                .upload_store
                .create(namespace, &session_id.to_string())
                .await
                .unwrap();

            assert!(
                registry
                    .upload_store
                    .summary(namespace, &session_id.to_string())
                    .await
                    .is_ok()
            );

            registry.delete_upload(namespace, session_id).await.unwrap();

            assert!(
                registry
                    .upload_store
                    .summary(namespace, &session_id.to_string())
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
                .upload_store
                .create(namespace, &session_id.to_string())
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
                .upload_store
                .create(namespace, &session_id.to_string())
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
        use std::str::FromStr;

        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let session_id = Uuid::new_v4();

            registry
                .upload_store
                .create(namespace, &session_id.to_string())
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
                .upload_store
                .create(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync> =
                Box::new(Cursor::new(content.to_vec()));
            let (digest, size) = registry
                .upload_store
                .write(
                    namespace,
                    &session_id.to_string(),
                    stream,
                    content.len() as u64,
                    false,
                )
                .await
                .unwrap();

            assert_eq!(size, content.len() as u64);
            assert_eq!(digest, sha256::digest(content));

            let summary = registry
                .upload_store
                .summary(namespace, &session_id.to_string())
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
                .upload_store
                .create(namespace, &session_id.to_string())
                .await
                .unwrap();

            let summary = registry
                .upload_store
                .summary(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(summary.size, 0);

            let stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync> =
                Box::new(Cursor::new(content.to_vec()));
            registry
                .upload_store
                .write(
                    namespace,
                    &session_id.to_string(),
                    stream,
                    content.len() as u64,
                    false,
                )
                .await
                .unwrap();

            let summary = registry
                .upload_store
                .summary(namespace, &session_id.to_string())
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
            .upload_store
            .create(namespace, &session_id.to_string())
            .await
            .unwrap();

        let stream = Cursor::new(content);
        registry
            .patch_upload(namespace, session_id, None, content.len() as u64, stream)
            .await
            .unwrap();

        let summary = registry
            .upload_store
            .summary(namespace, &session_id.to_string())
            .await
            .unwrap();

        assert_eq!(summary.size, content.len() as u64);

        // Corrupt the session record's hash_context field so that
        // `complete_upload` cannot reconstruct the final digest. On the
        // engine path the hash state lives in the session JSON (not in a
        // legacy `hashstates/sha256/<offset>` file).
        let session_path = path_builder::upload_session_path(namespace, &session_id.to_string());
        let session_file_path = test_case.temp_dir().path().join(&session_path);
        let session_bytes = std::fs::read(&session_file_path).unwrap();
        let mut session: serde_json::Value = serde_json::from_slice(&session_bytes).unwrap();
        session["hash_context"] = serde_json::Value::String("not-valid-base64!!!".to_string());
        std::fs::write(&session_file_path, serde_json::to_vec(&session).unwrap()).unwrap();

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
