use tokio::io::AsyncReadExt;
use tracing::{debug, info, instrument, warn};

use angos_oci::{
    Digest, Namespace, UploadSessionId,
    http_range::RequestRange,
    request::{DeleteBlobRequest, GetBlobRequest, HeadBlobRequest},
};
use angos_oci_service::{Accepted, BlobDescriptor, BlobGet, BlobStream};

use crate::{
    cache_fill::build_envelope,
    event_webhook::event::{Event, EventActor},
    jobs::Queue,
    metrics_provider::metrics_provider,
    registry::{
        Error, Registry, Repository,
        blob_ownership::promote_and_grant,
        blob_store::{BlobStore, BoxedReader, upload_session::HashStart},
        metadata_store::{LinkKind, MetadataStore},
        record_pull_through, repository_name,
    },
};

/// Default read buffer each frame of a streamed blob response is filled from.
/// tokio-util's own default is 4 KiB, which costs a quarter of a million
/// frames per GiB served.
pub const DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES: usize = 128 * 1024;

/// A whole-blob (`200`) stream, read locally or streamed from an upstream.
fn whole_blob_response(
    digest: &Digest,
    total_length: u64,
    body: BoxedReader,
) -> BlobStream<BoxedReader> {
    BlobStream {
        digest: digest.clone(),
        total_length,
        range: None,
        reader: body,
    }
}

/// Cache a pull-through blob: stage and finalize its bytes through the blob
/// store, then grant `namespace` a reference through the metadata store.
///
/// The two stores may be separate backends, so each write stands alone. Byte
/// presence is the dedup gate and the grant is idempotent, so a retry after a
/// partial fill re-grants without re-fetching; a crash before the grant leaves
/// the bytes for scrub to reclaim.
pub async fn cache_blob(
    blob_store: &BlobStore,
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
    stream: BoxedReader,
    content_length: u64,
) -> Result<(), Error> {
    debug!("Fetching blob: {digest}");
    let session_id = UploadSessionId::generate();
    // The fill knows what it is fetching, so the session hashes that alone.
    blob_store
        .create_upload(namespace, &session_id, Some(digest.algorithm()))
        .await?;

    let result = fill_cache_session(
        blob_store,
        metadata_store,
        namespace,
        digest,
        stream,
        content_length,
        &session_id,
    )
    .await;

    // Reclaim the session whatever the outcome: a fill that fails partway
    // otherwise strands a layer-sized staging directory until scrub runs, and
    // repeated failures would fill the disk.
    if let Err(error) = blob_store.delete_upload(namespace, &session_id).await {
        warn!("Failed to delete cache-fill upload state: {error}");
    }
    result?;

    info!("Caching of {digest} completed");
    Ok(())
}

/// Stream the upstream bytes into the staged session and promote them. The
/// caller owns the session's lifetime and reclaims it on every outcome.
async fn fill_cache_session(
    blob_store: &BlobStore,
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
    stream: BoxedReader,
    content_length: u64,
    session_key: &UploadSessionId,
) -> Result<(), Error> {
    // A single-shot copy of a known blob: hash only the target algorithm.
    let (computed_digest, hashed_size) = blob_store
        .write_upload(
            namespace,
            session_key,
            stream,
            Some(content_length),
            HashStart::Fresh(digest.algorithm()),
            digest.algorithm(),
        )
        .await?;
    // A compromised or man-in-the-middle upstream must not poison the cache
    // under a trusted digest, so mismatched bytes are never promoted.
    if &computed_digest != digest {
        warn!("Pull-through blob digest mismatch: expected {digest}, got {computed_digest}");
        return Err(Error::DigestInvalid);
    }
    // Bytes land before the grant, mirroring the manifest path's
    // bytes-then-link order; both are fresh and inside the grace period.
    promote_and_grant(
        blob_store,
        metadata_store,
        namespace,
        session_key,
        digest,
        hashed_size,
    )
    .await
}

impl Registry {
    #[instrument]
    /// `HEAD /v2/<name>/blobs/<digest>`: the blob's descriptor, no body.
    pub async fn handle_head_blob(
        &self,
        request: HeadBlobRequest,
    ) -> Result<BlobDescriptor, Error> {
        let has_access = self
            .metadata_store()
            .can_read(&request.namespace, &request.digest)
            .await?;
        // A namespace no `[repository]` entry matches has no upstream, so it
        // serves what it owns and nothing else.
        let repository = self.get_repository_for_namespace(&request.namespace).ok();
        let (digest, size) = match repository.filter(|repository| repository.is_pull_through()) {
            Some(upstream) => self.head_cached_blob(upstream, request, has_access).await?,
            None if has_access => {
                let size = self.blob_store.size(&request.digest).await?;
                (request.digest, size)
            }
            None => return Err(Error::BlobUnknown),
        };

        Ok(BlobDescriptor {
            digest,
            size,
            media_type: None,
        })
    }

    /// HEAD on a pull-through namespace: the cached descriptor when the
    /// namespace owns the blob and the bytes are there, else the upstream's.
    async fn head_cached_blob(
        &self,
        upstream: &Repository,
        request: HeadBlobRequest,
        has_access: bool,
    ) -> Result<(Digest, u64), Error> {
        if has_access {
            match self.blob_store.size(&request.digest).await {
                Ok(size) => {
                    record_pull_through(&upstream.name, "blob", "hit");
                    return Ok((request.digest, size));
                }
                // As on GET, a genuine miss re-heads upstream while every
                // other error propagates instead of masquerading as a 404.
                Err(Error::BlobUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        record_pull_through(&upstream.name, "blob", "miss");
        upstream
            .head_blob(&request.accepted_types, &request.namespace, &request.digest)
            .await
    }

    /// GET on a pull-through namespace: the cached copy when the namespace
    /// owns the blob and the bytes are there, else the upstream's, which a
    /// cache-fill job then stores. The caller resolves `has_access` once, so
    /// the hot path does not pay for the blob-index read twice.
    pub async fn get_cached_blob(
        &self,
        upstream: &Repository,
        request: &GetBlobRequest,
        has_access: bool,
        allow_redirect: bool,
    ) -> Result<BlobGet<BoxedReader>, Error> {
        if has_access {
            match self.serve_local_blob(request, allow_redirect).await {
                Ok(served) => {
                    record_pull_through(&upstream.name, "blob", "hit");
                    return Ok(served);
                }
                // Owned but the bytes are gone: re-fetch. Every other error
                // propagates instead of masquerading as a 404.
                Err(Error::BlobUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        record_pull_through(&upstream.name, "blob", "miss");
        let fetched = upstream
            .get_blob(
                &request.accepted_types,
                &request.namespace,
                &request.digest,
                request.range,
            )
            .await?;

        self.dispatch_cache_fill(&request.namespace, &request.digest)
            .await;

        // An upstream is free to ignore `Range` and answer the whole blob,
        // which stays a valid answer; only its `206` becomes partial content.
        let stream = match fetched.content_range {
            Some(range) => BlobStream {
                digest: request.digest.clone(),
                total_length: fetched.length,
                range: Some(range),
                reader: fetched.reader,
            },
            None => whole_blob_response(&request.digest, fetched.length, fetched.reader),
        };
        Ok(BlobGet::Content(stream))
    }

    /// The locally held blob: a presigned redirect when the caller and
    /// `enable_blob_redirect` allow one and no range is asked, else a stream.
    pub async fn serve_local_blob(
        &self,
        request: &GetBlobRequest,
        allow_redirect: bool,
    ) -> Result<BlobGet<BoxedReader>, Error> {
        if request.range.is_none()
            && allow_redirect
            && self.enable_blob_redirect
            && self.blob_store.size(&request.digest).await.is_ok()
            && let Ok(Some(location)) = self.blob_store.presigned_url(&request.digest, None).await
        {
            return Ok(BlobGet::Redirect {
                digest: request.digest.clone(),
                location,
            });
        }
        Ok(BlobGet::Content(
            self.get_local_blob(&request.digest, request.range).await?,
        ))
    }

    /// Fire-and-forget enqueue of a pull-through cache-fill job. A failure is
    /// logged and counted but never bubbles up, so a scheduling glitch cannot
    /// degrade the client response.
    async fn dispatch_cache_fill(&self, namespace: &Namespace, digest: &Digest) {
        // Build + enqueue as one fallible step so failures share the warn + metric path.
        let outcome = match build_envelope(namespace, digest) {
            Ok(envelope) => self
                .job_queue
                .enqueue(envelope)
                .await
                .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = outcome {
            warn!("Failed to enqueue cache job for {digest}: {e}");
            metrics_provider()
                .job_queue_enqueue_failures_total
                .with_label_values(&[Queue::Cache.as_str()])
                .inc();
        }
    }

    async fn get_local_blob(
        &self,
        digest: &Digest,
        range: Option<RequestRange>,
    ) -> Result<BlobStream<BoxedReader>, Error> {
        let Some(requested_range) = range else {
            let (reader, total_length) = self.blob_store.reader(digest, None).await?;
            return Ok(whole_blob_response(digest, total_length, reader));
        };

        let total_length = self.blob_store.size(digest).await?;
        let Some(served) = requested_range.resolve(total_length)? else {
            let (reader, _) = self.blob_store.reader(digest, None).await?;
            return Ok(whole_blob_response(digest, total_length, reader));
        };
        let (reader, _) = self.blob_store.reader(digest, Some(served.start)).await?;
        let reader: BoxedReader = Box::new(reader.take(served.length()));

        Ok(BlobStream {
            digest: digest.clone(),
            total_length: served.length(),
            range: Some(served),
            reader,
        })
    }

    #[instrument]
    /// `DELETE /v2/<name>/blobs/<digest>`: revokes ownership; the collector
    /// reclaims the bytes once every reference is stale.
    pub async fn handle_delete_blob(&self, request: DeleteBlobRequest) -> Result<Accepted, Error> {
        let ownership = self.metadata_store();
        let links = match ownership
            .read_blob_index_namespace(&request.namespace, &request.digest)
            .await
        {
            Ok(links) => links,
            Err(Error::NotFound) => return Err(Error::BlobUnknown),
            Err(error) => return Err(error),
        };

        // Writers never remove reference entries, so only an entry whose
        // backing link still resolves counts: a stale one must not block the
        // client's delete-manifest-then-blobs flow.
        for link in &links {
            if matches!(link, LinkKind::Blob(link_digest) if link_digest == &request.digest) {
                continue;
            }
            if self
                .metadata_store
                .reference_backed(&request.namespace, link, &request.digest)
                .await?
            {
                return Err(Error::BlobReferenced);
            }
        }

        // One delete of the `own` key; the bytes are the collector's to
        // reclaim once every reference is stale.
        self.metadata_store
            .revoke_blob_ownership(&request.namespace, &request.digest)
            .await?;

        Ok(Accepted)
    }

    /// Resolves a blob GET to either a presigned redirect URL or a stream,
    /// then emits a `blob.pull` event. The redirect fast-path needs
    /// `allow_redirect`, `enable_blob_redirect`, no range, and locally
    /// available bytes.
    #[instrument(skip(self, request))]
    pub async fn handle_get_blob(
        &self,
        actor: Option<EventActor>,
        request: GetBlobRequest,
        allow_redirect: bool,
    ) -> Result<BlobGet<BoxedReader>, Error> {
        let repository = self.get_repository_for_namespace(&request.namespace).ok();
        let repository_name = repository_name(repository);

        let has_access = self
            .metadata_store()
            .can_read(&request.namespace, &request.digest)
            .await?;

        let response = match repository.filter(|repository| repository.is_pull_through()) {
            Some(upstream) => {
                self.get_cached_blob(upstream, &request, has_access, allow_redirect)
                    .await?
            }
            None if has_access => self.serve_local_blob(&request, allow_redirect).await?,
            None => return Err(Error::BlobUnknown),
        };

        let event = Event::pull_blob(
            &request.namespace,
            &repository_name,
            &request.digest,
            actor.as_ref(),
        );
        self.dispatch_events(&[event]).await?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use http::{
        StatusCode,
        header::{CONTENT_LENGTH, CONTENT_RANGE},
    };
    use tempfile::TempDir;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use angos_oci::{Namespace, Tag, http_range::ByteWindow};
    use angos_storage::{
        Error as StorageError, ObjectStore, PresignedStore,
        fs::Backend as StorageFsBackend,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use crate::{
        metrics_provider::{init_for_tests, metrics_provider},
        registry::{
            Registry, RegistryConfig,
            blob::*,
            keys::{DigestKeys, NamespaceKeys},
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            repository::Config,
            test_utils::{
                RegistryTestCase, create_test_blob, create_test_registry, drop_links,
                for_each_backend, get_blob, metadata_store_over, put_blob_direct, response_body,
                response_digest, response_header, seed_links, single_repo_resolver, test_job_store,
                upload_blob,
            },
        },
        test_fixtures::client::test_client_config,
    };

    #[tokio::test]
    async fn test_head_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .handle_head_blob(HeadBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    accepted_types: Vec::new(),
                })
                .await
                .unwrap()
                .into_response()
                .unwrap();

            assert_eq!(response_digest(&response), digest);
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                content.len().to_string()
            );
        })
        .await;
    }

    /// Fails the `head` of one key, leaving every other operation intact.
    struct FailHeadOf {
        key: String,
    }

    #[async_trait]
    impl StoreHook for FailHeadOf {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            match op {
                StoreOp::Head { key } if key == self.key => {
                    Err(StorageError::Backend("injected head failure".to_string()))
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn head_blob_propagates_transient_error_instead_of_404() {
        let namespace = &Namespace::new("test-repo").unwrap();
        let digest = Digest::sha256_of_bytes(b"transient-head-blob");

        // Only the blob-size `head` fails, so the request still reaches the
        // size probe with access.
        let dir = TempDir::new().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
        let object: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
            inner,
            FailHeadOf {
                key: digest.blob_path(),
            },
        ));
        let blob_store = Arc::new(BlobStore::new(object.clone(), None));
        let registry = create_test_registry(blob_store, metadata_store_over(object));

        registry
            .metadata_store()
            .grant(namespace, &digest)
            .await
            .unwrap();

        let result = registry
            .handle_head_blob(HeadBlobRequest {
                namespace: namespace.clone(),
                digest: digest.clone(),
                accepted_types: Vec::new(),
            })
            .await;
        assert!(
            matches!(result, Err(Error::Internal(_))),
            "a transient size() error must propagate, not map to BlobUnknown; got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_get_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, repository) = create_test_blob(registry, namespace, content).await;
            let response = get_blob(registry, &repository, &[], namespace, &digest, None)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response_digest(&response), digest);
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                content.len().to_string()
            );
            assert_eq!(response_body(response).await, content);
        })
        .await;
    }

    #[tokio::test]
    async fn get_blob_rejects_local_blob_without_namespace_ownership() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"unowned blob content";
            let digest = put_blob_direct(registry.metadata_store.object_store(), content).await;
            let repository = registry.get_repository_for_namespace(namespace).unwrap();

            let head_result = registry
                .handle_head_blob(HeadBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    accepted_types: Vec::new(),
                })
                .await;
            assert!(matches!(head_result, Err(Error::BlobUnknown)));

            let get_result = get_blob(registry, repository, &[], namespace, &digest, None).await;
            assert!(matches!(get_result, Err(Error::BlobUnknown)));
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_blob_with_range() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, repository) = create_test_blob(registry, namespace, content).await;
            let range = Some(RequestRange::FromTo(ByteWindow {
                start: 5,
                end: Some(10),
            }));
            let response = get_blob(registry, &repository, &[], namespace, &digest, range)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(response_digest(&response), digest);
            assert_eq!(
                *response_header(&response, &CONTENT_RANGE),
                format!("bytes 5-10/{}", content.len())
            );
            assert_eq!(*response_header(&response, &CONTENT_LENGTH), "6");
            assert_eq!(response_body(response).await, &content[5..=10]);
        })
        .await;
    }

    #[tokio::test]
    async fn test_delete_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let digest = put_blob_direct(registry.metadata_store.object_store(), content).await;
            registry
                .metadata_store()
                .grant(namespace, &digest)
                .await
                .unwrap();

            let blob_index = registry
                .metadata_store
                .read_blob_index(&digest)
                .await
                .unwrap();
            assert!(blob_index.contains_key(namespace));
            let namespace_links = blob_index.get(namespace).unwrap();
            assert!(namespace_links.contains(&LinkKind::Blob(digest.clone())));

            registry
                .handle_delete_blob(DeleteBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            // The bytes and the stale entry wait for the collector.
            assert!(registry.blob_store.read(&digest).await.is_ok());
            assert!(
                !registry
                    .metadata_store()
                    .can_read(namespace, &digest)
                    .await
                    .unwrap()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn delete_blob_rejects_manifest_referenced_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"referenced blob content";
            let digest = put_blob_direct(registry.metadata_store.object_store(), content).await;

            // A live referring revision, whose per-referrer entry is what pins
            // the blob against the delete.
            let manifest =
                put_blob_direct(registry.metadata_store.object_store(), b"manifest").await;
            seed_links(
                &registry.metadata_store,
                namespace,
                &[(LinkKind::Digest(manifest.clone()), manifest.clone())],
            )
            .await
            .unwrap();
            registry
                .metadata_store
                .pin_references(
                    namespace,
                    &[(digest.clone(), LinkKind::ReferencedBy(manifest.clone()))],
                )
                .await
                .unwrap();

            let result = registry
                .handle_delete_blob(DeleteBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await;
            assert!(matches!(result, Err(Error::BlobReferenced)));

            let stored_content = registry.blob_store.read(&digest).await.unwrap();
            assert_eq!(stored_content, content);
            assert!(
                registry
                    .metadata_store()
                    .can_read(namespace, &digest)
                    .await
                    .unwrap(),
                "the referenced blob must stay readable after the refused delete"
            );
        })
        .await;
    }

    /// The conformance delete flow: a manifest, then its layer blob. The stale
    /// manifest entry writers leave behind must grant neither the delete gate
    /// nor read access.
    #[tokio::test]
    async fn deleted_blob_is_unreadable_despite_stale_manifest_reference() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"stale referenced blob";
            let digest = put_blob_direct(registry.metadata_store.object_store(), content).await;
            let ownership = registry.metadata_store();
            ownership.grant(namespace, &digest).await.unwrap();

            let link = LinkKind::ReferencedBy(Digest::sha256_of_bytes(b"manifest"));
            registry
                .metadata_store
                .pin_references(
                    namespace,
                    &[(
                        digest.clone(),
                        LinkKind::ReferencedBy(Digest::sha256_of_bytes(b"manifest")),
                    )],
                )
                .await
                .unwrap();
            drop_links(&registry.metadata_store, namespace, &[link])
                .await
                .unwrap();

            registry
                .handle_delete_blob(DeleteBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert!(!ownership.can_read(namespace, &digest).await.unwrap());
            let head = registry
                .handle_head_blob(HeadBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    accepted_types: Vec::new(),
                })
                .await;
            assert!(matches!(head, Err(Error::BlobUnknown)));
        })
        .await;
    }

    #[tokio::test]
    async fn delete_blob_rejects_all_metadata_references() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let parent =
                put_blob_direct(registry.metadata_store.object_store(), b"index manifest").await;
            // Every kind is backed only while a referring manifest's revision
            // resolves, so each case names `parent`.
            seed_links(
                &registry.metadata_store,
                namespace,
                &[(LinkKind::Digest(parent.clone()), parent.clone())],
            )
            .await
            .unwrap();
            let subject = Digest::sha256_of_bytes(b"subject manifest");

            let cases = [
                LinkKind::Digest(Digest::sha256_of_bytes(b"digest reference")),
                LinkKind::Tag(Tag::new("latest").unwrap()),
                LinkKind::ReferencedBy(parent.clone()),
                LinkKind::Referrer {
                    subject,
                    referrer: Digest::sha256_of_bytes(b"referrer manifest"),
                },
            ];

            for link in cases {
                let content = format!("content for {link}").into_bytes();
                let digest =
                    put_blob_direct(registry.metadata_store.object_store(), &content).await;
                registry
                    .metadata_store()
                    .grant(namespace, &digest)
                    .await
                    .unwrap();

                let retargeted = retarget_link(&link, &digest);
                match &link {
                    LinkKind::ReferencedBy(_) => {
                        registry
                            .metadata_store
                            .pin_references(
                                namespace,
                                &[(digest.clone(), LinkKind::ReferencedBy(parent.clone()))],
                            )
                            .await
                    }
                    _ => {
                        seed_links(
                            &registry.metadata_store,
                            namespace,
                            &[(retargeted, digest.clone())],
                        )
                        .await
                    }
                }
                .unwrap();

                let result = registry
                    .handle_delete_blob(DeleteBlobRequest {
                        namespace: namespace.clone(),
                        digest: digest.clone(),
                    })
                    .await;
                assert!(matches!(result, Err(Error::BlobReferenced)));
                assert_eq!(registry.blob_store.read(&digest).await.unwrap(), content);
            }
        })
        .await;
    }

    fn retarget_link(link: &LinkKind, digest: &Digest) -> LinkKind {
        match link {
            LinkKind::Digest(_) => LinkKind::Digest(digest.clone()),
            LinkKind::Referrer {
                subject,
                referrer: _,
            } => LinkKind::Referrer {
                subject: subject.clone(),
                referrer: digest.clone(),
            },
            // Nothing to retarget: these name no separate blob.
            LinkKind::Blob(_) | LinkKind::Tag(_) | LinkKind::ReferencedBy(_) => link.clone(),
        }
    }

    #[tokio::test]
    async fn delete_blob_keeps_data_owned_by_other_namespace() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let first = &Namespace::new("test-repo/first").unwrap();
            let second = &Namespace::new("test-repo/second").unwrap();
            let content = b"shared blob content";
            let digest = put_blob_direct(registry.metadata_store.object_store(), content).await;
            let ownership = registry.metadata_store();

            ownership.grant(first, &digest).await.unwrap();
            ownership.grant(second, &digest).await.unwrap();

            registry
                .handle_delete_blob(DeleteBlobRequest {
                    namespace: first.clone(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            assert_eq!(registry.blob_store.read(&digest).await.unwrap(), content);
            assert!(!ownership.can_read(first, &digest).await.unwrap());
            assert!(ownership.can_read(second, &digest).await.unwrap());

            registry
                .handle_delete_blob(DeleteBlobRequest {
                    namespace: second.clone(),
                    digest: digest.clone(),
                })
                .await
                .unwrap();

            // Every owner revoked: the bytes wait for the collector.
            assert!(registry.blob_store.read(&digest).await.is_ok());
            assert!(!ownership.can_read(second, &digest).await.unwrap());
        })
        .await;
    }

    #[tokio::test]
    async fn delete_blob_rejects_unowned_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"unowned delete content";
            let digest = put_blob_direct(registry.metadata_store.object_store(), content).await;

            let result = registry
                .handle_delete_blob(DeleteBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                })
                .await;
            assert!(matches!(result, Err(Error::BlobUnknown)));

            let stored_content = registry.blob_store.read(&digest).await.unwrap();
            assert_eq!(stored_content, content);
        })
        .await;
    }

    #[tokio::test]
    async fn cache_blob_updates_namespace_blob_index() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = Namespace::new("test-repo").unwrap();
            let content = b"cached pull-through blob content";
            let digest = Digest::sha256_of_bytes(content);
            let stream = Box::new(Cursor::new(content.to_vec()));

            cache_blob(
                &registry.blob_store,
                &registry.metadata_store,
                &namespace,
                &digest,
                stream,
                content.len() as u64,
            )
            .await
            .unwrap();

            let blob_index = registry
                .metadata_store
                .read_blob_index(&digest)
                .await
                .unwrap();
            let namespace_links = blob_index.get(&namespace).unwrap();
            assert!(namespace_links.contains(&LinkKind::Blob(digest.clone())));

            let repository = registry.get_repository_for_namespace(&namespace).unwrap();
            let response = get_blob(registry, repository, &[], &namespace, &digest, None)
                .await
                .unwrap();

            assert_eq!(response_body(response).await, content);
        })
        .await;
    }

    /// A range over a blob the cache does not hold yet must be forwarded to
    /// the upstream and answered `206`, not refused with `416`, so one URL
    /// answers the same whatever the cache state.
    #[tokio::test]
    async fn ranged_get_of_an_uncached_pull_through_blob_serves_partial_content() {
        let content = b"pull-through ranged blob content";
        let digest = Digest::sha256_of_bytes(content);
        let content_range = format!("bytes 5-10/{}", content.len());

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/v2/repo/blobs/{digest}")))
            .and(header("range", "bytes=5-10"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", content_range.as_str())
                    .set_body_bytes(&content[5..=10]),
            )
            .mount(&mock_server)
            .await;

        let config = Config {
            upstream: vec![test_client_config(mock_server.uri())],
            ..Default::default()
        };
        let cache_backend = angos_cache::Config::Memory.to_backend().unwrap();
        let repository = Repository::new(
            "local",
            &config,
            &cache_backend,
            DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        )
        .await
        .unwrap();

        let dir = TempDir::new().unwrap();
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
        let registry = create_test_registry(
            Arc::new(BlobStore::new(object.clone(), None)),
            metadata_store_over(object),
        );
        let namespace = &Namespace::new("local/repo").unwrap();
        let range = Some(RequestRange::FromTo(ByteWindow {
            start: 5,
            end: Some(10),
        }));

        let response = get_blob(&registry, &repository, &[], namespace, &digest, range)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(*response_header(&response, &CONTENT_RANGE), content_range);
        assert_eq!(response_body(response).await, &content[5..=10]);
        assert_eq!(
            metrics_provider()
                .pull_through_total
                .with_label_values(&["local", "blob", "miss"])
                .get(),
            1,
            "a blob the upstream served counts one pull-through miss"
        );
    }

    /// Hands out a fixed URL so the redirect fast path runs over the fs store.
    struct FixedPresigner;

    #[async_trait]
    impl PresignedStore for FixedPresigner {
        async fn presign_get(
            &self,
            key: &str,
            _ttl: Duration,
            _content_type: Option<&str>,
        ) -> Result<String, StorageError> {
            Ok(format!("https://presigned.test/{key}"))
        }
    }

    #[tokio::test]
    async fn redirected_get_of_a_cached_blob_counts_a_pull_through_hit() {
        let mock_server = MockServer::start().await;
        let config = Config {
            upstream: vec![test_client_config(mock_server.uri())],
            ..Default::default()
        };
        let cache_backend = angos_cache::Config::Memory.to_backend().unwrap();
        let repository = Repository::new(
            "redirect-cache",
            &config,
            &cache_backend,
            DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        )
        .await
        .unwrap();

        let dir = TempDir::new().unwrap();
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(dir.path()).build());
        let metadata_store = metadata_store_over(object.clone());
        let blob_store = Arc::new(BlobStore::new(
            object,
            Some((Arc::new(FixedPresigner), Duration::from_secs(60))),
        ));
        let registry = Registry::new(
            blob_store,
            metadata_store.clone(),
            single_repo_resolver("redirect-cache", repository),
            RegistryConfig::new(test_job_store(&metadata_store)),
        );
        let namespace = Namespace::new("redirect-cache/alpine").unwrap();
        let digest = upload_blob(&registry, &namespace, b"cached blob").await;
        let hits = || {
            metrics_provider()
                .pull_through_total
                .with_label_values(&["redirect-cache", "blob", "hit"])
                .get()
        };
        let before = hits();

        let response = registry
            .handle_get_blob(
                None,
                GetBlobRequest {
                    namespace,
                    digest,
                    accepted_types: Vec::new(),
                    range: None,
                },
                true,
            )
            .await
            .unwrap();

        assert!(
            matches!(response, BlobGet::Redirect { .. }),
            "a cached blob with a presigner must take the redirect fast path"
        );
        assert_eq!(
            hits(),
            before + 1,
            "a redirected pull of a cached blob counts one pull-through hit"
        );
    }

    /// Upload-session directories still staged under `namespace`.
    async fn staged_session_count(
        test_case: &dyn RegistryTestCase,
        namespace: &Namespace,
    ) -> usize {
        test_case
            .blob_store()
            .object_store()
            .list_all_children(&namespace.uploads_root_dir())
            .await
            .expect("list upload sessions")
            .sub_prefixes
            .len()
    }

    /// A reader that fails on its first poll, standing in for an upstream
    /// dropping mid-fill.
    struct FailingReader;

    impl tokio::io::AsyncRead for FailingReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("upstream dropped mid-fill")))
        }
    }

    /// A fill that fails partway must not strand its staged session: the bytes
    /// are layer-sized, so leaving them for scrub turns an ordinary upstream
    /// fault into disk pressure.
    #[tokio::test]
    async fn cache_blob_reclaims_its_session_when_the_fill_fails() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = Namespace::new("test-repo").unwrap();
            let digest = Digest::sha256_of_bytes(b"bytes that never arrive");

            let result = cache_blob(
                &registry.blob_store,
                &registry.metadata_store,
                &namespace,
                &digest,
                Box::new(FailingReader),
                64,
            )
            .await;

            assert!(result.is_err(), "a fill whose upstream drops must fail");
            assert_eq!(
                staged_session_count(test_case, &namespace).await,
                0,
                "a failed fill must not strand its staged upload session"
            );
        })
        .await;
    }

    /// Cache-poisoning guard: bytes that do not hash to the requested digest
    /// must be rejected and never cached under it.
    #[tokio::test]
    async fn cache_blob_rejects_content_not_matching_requested_digest() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = Namespace::new("test-repo").unwrap();
            let poisoned = b"bytes an upstream served for the wrong digest";
            let requested = Digest::sha256_of_bytes(b"what the client actually asked for");

            let result = cache_blob(
                &registry.blob_store,
                &registry.metadata_store,
                &namespace,
                &requested,
                Box::new(Cursor::new(poisoned.to_vec())),
                poisoned.len() as u64,
            )
            .await;

            assert!(
                matches!(result, Err(Error::DigestInvalid)),
                "mismatched pull-through content must be rejected; got: {result:?}"
            );
            assert!(
                registry.blob_store.read(&requested).await.is_err(),
                "poisoned bytes must not be cached under the requested digest"
            );
            assert_eq!(
                staged_session_count(test_case, &namespace).await,
                0,
                "the rejected fill must not leave its staged bytes behind"
            );
        })
        .await;
    }

    /// With the two stores on separate backends, `cache_blob` must write the
    /// bytes and grant the reference as independent idempotent work.
    #[tokio::test]
    async fn cache_blob_grants_reference_with_split_blob_and_metadata_backends() {
        init_for_tests();
        let blob_dir = TempDir::new().unwrap();
        let meta_dir = TempDir::new().unwrap();

        let blob_obj: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(blob_dir.path().to_str().unwrap()).build());
        let blob_store = Arc::new(BlobStore::new(blob_obj.clone(), None));

        let meta_obj: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder(meta_dir.path().to_str().unwrap()).build());
        let metadata_store = metadata_store_over(meta_obj);

        let namespace = Namespace::new("kubernetes.io/kube-apiserver").unwrap();
        let content = b"layer bytes";
        let digest = Digest::sha256_of_bytes(content);

        // A prior manifest pull already pinned the layer to its manifest.
        metadata_store
            .insert_reference(
                &namespace,
                &digest,
                &LinkKind::ReferencedBy(Digest::sha256_of_bytes(b"manifest")),
            )
            .await
            .unwrap();

        cache_blob(
            &blob_store,
            &metadata_store,
            &namespace,
            &digest,
            Box::new(Cursor::new(content.to_vec())),
            content.len() as u64,
        )
        .await
        .unwrap();

        assert_eq!(
            blob_store.read(&digest).await.unwrap(),
            content,
            "the blob bytes must land in the blob store"
        );
        let blob_index = metadata_store.read_blob_index(&digest).await.unwrap();
        let links = blob_index.get(&namespace).unwrap();
        assert!(
            links.contains(&LinkKind::Blob(digest.clone())),
            "the namespace must hold a blob ownership reference after caching"
        );
    }

    #[tokio::test]
    async fn test_get_local_blob_returns_correct_size() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"regression test blob content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let response = registry
                .get_local_blob(&digest, None)
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                content.len().to_string()
            );
            assert_eq!(response_body(response).await, content);

            let range = Some(RequestRange::FromTo(ByteWindow {
                start: 5,
                end: Some(15),
            }));
            let response = registry
                .get_local_blob(&digest, range)
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                *response_header(&response, &CONTENT_RANGE),
                format!("bytes 5-15/{}", content.len())
            );
            assert_eq!(*response_header(&response, &CONTENT_LENGTH), "11");
            assert_eq!(response_body(response).await, &content[5..=15]);
        })
        .await;
    }

    #[tokio::test]
    async fn get_local_blob_open_ended_range_returns_partial_content() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"open ended range content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .get_local_blob(
                    &digest,
                    Some(RequestRange::FromTo(ByteWindow {
                        start: 0,
                        end: None,
                    })),
                )
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                *response_header(&response, &CONTENT_RANGE),
                format!(
                    "bytes {}-{}/{}",
                    0,
                    content.len() as u64 - 1,
                    content.len() as u64
                )
            );
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                (content.len() as u64).to_string()
            );
            assert_eq!(response_body(response).await, content);
        })
        .await;
    }

    #[tokio::test]
    async fn get_local_blob_suffix_range_returns_tail() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"suffix range content";
            let suffix_length = 7;
            let start = content.len() - suffix_length;

            let (digest, _) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .get_local_blob(&digest, Some(RequestRange::Suffix(suffix_length as u64)))
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                *response_header(&response, &CONTENT_RANGE),
                format!(
                    "bytes {}-{}/{}",
                    start as u64,
                    content.len() as u64 - 1,
                    content.len() as u64
                )
            );
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                (suffix_length as u64).to_string()
            );
            assert_eq!(response_body(response).await, &content[start..]);
        })
        .await;
    }

    #[tokio::test]
    async fn get_local_blob_suffix_range_longer_than_blob_returns_full_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"short suffix";

            let (digest, _) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .get_local_blob(&digest, Some(RequestRange::Suffix(10_000)))
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                *response_header(&response, &CONTENT_RANGE),
                format!(
                    "bytes {}-{}/{}",
                    0,
                    content.len() as u64 - 1,
                    content.len() as u64
                )
            );
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                (content.len() as u64).to_string()
            );
            assert_eq!(response_body(response).await, content);
        })
        .await;
    }

    #[tokio::test]
    async fn get_local_blob_clamps_range_end_to_blob_length() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"clamped range content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .get_local_blob(
                    &digest,
                    Some(RequestRange::FromTo(ByteWindow {
                        start: 8,
                        end: Some(10_000),
                    })),
                )
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                *response_header(&response, &CONTENT_RANGE),
                format!(
                    "bytes {}-{}/{}",
                    8,
                    content.len() as u64 - 1,
                    content.len() as u64
                )
            );
            assert_eq!(
                *response_header(&response, &CONTENT_LENGTH),
                (content.len() as u64 - 8).to_string()
            );
            assert_eq!(response_body(response).await, &content[8..]);
        })
        .await;
    }

    #[tokio::test]
    async fn get_local_blob_rejects_range_start_at_blob_length() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"range boundary";

            let (digest, _) = create_test_blob(registry, namespace, content).await;
            let result = registry
                .get_local_blob(
                    &digest,
                    Some(RequestRange::FromTo(ByteWindow {
                        start: content.len() as u64,
                        end: None,
                    })),
                )
                .await;

            assert!(matches!(result, Err(Error::RangeNotSatisfiable)));
        })
        .await;
    }

    #[tokio::test]
    async fn get_local_blob_ignores_ranges_for_empty_blobs() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();

            let (digest, _) = create_test_blob(registry, namespace, b"").await;
            let response = registry
                .get_local_blob(
                    &digest,
                    Some(RequestRange::FromTo(ByteWindow {
                        start: 0,
                        end: None,
                    })),
                )
                .await
                .unwrap()
                .into_response(DEFAULT_BLOB_STREAM_FRAME_SIZE_BYTES)
                .unwrap();

            // An empty blob has no satisfiable window, so the range is ignored.
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response_body(response).await.is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn test_head_blob_independent_of_get() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"head blob independence test";

            let (digest, repository) = create_test_blob(registry, namespace, content).await;

            let head_response = registry
                .handle_head_blob(HeadBlobRequest {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    accepted_types: Vec::new(),
                })
                .await
                .unwrap()
                .into_response()
                .unwrap();
            assert_eq!(response_digest(&head_response), digest);
            let head_length = response_header(&head_response, &CONTENT_LENGTH);
            assert_eq!(*head_length, content.len().to_string());

            // HEAD and GET must agree on what they say the blob is.
            let get_response = get_blob(registry, &repository, &[], namespace, &digest, None)
                .await
                .unwrap();
            assert_eq!(
                *response_header(&get_response, &CONTENT_LENGTH),
                head_length
            );
        })
        .await;
    }
}
