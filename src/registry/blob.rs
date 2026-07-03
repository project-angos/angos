use std::collections::HashSet;

use hyper::header::{ACCEPT_RANGES, CONTENT_RANGE};
use tokio::io::AsyncReadExt;
use tracing::{debug, info, instrument, warn};

use crate::{
    metrics_provider::metrics_provider,
    oci::{Digest, Namespace, UploadSessionId},
    registry::{
        Error, HeaderMap, Registry, Repository, ResponseHeaders,
        blob_ownership::BlobOwnership,
        blob_store::{BlobStore, BoxedReader},
        cache_job_handler::{CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload},
        job_store::{JobEnvelope, Queue},
        metadata_store::{BlobIndexOperation, LinkKind, MetadataStore},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobRange {
    FromTo { start: u64, end: Option<u64> },
    Suffix(u64),
}

pub enum GetBlobResponse {
    Redirect {
        headers: HeaderMap,
    },
    Reader {
        headers: HeaderMap,
        body: BoxedReader,
    },
    RangedReader {
        headers: HeaderMap,
        body: BoxedReader,
    },
}

pub struct HeadBlobResponse {
    pub headers: HeaderMap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResolvedRange {
    start: u64,
    end: u64,
    length: u64,
    total_length: u64,
}

fn resolve_blob_range(
    requested: BlobRange,
    total_length: u64,
) -> Result<Option<ResolvedRange>, Error> {
    if total_length == 0 {
        return Ok(None);
    }

    let last_byte = total_length - 1;
    let (start, end) = match requested {
        BlobRange::FromTo { start, end } => {
            if start >= total_length {
                return Err(Error::RangeNotSatisfiable);
            }

            let end = end.unwrap_or(last_byte).min(last_byte);
            if end < start {
                return Err(Error::RangeNotSatisfiable);
            }

            (start, end)
        }
        BlobRange::Suffix(suffix_length) => {
            if suffix_length == 0 {
                return Err(Error::RangeNotSatisfiable);
            }

            let length = suffix_length.min(total_length);
            (total_length - length, last_byte)
        }
    };

    Ok(Some(ResolvedRange {
        start,
        end,
        length: end - start + 1,
        total_length,
    }))
}

fn head_blob_headers(digest: &Digest, size: u64) -> HeaderMap {
    ResponseHeaders::new()
        .docker_content_digest(digest)
        .content_length(size)
        .into_inner()
}

fn get_blob_headers(digest: &Digest, total_length: u64) -> HeaderMap {
    ResponseHeaders::new()
        .docker_content_digest(digest)
        .with(ACCEPT_RANGES.as_str(), "bytes")
        .content_length(total_length)
        .into_inner()
}

fn get_blob_range_headers(digest: &Digest, range: ResolvedRange) -> HeaderMap {
    ResponseHeaders::new()
        .docker_content_digest(digest)
        .with(ACCEPT_RANGES.as_str(), "bytes")
        .content_length(range.length)
        .with(
            CONTENT_RANGE.as_str(),
            format!("bytes {}-{}/{}", range.start, range.end, range.total_length),
        )
        .into_inner()
}

fn get_blob_redirect_headers(url: String, digest: &Digest) -> HeaderMap {
    ResponseHeaders::new()
        .location(url)
        .docker_content_digest(digest)
        .into_inner()
}

fn has_non_ownership_reference(links: &HashSet<LinkKind>, digest: &Digest) -> bool {
    links
        .iter()
        .any(|link| !matches!(link, LinkKind::Blob(link_digest) if link_digest == digest))
}

/// Cache a pull-through blob: stage and finalize its bytes through the blob
/// store, then grant `namespace` a reference through the metadata store.
///
/// Each write commits on its own store's executor, so the blob bytes and the
/// blob-index grant can live on separate backends without one being routed
/// through the other's executor. Byte presence is the dedup gate and the grant
/// is idempotent, so a retry after a partial fill re-grants without re-fetching;
/// a crash before the grant leaves the blob-data for scrub to reclaim.
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
    blob_store
        .create_upload(namespace, session_id.as_ref())
        .await?;
    // A single-shot copy of a known blob: hash only the target algorithm.
    blob_store
        .write_monolithic_upload(
            namespace,
            session_id.as_ref(),
            stream,
            Some(content_length),
            digest.algorithm(),
        )
        .await?;
    // Promote the staged bytes and grant the reference under the blob-data lock
    // so a concurrent delete cannot reclaim the blob in the window between the
    // two store-local commits. This is the same coarse lock `delete_blob` holds
    // while reclaiming, and mirrors the manifest path's bytes-then-link order.
    let lock = metadata_store.acquire_blob_data_lock(digest).await?;
    let result = async {
        blob_store
            .complete_upload(namespace, session_id.as_ref(), digest)
            .await?;
        grant_blob_index_reference(metadata_store, namespace, digest).await
    }
    .await;
    lock.release().await;
    result?;

    info!("Caching of {digest} completed");
    Ok(())
}

/// Grant `namespace` a reference to an already-present blob, holding the
/// blob-data lock so the grant is serialized against a concurrent reclaim (the
/// `bytes already present` branch of the cache-fill handler).
pub async fn grant_blob_reference(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<(), Error> {
    let lock = metadata_store.acquire_blob_data_lock(digest).await?;
    let result = grant_blob_index_reference(metadata_store, namespace, digest).await;
    lock.release().await;
    result
}

/// Insert `namespace`'s blob ownership reference into the metadata store's blob
/// index. The caller holds the blob-data lock. Committed on the metadata store's
/// executor with the engine's conflict retry, so it is never routed through the
/// blob store's executor; the insert is idempotent, so a cache-fill retry
/// re-grants harmlessly.
async fn grant_blob_index_reference(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<(), Error> {
    metadata_store
        .update_blob_index(
            namespace,
            digest,
            BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
        )
        .await?;
    Ok(())
}

impl Registry {
    #[instrument(skip(repository))]
    pub async fn head_blob(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HeadBlobResponse, Error> {
        let has_access = BlobOwnership::new(self.metadata_store.as_ref())
            .can_read(namespace, digest)
            .await?;

        if !repository.is_pull_through() && !has_access {
            return Err(Error::BlobUnknown);
        }

        if has_access {
            match self.blob_store.size(digest).await {
                Ok(size) => {
                    return Ok(HeadBlobResponse {
                        headers: head_blob_headers(digest, size),
                    });
                }
                Err(error) if !repository.is_pull_through() => {
                    warn!("Blob with digest {digest} not found: {error}");
                    return Err(Error::BlobUnknown);
                }
                Err(_) => {}
            }
        }

        if repository.is_pull_through() {
            let (digest, size) = repository
                .head_blob(accepted_types, namespace, digest)
                .await?;
            Ok(HeadBlobResponse {
                headers: head_blob_headers(&digest, size),
            })
        } else {
            Err(Error::BlobUnknown)
        }
    }

    async fn try_get_owned_local_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        range: Option<BlobRange>,
    ) -> Result<Option<GetBlobResponse>, Error> {
        if !BlobOwnership::new(self.metadata_store.as_ref())
            .can_read(namespace, digest)
            .await?
        {
            return Ok(None);
        }

        self.get_local_blob(digest, range).await.map(Some)
    }

    #[instrument(skip(repository))]
    pub async fn get_blob(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
        range: Option<BlobRange>,
    ) -> Result<GetBlobResponse, Error> {
        match self
            .try_get_owned_local_blob(namespace, digest, range)
            .await
        {
            Ok(Some(response)) => return Ok(response),
            Ok(None) if !repository.is_pull_through() => return Err(Error::BlobUnknown),
            Ok(None) => {}
            Err(Error::BlobUnknown) if repository.is_pull_through() => {}
            Err(error) => return Err(error),
        }

        if range.is_some() {
            warn!("Range requests are not supported for pull-through repositories");
            return Err(Error::RangeNotSatisfiable);
        }

        let (total_length, client_stream) = repository
            .get_blob(accepted_types, namespace, digest)
            .await?;

        self.dispatch_cache_fill(namespace, digest).await;

        Ok(GetBlobResponse::Reader {
            headers: get_blob_headers(digest, total_length),
            body: client_stream,
        })
    }

    /// Fire-and-forget enqueue of a pull-through cache-fill job. A failure is
    /// logged and counted on `angos_job_queue_enqueue_failures_total` but never
    /// bubbles up, so a scheduling glitch cannot degrade the client response.
    async fn dispatch_cache_fill(&self, namespace: &Namespace, digest: &Digest) {
        let payload = CacheFetchBlobPayload {
            namespace: namespace.clone(),
            digest: digest.to_string(),
        };
        let envelope = match JobEnvelope::new(
            Queue::Cache,
            CACHE_FETCH_BLOB_KIND,
            format!("{}.{namespace}:{digest}", Queue::Cache),
            &payload,
        ) {
            Ok(envelope) => envelope,
            Err(e) => {
                warn!("Failed to build cache job envelope for {digest}: {e}");
                metrics_provider()
                    .job_queue_enqueue_failures_total
                    .with_label_values(&[Queue::Cache.as_str()])
                    .inc();
                return;
            }
        };
        if let Err(e) = self.job_queue.enqueue(envelope).await {
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
        range: Option<BlobRange>,
    ) -> Result<GetBlobResponse, Error> {
        let Some(requested_range) = range else {
            let (reader, total_length) = self.blob_store.reader(digest, None).await?;
            return Ok(GetBlobResponse::Reader {
                headers: get_blob_headers(digest, total_length),
                body: reader,
            });
        };

        let total_length = self.blob_store.size(digest).await?;
        let Some(range) = resolve_blob_range(requested_range, total_length)? else {
            let (reader, _) = self.blob_store.reader(digest, None).await?;
            return Ok(GetBlobResponse::Reader {
                headers: get_blob_headers(digest, total_length),
                body: reader,
            });
        };
        let (reader, _) = self.blob_store.reader(digest, Some(range.start)).await?;
        let reader = Box::new(reader.take(range.length));

        Ok(GetBlobResponse::RangedReader {
            headers: get_blob_range_headers(digest, range),
            body: reader,
        })
    }

    #[instrument]
    pub async fn delete_blob(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        let ownership = BlobOwnership::new(self.metadata_store.as_ref());
        let links = ownership.references(namespace, digest).await?;

        if links.is_empty() {
            return Err(Error::BlobUnknown);
        }

        if has_non_ownership_reference(&links, digest) {
            return Err(Error::BlobReferenced);
        }

        // Hold the coarse `blob-data:{digest}` lock across the revoke + reclaim.
        // The revoke transaction is crash-atomic on its own, but the lock is what
        // serialises it against the upload `grant` path (which records ownership
        // in the shard without a transactional coarse lock), so a concurrent
        // reference grant cannot be missed during the unreferenced check, and the
        // blob-store reclaim cannot race a concurrent push of the same digest.
        let session = self.acquire_blob_data_lock(digest).await?;
        let result = async {
            if self
                .metadata_store
                .revoke_blob_ownership(namespace, digest)
                .await?
            {
                self.blob_store.delete_blob(digest).await?;
            }
            Ok::<_, Error>(())
        }
        .await;
        session.release().await;
        result
    }

    /// Resolves a blob GET request to either a presigned redirect URL or a stream.
    ///
    /// The redirect fast-path is only taken when `enable_blob_redirect` is set, the
    /// range is absent, and the blob is locally available (for pull-through repos).
    #[instrument(skip(self))]
    pub async fn resolve_get_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        mime_types: &[String],
        range: Option<BlobRange>,
    ) -> Result<GetBlobResponse, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        let has_access = BlobOwnership::new(self.metadata_store.as_ref())
            .can_read(namespace, digest)
            .await?;

        if !repository.is_pull_through() && !has_access {
            return Err(Error::BlobUnknown);
        }

        if range.is_none()
            && self.enable_blob_redirect
            && has_access
            && self.blob_store.size(digest).await.is_ok()
            && let Ok(Some(presigned_url)) = self.blob_store.presigned_url(digest, None).await
        {
            return Ok(GetBlobResponse::Redirect {
                headers: get_blob_redirect_headers(presigned_url, digest),
            });
        }

        self.get_blob(repository, mime_types, namespace, digest, range)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc, time::Duration};

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, LOCATION};
    use tempfile::TempDir;
    use tokio::{io::AsyncReadExt, time::sleep};

    use super::*;
    use crate::{
        oci::{Namespace, Tag},
        registry::{
            DOCKER_CONTENT_DIGEST,
            blob_ownership::BlobOwnership,
            metadata_store::{BlobIndexOperation, LinkOperation},
            test_utils::{
                create_test_blob, for_each_backend, metadata_store_over, put_blob_direct,
            },
        },
    };

    /// `delete_blob` must hold the `blob-data:{digest}` coarse lock across its
    /// revoke-and-reclaim transaction, so a concurrent reference grant cannot be
    /// missed while the unreferenced check and the blob-data delete are decided.
    /// Regression guard for the conformance `MANIFEST_BLOB_UNKNOWN` failure
    /// caused by dropping that lock during the transactional-engine migration.
    #[tokio::test]
    async fn delete_blob_waits_for_blob_data_lock_before_reclaiming_data() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let first = &Namespace::new("test-repo/first").unwrap();
            let second = &Namespace::new("test-repo/second").unwrap();
            let content = b"shared blob content";
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            let ownership = BlobOwnership::new(registry.metadata_store.as_ref());

            ownership.grant(first, &digest).await.unwrap();

            // Hold the blob-data lock, then start a delete: it must block on
            // the lock rather than reclaim the bytes.
            let session = registry.acquire_blob_data_lock(&digest).await.unwrap();
            let delete = registry.delete_blob(first, &digest);
            tokio::pin!(delete);

            tokio::select! {
                result = &mut delete => {
                    panic!("delete completed while blob-data lock was held: {result:?}");
                }
                () = sleep(Duration::from_millis(25)) => {}
            }

            // A second namespace grabs a reference while the delete is parked.
            ownership.grant(second, &digest).await.unwrap();
            session.release().await;
            delete.await.unwrap();

            // The data survives (still referenced by `second`); `first` lost its
            // reference, `second` keeps it.
            assert_eq!(registry.blob_store.read(&digest).await.unwrap(), content);
            assert!(!ownership.can_read(first, &digest).await.unwrap());
            assert!(ownership.can_read(second, &digest).await.unwrap());
        })
        .await;
    }

    #[tokio::test]
    async fn test_head_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, repository) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .head_blob(&repository, &[], namespace, &digest)
                .await
                .unwrap();

            assert_eq!(response.headers[DOCKER_CONTENT_DIGEST], digest.to_string());
            assert_eq!(
                response.headers[CONTENT_LENGTH.as_str()],
                content.len().to_string()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, repository) = create_test_blob(registry, namespace, content).await;
            let response = registry
                .get_blob(&repository, &[], namespace, &digest, None)
                .await
                .unwrap();

            match response {
                GetBlobResponse::Reader { headers, mut body } => {
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], content.len().to_string());
                    assert_eq!(headers[ACCEPT_RANGES.as_str()], "bytes");
                    assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, content);
                }
                GetBlobResponse::RangedReader { .. } => panic!("Expected Reader response"),
                GetBlobResponse::Redirect { .. } => panic!("unexpected redirect from get_blob"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn get_blob_rejects_local_blob_without_namespace_ownership() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"unowned blob content";
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            let repository = registry.get_repository_for_namespace(namespace).unwrap();

            let head_result = registry
                .head_blob(repository, &[], namespace, &digest)
                .await;
            assert!(matches!(head_result, Err(Error::BlobUnknown)));

            let get_result = registry
                .get_blob(repository, &[], namespace, &digest, None)
                .await;
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
            let range = Some(BlobRange::FromTo {
                start: 5,
                end: Some(10),
            });
            let response = registry
                .get_blob(&repository, &[], namespace, &digest, range)
                .await
                .unwrap();

            match response {
                GetBlobResponse::RangedReader { headers, mut body } => {
                    assert_eq!(
                        headers[CONTENT_RANGE.as_str()],
                        format!("bytes 5-10/{}", content.len())
                    );
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], "6");
                    assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());

                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, &content[5..=10]);
                }
                GetBlobResponse::Reader { .. } => panic!("Expected RangedReader response"),
                GetBlobResponse::Redirect { .. } => panic!("unexpected redirect from get_blob"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_delete_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            BlobOwnership::new(registry.metadata_store.as_ref())
                .grant(namespace, &digest)
                .await
                .unwrap();

            let blob_index = registry
                .metadata_store
                .read_blob_index(&digest)
                .await
                .unwrap();
            assert!(blob_index.namespace.contains_key(namespace));
            let namespace_links = blob_index.namespace.get(namespace).unwrap();
            assert!(namespace_links.contains(&LinkKind::Blob(digest.clone())));

            registry.delete_blob(namespace, &digest).await.unwrap();

            assert!(registry.blob_store.read(&digest).await.is_err());
            assert!(
                registry
                    .metadata_store
                    .read_blob_index(&digest)
                    .await
                    .is_err()
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
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            let link = LinkKind::Config(digest.clone());

            registry
                .metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create_with_referrer(
                        link.clone(),
                        digest.clone(),
                        Digest::sha256_of_bytes(b"manifest"),
                    )],
                )
                .await
                .unwrap();

            let result = registry.delete_blob(namespace, &digest).await;
            assert!(matches!(result, Err(Error::BlobReferenced)));

            let stored_content = registry.blob_store.read(&digest).await.unwrap();
            assert_eq!(stored_content, content);
            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &link)
                    .await
                    .is_ok()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn delete_blob_rejects_all_metadata_references() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let parent = Digest::sha256_of_bytes(b"index manifest");
            let subject = Digest::sha256_of_bytes(b"subject manifest");

            let cases = [
                LinkKind::Digest(Digest::sha256_of_bytes(b"digest reference")),
                LinkKind::Tag(Tag::new("latest").unwrap()),
                LinkKind::Layer(Digest::sha256_of_bytes(b"layer reference")),
                LinkKind::Config(Digest::sha256_of_bytes(b"config reference")),
                LinkKind::Manifest(parent.clone(), Digest::sha256_of_bytes(b"child manifest")),
                LinkKind::Referrer(subject, Digest::sha256_of_bytes(b"referrer manifest")),
            ];

            for link in cases {
                let content = format!("content for {link}").into_bytes();
                let digest = put_blob_direct(registry.metadata_store.store(), &content).await;
                BlobOwnership::new(registry.metadata_store.as_ref())
                    .grant(namespace, &digest)
                    .await
                    .unwrap();

                let retargeted = retarget_link(&link, &digest);
                let op = if let LinkKind::Manifest(parent, _) = &link {
                    LinkOperation::create_with_referrer(retargeted, digest.clone(), parent.clone())
                } else {
                    LinkOperation::create(retargeted, digest.clone())
                };
                registry
                    .metadata_store
                    .update_links(namespace, &[op])
                    .await
                    .unwrap();

                let result = registry.delete_blob(namespace, &digest).await;
                assert!(matches!(result, Err(Error::BlobReferenced)));
                assert_eq!(registry.blob_store.read(&digest).await.unwrap(), content);
            }
        })
        .await;
    }

    fn retarget_link(link: &LinkKind, digest: &Digest) -> LinkKind {
        match link {
            LinkKind::Digest(_) => LinkKind::Digest(digest.clone()),
            LinkKind::Layer(_) => LinkKind::Layer(digest.clone()),
            LinkKind::Config(_) => LinkKind::Config(digest.clone()),
            LinkKind::Manifest(parent, _) => LinkKind::Manifest(parent.clone(), digest.clone()),
            LinkKind::Referrer(subject, _) => LinkKind::Referrer(subject.clone(), digest.clone()),
            LinkKind::Blob(_) | LinkKind::Tag(_) => link.clone(),
        }
    }

    #[tokio::test]
    async fn delete_blob_keeps_data_owned_by_other_namespace() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let first = &Namespace::new("test-repo/first").unwrap();
            let second = &Namespace::new("test-repo/second").unwrap();
            let content = b"shared blob content";
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;
            let ownership = BlobOwnership::new(registry.metadata_store.as_ref());

            ownership.grant(first, &digest).await.unwrap();
            ownership.grant(second, &digest).await.unwrap();

            registry.delete_blob(first, &digest).await.unwrap();

            assert_eq!(registry.blob_store.read(&digest).await.unwrap(), content);
            assert!(!ownership.can_read(first, &digest).await.unwrap());
            assert!(ownership.can_read(second, &digest).await.unwrap());

            registry.delete_blob(second, &digest).await.unwrap();

            assert!(registry.blob_store.read(&digest).await.is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn delete_blob_rejects_unowned_blob() {
        for_each_backend(async |test_case| {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"unowned delete content";
            let digest = put_blob_direct(registry.metadata_store.store(), content).await;

            let result = registry.delete_blob(namespace, &digest).await;
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
            let namespace_links = blob_index.namespace.get(&namespace).unwrap();
            assert!(namespace_links.contains(&LinkKind::Blob(digest.clone())));

            let repository = registry.get_repository_for_namespace(&namespace).unwrap();
            let response = registry
                .get_blob(repository, &[], &namespace, &digest, None)
                .await
                .unwrap();

            match response {
                GetBlobResponse::Reader { mut body, .. } => {
                    let mut stored_content = Vec::new();
                    body.read_to_end(&mut stored_content).await.unwrap();
                    assert_eq!(stored_content, content);
                }
                GetBlobResponse::RangedReader { .. } => panic!("Expected Reader response"),
                GetBlobResponse::Redirect { .. } => panic!("unexpected redirect from get_blob"),
            }
        })
        .await;
    }

    /// Regression guard for the split-backend pull-through cache-fill failure:
    /// with the blob store and metadata store on separate backends, `cache_blob`
    /// must store the bytes in the blob store and grant the reference in the
    /// metadata store as independent idempotent work. The previous design folded
    /// both into one transaction and conflicted on every layer because the
    /// blob-index read was verified against the wrong backend.
    #[tokio::test]
    async fn cache_blob_grants_reference_with_split_blob_and_metadata_backends() {
        crate::metrics_provider::init_for_tests();
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

        // A prior manifest pull recorded the layer's ownership link in the
        // metadata store, which is what made the old design conflict.
        metadata_store
            .update_blob_index(
                &namespace,
                &digest,
                BlobIndexOperation::Insert(LinkKind::Layer(digest.clone())),
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
        let links = blob_index.namespace.get(&namespace).unwrap();
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

            let response = registry.get_local_blob(&digest, None).await.unwrap();
            match response {
                GetBlobResponse::Reader { headers, mut body } => {
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], content.len().to_string());
                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, content);
                }
                GetBlobResponse::RangedReader { .. } => {
                    panic!("Expected Reader response for full read")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }

            let range = Some(BlobRange::FromTo {
                start: 5,
                end: Some(15),
            });
            let response = registry.get_local_blob(&digest, range).await.unwrap();
            match response {
                GetBlobResponse::RangedReader { headers, mut body } => {
                    assert_eq!(
                        headers[CONTENT_RANGE.as_str()],
                        format!("bytes 5-15/{}", content.len())
                    );
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], "11");
                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, &content[5..=15]);
                }
                GetBlobResponse::Reader { .. } => {
                    panic!("Expected RangedReader response for ranged read")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }
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
                    Some(BlobRange::FromTo {
                        start: 0,
                        end: None,
                    }),
                )
                .await
                .unwrap();

            match response {
                GetBlobResponse::RangedReader { headers, mut body } => {
                    assert_eq!(
                        headers[CONTENT_RANGE.as_str()],
                        format!("bytes 0-{}/{}", content.len() - 1, content.len())
                    );
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], content.len().to_string());

                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, content);
                }
                GetBlobResponse::Reader { .. } => {
                    panic!("Expected RangedReader response for explicit range")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }
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
                .get_local_blob(&digest, Some(BlobRange::Suffix(suffix_length as u64)))
                .await
                .unwrap();

            match response {
                GetBlobResponse::RangedReader { headers, mut body } => {
                    assert_eq!(
                        headers[CONTENT_RANGE.as_str()],
                        format!("bytes {}-{}/{}", start, content.len() - 1, content.len())
                    );
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], suffix_length.to_string());

                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, &content[start..]);
                }
                GetBlobResponse::Reader { .. } => {
                    panic!("Expected RangedReader response for suffix range")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }
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
                .get_local_blob(&digest, Some(BlobRange::Suffix(10_000)))
                .await
                .unwrap();

            match response {
                GetBlobResponse::RangedReader { headers, mut body } => {
                    assert_eq!(
                        headers[CONTENT_RANGE.as_str()],
                        format!("bytes 0-{}/{}", content.len() - 1, content.len())
                    );
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], content.len().to_string());

                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, content);
                }
                GetBlobResponse::Reader { .. } => {
                    panic!("Expected RangedReader response for suffix range")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }
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
                    Some(BlobRange::FromTo {
                        start: 8,
                        end: Some(10_000),
                    }),
                )
                .await
                .unwrap();

            match response {
                GetBlobResponse::RangedReader { headers, mut body } => {
                    assert_eq!(
                        headers[CONTENT_RANGE.as_str()],
                        format!("bytes 8-{}/{}", content.len() - 1, content.len())
                    );
                    assert_eq!(
                        headers[CONTENT_LENGTH.as_str()],
                        (content.len() - 8).to_string()
                    );

                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, &content[8..]);
                }
                GetBlobResponse::Reader { .. } => {
                    panic!("Expected RangedReader response for explicit range")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }
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
                    Some(BlobRange::FromTo {
                        start: content.len() as u64,
                        end: None,
                    }),
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
                    Some(BlobRange::FromTo {
                        start: 0,
                        end: None,
                    }),
                )
                .await
                .unwrap();

            match response {
                GetBlobResponse::Reader { headers, mut body } => {
                    assert_eq!(headers[CONTENT_LENGTH.as_str()], "0");
                    let mut buf = Vec::new();
                    body.read_to_end(&mut buf).await.unwrap();
                    assert!(buf.is_empty());
                }
                GetBlobResponse::RangedReader { .. } => {
                    panic!("Expected Reader response for empty blob range")
                }
                GetBlobResponse::Redirect { .. } => {
                    panic!("unexpected redirect from get_local_blob")
                }
            }
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
                .head_blob(&repository, &[], namespace, &digest)
                .await
                .unwrap();
            assert_eq!(
                head_response.headers[DOCKER_CONTENT_DIGEST],
                digest.to_string()
            );
            assert_eq!(
                head_response.headers[CONTENT_LENGTH.as_str()],
                content.len().to_string()
            );

            let get_response = registry
                .get_blob(&repository, &[], namespace, &digest, None)
                .await
                .unwrap();
            match get_response {
                GetBlobResponse::Reader { headers, .. } => {
                    assert_eq!(
                        headers[CONTENT_LENGTH.as_str()],
                        head_response.headers[CONTENT_LENGTH.as_str()]
                    );
                }
                GetBlobResponse::RangedReader { .. } => panic!("Expected Reader response"),
                GetBlobResponse::Redirect { .. } => panic!("unexpected redirect from get_blob"),
            }
        })
        .await;
    }

    #[test]
    fn test_head_blob_headers_contains_required_fields() {
        let digest: Digest =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap();
        let headers = head_blob_headers(&digest, 42);
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "42");
    }

    #[test]
    fn test_get_blob_headers_includes_accept_ranges() {
        let digest: Digest =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap();
        let headers = get_blob_headers(&digest, 1024);
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
        assert_eq!(headers[ACCEPT_RANGES.as_str()], "bytes");
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "1024");
    }

    #[test]
    fn test_get_blob_range_headers_computes_content_length() {
        let digest: Digest =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap();
        let range = ResolvedRange {
            start: 5,
            end: 10,
            length: 6,
            total_length: 100,
        };
        let headers = get_blob_range_headers(&digest, range);
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "6");
        assert_eq!(headers[CONTENT_RANGE.as_str()], "bytes 5-10/100");
    }

    #[test]
    fn resolve_blob_range_ignores_ranges_for_empty_blob() {
        assert!(matches!(
            resolve_blob_range(
                BlobRange::FromTo {
                    start: 0,
                    end: None,
                },
                0
            ),
            Ok(None)
        ));
        assert!(matches!(
            resolve_blob_range(BlobRange::Suffix(1), 0),
            Ok(None)
        ));
    }

    #[test]
    fn resolve_blob_range_rejects_start_at_or_after_length() {
        assert!(matches!(
            resolve_blob_range(
                BlobRange::FromTo {
                    start: 10,
                    end: None,
                },
                10
            ),
            Err(Error::RangeNotSatisfiable)
        ));
        assert!(matches!(
            resolve_blob_range(
                BlobRange::FromTo {
                    start: 11,
                    end: None,
                },
                10
            ),
            Err(Error::RangeNotSatisfiable)
        ));
    }

    #[test]
    fn resolve_blob_range_clamps_end_to_last_byte() {
        let range = resolve_blob_range(
            BlobRange::FromTo {
                start: 4,
                end: Some(99),
            },
            10,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            range,
            ResolvedRange {
                start: 4,
                end: 9,
                length: 6,
                total_length: 10,
            }
        );
    }

    #[test]
    fn resolve_blob_range_expands_open_ended_range() {
        let range = resolve_blob_range(
            BlobRange::FromTo {
                start: 4,
                end: None,
            },
            10,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            range,
            ResolvedRange {
                start: 4,
                end: 9,
                length: 6,
                total_length: 10,
            }
        );
    }

    #[test]
    fn resolve_blob_range_resolves_suffix_range() {
        let range = resolve_blob_range(BlobRange::Suffix(4), 10)
            .unwrap()
            .unwrap();

        assert_eq!(
            range,
            ResolvedRange {
                start: 6,
                end: 9,
                length: 4,
                total_length: 10,
            }
        );
    }

    #[test]
    fn resolve_blob_range_clamps_suffix_range_to_blob_length() {
        let range = resolve_blob_range(BlobRange::Suffix(99), 10)
            .unwrap()
            .unwrap();

        assert_eq!(
            range,
            ResolvedRange {
                start: 0,
                end: 9,
                length: 10,
                total_length: 10,
            }
        );
    }

    #[test]
    fn resolve_blob_range_rejects_zero_suffix_range() {
        assert!(matches!(
            resolve_blob_range(BlobRange::Suffix(0), 10),
            Err(Error::RangeNotSatisfiable)
        ));
    }

    #[test]
    fn resolve_blob_range_rejects_end_before_start() {
        assert!(matches!(
            resolve_blob_range(
                BlobRange::FromTo {
                    start: 5,
                    end: Some(4),
                },
                10
            ),
            Err(Error::RangeNotSatisfiable)
        ));
    }

    #[test]
    fn test_get_blob_redirect_headers_carries_location_and_digest() {
        let digest: Digest =
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap();
        let headers = get_blob_redirect_headers("https://cdn/blob".to_string(), &digest);
        assert_eq!(headers[LOCATION.as_str()], "https://cdn/blob");
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
    }
}
