use std::{collections::HashMap, sync::Arc};

use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, LOCATION};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    oci::{Digest, Namespace},
    registry::{
        DOCKER_CONTENT_DIGEST, Error, Registry, Repository,
        blob_store::{BlobStore, BoxedReader},
        metadata_store::{self, BlobIndexOperation, MetadataStoreExt, link_kind::LinkKind},
        task_queue,
    },
};

pub enum GetBlobResponse {
    Redirect {
        headers: HashMap<&'static str, String>,
    },
    Reader {
        headers: HashMap<&'static str, String>,
        body: BoxedReader,
    },
    RangedReader {
        headers: HashMap<&'static str, String>,
        body: BoxedReader,
    },
}

pub struct HeadBlobResponse {
    pub headers: HashMap<&'static str, String>,
}

fn head_blob_headers(digest: &Digest, size: u64) -> HashMap<&'static str, String> {
    HashMap::from([
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
        (CONTENT_LENGTH.as_str(), size.to_string()),
    ])
}

fn get_blob_headers(digest: &Digest, total_length: u64) -> HashMap<&'static str, String> {
    HashMap::from([
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
        (ACCEPT_RANGES.as_str(), "bytes".to_string()),
        (CONTENT_LENGTH.as_str(), total_length.to_string()),
    ])
}

fn get_blob_range_headers(
    digest: &Digest,
    start: u64,
    end: u64,
    total_length: u64,
) -> HashMap<&'static str, String> {
    let length = end - start + 1;
    HashMap::from([
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
        (ACCEPT_RANGES.as_str(), "bytes".to_string()),
        (CONTENT_LENGTH.as_str(), length.to_string()),
        (
            CONTENT_RANGE.as_str(),
            format!("bytes {start}-{end}/{total_length}"),
        ),
    ])
}

fn get_blob_redirect_headers(url: String, digest: &Digest) -> HashMap<&'static str, String> {
    HashMap::from([
        (LOCATION.as_str(), url),
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
    ])
}

impl Registry {
    async fn check_blob_namespace_access(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(), Error> {
        match self.metadata_store.read_blob_index(digest).await {
            Ok(blob_index) => {
                if blob_index.namespace.contains_key(namespace.as_ref()) {
                    Ok(())
                } else {
                    Err(Error::BlobUnknown)
                }
            }
            Err(metadata_store::Error::ReferenceNotFound) => Ok(()),
            Err(e) => {
                warn!("Failed to read blob index for {digest}: {e}");
                Err(Error::Internal(format!("storage error: {e}")))
            }
        }
    }

    #[instrument(skip(repository))]
    pub async fn head_blob(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<HeadBlobResponse, Error> {
        if !repository.is_pull_through() {
            self.check_blob_namespace_access(namespace, digest).await?;
        }

        let blob = self.blob_store.get_blob_size(digest).await;

        match blob {
            Ok(size) => Ok(HeadBlobResponse {
                headers: head_blob_headers(digest, size),
            }),
            Err(_) if repository.is_pull_through() => {
                let (digest, size) = repository
                    .head_blob(accepted_types, namespace, digest)
                    .await?;
                Ok(HeadBlobResponse {
                    headers: head_blob_headers(&digest, size),
                })
            }
            Err(e) => {
                warn!("Blob with digest {digest} not found: {e}");
                Err(Error::BlobUnknown)
            }
        }
    }

    #[instrument(skip(storage_engine, stream))]
    async fn copy_blob(
        storage_engine: Arc<dyn BlobStore + Send + Sync>,
        stream: impl AsyncRead + Send + Sync + Unpin + 'static,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(), Error> {
        let session_id = Uuid::new_v4().to_string();

        storage_engine.create_upload(namespace, &session_id).await?;

        storage_engine
            .write_upload(namespace, &session_id, Box::new(stream), 0, false, None)
            .await?;

        if let Err(error) = storage_engine
            .complete_upload(namespace, &session_id, Some(digest))
            .await
        {
            debug!("Failed to complete upload: {error}");
            return Err(error.into());
        }

        Ok::<(), Error>(())
    }

    async fn cache_blob(
        store: Arc<dyn BlobStore + Send + Sync>,
        stream: BoxedReader,
        namespace: Namespace,
        digest: Digest,
    ) -> Result<(), task_queue::Error> {
        debug!("Fetching blob: {digest}");
        Self::copy_blob(store, stream, &namespace, &digest)
            .await
            .map_err(|e| task_queue::Error::TaskExecution(e.to_string()))?;
        info!("Caching of {digest} completed");
        Ok(())
    }

    #[instrument(skip(repository))]
    pub async fn get_blob(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
        range: Option<(u64, Option<u64>)>,
    ) -> Result<GetBlobResponse, Error> {
        if !repository.is_pull_through() {
            self.check_blob_namespace_access(namespace, digest).await?;
        }

        let local_blob = self.get_local_blob(digest, range).await;

        if let Ok(response) = local_blob {
            return Ok(response);
        } else if !repository.is_pull_through() {
            warn!("Blob not found locally: {digest}");
            return Err(Error::BlobUnknown);
        }

        if range.is_some() {
            warn!("Range requests are not supported for pull-through repositories");
            return Err(Error::RangeNotSatisfiable);
        }

        let (total_length, client_stream) = repository
            .get_blob(accepted_types, namespace, digest)
            .await?;

        let (_, caching_stream) = repository
            .get_blob(accepted_types, namespace, digest)
            .await?;

        let task_key = format!("{namespace}/{digest}");

        self.task_queue.submit(
            &task_key,
            Self::cache_blob(
                self.blob_store.clone(),
                caching_stream,
                namespace.clone(),
                digest.clone(),
            ),
        );
        info!("Scheduled blob copy task '{task_key}'");

        Ok(GetBlobResponse::Reader {
            headers: get_blob_headers(digest, total_length),
            body: client_stream,
        })
    }

    async fn get_local_blob(
        &self,
        digest: &Digest,
        range: Option<(u64, Option<u64>)>,
    ) -> Result<GetBlobResponse, Error> {
        let start = range.map(|(start, _)| start);

        let (reader, total_length) = self.blob_store.build_blob_reader(digest, start).await?;

        if let Some((start, _)) = range
            && start > total_length
        {
            warn!("Range start does not match content length");
            return Err(Error::RangeNotSatisfiable);
        }

        match range {
            Some((0, None)) | None => Ok(GetBlobResponse::Reader {
                headers: get_blob_headers(digest, total_length),
                body: reader,
            }),
            Some((start, end)) => {
                let end = end.unwrap_or(total_length - 1);
                let reader = Box::new(reader.take(end - start + 1));

                Ok(GetBlobResponse::RangedReader {
                    headers: get_blob_range_headers(digest, start, end, total_length),
                    body: reader,
                })
            }
        }
    }

    #[instrument]
    pub async fn delete_blob(&self, namespace: &Namespace, digest: &Digest) -> Result<(), Error> {
        let mut tx = self.metadata_store.begin_transaction(namespace);
        tx.delete_link(&LinkKind::Layer(digest.clone()));
        tx.delete_link(&LinkKind::Config(digest.clone()));

        if let Err(error) = tx.commit().await {
            warn!("Failed to delete blob links: {error}");
        }

        if let Ok(blob_index) = self.metadata_store.read_blob_index(digest).await
            && let Some(links) = blob_index.namespace.get(namespace.as_ref())
        {
            for link in links {
                if let Err(error) = self
                    .metadata_store
                    .update_blob_index(
                        namespace.as_ref(),
                        digest,
                        BlobIndexOperation::Remove(link.clone()),
                    )
                    .await
                {
                    warn!("Failed to remove blob index entry: {error}");
                }
            }
        }

        let should_delete = match self.metadata_store.read_blob_index(digest).await {
            Ok(blob_index) => blob_index.namespace.is_empty(),
            Err(_) => true,
        };

        if should_delete && let Err(error) = self.blob_store.delete_blob(digest).await {
            warn!("Failed to delete blob data: {error}");
        }

        Ok(())
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
        range: Option<(u64, Option<u64>)>,
    ) -> Result<GetBlobResponse, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        if !repository.is_pull_through() {
            self.check_blob_namespace_access(namespace, digest).await?;
        }

        // For pull-through repositories the blob index is populated when the
        // manifest is cached, but the layer/config blobs themselves are only
        // fetched on their first GET. Verify the blob is actually present in
        // the local store before issuing a redirect; otherwise the client
        // would follow the 307 to a missing object and see a 404.
        if range.is_none()
            && self.enable_blob_redirect
            && (!repository.is_pull_through()
                || self.blob_store.get_blob_size(digest).await.is_ok())
            && let Ok(Some(presigned_url)) = self.blob_store.get_blob_url(digest, None).await
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
    use std::io::Cursor;

    use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, LOCATION};
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{
        oci::Namespace,
        registry::{DOCKER_CONTENT_DIGEST, test_utils::create_test_blob, tests::backends},
    };

    #[tokio::test]
    async fn test_head_blob() {
        for test_case in backends() {
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
        }
    }

    #[tokio::test]
    async fn test_get_blob() {
        for test_case in backends() {
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
        }
    }

    #[tokio::test]
    async fn test_get_blob_with_range() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, repository) = create_test_blob(registry, namespace, content).await;
            let range = Some((5, Some(10)));
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
        }
    }

    #[tokio::test]
    async fn test_delete_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let layer_link = LinkKind::Layer(digest.clone());
            let config_link = LinkKind::Config(digest.clone());

            let mut tx = registry.metadata_store.begin_transaction(namespace);
            tx.create_link(&layer_link, &digest).add();
            tx.create_link(&config_link, &digest).add();
            tx.commit().await.unwrap();

            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &layer_link, false)
                    .await
                    .is_ok()
            );
            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &config_link, false)
                    .await
                    .is_ok()
            );

            let blob_index = registry
                .metadata_store
                .read_blob_index(&digest)
                .await
                .unwrap();
            assert!(blob_index.namespace.contains_key(namespace.as_ref()));
            let namespace_links = blob_index.namespace.get(namespace.as_ref()).unwrap();
            assert!(namespace_links.contains(&layer_link));
            assert!(namespace_links.contains(&config_link));

            registry.delete_blob(namespace, &digest).await.unwrap();

            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &layer_link, false)
                    .await
                    .is_err()
            );
            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &config_link, false)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn test_copy_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let stream = Cursor::new(content.to_vec());
            let storage_engine = registry.blob_store.clone();

            Registry::copy_blob(storage_engine, stream, namespace, &digest)
                .await
                .unwrap();

            let stored_content = registry.blob_store.read_blob(&digest).await.unwrap();
            assert_eq!(stored_content, content);
        }
    }

    #[tokio::test]
    async fn test_get_local_blob_returns_correct_size() {
        for test_case in backends() {
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

            let range = Some((5, Some(15)));
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
        }
    }

    #[tokio::test]
    async fn test_head_blob_independent_of_get() {
        for test_case in backends() {
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
        }
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
        let headers = get_blob_range_headers(&digest, 5, 10, 100);
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "6");
        assert_eq!(headers[CONTENT_RANGE.as_str()], "bytes 5-10/100");
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
