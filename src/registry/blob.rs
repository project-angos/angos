use std::sync::Arc;

use hyper::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE};
use hyper::{Response, StatusCode};
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::command::server::response_body::ResponseBody;
use crate::oci::{Digest, Namespace};
use crate::registry::blob_store::{BlobStore, BoxedReader};
use crate::registry::metadata_store::MetadataStoreExt;
use crate::registry::metadata_store::link_kind::LinkKind;
use crate::registry::{Error, Registry, Repository, blob_store, task_queue};

pub const DOCKER_CONTENT_DIGEST: &str = "Docker-Content-Digest";

pub enum GetBlobResponse<R>
where
    R: AsyncRead + Send + Unpin,
{
    Empty,
    Reader(R, u64),
    RangedReader(R, (u64, u64), u64),
}

pub struct HeadBlobResponse {
    pub digest: Digest,
    pub size: u64,
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
        let blob = self.blob_store.get_blob_size(digest).await;

        match blob {
            Ok(size) => Ok(HeadBlobResponse {
                digest: digest.clone(),
                size,
            }),
            Err(_) if repository.is_pull_through() => {
                let (digest, size) = repository
                    .head_blob(accepted_types, namespace, digest)
                    .await?;
                Ok(HeadBlobResponse { digest, size })
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
            .write_upload(namespace, &session_id, Box::new(stream), false)
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

    #[instrument(skip(repository))]
    pub async fn get_blob(
        &self,
        repository: &Repository,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
        range: Option<(u64, Option<u64>)>,
    ) -> Result<GetBlobResponse<BoxedReader>, Error> {
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

        // Proxying stream
        let (total_length, client_stream) = repository
            .get_blob(accepted_types, namespace, digest)
            .await?;

        let (_, caching_stream) = repository
            .get_blob(accepted_types, namespace, digest)
            .await?;
        let cache_namespace = namespace.clone();

        let task_key = format!("{namespace}/{digest}");
        let cache_digest = digest.clone();
        let store = self.blob_store.clone();

        self.task_queue.submit(&task_key, async move {
            let digest_string = cache_digest.to_string();

            debug!("Fetching blob: {digest_string}");
            Self::copy_blob(store, caching_stream, &cache_namespace, &cache_digest)
                .await
                .map_err(|e| task_queue::Error::TaskExecution(e.to_string()))?;

            info!("Caching of {digest_string} completed");
            Ok(())
        });
        info!("Scheduled blob copy task '{task_key}'");
        //

        Ok(GetBlobResponse::Reader(client_stream, total_length))
    }

    async fn get_local_blob(
        &self,
        digest: &Digest,
        range: Option<(u64, Option<u64>)>,
    ) -> Result<GetBlobResponse<BoxedReader>, Error> {
        let start = range.map(|(start, _)| start);

        let (reader, total_length) = match self.blob_store.build_blob_reader(digest, start).await {
            Ok(result) => result,
            Err(blob_store::Error::BlobNotFound) => return Ok(GetBlobResponse::Empty),
            Err(err) => Err(err)?,
        };

        if let Some((start, _)) = range
            && start > total_length
        {
            warn!("Range start does not match content length");
            return Err(Error::RangeNotSatisfiable);
        }

        match range {
            Some((0, None)) | None => Ok(GetBlobResponse::Reader(reader, total_length)),
            Some((start, end)) => {
                let end = end.unwrap_or(total_length - 1);
                let reader = Box::new(reader.take(end - start + 1));

                Ok(GetBlobResponse::RangedReader(
                    reader,
                    (start, end),
                    total_length,
                ))
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

        Ok(())
    }

    // API Handlers

    #[instrument(skip(self))]
    pub async fn handle_head_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        mime_types: &[String],
    ) -> Result<Response<ResponseBody>, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        let blob = self
            .head_blob(repository, mime_types, namespace, digest)
            .await?;

        let res = Response::builder()
            .status(StatusCode::OK)
            .header(DOCKER_CONTENT_DIGEST, blob.digest.to_string())
            .header(CONTENT_LENGTH, blob.size.to_string())
            .body(ResponseBody::empty())?;

        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn handle_delete_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<Response<ResponseBody>, Error> {
        self.delete_blob(namespace, digest).await?;

        let res = Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(ResponseBody::empty())?;

        Ok(res)
    }

    #[instrument(skip(self))]
    pub async fn handle_get_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        mime_types: &[String],
        range: Option<(u64, Option<u64>)>,
    ) -> Result<Response<ResponseBody>, Error> {
        let repository = self.get_repository_for_namespace(namespace)?;

        if range.is_none()
            && self.enable_redirect
            && let Ok(Some(presigned_url)) = self.blob_store.get_blob_url(digest).await
        {
            return Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(hyper::header::LOCATION, presigned_url)
                .header(DOCKER_CONTENT_DIGEST, digest.to_string())
                .body(ResponseBody::empty())
                .map_err(Into::into);
        }

        let res = match self
            .get_blob(repository, mime_types, namespace, digest, range)
            .await?
        {
            GetBlobResponse::RangedReader(reader, (start, end), total_length) => {
                let length = end - start + 1;
                let stream = reader.take(length);
                let range = format!("bytes {start}-{end}/{total_length}");

                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(DOCKER_CONTENT_DIGEST, digest.to_string())
                    .header(ACCEPT_RANGES, "bytes")
                    .header(CONTENT_LENGTH, length.to_string())
                    .header(CONTENT_RANGE, range)
                    .body(ResponseBody::streaming(stream))?
            }
            GetBlobResponse::Reader(stream, total_length) => Response::builder()
                .status(StatusCode::OK)
                .header(DOCKER_CONTENT_DIGEST, digest.to_string())
                .header(ACCEPT_RANGES, "bytes")
                .header(CONTENT_LENGTH, total_length)
                .body(ResponseBody::streaming(stream))?,
            GetBlobResponse::Empty => Response::builder()
                .status(StatusCode::OK)
                .header(ACCEPT_RANGES, "bytes")
                .header(CONTENT_LENGTH, 0)
                .body(ResponseBody::empty())?,
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures_util::TryStreamExt;
    use http_body_util::BodyExt;
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;

    use super::*;
    use crate::command::server::request_ext::HeaderExt;
    use crate::oci::Namespace;
    use crate::registry::test_utils::create_test_blob;
    use crate::registry::tests::backends;

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

            assert_eq!(response.digest, digest);
            assert_eq!(response.size, content.len() as u64);
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
                GetBlobResponse::Reader(mut reader, size) => {
                    assert_eq!(size, content.len() as u64);
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, content);
                }
                _ => panic!("Expected Reader response"),
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
                GetBlobResponse::RangedReader(mut reader, (start, end), total_size) => {
                    assert_eq!(start, 5);
                    assert_eq!(end, 10);
                    assert_eq!(total_size, content.len() as u64);

                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, &content[5..=10]);
                }
                _ => panic!("Expected RangedReader response"),
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
            tx.create_link(&layer_link, &digest);
            tx.create_link(&config_link, &digest);
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
    async fn test_handle_head_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";
            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let accepted_content_types = Vec::new();

            let response = registry
                .handle_head_blob(namespace, &digest, &accepted_content_types)
                .await
                .unwrap();

            let (parts, _) = response.into_parts();

            assert_eq!(parts.status, StatusCode::OK);
            assert_eq!(
                parts.get_header(DOCKER_CONTENT_DIGEST),
                Some(digest.to_string())
            );
            assert_eq!(
                parts.get_header(CONTENT_LENGTH),
                Some(content.len().to_string())
            );
        }
    }

    #[tokio::test]
    async fn test_handle_delete_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";
            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let layer_link = LinkKind::Layer(digest.clone());
            let config_link = LinkKind::Config(digest.clone());
            let latest_link = LinkKind::Tag("latest".to_string());

            let mut tx = registry.metadata_store.begin_transaction(namespace);
            tx.create_link(&layer_link, &digest);
            tx.create_link(&config_link, &digest);
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
            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &latest_link, false)
                    .await
                    .is_ok()
            );

            assert!(registry.blob_store.read_blob(&digest).await.is_ok());

            let response = registry
                .handle_delete_blob(namespace, &digest)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::ACCEPTED);

            let mut tx = registry.metadata_store.begin_transaction(namespace);
            tx.delete_link(&latest_link);
            tx.commit().await.unwrap();

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
            assert!(
                registry
                    .metadata_store
                    .read_link(namespace, &latest_link, false)
                    .await
                    .is_err()
            );

            let blob_index = registry.metadata_store.read_blob_index(&digest).await;
            assert!(blob_index.is_err());

            assert!(registry.blob_store.read_blob(&digest).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_handle_get_blob() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";
            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let accepted_content_types = Vec::new();

            let response = registry
                .handle_get_blob(namespace, &digest, &accepted_content_types, None)
                .await
                .unwrap();
            let status = response.status();
            let (parts, body) = response.into_parts();

            assert_eq!(
                parts.get_header(DOCKER_CONTENT_DIGEST),
                Some(digest.to_string())
            );

            if status == StatusCode::TEMPORARY_REDIRECT {
                assert!(parts.headers.get(hyper::header::LOCATION).is_some());
            } else {
                assert_eq!(parts.status, StatusCode::OK);
                assert_eq!(
                    parts.get_header(CONTENT_LENGTH),
                    Some(content.len().to_string())
                );

                let stream = body.into_data_stream().map_err(std::io::Error::other);
                let mut reader = StreamReader::new(stream);
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, content);
            }
        }
    }

    #[tokio::test]
    async fn test_get_local_blob_returns_correct_size() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"regression test blob content";

            let (digest, _) = create_test_blob(registry, namespace, content).await;

            // Full read: verify Reader variant returns correct total_length
            let response = registry.get_local_blob(&digest, None).await.unwrap();
            match response {
                GetBlobResponse::Reader(mut reader, total_length) => {
                    assert_eq!(total_length, content.len() as u64);
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, content);
                }
                _ => panic!("Expected Reader response for full read"),
            }

            // Ranged read: verify RangedReader returns correct total_length
            let range = Some((5, Some(15)));
            let response = registry.get_local_blob(&digest, range).await.unwrap();
            match response {
                GetBlobResponse::RangedReader(mut reader, (start, end), total_length) => {
                    assert_eq!(start, 5);
                    assert_eq!(end, 15);
                    assert_eq!(total_length, content.len() as u64);
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, &content[5..=15]);
                }
                _ => panic!("Expected RangedReader response for ranged read"),
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

            // head_blob should return correct size via get_blob_size
            let head_response = registry
                .head_blob(&repository, &[], namespace, &digest)
                .await
                .unwrap();
            assert_eq!(head_response.digest, digest);
            assert_eq!(head_response.size, content.len() as u64);

            // get_blob should also work and return the same size
            let get_response = registry
                .get_blob(&repository, &[], namespace, &digest, None)
                .await
                .unwrap();
            match get_response {
                GetBlobResponse::Reader(_, total_length) => {
                    assert_eq!(total_length, head_response.size);
                }
                _ => panic!("Expected Reader response"),
            }
        }
    }

    #[tokio::test]
    async fn test_handle_get_blob_with_range() {
        for test_case in backends() {
            let registry = test_case.registry();
            let namespace = &Namespace::new("test-repo").unwrap();
            let content = b"test blob content";
            let (digest, _) = create_test_blob(registry, namespace, content).await;

            let accepted_content_types = Vec::new();
            let range = Some((5, Some(10)));

            let response = registry
                .handle_get_blob(namespace, &digest, &accepted_content_types, range)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            let (parts, body) = response.into_parts();

            assert_eq!(
                parts.get_header(DOCKER_CONTENT_DIGEST),
                Some(digest.to_string())
            );
            assert_eq!(parts.get_header(CONTENT_LENGTH), Some("6".to_string()));
            assert_eq!(
                parts.get_header(CONTENT_RANGE),
                Some(format!("bytes 5-10/{}", content.len()))
            );

            let stream = body.into_data_stream().map_err(std::io::Error::other);
            let mut reader = StreamReader::new(stream);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, &content[5..=10]);
        }
    }
}
