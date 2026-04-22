use std::collections::HashMap;

use hyper::header::{CONTENT_LENGTH, LOCATION, RANGE};
use tokio::io::AsyncRead;
use tracing::{instrument, warn};
use uuid::Uuid;

use crate::{
    event_webhook::event::{Event, EventActor, EventKind},
    oci::{Digest, Namespace},
    registry::{DOCKER_CONTENT_DIGEST, DOCKER_UPLOAD_UUID, Error, Registry, blob_store},
};

pub enum StartUploadResponse {
    ExistingBlob {
        headers: HashMap<&'static str, String>,
    },
    Session {
        headers: HashMap<&'static str, String>,
    },
}

pub struct GetUploadResponse {
    pub headers: HashMap<&'static str, String>,
}

pub struct PatchUploadResponse {
    pub headers: HashMap<&'static str, String>,
}

pub struct CompleteUploadResponse {
    pub headers: HashMap<&'static str, String>,
    pub events: Vec<Event>,
}

/// Headers for a completed-blob response (used by `StartUpload` when the digest
/// already exists, and by `CompleteUpload` when the upload finishes).
fn blob_location_headers(namespace: &Namespace, digest: &Digest) -> HashMap<&'static str, String> {
    HashMap::from([
        (LOCATION.as_str(), format!("/v2/{namespace}/blobs/{digest}")),
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
    ])
}

fn upload_session_headers(
    namespace: &Namespace,
    session_uuid: &str,
) -> HashMap<&'static str, String> {
    HashMap::from([
        (
            LOCATION.as_str(),
            format!("/v2/{namespace}/blobs/uploads/{session_uuid}"),
        ),
        (RANGE.as_str(), "0-0".to_string()),
        (DOCKER_UPLOAD_UUID, session_uuid.to_string()),
    ])
}

fn upload_status_headers(
    namespace: &Namespace,
    session_id: Uuid,
    range_max: u64,
) -> HashMap<&'static str, String> {
    HashMap::from([
        (
            LOCATION.as_str(),
            format!("/v2/{namespace}/blobs/uploads/{session_id}"),
        ),
        (RANGE.as_str(), format!("0-{range_max}")),
        (DOCKER_UPLOAD_UUID, session_id.to_string()),
    ])
}

fn patch_upload_headers(
    namespace: &Namespace,
    session_id: Uuid,
    range_max: u64,
) -> HashMap<&'static str, String> {
    let mut headers = upload_status_headers(namespace, session_id, range_max);
    headers.insert(CONTENT_LENGTH.as_str(), "0".to_string());
    headers
}

impl Registry {
    #[instrument]
    pub async fn start_upload(
        &self,
        namespace: &Namespace,
        digest: Option<Digest>,
    ) -> Result<StartUploadResponse, Error> {
        if let Some(digest) = digest
            && self.blob_store.get_blob_size(&digest).await.is_ok()
        {
            return Ok(StartUploadResponse::ExistingBlob {
                headers: blob_location_headers(namespace, &digest),
            });
        }

        let session_uuid = Uuid::new_v4().to_string();
        self.blob_store
            .create_upload(namespace, &session_uuid)
            .await?;

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

        let state = self
            .blob_store
            .get_upload_state(namespace, &session_key)
            .await?;

        if let Some(offset) = start_offset
            && offset != state.size
        {
            return Err(Error::RangeNotSatisfiable);
        }

        let (_, size) = self
            .blob_store
            .write_upload(
                namespace,
                &session_key,
                Box::new(stream),
                content_length,
                true,
                Some(state),
            )
            .await?;

        let range_max = size.saturating_sub(1);

        Ok(PatchUploadResponse {
            headers: patch_upload_headers(namespace, session_id, range_max),
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

        let state = match self
            .blob_store
            .get_upload_state(namespace, &session_key)
            .await
        {
            Ok(state) => Some(state),
            Err(blob_store::Error::UploadNotFound) => None,
            Err(e) => return Err(e.into()),
        };
        let append = state.as_ref().is_some_and(|s| s.size > 0);

        let (upload_digest, _) = self
            .blob_store
            .write_upload(
                namespace,
                &session_key,
                Box::new(stream),
                content_length,
                append,
                state,
            )
            .await?;

        if &upload_digest != digest {
            warn!("Expected digest '{digest}', got '{upload_digest}'");
            return Err(Error::DigestInvalid);
        }

        self.blob_store
            .complete_upload(namespace, &session_key, Some(digest))
            .await?;
        self.blob_store
            .delete_upload(namespace, &session_key)
            .await?;

        let repository = self.repository_name_for(namespace);
        let event = Event::new(EventKind::BlobPush, namespace.to_string(), repository)
            .digest(Some(digest.to_string()))
            .actor(actor);

        Ok(CompleteUploadResponse {
            headers: blob_location_headers(namespace, digest),
            events: vec![event],
        })
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
        let (_, size, _) = self
            .blob_store
            .read_upload_summary(namespace, &uuid)
            .await?;

        let range_max = size.saturating_sub(1);

        Ok(GetUploadResponse {
            headers: upload_status_headers(namespace, session_id, range_max),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use uuid::Uuid;

    use super::*;
    use crate::{
        oci::Namespace,
        registry::{
            path_builder,
            tests::{FSRegistryTestCase, backends},
        },
    };

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

            let digest = registry.blob_store.create_blob(content).await.unwrap();
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

            let (_, size, _) = registry
                .blob_store
                .read_upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(size, (content.len() + additional_content.len()) as u64);
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

            let (upload_digest, _, _) = registry
                .blob_store
                .read_upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();

            let empty_stream = Cursor::new(Vec::new());
            let response = registry
                .complete_upload(None, namespace, session_id, &upload_digest, 0, empty_stream)
                .await
                .unwrap();

            assert_eq!(
                response.headers[DOCKER_CONTENT_DIGEST],
                upload_digest.to_string()
            );
            assert_eq!(response.events.len(), 1);
            assert!(matches!(
                response.events[0].kind,
                crate::event_webhook::event::EventKind::BlobPush
            ));

            let stored_content = registry.blob_store.read_blob(&upload_digest).await.unwrap();
            assert_eq!(stored_content, content);
        }
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
                    .read_upload_summary(namespace, &session_id.to_string())
                    .await
                    .is_ok()
            );

            registry.delete_upload(namespace, session_id).await.unwrap();

            assert!(
                registry
                    .blob_store
                    .read_upload_summary(namespace, &session_id.to_string())
                    .await
                    .is_err()
            );
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
                .blob_store
                .create_upload(namespace, &session_id.to_string())
                .await
                .unwrap();

            let stream = Cursor::new(b"test content".to_vec());
            registry
                .patch_upload(namespace, session_id, None, 12, stream)
                .await
                .unwrap();

            let wrong_digest = crate::oci::Digest::from_str(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

            let empty_stream = Cursor::new(Vec::new());
            let result = registry
                .complete_upload(None, namespace, session_id, &wrong_digest, 0, empty_stream)
                .await;

            assert!(matches!(result, Err(Error::DigestInvalid)));
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
                    false,
                    None,
                )
                .await
                .unwrap();

            assert_eq!(size, content.len() as u64);

            let (expected_digest, expected_size, _) = registry
                .blob_store
                .read_upload_summary(namespace, &session_id.to_string())
                .await
                .unwrap();

            assert_eq!(digest, expected_digest);
            assert_eq!(size, expected_size);
        }
    }

    #[tokio::test]
    async fn test_get_upload_state() {
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

            let state = registry
                .blob_store
                .get_upload_state(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(state.size, 0);

            let stream: Box<dyn tokio::io::AsyncRead + Unpin + Send + Sync> =
                Box::new(Cursor::new(content.to_vec()));
            registry
                .blob_store
                .write_upload(
                    namespace,
                    &session_id.to_string(),
                    stream,
                    content.len() as u64,
                    false,
                    None,
                )
                .await
                .unwrap();

            let state = registry
                .blob_store
                .get_upload_state(namespace, &session_id.to_string())
                .await
                .unwrap();
            assert_eq!(state.size, content.len() as u64);
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

        let (upload_digest, size, _) = registry
            .blob_store
            .read_upload_summary(namespace, &session_id.to_string())
            .await
            .unwrap();

        assert_eq!(size, content.len() as u64);

        let hash_state_path = path_builder::upload_hash_context_path(
            namespace,
            &session_id.to_string(),
            "sha256",
            size,
        );
        let full_path = test_case.temp_dir().path().join(&hash_state_path);
        std::fs::write(&full_path, b"corrupted data").unwrap();

        let empty_stream = Cursor::new(Vec::new());
        let result = registry
            .complete_upload(None, namespace, session_id, &upload_digest, 0, empty_stream)
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
