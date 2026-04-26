mod config;
mod error;
pub mod fs;
mod hashing_reader;
pub mod s3;
mod sha256_ext;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
pub use config::BlobStorageConfig;
pub use error::Error;
use tokio::io::AsyncRead;

use crate::oci::Digest;

pub type BoxedReader = Box<dyn AsyncRead + Unpin + Send + Sync>;

/// Summary of an in-progress or completed upload session.
#[derive(Debug, Clone)]
pub struct UploadSummary {
    pub size: u64,
    pub started_at: DateTime<Utc>,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn list(
        &self,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<Digest>, Option<String>), Error>;

    async fn create(&self, content: &[u8]) -> Result<Digest, Error>;

    async fn read(&self, digest: &Digest) -> Result<Vec<u8>, Error>;

    async fn size(&self, digest: &Digest) -> Result<u64, Error>;

    async fn reader(
        &self,
        digest: &Digest,
        start_offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error>;

    async fn delete(&self, digest: &Digest) -> Result<(), Error>;
}

#[async_trait]
pub trait UploadStore: Send + Sync {
    async fn list(
        &self,
        namespace: &str,
        n: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error>;

    async fn create(&self, namespace: &str, uuid: &str) -> Result<String, Error>;

    async fn write(
        &self,
        namespace: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error>;

    async fn summary(&self, namespace: &str, uuid: &str) -> Result<UploadSummary, Error>;

    async fn complete(
        &self,
        namespace: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error>;

    async fn delete(&self, namespace: &str, uuid: &str) -> Result<(), Error>;
}

#[async_trait]
pub trait PresignedBlobStore: Send + Sync {
    async fn url(
        &self,
        digest: &Digest,
        content_type: Option<&str>,
    ) -> Result<Option<String>, Error>;
}

#[async_trait]
pub trait MultipartCleanup: Send + Sync {
    async fn cleanup_orphan_multipart_uploads(
        &self,
        timeout: Duration,
        dry_run: bool,
    ) -> Result<usize, Error>;
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use chrono::Duration;
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::*;
    use crate::{oci::Namespace, registry::blob_store::sha256_ext::Sha256Ext};

    pub async fn test_datastore_list_uploads(store: &impl UploadStore) {
        let namespace = &Namespace::new("test-repo").unwrap();

        let upload_ids = ["upload1", "upload2", "upload3"];
        for id in upload_ids {
            store.create(namespace, id).await.unwrap();

            let content = format!("Content for upload {id}").into_bytes();
            store
                .write(namespace, id, Box::new(Cursor::new(content)), 0, false)
                .await
                .unwrap();
        }

        // Verify we can list all uploads
        let (uploads, _token) = store.list(namespace, 10, None).await.unwrap();
        assert_eq!(uploads.len(), upload_ids.len());
        for id in upload_ids {
            assert!(uploads.contains(&id.to_string()));
        }

        // Test pagination (2 items per page)
        let (page1, token1) = store.list(namespace, 2, None).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        let (page2, token2) = store.list(namespace, 2, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_none());

        // Test pagination (1 item per page)
        let (page1, token1) = store.list(namespace, 1, None).await.unwrap();
        assert_eq!(page1.len(), 1);
        assert!(token1.is_some());

        let (page2, token2) = store.list(namespace, 1, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_some());

        let (page3, token3) = store.list(namespace, 1, token2).await.unwrap();
        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());

        // Test upload operations - verify we can complete an upload
        let upload_to_complete = upload_ids[0];
        store
            .complete(namespace, upload_to_complete, None)
            .await
            .unwrap();

        // The upload should be gone after completion
        let (uploads_after_complete, _) = store.list(namespace, 10, None).await.unwrap();
        assert_eq!(uploads_after_complete.len(), upload_ids.len() - 1);
        assert!(!uploads_after_complete.contains(&upload_to_complete.to_string()));
    }

    pub async fn test_datastore_list_blobs(store: &impl BlobStore) {
        let blob_contents = [
            b"aaa_content_1".to_vec(),
            b"bbb_content_2".to_vec(),
            b"ccc_content_3".to_vec(),
        ];

        let mut digests = Vec::new();
        for content in &blob_contents {
            let digest = store.create(content).await.unwrap();
            digests.push(digest);
        }

        // Test without pagination
        let (blobs, _token) = store.list(10, None).await.unwrap();
        assert!(blobs.len() >= blob_contents.len());
        for digest in &digests {
            assert!(blobs.contains(digest));
        }

        // Test pagination (2 items per page)
        let (page1, token1) = store.list(2, None).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        let (page2, token2) = store.list(2, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_none());

        // Test pagination (1 item per page)
        let (page1, token1) = store.list(1, None).await.unwrap();
        assert_eq!(page1.len(), 1);
        assert!(token1.is_some());

        let (page2, token2) = store.list(1, token1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert!(token2.is_some());

        let (page3, token3) = store.list(1, token2).await.unwrap();
        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());
    }

    pub async fn test_datastore_blob_operations(store: &impl BlobStore) {
        let test_content = b"Test blob content";
        let digest = store.create(test_content).await.unwrap();

        let retrieved_content = store.read(&digest).await.unwrap();
        assert_eq!(retrieved_content, test_content);

        let size = store.size(&digest).await.unwrap();
        assert_eq!(size, test_content.len() as u64);

        // Test blob reader
        let (mut reader, _) = store.reader(&digest, None).await.unwrap();
        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer)
            .await
            .unwrap();
        assert_eq!(buffer, test_content);
    }

    pub async fn test_build_blob_reader_returns_size(store: &impl BlobStore) {
        use tokio::io::AsyncReadExt;

        let test_content = b"blob reader size test content";
        let digest = store.create(test_content).await.unwrap();

        let (mut reader, size) = store.reader(&digest, None).await.unwrap();
        assert_eq!(size, test_content.len() as u64);

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, test_content);
    }

    #[allow(clippy::cast_possible_truncation)]
    pub async fn test_build_blob_reader_with_offset_returns_full_size(store: &impl BlobStore) {
        use tokio::io::AsyncReadExt;

        let test_content = b"offset blob reader content here";
        let digest = store.create(test_content).await.unwrap();
        let offset = 10u64;

        let (mut reader, size) = store.reader(&digest, Some(offset)).await.unwrap();

        // Should return the TOTAL blob size, not remaining size
        assert_eq!(size, test_content.len() as u64);

        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &test_content[offset as usize..]);
    }

    pub async fn test_datastore_upload_operations<S>(store: &S)
    where
        S: BlobStore + UploadStore,
    {
        let blob: &dyn BlobStore = store;
        let upload: &dyn UploadStore = store;

        let namespace = &Namespace::new("test-namespace").unwrap();
        let uuid = Uuid::new_v4().to_string();

        let upload_id = upload.create(namespace, &uuid).await.unwrap();
        assert_eq!(upload_id, uuid);

        let test_content = b"Test upload content";

        let mut hasher = Sha256::new();
        hasher.update(test_content);
        let expected_digest = hasher.digest();

        upload
            .write(
                namespace,
                &uuid,
                Box::new(Cursor::new(test_content.to_vec())),
                test_content.len() as u64,
                false,
            )
            .await
            .unwrap();

        let summary = upload.summary(namespace, &uuid).await.unwrap();
        assert_eq!(summary.size, test_content.len() as u64);
        assert!(Utc::now().signed_duration_since(summary.started_at) < Duration::hours(1));

        let final_digest = upload.complete(namespace, &uuid, None).await.unwrap();
        assert_eq!(final_digest, expected_digest);

        let blob_content = blob.read(&final_digest).await.unwrap();
        assert_eq!(blob_content, test_content);

        // Test upload not found after completion
        let upload_result = upload.summary(namespace, &uuid).await;
        assert!(upload_result.is_err());
    }
}
