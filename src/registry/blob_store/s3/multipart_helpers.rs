use std::fmt::{self, Display, Formatter};

use bytes::Bytes;
use tokio::task::JoinError;
use tracing::{error, warn};

use crate::{
    oci::Digest,
    registry::{
        blob_store::{Error, s3::Backend, sha256_ext::Sha256Ext},
        path_builder,
    },
};
use angos_storage::{MultipartStore, ObjectStore, Part, UploadId};

#[derive(Debug, Clone, Copy)]
pub enum UploadMode {
    Uniform,
    Nonuniform,
}

impl Display for UploadMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uniform => f.write_str("uniform"),
            Self::Nonuniform => f.write_str("nonuniform"),
        }
    }
}

impl Backend {
    pub async fn discover_multipart_upload(
        &self,
        upload_path: &str,
    ) -> Result<(Option<String>, Vec<Part>), Error> {
        let Some(upload_id) = self.get_or_search_upload_id(upload_path).await? else {
            return Ok((None, Vec::new()));
        };
        let id = UploadId::new(&upload_id);
        let parts = self.store.list_parts(upload_path, &id).await?;
        Ok((Some(upload_id), parts))
    }

    pub async fn ensure_multipart_upload(
        &self,
        upload_path: &str,
    ) -> Result<(String, Vec<Part>), Error> {
        if let Some(upload_id) = self.get_or_search_upload_id(upload_path).await? {
            let id = UploadId::new(&upload_id);
            let parts = self.store.list_parts(upload_path, &id).await?;
            return Ok((upload_id, parts));
        }

        let upload_id = self.store.create_multipart(upload_path).await?.into_inner();
        self.cache_upload_id(upload_path, &upload_id).await;
        Ok((upload_id, Vec::new()))
    }

    pub async fn complete_multipart_upload_and_store(
        &self,
        upload_path: &str,
        upload_id: &str,
        parts: &[Part],
        name: &str,
        uuid: &str,
        digest: &Digest,
    ) -> Result<(), Error> {
        let id = UploadId::new(upload_id);
        self.store
            .complete_multipart(upload_path, &id, parts)
            .await?;

        self.evict_upload_id(upload_path).await;
        self.evict_upload_state(name, uuid).await;

        let blob_path = path_builder::blob_path(digest);
        self.store.copy(upload_path, &blob_path).await?;

        let container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&container).await?;

        Ok(())
    }

    pub async fn copy_staged_object_to_blob_and_cleanup(
        &self,
        source_path: &str,
        name: &str,
        uuid: &str,
        digest: &Digest,
    ) -> Result<(), Error> {
        let blob_path = path_builder::blob_path(digest);
        self.store.copy(source_path, &blob_path).await?;

        self.evict_upload_state(name, uuid).await;

        let container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&container).await?;

        Ok(())
    }

    pub async fn put_empty_blob_and_cleanup(
        &self,
        name: &str,
        uuid: &str,
        digest: &Digest,
    ) -> Result<(), Error> {
        let blob_path = path_builder::blob_path(digest);
        self.store.put(&blob_path, Bytes::new()).await?;

        self.evict_upload_state(name, uuid).await;

        let container = path_builder::upload_container_path(name, uuid);
        self.store.delete_prefix(&container).await?;

        Ok(())
    }

    pub async fn resolve_upload_digest(
        &self,
        name: &str,
        uuid: &str,
        size: u64,
        digest: Option<&Digest>,
    ) -> Result<Digest, Error> {
        match digest {
            Some(digest) => Ok(digest.clone()),
            None => Ok(self.load_hasher(name, uuid, size).await?.digest()),
        }
    }

    pub async fn handle_upload_task_failure(
        &self,
        mode: UploadMode,
        name: &str,
        uuid: &str,
        upload_path: &str,
        upload_id: &str,
        join_error: JoinError,
    ) -> Error {
        let kind = if join_error.is_panic() {
            "panicked"
        } else if join_error.is_cancelled() {
            "was cancelled"
        } else {
            "failed unexpectedly"
        };
        error!(
            "{mode} upload task {kind} for '{name}/{uuid}'; aborting multipart upload {upload_id}: {join_error}"
        );
        let id = UploadId::new(upload_id);
        if let Err(abort_err) = self.store.abort_multipart(upload_path, &id).await {
            warn!("abort_multipart failed during cleanup of '{name}/{uuid}': {abort_err}");
        }
        self.evict_upload_id(upload_path).await;
        self.evict_upload_state(name, uuid).await;
        Error::StorageBackend(format!("upload task {kind}: {join_error}"))
    }
}

/// Returns the 1-based S3 part number for the next part to upload, given the
/// number of parts already completed.
pub fn next_part_number(completed_parts: usize) -> Result<u32, Error> {
    Ok(u32::try_from(completed_parts + 1)?)
}

pub fn uploaded_size(parts: &[Part]) -> u64 {
    parts.iter().map(|p| p.size).sum()
}
