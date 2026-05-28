use std::fmt::{self, Display, Formatter};

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

/// Describes what the transactional step in `complete_via_session` must do
/// after any S3-protocol multipart-complete has already run.
///
/// The `complete_upload_uniform` / `complete_upload_nonuniform` helpers return
/// this value so that `complete_via_session` can build the single `Transaction`
/// that atomically relocates the data and deletes the session record.
pub enum CompletionPlan {
    /// Move the object at `src` to the canonical blob path.
    ///
    /// Used when the data already exists as a single object (assembled
    /// multipart or single staged chunk).
    Move { src: String, digest: Digest },
    /// Write an empty object to the canonical blob path.
    ///
    /// Used when the upload had zero bytes.
    PutEmpty { digest: Digest },
}

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
        let Some(upload_id) = self
            .store
            .search_multipart_upload_id(upload_path)
            .await?
            .map(UploadId::into_inner)
        else {
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
        if let Some(upload_id) = self
            .store
            .search_multipart_upload_id(upload_path)
            .await?
            .map(UploadId::into_inner)
        {
            let id = UploadId::new(&upload_id);
            let parts = self.store.list_parts(upload_path, &id).await?;
            return Ok((upload_id, parts));
        }

        let upload_id = self.store.create_multipart(upload_path).await?.into_inner();
        Ok((upload_id, Vec::new()))
    }

    /// Complete the S3 multipart upload, assembling the object at `upload_path`.
    ///
    /// This is the S3-protocol step that must run before the transactional
    /// move. The assembled object lands at `upload_path`; callers then issue a
    /// `Mutation::Move { src: upload_path, dst: blob_path }` inside a
    /// `Transaction` to atomically relocate it and delete the session record.
    pub async fn complete_multipart_upload(
        &self,
        upload_path: &str,
        upload_id: &str,
        parts: &[Part],
    ) -> Result<(), Error> {
        let id = UploadId::new(upload_id);
        self.store
            .complete_multipart(upload_path, &id, parts)
            .await?;
        Ok(())
    }

    /// Delete the staging container for a completed or aborted upload.
    ///
    /// Called after the engine transaction has committed the Move + Delete, so
    /// the upload-container artifacts (hash-state files, pending chunks) can be
    /// cleaned up without being in the critical path of atomicity.
    pub async fn delete_upload_container(&self, name: &str, uuid: &str) -> Result<(), Error> {
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
