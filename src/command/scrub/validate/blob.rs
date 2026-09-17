//! Blob-data and upload-artifact validation. Runs last, once the index has
//! been healed, so an empty index really means no references.

use tracing::warn;

use angos_oci::{Digest, Namespace};
use angos_storage::Error as StorageError;

use crate::registry::keys::DigestKeys;
use crate::{
    command::{
        maintenance::{
            Error,
            action::{Action, WalkedStore},
            categorize::UploadArtifact,
            executor::object_younger_than_grace,
        },
        scrub::validate::Validator,
    },
    registry::{Error as RegistryError, blob_store::upload_session::SessionFile},
};

impl Validator {
    /// Validate one blob's `data` object against its (already healed) index.
    pub async fn validate_blob(&self, digest: &Digest) -> Result<(), Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(RegistryError::NotFound) => {
                // No index at all: unreferenced bytes. The executor re-verifies
                // liveness under a gc run marker before deleting.
                return self.reclaim_orphan_blob(digest).await;
            }
            Err(e) => return Err(e.into()),
        };
        if index.is_empty() {
            return self.reclaim_orphan_blob(digest).await;
        }
        // Referenced: per-entry link probing is the reference pass's job, and
        // config-relative grant reclamation is prune's.
        Ok(())
    }

    /// Delete unreferenced bytes, unless the reference walk read references
    /// the per-blob index read did not, in which case reclaim waits for the
    /// next run.
    async fn reclaim_orphan_blob(&self, digest: &Digest) -> Result<(), Error> {
        // The index read pages one directory listing while the reference walk
        // scanned the whole store; disagreement means a listing dropped a key
        // it holds, so the bytes may well be live.
        if self.reference_walk_saw(digest) {
            warn!(
                "scrub: blob '{digest}' reads as unreferenced but the reference walk found references \
                 for it; refusing to reclaim, the backend's listing is incomplete"
            );
            return Ok(());
        }
        // The executor re-checks this gate before deleting; testing it here too
        // keeps in-flight uploads from being emitted and counted as repairs on
        // every walk.
        let young = object_younger_than_grace(
            self.blob_store.object_store().as_ref(),
            &digest.blob_path(),
            self.metadata_store.gc_grace_secs,
        )
        .await
        .map_err(RegistryError::from)?;
        if young.is_none_or(|fresh| fresh) {
            return Ok(());
        }
        self.emit(Action::DeleteOrphanBlob(digest.clone())).await
    }

    /// One upload-session artifact: scrub validates the session record's
    /// content and reclaims invalid-name upload directories, which no angos
    /// API can address. Session aging is prune's job.
    pub async fn validate_upload_artifact(
        &self,
        key: &str,
        namespace_raw: &str,
        artifact: UploadArtifact,
    ) -> Result<(), Error> {
        if Namespace::new(namespace_raw).is_err() {
            if !self.claim(format!("invalid-upload-ns:{namespace_raw}")) {
                return Ok(());
            }
            warn!("scrub: reclaiming invalid upload namespace directory '{namespace_raw}'");
            return self
                .emit(Action::DeleteInvalidUploadNamespace {
                    name: namespace_raw.to_string(),
                })
                .await;
        }
        match artifact {
            UploadArtifact::SessionJson => self.validate_session_json(key).await,
            UploadArtifact::Data | UploadArtifact::Staged => Ok(()),
        }
    }

    /// Delete an undecodable `session.json`; the session then reads as broken
    /// and prune's upload sweep reaps it.
    async fn validate_session_json(&self, key: &str) -> Result<(), Error> {
        let raw = match self.blob_store.object_store().get(key).await {
            Ok(raw) => raw,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(RegistryError::from(e).into()),
        };
        if SessionFile::decode(&raw).is_err() {
            warn!("scrub: upload session record '{key}' does not parse; deleting");
            self.delete_corrupt(WalkedStore::Blob, key).await?;
        }
        Ok(())
    }
}
