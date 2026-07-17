//! Blob-data and upload-artifact validation (absorbed from the old
//! `BlobChecker` orphan GC and the orphan-namespace sweeps). Runs last: the
//! index has been healed, so an empty index really means no references.

use chrono::DateTime;
use tracing::warn;

use angos_tx_engine::StorageError;

use crate::{
    command::scrub::{
        action::{Action, WalkedStore},
        categorize::UploadArtifact,
        error::Error,
        validate::Validator,
    },
    oci::{Digest, Namespace},
    registry::Error as RegistryError,
};

impl Validator {
    /// Validate one blob's `data` object against its (already healed) index.
    pub async fn validate_blob(&self, digest: &Digest) -> Result<(), Error> {
        let index = match self.metadata_store.read_blob_index(digest).await {
            Ok(index) => index,
            Err(RegistryError::NotFound) => {
                // No index at all: unreferenced bytes. The executor re-checks
                // under the blob-data lock before deleting.
                return self.reclaim_orphan_blob(digest).await;
            }
            Err(e) => return Err(e.into()),
        };
        if index.namespace.is_empty() {
            return self.reclaim_orphan_blob(digest).await;
        }
        // Referenced: per-entry link probing is the shard pass's job, and
        // config-relative grant reclamation is prune's.
        Ok(())
    }

    /// Delete unreferenced bytes unless this run force-deleted one of the
    /// blob's shards, in which case the references vanished unrepaired and
    /// reclaim must wait for the next run's re-grant.
    async fn reclaim_orphan_blob(&self, digest: &Digest) -> Result<(), Error> {
        if self.blob_gc_held(digest) {
            warn!(
                "scrub: blob '{digest}' lost a corrupt shard this run; leaving bytes for the next run"
            );
            return Ok(());
        }
        self.emit(Action::DeleteOrphanBlob(digest.clone())).await
    }

    /// One upload-session artifact seen by the blob walk. Session aging and
    /// orphan-namespace clearing are prune's job; scrub validates the
    /// `startedat` marker's content and reclaims invalid-name upload
    /// directories (deduped per name), which no angos API can address.
    pub async fn validate_upload_artifact(
        &self,
        key: &str,
        namespace_raw: &str,
        _uuid: &str,
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
        if artifact == UploadArtifact::StartedAt {
            self.validate_started_at(key).await?;
        }
        Ok(())
    }

    /// An unreadable `startedat` marker is deleted; the session then reads
    /// as broken and prune's upload sweep reaps it.
    async fn validate_started_at(&self, key: &str) -> Result<(), Error> {
        let raw = match self.blob_store.object_store().get(key).await {
            Ok(raw) => raw,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(RegistryError::from(e).into()),
        };
        let parses = std::str::from_utf8(&raw)
            .ok()
            .and_then(|text| DateTime::parse_from_rfc3339(text.trim()).ok())
            .is_some();
        if !parses {
            warn!("scrub: upload marker '{key}' does not parse as RFC3339; deleting");
            self.delete_corrupt(WalkedStore::Blob, key).await?;
        }
        Ok(())
    }
}
