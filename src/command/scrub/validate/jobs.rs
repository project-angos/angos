//! Structural job-record validation: a record whose envelope cannot be
//! parsed can never be drained and is deleted. Config-relative orphan
//! classification (a job whose downstream or repository left the config) is
//! prune's job.

use tracing::warn;

use angos_tx_engine::StorageError;

use crate::{
    command::scrub::{action::WalkedStore, error::Error, validate::Validator},
    jobs::{
        JobState, Queue,
        store::{DeadLetterRead, JobEnvelope, LockKeyIndex},
    },
    registry::Error as RegistryError,
};

impl Validator {
    /// Validate one pending or dead-lettered job record.
    pub async fn validate_job_record(
        &self,
        key: &str,
        _queue: Queue,
        state: JobState,
    ) -> Result<(), Error> {
        let Some(raw) = self.read_metadata_object(key).await? else {
            return Ok(());
        };
        let parses = match state {
            JobState::Pending => serde_json::from_slice::<JobEnvelope>(&raw).is_ok(),
            JobState::Failed => serde_json::from_slice::<DeadLetterRead>(&raw).is_ok(),
        };
        if !parses {
            warn!("scrub: job record '{key}' does not parse; deleting");
            self.delete_corrupt(WalkedStore::Metadata, key).await?;
        }
        Ok(())
    }

    /// Validate one `lock_key` dedup index entry. A dangling entry (its
    /// `storage_key` no longer pending) is left alone: the store self-heals
    /// it at the next enqueue.
    pub async fn validate_job_index(&self, key: &str) -> Result<(), Error> {
        let Some(raw) = self.read_metadata_object(key).await? else {
            return Ok(());
        };
        if serde_json::from_slice::<LockKeyIndex>(&raw).is_err() {
            warn!("scrub: job index entry '{key}' does not parse; deleting");
            self.delete_corrupt(WalkedStore::Metadata, key).await?;
        }
        Ok(())
    }

    /// Raw metadata-store read tolerating a concurrent deletion.
    async fn read_metadata_object(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.metadata_store.store().object_store().get(key).await {
            Ok(raw) => Ok(Some(raw)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(RegistryError::from(e).into()),
        }
    }
}
