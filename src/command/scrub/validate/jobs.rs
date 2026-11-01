//! Structural job-record validation: a record whose envelope cannot be parsed
//! can never be drained and is deleted. Config-relative orphan classification
//! is prune's job.

use tracing::warn;

use angos_storage::Error as StorageError;

use crate::command::maintenance::Error;
use crate::command::maintenance::action::{Action, WalkedStore};
use crate::command::scrub::validate::Validator;
use crate::jobs::store::{DeadLetterRecord, JobEnvelope, LockKeyIndex, job_pending_path};
use crate::jobs::{JobState, Queue};
use crate::registry::Error as RegistryError;

impl Validator {
    /// Validate one pending or dead-lettered job record.
    pub async fn validate_job_record(&self, key: &str, state: JobState) -> Result<(), Error> {
        let Some(raw) = self.read_metadata_object(key).await? else {
            return Ok(());
        };
        let parses = match state {
            JobState::Pending => serde_json::from_slice::<JobEnvelope>(&raw).is_ok(),
            JobState::Failed => serde_json::from_slice::<DeadLetterRecord>(&raw).is_ok(),
        };
        if !parses {
            warn!("scrub: job record '{key}' does not parse; deleting");
            self.delete_corrupt(WalkedStore::Metadata, key).await?;
        }
        Ok(())
    }

    /// Validate one `lock_key` dedup index entry, reclaiming one whose pending
    /// job never landed. Enqueue writes the index first, so a young index
    /// naming no pending file is an in-flight enqueue rather than an orphan;
    /// only the grace period tells the two apart.
    pub async fn validate_job_index(&self, key: &str, queue: Queue) -> Result<(), Error> {
        let Some(raw) = self.read_metadata_object(key).await? else {
            return Ok(());
        };
        let Ok(index) = serde_json::from_slice::<LockKeyIndex>(&raw) else {
            warn!("scrub: job index entry '{key}' does not parse; deleting");
            return self.delete_corrupt(WalkedStore::Metadata, key).await;
        };

        let pending = job_pending_path(queue.as_str(), &index.storage_key);
        if self
            .metadata_store
            .object_store()
            .exists(&pending)
            .await
            .map_err(RegistryError::from)?
        {
            return Ok(());
        }
        if self.younger_than_grace(key).await? {
            return Ok(());
        }
        self.emit(Action::DeleteOrphanJobIndex {
            queue,
            key: key.to_string(),
            storage_key: index.storage_key,
        })
        .await
    }

    /// Raw metadata-store read tolerating a concurrent deletion.
    async fn read_metadata_object(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.metadata_store.object_store().get(key).await {
            Ok(raw) => Ok(Some(raw)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(RegistryError::from(e).into()),
        }
    }
}
