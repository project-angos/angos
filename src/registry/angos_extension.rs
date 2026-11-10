//! The `_angos/` extension's job-administration types are its own, so they
//! convert into the job engine's [`Queue`]/[`JobState`] here.

use angos_extension_service::{JobState as ExtJobState, Queue as ExtQueue};

use crate::jobs::{JobState, Queue};

impl From<ExtQueue> for Queue {
    fn from(queue: ExtQueue) -> Self {
        match queue {
            ExtQueue::Cache => Queue::Cache,
            ExtQueue::Replication => Queue::Replication,
            ExtQueue::Scan => Queue::Scan,
            ExtQueue::Index => Queue::Index,
        }
    }
}

impl From<ExtJobState> for JobState {
    fn from(state: ExtJobState) -> Self {
        match state {
            ExtJobState::Pending => JobState::Pending,
            ExtJobState::Failed => JobState::Failed,
        }
    }
}
