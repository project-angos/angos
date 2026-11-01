//! The `_angos/` extension's wire vocabulary at the registry edge. The
//! job-administration types ([`ExtQueue`]/[`ExtJobState`]) are the
//! extension's own, so they convert into the job engine's
//! [`Queue`]/[`JobState`] here.

use angos_extension_service::{
    JobState as ExtJobState, LayerEntry, LayerListing, Queue as ExtQueue,
};

use crate::jobs::{JobState, Queue};
use crate::layer::Listing;

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

/// The wire listing of a layer: its stored listing and every entry of it.
pub fn layer_listing(listing: &Listing, entries: Vec<LayerEntry>) -> LayerListing {
    LayerListing {
        refreshing: listing.is_outdated(),
        compressed: listing.compressed,
        uncompressed_size: listing.uncompressed_size,
        entries,
    }
}
