//! The `_angos/` extension's wire vocabulary at the registry edge. The
//! job-administration types ([`ext::Queue`]/[`ext::JobState`]) are the
//! extension's own, so they convert into the job engine's
//! [`jobs::Queue`]/[`jobs::JobState`] here.

use angos_extension_service as ext;

use crate::jobs;
use crate::layer;

impl From<ext::Queue> for jobs::Queue {
    fn from(queue: ext::Queue) -> Self {
        match queue {
            ext::Queue::Cache => jobs::Queue::Cache,
            ext::Queue::Replication => jobs::Queue::Replication,
            ext::Queue::Scan => jobs::Queue::Scan,
            ext::Queue::Index => jobs::Queue::Index,
        }
    }
}

impl From<ext::JobState> for jobs::JobState {
    fn from(state: ext::JobState) -> Self {
        match state {
            ext::JobState::Pending => jobs::JobState::Pending,
            ext::JobState::Failed => jobs::JobState::Failed,
        }
    }
}

/// The wire listing of a layer: its stored listing and every entry of it.
pub fn layer_listing(listing: &layer::Listing, entries: Vec<ext::LayerEntry>) -> ext::LayerListing {
    ext::LayerListing {
        refreshing: listing.is_outdated(),
        compressed: listing.compressed,
        uncompressed_size: listing.uncompressed_size,
        entries,
    }
}
