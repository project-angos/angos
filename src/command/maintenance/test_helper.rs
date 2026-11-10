//! Test-only executors and sinks for the maintenance seam.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::command::maintenance::Error;
use crate::command::maintenance::action::Action;
use crate::command::maintenance::executor::{ActionSink, Executor};
use crate::jobs::store::{ClaimMode, JobStore};
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::MetadataStore;
use crate::registry::repository_resolver::RepositoryResolver;
use crate::registry::test_utils::create_test_repositories;
use crate::registry::{Registry, RegistryConfig};

/// Captures actions without performing any I/O, so a test can assert which
/// actions a check produces.
#[async_trait]
impl ActionSink for Mutex<Vec<Action>> {
    async fn apply(&self, action: Action) -> Result<(), Error> {
        self.lock()
            .map_err(|e| Error::Initialization(format!("capture sink poisoned: {e}")))?
            .push(action);
        Ok(())
    }
}

/// An executor with a `JobStore` and a registry over the same stores, so the
/// retention arms work out of the box.
pub fn test_executor(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Executor {
    let job_store = Arc::new(JobStore::new(
        &metadata_store,
        "scrub-test",
        ClaimMode::Atomic,
    ));
    let resolver = Arc::new(
        RepositoryResolver::new(create_test_repositories())
            .expect("test repositories must not have overlapping prefixes"),
    );
    let registry = Registry::new(
        blob_store.clone(),
        metadata_store.clone(),
        resolver,
        RegistryConfig::new(job_store.clone()),
    );
    Executor::new(blob_store, metadata_store, job_store, Some(registry))
}
