mod blob;
mod layout;
mod link_references;
mod link_repair;
pub mod list_all;
mod manifest;
mod media_type;
mod multipart;
mod orphan_grants;
mod orphan_jobs;
mod referrer;
mod replication;
mod retention;
mod tag;
mod upload;

use async_trait::async_trait;
pub use blob::BlobChecker;
pub use layout::LayoutChecker;
pub use link_references::LinkReferencesChecker;
pub use link_repair::ensure_link;
pub use manifest::ManifestChecker;
pub use media_type::MediaTypeChecker;
pub use multipart::MultipartChecker;
pub use orphan_grants::OrphanGrantChecker;
pub use orphan_jobs::{OrphanJobChecker, OrphanQueue};
pub use referrer::ReferrerChecker;
pub use replication::ReplicationChecker;
pub use retention::RetentionChecker;
pub use tag::TagChecker;
pub use upload::UploadChecker;

use crate::command::scrub::{error::Error, executor::ActionSink};

/// A checker that operates on a single namespace at a time.
///
/// Implementations must not contain `dry_run` logic; they emit `Action` values
/// to the supplied `sink` and the `Executor` decides whether to apply or skip
/// each one.
#[async_trait]
pub trait NamespaceChecker: Send + Sync {
    async fn check(&self, namespace: &str, sink: &mut (dyn ActionSink + Send))
    -> Result<(), Error>;
}

/// A checker that operates across the entire store (not namespace-scoped).
///
/// `BlobChecker` and `MultipartChecker` implement this trait because their
/// work spans the whole store, not a single namespace.
#[async_trait]
pub trait StoreChecker: Send + Sync {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error>;
}
