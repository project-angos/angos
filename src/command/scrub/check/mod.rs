mod blob;
mod blob_index;
mod jobs;
mod layout;
mod link_references;
mod link_repair;
pub mod list_all;
mod manifest;
mod media_type;
mod multipart;
mod orphan_grants;
mod orphan_jobs;
mod orphan_namespaces;
mod referrer;
mod replication;
mod retention;
mod tag;
mod upload;

use async_trait::async_trait;
pub use blob::BlobChecker;
pub use blob_index::BlobIndexChecker;
pub use jobs::JobChecker;
pub use layout::LayoutChecker;
pub use link_references::LinkReferencesChecker;
pub use link_repair::{ensure_link, orphan_on_missing_manifest};
pub use manifest::{DanglingReference, ManifestChecker, ReferenceKind};
pub use media_type::MediaTypeChecker;
pub use multipart::MultipartChecker;
pub use orphan_grants::OrphanGrantChecker;
pub use orphan_jobs::{OrphanJobChecker, OrphanQueue};
pub use orphan_namespaces::OrphanNamespaceChecker;
pub use referrer::ReferrerChecker;
pub use replication::ReplicationChecker;
pub use retention::RetentionChecker;
pub use tag::DigestLinkChecker;
pub use upload::UploadChecker;

use crate::{
    command::scrub::{error::Error, executor::ActionSink},
    oci::{Namespace, Tag},
};

/// A checker that operates on a single namespace at a time.
///
/// Implementations must not contain `dry_run` logic; they emit `Action` values
/// to the supplied `sink` and the `Executor` decides whether to apply or skip
/// each one.
#[async_trait]
pub trait NamespaceChecker: Send + Sync {
    async fn check(
        &self,
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error>;
}

/// A checker that operates across the entire store (not namespace-scoped).
///
/// `BlobChecker` and `MultipartChecker` implement this trait because their
/// work spans the whole store, not a single namespace.
#[async_trait]
pub trait StoreChecker: Send + Sync {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error>;
}

/// A checker that inspects a single already-validated tag. The tag walk is
/// driven once by the scrub `metadata` node, which dispatches every valid tag
/// to each enabled `TagChecker`.
#[async_trait]
pub trait TagChecker: Send + Sync {
    async fn check_tag(
        &self,
        namespace: &Namespace,
        tag: &Tag,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error>;
}
