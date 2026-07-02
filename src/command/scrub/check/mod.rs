mod jobs;
pub mod list_all;
mod multipart;
mod orphan_jobs;
mod rebuild;
mod replication;
mod retention;
mod upload;

use async_trait::async_trait;
pub use jobs::JobChecker;
pub use multipart::MultipartChecker;
pub use orphan_jobs::{OrphanJobChecker, OrphanQueue};
pub use rebuild::{DanglingReference, RebuildChecker, ReferenceKind};
pub use replication::ReplicationChecker;
pub use retention::RetentionChecker;
pub use upload::UploadChecker;

use crate::{
    command::scrub::{error::Error, executor::ActionSink},
    oci::Namespace,
};

/// A checker over a single namespace. Implementations emit `Action` values to
/// the sink for the `Executor` to apply or skip; [`RebuildChecker`] is the
/// exception, committing through one atomic `update_links` transaction and
/// carrying its own `dry_run` field.
#[async_trait]
pub trait NamespaceChecker: Send + Sync {
    async fn check(
        &self,
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error>;
}

/// A checker whose work spans the whole store rather than a single namespace.
#[async_trait]
pub trait StoreChecker: Send + Sync {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error>;
}
