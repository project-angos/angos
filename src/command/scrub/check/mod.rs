//! The namespace-checker seam shared by `prune` (retention) and `replicate`
//! (reconciliation), plus the paginated enumeration streams in [`list_all`].
//! Scrub itself validates per key through `validate`, not through checkers.

pub mod list_all;

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::warn;

use crate::{
    command::scrub::{error::Error, executor::ActionSink},
    oci::Namespace,
    registry::metadata_store::MetadataStore,
};

/// A checker that operates on a single namespace at a time.
///
/// Implementations must not contain `dry_run` logic; they emit `Action` values
/// to the supplied `sink` and the `Executor` decides whether to apply or skip
/// each one.
#[async_trait]
pub trait NamespaceChecker: Send + Sync {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error>;
}

/// Walks every namespace and applies `checker` to each, skipping (with a
/// warning) an enumerated name that fails validation and continuing past a
/// failed check, since maintenance is best-effort.
pub async fn check_namespaces(
    metadata_store: &Arc<MetadataStore>,
    checker: &dyn NamespaceChecker,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let mut namespaces = list_all::namespaces(metadata_store);
    while let Some(namespace) = namespaces.next().await {
        let namespace = namespace?;
        let namespace = match Namespace::new(&namespace) {
            Ok(namespace) => namespace,
            Err(e) => {
                warn!("Skipping invalid enumerated namespace '{namespace}': {e}");
                continue;
            }
        };
        if let Err(e) = checker.check(&namespace, sink).await {
            warn!("Check failed for namespace '{namespace}': {e}");
        }
    }
    Ok(())
}
