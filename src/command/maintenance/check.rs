//! The namespace-checker seam shared by `prune` (retention) and `replicate`
//! (reconciliation). Scrub itself validates per key through `validate`, not
//! through checkers.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::TryStreamExt;
use tracing::warn;

use angos_oci::Namespace;

use crate::{
    command::maintenance::{error::Error, executor::ActionSink},
    registry::{
        content_discovery::holds_manifest_content,
        metadata_store::{LIST_PAGE, MetadataStore},
    },
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

/// Walks every namespace and applies `checker` to up to `concurrency` of
/// them at a time, skipping (with a warning) an enumerated name that fails
/// validation and continuing past a failed check, since maintenance is
/// best-effort. Only a listing failure aborts the walk.
pub async fn check_namespaces(
    metadata_store: &Arc<MetadataStore>,
    checker: &dyn NamespaceChecker,
    sink: &dyn ActionSink,
    concurrency: usize,
) -> Result<(), Error> {
    metadata_store
        .stream_namespaces(None, LIST_PAGE, &|_| true, |namespace| {
            holds_manifest_content(metadata_store, namespace)
        })
        .try_for_each_concurrent(concurrency, |namespace| async move {
            if let Err(e) = checker.check(&namespace, sink).await {
                warn!("Check failed for namespace '{namespace}': {e}");
            }
            Ok(())
        })
        .await
        .map_err(Error::from)
}
