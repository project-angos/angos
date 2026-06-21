use std::sync::Arc;

use tracing::{debug, warn};

use crate::{
    command::scrub::{action::Action, error::Error, executor::ActionSink},
    oci::{Digest, Namespace},
    registry::{
        blob_store,
        metadata_store::{LinkKind, MetadataStore},
    },
};

pub async fn ensure_link(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    link: &LinkKind,
    expected_target: &Digest,
    sink: &mut (dyn ActionSink + Send),
) -> Result<(), Error> {
    match metadata_store.read_link(namespace, link).await {
        Ok(metadata) if &metadata.target == expected_target => {
            debug!("Link {link} -> {expected_target} is valid");
            Ok(())
        }
        _ => {
            debug!("Missing or invalid link: {link} -> {expected_target}");
            sink.apply(Action::RecreateLink {
                namespace: namespace.clone(),
                link: link.clone(),
                target: expected_target.clone(),
            })
            .await
        }
    }
}

/// The single "a revision whose manifest blob is gone is an orphan" rule, shared
/// by the tag, link-references, and media-type checkers.
///
/// Maps a manifest-blob load result to a liveness decision: a present blob
/// returns `Some(value)`; a not-found emits a `DeleteOrphanManifest` for
/// `digest` and returns `None`; any other blob-store error propagates. Generic
/// over the load result so a caller can probe with `size` (when it only needs
/// existence) or `read` (when it needs the bytes) without changing its I/O.
pub async fn orphan_on_missing_manifest<T>(
    result: Result<T, blob_store::Error>,
    namespace: &Namespace,
    digest: &Digest,
    sink: &mut (dyn ActionSink + Send),
) -> Result<Option<T>, Error> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {
            warn!("Manifest blob missing for '{namespace}@{digest}'; removing revision link");
            sink.apply(Action::DeleteOrphanManifest {
                namespace: namespace.clone(),
                digest: digest.clone(),
            })
            .await?;
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}
