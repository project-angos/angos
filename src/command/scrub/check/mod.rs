mod blob;
mod link_references;
mod manifest;
mod media_type;
mod multipart;
mod retention;
mod tag;
mod upload;

use std::sync::Arc;

pub use blob::BlobChecker;
pub use link_references::LinkReferencesChecker;
pub use manifest::ManifestChecker;
pub use media_type::MediaTypeChecker;
pub use multipart::MultipartChecker;
pub use retention::RetentionChecker;
pub use tag::TagChecker;
use tracing::{debug, info};
pub use upload::UploadChecker;

use crate::oci::Digest;
use crate::registry::Error;
use crate::registry::metadata_store::link_kind::LinkKind;
use crate::registry::metadata_store::{MetadataStore, MetadataStoreExt};

pub async fn ensure_link(
    metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
    namespace: &str,
    link: &LinkKind,
    expected_target: &Digest,
    dry_run: bool,
) -> Result<(), Error> {
    match metadata_store.read_link(namespace, link, false).await {
        Ok(metadata) if &metadata.target == expected_target => {
            debug!("Link {link} -> {expected_target} is valid");
            Ok(())
        }
        _ => {
            debug!("Missing or invalid link: {link} -> {expected_target}");
            recreate_link(metadata_store, namespace, link, expected_target, dry_run).await
        }
    }
}

async fn recreate_link(
    metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
    namespace: &str,
    link: &LinkKind,
    target: &Digest,
    dry_run: bool,
) -> Result<(), Error> {
    if dry_run {
        info!(
            "DRY RUN: would recreate invalid link from namespace '{namespace}': {link}' -> '{target}'"
        );
        return Ok(());
    }

    info!("Recreating invalid link from namespace '{namespace}': {link}' -> '{target}'");
    let mut tx = metadata_store.begin_transaction(namespace);
    tx.create_link(link, target);
    tx.commit().await?;
    Ok(())
}
