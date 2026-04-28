use std::{future::Future, io::ErrorKind};

use tracing::warn;

use crate::{
    oci::{Descriptor, Digest, Manifest},
    registry::{
        metadata_store::{Error, LinkMetadata, link_kind::LinkKind},
        path_builder,
    },
};

/// Resolves a single referrer entry to an [`OCI Descriptor`], applying an
/// optional `artifact_type` filter.
///
/// The resolution strategy is:
/// 1. Try the cached link metadata via `read_link`. If a `descriptor` is
///    present and either passes the filter or has no `artifact_type` to compare
///    (in which case we fall through to the manifest), return immediately.
/// 2. Fall back to reading the manifest blob via `read_blob` and deriving the
///    descriptor from it. This covers the initial push path where the link was
///    created without a stored descriptor, and the fall-through case where the
///    cached descriptor lacks `artifact_type` information needed for filtering.
///
/// `read_link` and `read_blob` are caller-supplied closures so the function
/// stays backend-agnostic without introducing a new trait.
pub async fn resolve_referrer_descriptor<L, LFut, B, BFut>(
    subject_digest: &Digest,
    manifest_digest: Digest,
    artifact_type: Option<&String>,
    read_link: L,
    read_blob: B,
) -> Option<Descriptor>
where
    L: FnOnce(LinkKind) -> LFut,
    LFut: Future<Output = Result<LinkMetadata, Error>>,
    B: FnOnce(String) -> BFut,
    BFut: Future<Output = Result<Vec<u8>, std::io::Error>>,
{
    let referrer_link = LinkKind::Referrer(subject_digest.clone(), manifest_digest.clone());

    if let Ok(metadata) = read_link(referrer_link).await
        && let Some(desc) = metadata.descriptor
    {
        match artifact_type {
            Some(at) if desc.artifact_type.as_ref() == Some(at) => return Some(desc),
            None => return Some(desc),
            // Cached descriptor has no artifact_type; fall through to manifest
            // read so the filter can be evaluated against the full manifest data.
            Some(_) if desc.artifact_type.is_none() => {}
            Some(_) => return None,
        }
    }

    let blob_path = path_builder::blob_path(&manifest_digest);
    match read_blob(blob_path.clone()).await {
        Ok(data) => {
            let manifest_len = data.len();
            match Manifest::from_slice(&data) {
                Ok(manifest) => {
                    manifest.to_descriptor(artifact_type, manifest_digest, manifest_len as u64)
                }
                Err(e) => {
                    warn!("Failed to parse manifest at {blob_path}: {e}");
                    None
                }
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            warn!("Referrer blob not found at {blob_path}, skipping");
            None
        }
        Err(e) => {
            warn!("Failed to read referrer blob at {blob_path}: {e}");
            None
        }
    }
}
