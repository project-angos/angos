//! The two-store blob-ownership flows. The reference-index reads and writes
//! themselves belong to the metadata store; what lives here needs the blob
//! store's bytes in the same breath.

use angos_oci::{Digest, Namespace, UploadSessionId};

use crate::registry::{Error, blob_store::BlobStore, metadata_store::MetadataStore};

/// Promote the upload session's staged bytes to the canonical blob path and
/// grant `namespace` its reference. No lock is needed: fresh bytes and a fresh
/// `own` key sit inside the collector's grace period, and both steps are
/// idempotent.
pub async fn promote_and_grant(
    blob_store: &BlobStore,
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    session_key: &UploadSessionId,
    digest: &Digest,
    hashed_size: u64,
) -> Result<(), Error> {
    match blob_store.size(digest).await {
        Ok(_) => {
            // The bytes may be old, so the guarded grant catches a mid-flight
            // reclaim; only vanished bytes fall back to a fresh promotion.
            match grant_existing(blob_store, metadata_store, namespace, digest).await? {
                GrantOutcome::Granted => return Ok(()),
                GrantOutcome::BytesAbsent => {}
                GrantOutcome::ReclaimBlocked => {
                    return Err(Error::ReclamationInProgress(
                        "blob reclamation in progress; retry".to_string(),
                    ));
                }
            }
        }
        Err(Error::BlobUnknown | Error::NotFound) => {}
        Err(error) => return Err(error),
    }
    blob_store
        .complete_upload(namespace, session_key, digest, hashed_size)
        .await?;
    metadata_store.grant(namespace, digest).await
}

/// Outcome of a guarded grant against pre-existing bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantOutcome {
    /// The grant landed and the bytes are still present.
    Granted,
    /// The bytes are gone; the dangling grant is byteless and prune reaps it.
    BytesAbsent,
    /// An unexpired collector run still covers the digest after the backoff
    /// budget, so a reclaim may be mid-flight and the caller must fail closed.
    ReclaimBlocked,
}

/// Grant a reference to bytes that already exist (a mount, a cache fill, a
/// re-upload). The grant must land before the collector check and the
/// re-probe, so any reclaim that could still take the bytes is seen here;
/// anything but [`GrantOutcome::Granted`] must not be relied on.
pub async fn grant_existing(
    blob_store: &BlobStore,
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<GrantOutcome, Error> {
    metadata_store.grant(namespace, digest).await?;
    if !metadata_store.gc_clear(&[digest]).await? {
        return Ok(GrantOutcome::ReclaimBlocked);
    }
    match blob_store.size(digest).await {
        Ok(_) => Ok(GrantOutcome::Granted),
        Err(Error::BlobUnknown | Error::NotFound) => Ok(GrantOutcome::BytesAbsent),
        Err(error) => Err(error),
    }
}
