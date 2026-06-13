//! Wire constants for replication last-writer-wins and downstream manifest probes.

use crate::oci::{
    DOCKER_MANIFEST_LIST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE, OCI_INDEX_MEDIA_TYPE,
    OCI_MANIFEST_MEDIA_TYPE,
};

/// Manifest media types stamped as `Accept` headers on every downstream
/// manifest probe. Without them a content-negotiating registry may return a
/// converted representation whose digest never matches the local one.
#[must_use]
pub fn manifest_accept_types() -> Vec<String> {
    [
        OCI_MANIFEST_MEDIA_TYPE,
        OCI_INDEX_MEDIA_TYPE,
        DOCKER_MANIFEST_MEDIA_TYPE,
        DOCKER_MANIFEST_LIST_MEDIA_TYPE,
    ]
    .map(str::to_string)
    .to_vec()
}

/// Header carrying the originating event timestamp (RFC 3339) of a replication
/// request; the receiver rejects with a 409 [`REPLICATION_SUPERSEDED_CODE`]
/// when its local tag is strictly newer (last-writer-wins).
pub const X_ANGOS_SOURCE_TIMESTAMP: &str = "X-Angos-Source-Timestamp";

/// OCI error `code` returned when a replication write loses last-writer-wins.
/// Shared by sender and receiver so the sender can treat this 409 as
/// convergence (job completes) while any other 409 still retries/dead-letters.
pub const REPLICATION_SUPERSEDED_CODE: &str = "REPLICATION_SUPERSEDED";
