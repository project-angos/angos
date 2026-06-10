//! Replication wire constants: the header and OCI error code carried on
//! replication manifest writes for receiver-side last-writer-wins, and the
//! `Accept` set stamped on downstream manifest probes.

use crate::oci::{
    DOCKER_MANIFEST_LIST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE, OCI_INDEX_MEDIA_TYPE,
    OCI_MANIFEST_MEDIA_TYPE,
};

/// The manifest media types stamped as `Accept` headers on every downstream
/// manifest probe (the converged-skip HEAD, the reconcile HEAD, the
/// referrers-fallback GET).
///
/// Without them a content-negotiating registry may return a CONVERTED
/// representation: the probe digest then never matches the local one (every
/// push re-transfers a converged manifest, and every reconcile pass re-enqueues
/// it), and the referrers-fallback GET can come back unparseable and
/// dead-letter the job — precisely on the third-party OCI-1.0 downstreams the
/// fallback exists for.
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

/// HTTP header carrying the originating event timestamp (RFC 3339) of a
/// replication request, used by the receiver for last-writer-wins: the receiver
/// compares this against the local tag's `created_at` and rejects with a 409
/// carrying [`REPLICATION_SUPERSEDED_CODE`] when the local copy is strictly
/// newer.
pub const X_ANGOS_SOURCE_TIMESTAMP: &str = "X-Angos-Source-Timestamp";

/// OCI error `code` returned by the receiver when a replication push/delete is
/// rejected by last-writer-wins (the local tag is strictly newer than the
/// incoming `source_ts`).
///
/// Shared by sender and receiver so the sender can disambiguate an LWW-loss
/// (convergence — the job completes) from any other 409 (e.g. an immutable-tag
/// conflict, which must surface so the job retries/dead-letters).
pub const REPLICATION_SUPERSEDED_CODE: &str = "REPLICATION_SUPERSEDED";
