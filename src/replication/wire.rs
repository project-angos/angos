//! Replication wire constants: the header and OCI error code carried on
//! replication manifest writes for receiver-side last-writer-wins.

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

#[cfg(test)]
mod tests {
    use crate::replication::{REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP};

    #[test]
    fn header_name_is_stable() {
        assert_eq!(X_ANGOS_SOURCE_TIMESTAMP, "X-Angos-Source-Timestamp");
        assert_eq!(REPLICATION_SUPERSEDED_CODE, "REPLICATION_SUPERSEDED");
    }
}
