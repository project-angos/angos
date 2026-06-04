use std::collections::HashSet;

/// HTTP header carrying the originating instance-id of a replication request.
///
/// The value is the *original-author* instance-id, propagated verbatim across
/// every hop of a replication mesh so that loops can be detected regardless of
/// path length.
pub const X_ANGOS_ORIGIN: &str = "X-Angos-Origin";

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

/// Decides whether a replication event should be processed (`true` = keep) or
/// dropped (`false`) based on its origin.
///
/// This is a pure function with no I/O; it is invoked both before enqueue (event
/// path) and again inside the job handler (defensive).
///
/// An event is dropped when:
/// 1. its `origin` equals this instance's `instance_id` (self-bounce), or
/// 2. its `origin` is in the set of `known` downstream instance-ids
///    (downstream-bounce).
///
/// An event with no origin (`None`) is always kept: it is a fresh local change
/// that has not yet been attributed to any instance.
#[must_use]
pub fn loop_filter(origin: Option<&str>, instance_id: &str, known: &HashSet<String>) -> bool {
    match origin {
        None => true,
        Some(origin) => origin != instance_id && !known.contains(origin),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::replication::{
        REPLICATION_SUPERSEDED_CODE, X_ANGOS_ORIGIN, X_ANGOS_SOURCE_TIMESTAMP, loop_filter,
    };

    fn known_set(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn header_name_is_stable() {
        assert_eq!(X_ANGOS_ORIGIN, "X-Angos-Origin");
        assert_eq!(X_ANGOS_SOURCE_TIMESTAMP, "X-Angos-Source-Timestamp");
        assert_eq!(REPLICATION_SUPERSEDED_CODE, "REPLICATION_SUPERSEDED");
    }

    #[test]
    fn missing_origin_is_kept() {
        let known = known_set(&["b", "c"]);
        assert!(loop_filter(None, "a", &known));
    }

    #[test]
    fn self_origin_is_dropped() {
        let known = known_set(&["b", "c"]);
        assert!(!loop_filter(Some("a"), "a", &known));
    }

    #[test]
    fn known_origin_is_dropped() {
        let known = known_set(&["b", "c"]);
        assert!(!loop_filter(Some("b"), "a", &known));
        assert!(!loop_filter(Some("c"), "a", &known));
    }

    #[test]
    fn unknown_foreign_origin_is_kept() {
        let known = known_set(&["b", "c"]);
        assert!(loop_filter(Some("d"), "a", &known));
    }

    #[test]
    fn empty_known_set_only_drops_self() {
        let known = HashSet::new();
        assert!(!loop_filter(Some("a"), "a", &known));
        assert!(loop_filter(Some("b"), "a", &known));
    }
}
