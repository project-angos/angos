//! Cron-visible run marker for the scrub sweep.
//!
//! `_scrub-audit/latest.json` is overwritten at the end of every run with the
//! terminal status, exit code, and tallies, so a watcher reads one object for
//! the last outcome. An absent or stale marker signals a silent GC stall. The
//! log lines are the audit trail of individual deletions.

use angos_storage::Error as StorageError;
use angos_tx_engine::store::Store;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Run-epoch timestamp format naming the run in the marker.
const RUN_TS_FORMAT: &str = "%Y%m%dT%H%M%SZ";

/// Bucket-root prefix holding the run marker, shared with the key classifier.
pub const SCRUB_AUDIT_PREFIX: &str = "_scrub-audit";

/// Stable key of the liveness / result marker, overwritten each run so a watcher
/// reads one object for the last outcome.
const LATEST_MARKER_KEY: &str = "_scrub-audit/latest.json";

/// The terminal disposition of a maintenance run (`scrub`, `policy`,
/// `replication`), and the single source of the process exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarkerStatus {
    /// Ran clean.
    Clean,
    /// Refused at entry (lock held, or a mutating run on a `memory` strategy).
    Refused,
    /// Completed degraded (a failed delete/convergence, a partial de-configured
    /// delete, a failed namespace walk, or an incomplete enumeration).
    Degraded,
    /// Aborted by the cancellation gate (lock lost or max hold elapsed).
    Aborted,
}

impl MarkerStatus {
    /// The process exit code for this disposition. Schedulers treat `2` and `3`
    /// as failed runs.
    pub fn exit_code(self) -> i32 {
        match self {
            MarkerStatus::Clean => 0,
            MarkerStatus::Refused => 1,
            MarkerStatus::Degraded => 2,
            MarkerStatus::Aborted => 3,
        }
    }
}

/// How the run was allowed to mutate. Distinguishes performed deletions from
/// previews: under `ReportOnly` and `DryRun` every removal/reap tally holds a
/// would-count and nothing was deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunMode {
    /// `--commit`: the tallies count performed mutations.
    Commit,
    /// The default: classification only, the tallies are would-counts.
    ReportOnly,
    /// The deprecated `--dry-run` alias: fully read-only, would-counts.
    DryRun,
}

/// The sweep counts in the marker, mirroring the run's `SweepTally` plus the
/// action counter that lives outside it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarkerTallies {
    pub total: u64,
    /// Unrecognized keys deleted (or would-delete under report-only).
    pub deleted: u64,
    pub orphan_reaped: u64,
    pub token_rejected: u64,
    pub converged: u64,
    pub failed: u64,
    pub deconfigured_namespaces: u64,
    pub deconfigured_deleted: u64,
    pub deconfigured_partial: u64,
    pub deconfigured_grants_revoked: u64,
    pub deconfigured_grants_partial: u64,
    pub deconfigured_uploads_deleted: u64,
    pub deconfigured_uploads_partial: u64,
    pub stale_grants_removed: u64,
    pub self_grants_revoked: u64,
    pub orphan_links_removed: u64,
    pub missing_bodies_removed: u64,
    pub unrecognized: u64,
    /// Reap candidates kept because the backend reported no `last_modified`.
    pub age_unknown_kept: u64,
    /// Gated sweep deletes that returned an error and left the object in place.
    pub failed_deletes: u64,
    /// Action-sink applies that returned `Err` and left the fault in place.
    pub action_failed: u64,
}

/// The liveness / result marker written to `_scrub-audit/latest.json` at run end.
/// Carries the run timestamps, terminal status and exit code, the run mode
/// (whether the tallies are performed deletions or would-counts), sweep
/// tallies, the namespaces whose metadata walk or rebuild errored (their
/// destructive sweep passes were skipped), and whether the namespace
/// enumeration itself completed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMarker {
    pub run_ts: String,
    pub started_at: String,
    pub finished_at: String,
    pub status: MarkerStatus,
    pub exit_code: i32,
    pub mode: RunMode,
    pub tallies: MarkerTallies,
    pub failed_namespaces: Vec<String>,
    pub walk_complete: bool,
}

impl RunMarker {
    /// Build the run-ts anchor naming the run in the marker.
    pub fn run_ts_for(run_epoch: DateTime<Utc>) -> String {
        run_epoch.format(RUN_TS_FORMAT).to_string()
    }
}

/// Write the marker to `_scrub-audit/latest.json` on `store`. Best-effort at the
/// call site.
pub async fn write_latest_marker(store: &Store, marker: &RunMarker) -> Result<(), StorageError> {
    let body = serde_json::to_vec(marker)
        .map_err(|e| StorageError::Backend(format!("serialize scrub run marker: {e}")))?;
    store.put(LATEST_MARKER_KEY, Bytes::from(body)).await
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn run_ts_matches_marker_format() {
        let epoch = Utc.with_ymd_and_hms(2026, 6, 30, 12, 0, 0).unwrap();
        assert_eq!(RunMarker::run_ts_for(epoch), "20260630T120000Z");
    }

    /// The marker's mode strings are a watcher-facing contract.
    #[test]
    fn run_mode_serializes_kebab_case() {
        assert_eq!(
            serde_json::to_string(&RunMode::Commit).unwrap(),
            "\"commit\""
        );
        assert_eq!(
            serde_json::to_string(&RunMode::ReportOnly).unwrap(),
            "\"report-only\""
        );
        assert_eq!(
            serde_json::to_string(&RunMode::DryRun).unwrap(),
            "\"dry-run\""
        );
    }
}
