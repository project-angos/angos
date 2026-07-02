//! Shared context threaded through the scrub nodes.
//!
//! [`Ctx`] bundles the stores, the flag gates ([`ScrubFlags`]), and the shared
//! [`ActionSink`] (behind a `Mutex` so parallel namespace tasks serialize their
//! applies while their enumeration fans out).

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::registry::metadata_store::MetadataStore;

use super::{
    command::Options,
    executor::ActionSink,
    report::{ActionTally, Findings},
};

/// Ceiling applied when clamping `max_concurrent_scrub_tasks` into the per-node
/// fan-out budget, so a mis-set value cannot exhaust memory or the backend
/// connection pool. A larger configured value is clamped with a warning.
pub const MAX_FANOUT: usize = 1024;

/// Flag-derived configuration for a scrub run, so node-builders read the gates
/// without re-touching argh `Options`.
#[allow(clippy::struct_excessive_bools)]
pub struct ScrubFlags {
    pub dry_run: bool,
    /// Effective deletion gate for the Action path: `scrub` sets it to `--commit`,
    /// `policy`/`replication` to `!--dry-run`. When false the sink is the
    /// `DryRunSink` and the summary renders "would" rather than "done".
    pub action_mutate: bool,
    pub retention: bool,
    pub replicate: bool,
    /// Opt-in gate for every destructive validity action (invalid-named namespace
    /// delete, unknown job-queue removal); report-only when unset.
    pub prune_unknown: bool,
    /// Whether `--uploads <duration>` was passed; the duration is re-parsed by the
    /// `UploadChecker` builder in `setup`.
    pub uploads: bool,
    pub fanout: usize,
}

impl ScrubFlags {
    /// Project the argh `Options` into the flag gates. `fanout` is the per-node
    /// concurrency budget (`max_concurrent_scrub_tasks`); `action_mutate` is the
    /// caller-computed deletion gate.
    pub fn from_options(options: &Options, fanout: usize, action_mutate: bool) -> Self {
        Self {
            dry_run: options.dry_run,
            action_mutate,
            retention: options.retention,
            replicate: options.replicate,
            prune_unknown: options.prune_unknown,
            uploads: options.uploads.is_some(),
            fanout,
        }
    }

    /// True when a flag-driven namespace-scoped checker (`retention`, `replicate`,
    /// `uploads`) is enabled. Scrub's metadata node also runs when its
    /// unconditional rebuild checker is present; the command gates on that
    /// separately.
    pub fn metadata_enabled(&self) -> bool {
        self.retention || self.replicate || self.uploads
    }
}

/// Shared, cloneable context handed to every scrub node body.
pub struct Ctx {
    pub metadata_store: Arc<MetadataStore>,
    /// Shared sink behind a `Mutex`; each task wraps it in a `SharedSink` locking
    /// per apply, so namespace tasks serialize mutations while enumeration fans
    /// out. Wraps either an `Executor` or `DryRunSink`.
    pub sink: Arc<Mutex<Box<dyn ActionSink + Send>>>,
    pub opts: ScrubFlags,
    /// Lock-free per-category tally, bumped by the `CountingSink` and read once
    /// for the end-of-run summary.
    pub tally: Arc<ActionTally>,
    /// Report-only findings recorded at the `warn!` sites; never an `Action`.
    pub findings: Findings,
    /// Whether the subcommand exposes `--prune-unknown` (only `scrub`), gating the
    /// "run --prune-unknown to delete" hints.
    pub prune_unknown_supported: bool,
    /// Run-level cancellation, fired when the maintenance lock is lost or its
    /// max hold elapses. The sink rejects every apply after it fires, and the
    /// walks check it per item to stop enumerating.
    pub cancel: CancellationToken,
}
