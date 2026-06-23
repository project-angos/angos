//! Shared context threaded through the scrub DAG nodes.
//!
//! [`Ctx`] bundles the stores, the parsed flag gates ([`ScrubFlags`]), and the
//! shared [`ActionSink`] (behind a `Mutex` so parallel namespace tasks serialize
//! their *applies* while their *enumeration* fans out). Every node body receives
//! an `Arc<Ctx>`.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::registry::metadata_store::MetadataStore;

use super::{
    command::Options,
    executor::ActionSink,
    report::{ActionTally, Findings},
};

/// Ceiling applied when clamping `max_concurrent_scrub_tasks` into the per-node
/// fan-out budget. Caps how many namespaces a single node enumerates in flight,
/// bounding concurrent metadata-store probes and `SharedSink` handles so a
/// mis-set value cannot exhaust memory or the backend connection pool. A
/// configured value above this is clamped (with a warning).
pub const MAX_FANOUT: usize = 1024;

/// Flag-derived configuration for a scrub run. One struct so node-builders read
/// the gates without re-touching argh `Options`.
#[allow(clippy::struct_excessive_bools)]
pub struct ScrubFlags {
    pub dry_run: bool,
    pub tags: bool,
    pub manifests: bool,
    pub links: bool,
    pub referrers: bool,
    pub media_types: bool,
    pub reconcile_blob_index: bool,
    pub retention: bool,
    pub replicate: bool,
    /// `--prune-unknown`: opt-in gate for every net-new destructive validity
    /// action (invalid-named namespace delete, unknown job-queue removal). Off
    /// by default so no existing flag deletes more than on main; report-only
    /// (a `warn!`, no `Action`) when unset.
    pub prune_unknown: bool,
    /// Whether `--uploads <duration>` was passed. Only the gate is kept here; the
    /// duration itself is re-parsed by the `UploadChecker` builder in `setup`.
    pub uploads: bool,
    pub fanout: usize,
}

impl ScrubFlags {
    /// Project the argh `Options` into the flag gates. `fanout` is the per-node
    /// concurrency budget from the loaded configuration
    /// (`max_concurrent_scrub_tasks`).
    pub fn from_options(options: &Options, fanout: usize) -> Self {
        Self {
            dry_run: options.dry_run,
            tags: options.tags,
            manifests: options.manifests,
            links: options.links,
            referrers: options.referrers,
            media_types: options.media_types,
            reconcile_blob_index: options.reconcile_blob_index,
            retention: options.retention,
            replicate: options.replicate,
            prune_unknown: options.prune_unknown,
            uploads: options.uploads.is_some(),
            fanout,
        }
    }

    /// True when any namespace-scoped step is enabled, i.e. the `metadata` node
    /// must run. Mirrors the union the `metadata` node gate describes.
    pub fn metadata_enabled(&self) -> bool {
        self.tags
            || self.manifests
            || self.links
            || self.referrers
            || self.media_types
            || self.reconcile_blob_index
            || self.retention
            || self.replicate
            || self.uploads
    }
}

/// Shared, cloneable context handed to every scrub DAG node body.
pub struct Ctx {
    pub metadata_store: Arc<MetadataStore>,
    /// Shared sink behind a `Mutex`; each task wraps it in a `SharedSink` that
    /// locks per apply, so parallel namespace tasks serialize their mutations
    /// one-at-a-time while their enumeration fans out. Wraps either an `Executor`
    /// (mutate) or `DryRunSink` (`-d`).
    pub sink: Arc<Mutex<Box<dyn ActionSink + Send>>>,
    pub opts: ScrubFlags,
    /// Lock-free per-category tally of every applied action, bumped by the
    /// `CountingSink` wrapping `sink`. Read once at end-of-run for the summary.
    /// Shared (cheaply, via the `Arc`) so the wrapper and the print site see the
    /// same counters; all three commands build it the same way.
    pub tally: Arc<ActionTally>,
    /// Report-only findings (dangling references, skipped invalid namespaces)
    /// recorded at the existing `warn!` sites. Never becomes an `Action`; surfaced
    /// in the end-of-run summary alongside `tally`.
    pub findings: Findings,
    /// Whether the running subcommand exposes `--prune-unknown`. Only `scrub`
    /// does, so the report-only "run --prune-unknown to delete" hints are gated
    /// on this to avoid steering `policy`/`replication` operators at a flag they
    /// cannot pass.
    pub prune_unknown_supported: bool,
}
