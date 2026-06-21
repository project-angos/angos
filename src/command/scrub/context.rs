//! Shared context threaded through the scrub DAG nodes.
//!
//! [`Ctx`] bundles the stores, the parsed flag gates ([`ScrubFlags`]), the
//! shared [`ActionSink`] (behind a `Mutex` so parallel namespace/blob tasks
//! serialize their *applies* while their *enumeration* fans out), and the
//! shared fan-out [`Semaphore`]. Every node body receives an `Arc<Ctx>`.

use std::sync::Arc;

use chrono::Duration;
use tokio::sync::{Mutex, Semaphore};

use crate::registry::{
    blob_store::BlobStore, metadata_store::MetadataStore, repository_resolver::RepositoryResolver,
};

use super::{command::Options, error::Error, executor::ActionSink};

/// Default per-node fan-out budget: how many namespaces/blobs may be in-flight
/// (doing lock-free enumeration) at once. The shared sink still serializes the
/// actual `apply`, so this never changes the set of emitted actions.
pub(crate) const DEFAULT_FANOUT: usize = 16;

/// Convert a CLI `humantime::Duration` flag to a `chrono::Duration`, surfacing a
/// bad value as an initialization error (same wording the `setup::*` builders
/// use, kept for parity).
fn to_chrono(d: humantime::Duration, what: &str) -> Result<Duration, Error> {
    Duration::from_std(d.into())
        .map_err(|e| Error::Initialization(format!("{what} is invalid: {e}")))
}

/// Flag-derived configuration for a scrub run. One struct so node-builders read
/// the gates without re-touching argh `Options`; durations are pre-converted to
/// `chrono::Duration` so a parse failure is surfaced once, at construction.
///
/// Several fields are not yet read in this STEP-A re-orchestration (they gate
/// the blob fan-out and the jobs/prune nodes landing in later steps); the allow
/// keeps the design's full shape without a per-field annotation churn.
#[allow(dead_code)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct ScrubFlags {
    pub dry_run: bool,
    pub tags: bool,
    pub manifests: bool,
    pub links: bool,
    pub referrers: bool,
    pub media_types: bool,
    pub reconcile_blob_index: bool,
    pub blobs: bool,
    pub retention: bool,
    pub replicate: bool,
    pub migrate: bool,
    pub orphan_namespaces: bool,
    /// `--jobs`: enable the structural job reconcile node (lock-key index
    /// reconcile + unknown-queue removal under `prune_unknown`).
    pub jobs: bool,
    /// `--prune-unknown`: opt-in gate for every net-new destructive validity
    /// action (invalid-named namespace delete, unknown job-queue removal). Off
    /// by default so no existing flag deletes more than on main; report-only
    /// (a `warn!`, no `Action`) when unset.
    pub prune_unknown: bool,
    pub upload_timeout: Option<Duration>,
    pub multipart_timeout: Option<Duration>,
    pub orphan_grant_age: Option<Duration>,
    pub replication_orphans: bool,
    pub cache_orphans: bool,
    pub fanout: usize,
}

impl ScrubFlags {
    /// Parse the argh `Options` into the flag gates, converting the duration
    /// options to `chrono::Duration` (surfacing a bad value as an error).
    pub(crate) fn from_options(options: &Options) -> Result<Self, Error> {
        Ok(Self {
            dry_run: options.dry_run,
            tags: options.tags,
            manifests: options.manifests,
            links: options.links,
            referrers: options.referrers,
            media_types: options.media_types,
            reconcile_blob_index: options.reconcile_blob_index,
            blobs: options.blobs,
            retention: options.retention,
            replicate: options.replicate,
            migrate: options.migrate,
            orphan_namespaces: options.orphan_namespaces,
            jobs: options.jobs,
            prune_unknown: options.prune_unknown,
            upload_timeout: options
                .uploads
                .map(|d| to_chrono(d, "Upload timeout"))
                .transpose()?,
            multipart_timeout: options
                .multipart
                .map(|d| to_chrono(d, "Multipart timeout"))
                .transpose()?,
            orphan_grant_age: options
                .orphan_grants
                .map(|d| to_chrono(d, "Orphan-grant age"))
                .transpose()?,
            replication_orphans: options.replication_orphans,
            cache_orphans: options.cache_orphans,
            fanout: DEFAULT_FANOUT,
        })
    }

    /// True when any namespace-scoped step is enabled, i.e. the `metadata` node
    /// must run. Mirrors the union the plan's `metadata` node gate describes.
    pub(crate) fn metadata_enabled(&self) -> bool {
        self.tags
            || self.manifests
            || self.links
            || self.referrers
            || self.media_types
            || self.reconcile_blob_index
            || self.retention
            || self.replicate
            || self.upload_timeout.is_some()
    }
}

/// Shared, cloneable context handed to every scrub DAG node body.
///
/// `blob_store` and `resolver` are held for the later blob-fan-out / jobs nodes
/// and are not yet read by the STEP-A node bodies (which drive pre-built
/// checkers that already own their store handles); the allow keeps the shape.
#[allow(dead_code)]
pub(crate) struct Ctx {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub resolver: Arc<RepositoryResolver>,
    /// Shared sink behind a `Mutex` so parallel namespace/blob tasks serialize
    /// their action application (mutations stay one-at-a-time; only enumeration
    /// fans out). Wraps either an `Executor` (mutate) or `DryRunSink` (`-d`).
    pub sink: Arc<Mutex<Box<dyn ActionSink + Send>>>,
    pub opts: ScrubFlags,
    /// Shared fan-out budget; cloned into each node body and used via
    /// `acquire_owned` to bound per-item concurrency.
    pub sem: Arc<Semaphore>,
}
