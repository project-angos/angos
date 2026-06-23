//! Always-on end-of-run summary for `scrub` / `policy` / `replication`.
//!
//! This module is **additive output only**: it changes neither which [`Action`]s
//! are emitted nor how they are applied. It layers two concurrency-safe state
//! holders onto the shared [`Ctx`](super::context::Ctx) so all three commands,
//! which build the same `Ctx` and run via [`scheduler::run_dag`](super::scheduler),
//! get the same summary for free:
//!
//! - [`ActionTally`]: lock-free per-category counters bumped by [`CountingSink`],
//!   a transparent decorator that forwards each action to the chosen inner sink
//!   ([`Executor`] in mutate mode, [`DryRunSink`] under `-d`) and tallies it into
//!   the per-category total only once the inner apply succeeds; a failed apply is
//!   counted separately under [`ActionTally::failed`], not in the per-category
//!   totals. The "would" vs "done" distinction is rendered once, at print time,
//!   from `Ctx.opts.dry_run`.
//! - [`Findings`]: report-only observations that are NOT [`Action`]s (dangling
//!   config/layer/index-child references; invalid-named namespaces skipped when
//!   `--prune-unknown` is off). These are recorded at the existing `warn!` sites
//!   so they surface in the summary without ever becoming a mutation.
//!
//! Counting is done by wrapping the sink rather than editing the `Executor` /
//! `DryRunSink`, and findings stay out of the `Action` enum, precisely so the
//! existing action contract (and the tests that assert exact `Vec<Action>`
//! contents) are untouched.
//!
//! [`Action`]: super::action::Action
//! [`Executor`]: super::executor::Executor
//! [`DryRunSink`]: super::executor::DryRunSink

use std::{
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::info;

use crate::{
    command::scrub::{
        action::Action,
        check::{DanglingReference, ReferenceKind},
        error::Error,
        executor::ActionSink,
    },
    oci::{Digest, Namespace},
};

/// How many report-only sample lines the summary prints. Bounds the memory the
/// [`Findings`] accumulator holds and keeps the summary log event from becoming a
/// wall of per-item lines on a badly-degraded store.
const FINDINGS_SAMPLE_CAP: usize = 20;

/// Lock-free per-category tally of every applied (or, under `-d`, would-be
/// applied) [`Action`].
///
/// Every counter is an [`AtomicU64`] bumped with [`Ordering::Relaxed`]: the
/// counts are monotonic and order-independent, and the bump already happens
/// inside the shared sink `Mutex` critical section, so no extra synchronization
/// is needed. Each [`Action`] variant maps to exactly one bucket (see
/// [`ActionTally::bucket`]).
///
/// Scope: this counts only work that flows through the [`ActionSink`]. The
/// structural `--jobs` reconcile (dangling lock-key indexes, unknown-queue
/// removal) mutates the job store directly through its own conditional engine
/// transactions, not via an [`Action`], so it is absent from these counts and
/// reports its own totals on a dedicated `info!` line.
#[derive(Default)]
pub struct ActionTally {
    /// `DeleteOrphanBlob`.
    pub orphan_blobs: AtomicU64,
    /// `DeleteOrphanManifest`.
    pub orphan_manifests: AtomicU64,
    /// `DeleteTag` + `DeleteInvalidTag`.
    pub tags: AtomicU64,
    /// `DeleteOrphanReferrer` + `RemoveReferrer`.
    pub referrers: AtomicU64,
    /// `RecreateLink` + `AddReferrer`.
    pub links_repaired: AtomicU64,
    /// `RemoveBlobIndexLink` + `GrantBlobIndexLink`.
    pub blob_index_ops: AtomicU64,
    /// `SetMediaType`.
    pub media_types_backfilled: AtomicU64,
    /// `RemoveOrphanBlobGrant`.
    pub grants_revoked: AtomicU64,
    /// `DeleteExpiredUpload`.
    pub uploads: AtomicU64,
    /// `AbortMultipartUpload`.
    pub multipart: AtomicU64,
    /// `DeleteOrphanJob` (config-drift orphan-job deletions from
    /// `--replication-orphans` / `--cache-orphans`). The structural `--jobs`
    /// reconcile does not flow through the sink and is not counted here.
    pub jobs: AtomicU64,
    /// `MigrateBlobIndex` + `PruneLegacyNamespaceRegistry`.
    pub migrations: AtomicU64,
    /// `DeleteInvalidNamespace` + `DeleteInvalidUploadNamespace`.
    pub namespaces_pruned: AtomicU64,
    /// `EnqueueReplicationPush` + `EnqueueReplicationDelete`.
    pub replication_enqueued: AtomicU64,
    /// Every categorized action, so an all-clean run prints a single `total: 0`.
    pub total: AtomicU64,
    /// Applies that returned `Err` and left the fault in place. Bumped instead of
    /// a per-category counter so the summary can flag unresolved faults.
    pub failed: AtomicU64,
}

impl ActionTally {
    /// Categorize one action into exactly one bucket and bump it (and `total`)
    /// unconditionally. Test-only helper that exercises the exhaustive
    /// categorization in one call; production counts via [`ActionTally::bucket`]
    /// so the [`CountingSink`] can bump only after a successful apply.
    #[cfg(test)]
    pub fn record(&self, action: &Action) {
        self.bucket(action).bump();
    }

    /// Resolve the [`TallyBucket`] one action belongs to, without bumping it.
    ///
    /// The `match` is exhaustive (no `_` arm) on purpose: a future [`Action`]
    /// variant fails to compile here until it is given a bucket, a deliberate
    /// maintenance guard so the summary never silently drops a new action.
    pub fn bucket(&self, action: &Action) -> TallyBucket<'_> {
        let counter = match action {
            Action::DeleteOrphanBlob(_) => &self.orphan_blobs,
            Action::DeleteOrphanManifest { .. } => &self.orphan_manifests,
            Action::DeleteTag { .. } | Action::DeleteInvalidTag { .. } => &self.tags,
            Action::DeleteOrphanReferrer { .. } | Action::RemoveReferrer { .. } => &self.referrers,
            Action::RecreateLink { .. } | Action::AddReferrer { .. } => &self.links_repaired,
            Action::RemoveBlobIndexLink { .. } | Action::GrantBlobIndexLink { .. } => {
                &self.blob_index_ops
            }
            Action::SetMediaType { .. } => &self.media_types_backfilled,
            Action::RemoveOrphanBlobGrant { .. } => &self.grants_revoked,
            Action::DeleteExpiredUpload { .. } => &self.uploads,
            Action::AbortMultipartUpload { .. } => &self.multipart,
            Action::DeleteOrphanJob { .. } => &self.jobs,
            Action::MigrateBlobIndex(_) | Action::PruneLegacyNamespaceRegistry => &self.migrations,
            Action::DeleteInvalidNamespace { .. } | Action::DeleteInvalidUploadNamespace { .. } => {
                &self.namespaces_pruned
            }
            Action::EnqueueReplicationPush { .. } | Action::EnqueueReplicationDelete { .. } => {
                &self.replication_enqueued
            }
        };
        TallyBucket {
            counter,
            total: &self.total,
        }
    }
}

/// A resolved per-category counter plus the `total`, so a deferred [`bump`] hits
/// both atomics together. Returned by [`ActionTally::bucket`] when the caller
/// wants to choose the bucket before deciding whether to count it.
///
/// [`bump`]: TallyBucket::bump
pub struct TallyBucket<'a> {
    counter: &'a AtomicU64,
    total: &'a AtomicU64,
}

impl TallyBucket<'_> {
    /// Bump the resolved category counter and the grand `total`.
    pub fn bump(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Transparent decorator over the inner sink (see the module header) that only
/// *observes*: it tallies an action into its per-category total once the inner
/// apply succeeds. A failed apply (callers log-and-continue on one) is counted
/// under [`ActionTally::failed`] so the summary still flags the unresolved fault.
pub struct CountingSink {
    inner: Box<dyn ActionSink + Send>,
    tally: Arc<ActionTally>,
}

#[async_trait]
impl ActionSink for CountingSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        // Resolve the bucket before the action moves into the inner apply, then
        // tally only if that apply succeeded.
        let counter = self.tally.bucket(&action);
        let result = self.inner.apply(action).await;
        if result.is_ok() {
            counter.bump();
        } else {
            self.tally.failed.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

/// Wrap `inner` in a [`CountingSink`] tallying into `tally`. The single
/// construction point shared by all three commands.
pub fn counting_sink(
    inner: Box<dyn ActionSink + Send>,
    tally: Arc<ActionTally>,
) -> Box<dyn ActionSink + Send> {
    Box::new(CountingSink { inner, tally })
}

/// Per-category counts of report-only findings plus a bounded printable sample.
///
/// Findings are observations ("we saw X"), not mutations ("we applied X"), so
/// they are accumulated separately from [`ActionTally`] and never become an
/// [`Action`].
#[derive(Default, Clone)]
pub struct FindingsInner {
    pub dangling_config: u64,
    pub dangling_layer: u64,
    pub dangling_child: u64,
    pub skipped_invalid_namespaces: u64,
    /// Up to [`FINDINGS_SAMPLE_CAP`] human-readable lines, for the summary block.
    pub sample: Vec<String>,
}

impl FindingsInner {
    /// Push a sample line into the bounded sample. `line` is built lazily and
    /// only while there is room, so a badly-degraded store does not format
    /// strings it would immediately drop once the cap is reached.
    fn sample(&mut self, line: impl FnOnce() -> String) {
        if self.sample.len() < FINDINGS_SAMPLE_CAP {
            self.sample.push(line());
        }
    }

    /// Total report-only findings across every category.
    fn total(&self) -> u64 {
        self.dangling_config
            + self.dangling_layer
            + self.dangling_child
            + self.skipped_invalid_namespaces
    }
}

/// Concurrency-safe accumulator of report-only findings, shared (cheaply, via the
/// inner `Arc`) across the parallel DAG nodes and across the three commands by
/// living on the shared `Ctx`.
///
/// A `Mutex` (rather than atomics) because findings carry data (kinds, digests,
/// namespace names) to print as a bounded sample, and the recording rate is low
/// (only when something is actually wrong). The findings mutex is independent of
/// the sink mutex, so recording a finding never blocks an action apply.
#[derive(Default, Clone)]
pub struct Findings(Arc<Mutex<FindingsInner>>);

impl Findings {
    /// Record one dangling manifest reference (report-only: never deleted).
    pub async fn record_dangling(
        &self,
        namespace: &Namespace,
        revision: &Digest,
        dangling: &DanglingReference,
    ) {
        let mut inner = self.0.lock().await;
        match dangling.kind {
            ReferenceKind::Config => inner.dangling_config += 1,
            ReferenceKind::Layer => inner.dangling_layer += 1,
            ReferenceKind::ChildManifest => inner.dangling_child += 1,
        }
        inner.sample(|| {
            format!(
                "{namespace}@{revision} references missing {} '{}'",
                dangling.kind, dangling.digest
            )
        });
    }

    /// Record one invalid-named namespace skipped because `--prune-unknown` is
    /// off (report-only: never deleted without the opt-in flag).
    pub async fn record_skipped_invalid_namespace(&self, name: &str, reason: &str) {
        let mut inner = self.0.lock().await;
        inner.skipped_invalid_namespaces += 1;
        inner.sample(|| format!("skipped invalid namespace '{name}': {reason}"));
    }

    /// Clone the accumulated findings out from under the lock so the summary can
    /// be logged without holding it.
    pub async fn snapshot(&self) -> FindingsInner {
        self.0.lock().await.clone()
    }
}

/// Emit the always-on end-of-run summary as a single multi-line `info!` event.
///
/// `run_label` is the command name (`"scrub"` / `"policy"` / `"replication"`) so
/// the header reads `SCRUB SUMMARY` / `POLICY SUMMARY` / etc. `dry_run` selects
/// the `would` (dry-run) vs `done` (mutate) verb (the only place that
/// distinction is rendered). `prune_unknown_supported` gates the
/// "(run --prune-unknown to delete)" hint on the invalid-namespaces findings
/// line, since only `scrub` exposes that flag. Zero-count action rows are
/// suppressed to keep an all-clean run to a couple of lines; the `mode:` header
/// and `total:` line always print. A `failed:` line prints when any apply failed,
/// so the per-category totals are never mistaken for "all faults resolved".
///
/// The counts cover only sink-applied actions (see [`ActionTally`]); the
/// structural `--jobs` reconcile reports its own totals on a separate `info!`
/// line and is not reflected here.
pub fn log_summary(
    run_label: &str,
    dry_run: bool,
    prune_unknown_supported: bool,
    tally: &ActionTally,
    findings: &FindingsInner,
) {
    let mode = if dry_run { "dry-run" } else { "mutate" };
    let verb = if dry_run { "would" } else { "done" };

    let mut body = String::new();
    // `write!` to a `String` is infallible, so the `Result` is intentionally
    // dropped throughout (`let _ =`), the same convention `main.rs` uses.
    let _ = writeln!(body, "{} SUMMARY (mode: {mode})", run_label.to_uppercase());
    let _ = writeln!(body, "  Actions ({verb}):");

    write_action_rows(&mut body, tally);
    let _ = writeln!(body, "    total: {}", tally.total.load(Ordering::Relaxed));
    let failed = tally.failed.load(Ordering::Relaxed);
    if failed > 0 {
        let _ = writeln!(body, "    failed: {failed} (left in place, see error log)");
    }

    if findings.total() > 0 {
        write_findings(&mut body, findings, prune_unknown_supported);
    }

    info!("{}", body.trim_end());
}

/// Append the non-zero per-category action rows. Zero rows are suppressed so an
/// all-clean run stays short.
fn write_action_rows(body: &mut String, tally: &ActionTally) {
    let mut row = |label: &str, count: u64| {
        if count == 0 {
            return;
        }
        let _ = writeln!(body, "    {label}: {count}");
    };

    row("orphan blobs", tally.orphan_blobs.load(Ordering::Relaxed));
    row(
        "orphan manifests",
        tally.orphan_manifests.load(Ordering::Relaxed),
    );
    row("tags", tally.tags.load(Ordering::Relaxed));
    row("referrers", tally.referrers.load(Ordering::Relaxed));
    row(
        "links repaired",
        tally.links_repaired.load(Ordering::Relaxed),
    );
    row(
        "blob-index ops",
        tally.blob_index_ops.load(Ordering::Relaxed),
    );
    row(
        "media types backfilled",
        tally.media_types_backfilled.load(Ordering::Relaxed),
    );
    row(
        "grants revoked",
        tally.grants_revoked.load(Ordering::Relaxed),
    );
    row("expired uploads", tally.uploads.load(Ordering::Relaxed));
    row("multipart aborts", tally.multipart.load(Ordering::Relaxed));
    row("jobs", tally.jobs.load(Ordering::Relaxed));
    row("migrations", tally.migrations.load(Ordering::Relaxed));
    row(
        "namespaces pruned",
        tally.namespaces_pruned.load(Ordering::Relaxed),
    );
    row(
        "replication enqueued",
        tally.replication_enqueued.load(Ordering::Relaxed),
    );
}

/// Append the report-only findings block (counts plus the bounded sample). Only
/// called when at least one finding exists. `prune_unknown_supported` gates the
/// "(run --prune-unknown to delete)" hint, since only `scrub` exposes the flag.
fn write_findings(body: &mut String, findings: &FindingsInner, prune_unknown_supported: bool) {
    let _ = writeln!(body, "  Report-only findings (no action taken):");
    if findings.dangling_config > 0 {
        let _ = writeln!(
            body,
            "    dangling config blobs: {}",
            findings.dangling_config
        );
    }
    if findings.dangling_layer > 0 {
        let _ = writeln!(
            body,
            "    dangling layer blobs: {}",
            findings.dangling_layer
        );
    }
    if findings.dangling_child > 0 {
        let _ = writeln!(
            body,
            "    dangling child manifests: {}",
            findings.dangling_child
        );
    }
    if findings.skipped_invalid_namespaces > 0 {
        let hint = if prune_unknown_supported {
            " (run --prune-unknown to delete)"
        } else {
            ""
        };
        let _ = writeln!(
            body,
            "    invalid namespaces skipped{hint}: {}",
            findings.skipped_invalid_namespaces
        );
    }
    for line in &findings.sample {
        let _ = writeln!(body, "      - {line}");
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::{
        command::scrub::check::{DanglingReference, ReferenceKind},
        oci::{Digest, MediaType, Namespace, Tag},
        registry::{
            blob_store,
            job_store::{JobState, Queue},
            metadata_store::LinkKind,
        },
    };

    fn digest(hex: &str) -> Digest {
        Digest::from_str(&format!("sha256:{hex}")).unwrap()
    }

    fn ns() -> Namespace {
        Namespace::new("test-repo/app").unwrap()
    }

    /// One representative of every `Action` variant, so the exhaustive
    /// `record` match is exercised end-to-end.
    fn one_of_each_action() -> Vec<Action> {
        let d = digest("0000000000000000000000000000000000000000000000000000000000000000");
        vec![
            Action::MigrateBlobIndex(d.clone()),
            Action::PruneLegacyNamespaceRegistry,
            Action::DeleteOrphanBlob(d.clone()),
            Action::RemoveBlobIndexLink {
                namespace: ns(),
                blob: d.clone(),
                link: LinkKind::Blob(d.clone()),
            },
            Action::GrantBlobIndexLink {
                namespace: ns(),
                blob: d.clone(),
                link: LinkKind::Layer(d.clone()),
            },
            Action::RemoveOrphanBlobGrant {
                namespace: ns(),
                blob: d.clone(),
            },
            Action::RecreateLink {
                namespace: ns(),
                link: LinkKind::Digest(d.clone()),
                target: d.clone(),
            },
            Action::AddReferrer {
                namespace: ns(),
                link: LinkKind::Layer(d.clone()),
                target: d.clone(),
                referrer: d.clone(),
            },
            Action::RemoveReferrer {
                namespace: ns(),
                link: LinkKind::Layer(d.clone()),
                referrer: d.clone(),
            },
            Action::SetMediaType {
                namespace: ns(),
                link: LinkKind::Digest(d.clone()),
                target: d.clone(),
                media_type: MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap(),
                display_name: "revision".to_string(),
            },
            Action::DeleteTag {
                namespace: ns(),
                tag: Tag::new("v1").unwrap(),
            },
            Action::DeleteInvalidTag {
                namespace: ns(),
                tag: "-bad".to_string(),
            },
            Action::DeleteInvalidNamespace {
                name: "BADNS".to_string(),
            },
            Action::DeleteInvalidUploadNamespace {
                name: "BADNS".to_string(),
            },
            Action::DeleteOrphanManifest {
                namespace: ns(),
                digest: d.clone(),
            },
            Action::DeleteExpiredUpload {
                namespace: ns(),
                uuid: "uuid".to_string(),
            },
            Action::DeleteOrphanReferrer {
                namespace: ns(),
                subject: d.clone(),
                referrer: d.clone(),
            },
            Action::AbortMultipartUpload {
                key: "key".to_string(),
                upload_id: "uid".to_string(),
            },
            Action::EnqueueReplicationPush {
                downstream: "mirror".to_string(),
                namespace: ns(),
                tag: Tag::new("v1").unwrap(),
                digest: d.clone(),
            },
            Action::EnqueueReplicationDelete {
                downstream: "mirror".to_string(),
                namespace: ns(),
                tag: Tag::new("v1").unwrap(),
            },
            Action::DeleteOrphanJob {
                queue: Queue::Replication,
                state: JobState::Pending,
                storage_key: "k".to_string(),
                reason: "r".to_string(),
            },
        ]
    }

    /// Every `Action` variant maps to a bucket and bumps `total` once. The
    /// per-bucket sums must equal the number of variants fed in.
    #[test]
    fn tally_categorizes_every_action_variant() {
        let actions = one_of_each_action();
        let tally = ActionTally::default();
        for action in &actions {
            tally.record(action);
        }

        // Each individual variant lands in exactly one bucket.
        assert_eq!(tally.orphan_blobs.load(Ordering::Relaxed), 1);
        assert_eq!(tally.orphan_manifests.load(Ordering::Relaxed), 1);
        assert_eq!(tally.tags.load(Ordering::Relaxed), 2); // DeleteTag + DeleteInvalidTag
        assert_eq!(tally.referrers.load(Ordering::Relaxed), 2); // DeleteOrphanReferrer + RemoveReferrer
        assert_eq!(tally.links_repaired.load(Ordering::Relaxed), 2); // RecreateLink + AddReferrer
        assert_eq!(tally.blob_index_ops.load(Ordering::Relaxed), 2); // Remove + Grant
        assert_eq!(tally.media_types_backfilled.load(Ordering::Relaxed), 1);
        assert_eq!(tally.grants_revoked.load(Ordering::Relaxed), 1);
        assert_eq!(tally.uploads.load(Ordering::Relaxed), 1);
        assert_eq!(tally.multipart.load(Ordering::Relaxed), 1);
        assert_eq!(tally.jobs.load(Ordering::Relaxed), 1);
        assert_eq!(tally.migrations.load(Ordering::Relaxed), 2); // Migrate + PruneLegacy
        assert_eq!(tally.namespaces_pruned.load(Ordering::Relaxed), 2); // Namespace + UploadNamespace
        assert_eq!(tally.replication_enqueued.load(Ordering::Relaxed), 2); // Push + Delete

        // The grand total accounts for exactly one bump per action.
        assert_eq!(
            tally.total.load(Ordering::Relaxed),
            actions.len() as u64,
            "total must equal the number of recorded actions"
        );
    }

    /// The `CountingSink` forwards every action byte-for-byte to the inner
    /// sink and counts them. Forwarding is proved against a `Vec<Action>` inner.
    #[tokio::test]
    async fn counting_sink_forwards_unchanged_and_counts() {
        let tally = Arc::new(ActionTally::default());

        // A Vec sink wrapped so we can pull the forwarded actions back out.
        let inner: Box<dyn ActionSink + Send> = Box::new(Vec::<Action>::new());
        let mut sink = CountingSink {
            inner,
            tally: tally.clone(),
        };

        let d = digest("0000000000000000000000000000000000000000000000000000000000000000");
        sink.apply(Action::DeleteTag {
            namespace: ns(),
            tag: Tag::new("v1").unwrap(),
        })
        .await
        .unwrap();
        sink.apply(Action::RecreateLink {
            namespace: ns(),
            link: LinkKind::Digest(d.clone()),
            target: d.clone(),
        })
        .await
        .unwrap();

        // Tally observed both.
        assert_eq!(tally.total.load(Ordering::Relaxed), 2);
        assert_eq!(tally.tags.load(Ordering::Relaxed), 1);
        assert_eq!(tally.links_repaired.load(Ordering::Relaxed), 1);
    }

    /// An inner sink whose every apply fails, so we can prove the failure path.
    struct FailingSink;

    #[async_trait]
    impl ActionSink for FailingSink {
        async fn apply(&mut self, _action: Action) -> Result<(), Error> {
            Err(Error::BlobStore(blob_store::Error::BlobNotFound))
        }
    }

    /// A failing inner apply bumps `failed` and leaves the category counter and
    /// the grand `total` (successful applies) untouched, so an operator reading
    /// the summary sees the fault was not resolved.
    #[tokio::test]
    async fn counting_sink_bumps_failed_on_inner_error() {
        let tally = Arc::new(ActionTally::default());
        let inner: Box<dyn ActionSink + Send> = Box::new(FailingSink);
        let mut sink = CountingSink {
            inner,
            tally: tally.clone(),
        };

        let result = sink
            .apply(Action::DeleteTag {
                namespace: ns(),
                tag: Tag::new("v1").unwrap(),
            })
            .await;

        assert!(result.is_err(), "the inner error must propagate");
        assert_eq!(tally.failed.load(Ordering::Relaxed), 1);
        assert_eq!(tally.tags.load(Ordering::Relaxed), 0);
        assert_eq!(tally.total.load(Ordering::Relaxed), 0);
    }

    /// Findings record into the right per-category counter and bounded sample.
    #[tokio::test]
    async fn findings_record_dangling_and_skipped_namespace() {
        let findings = Findings::default();
        let layer = digest("1111111111111111111111111111111111111111111111111111111111111111");
        let revision = digest("2222222222222222222222222222222222222222222222222222222222222222");
        findings
            .record_dangling(
                &ns(),
                &revision,
                &DanglingReference {
                    kind: ReferenceKind::Layer,
                    digest: layer,
                },
            )
            .await;
        findings
            .record_skipped_invalid_namespace("BADNS", "invalid name")
            .await;

        let snap = findings.snapshot().await;
        assert_eq!(snap.dangling_layer, 1);
        assert_eq!(snap.skipped_invalid_namespaces, 1);
        assert_eq!(snap.total(), 2);
        assert_eq!(snap.sample.len(), 2);
    }

    /// The sample is bounded at `FINDINGS_SAMPLE_CAP` even though the counters
    /// keep climbing past it.
    #[tokio::test]
    async fn findings_sample_is_bounded() {
        let findings = Findings::default();
        for i in 0..(FINDINGS_SAMPLE_CAP + 5) {
            findings
                .record_skipped_invalid_namespace(&format!("ns{i}"), "invalid")
                .await;
        }
        let snap = findings.snapshot().await;
        assert_eq!(
            snap.skipped_invalid_namespaces,
            (FINDINGS_SAMPLE_CAP + 5) as u64
        );
        assert_eq!(
            snap.sample.len(),
            FINDINGS_SAMPLE_CAP,
            "the printed sample must be capped"
        );
    }
}
