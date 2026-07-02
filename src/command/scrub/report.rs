//! Always-on end-of-run summary for `scrub` / `policy` / `replication`.
//!
//! Additive output only: it changes neither which [`Action`]s are emitted nor how
//! they are applied. It layers two concurrency-safe holders onto the shared
//! [`Ctx`](super::context::Ctx):
//!
//! - [`ActionTally`]: per-category counters bumped by [`CountingSink`], which
//!   forwards each action to the inner sink and tallies it only on a successful
//!   apply (a failed apply counts under [`ActionTally::failed`]).
//! - [`Findings`]: report-only observations that are not [`Action`]s (dangling
//!   references; invalid-named namespaces skipped without `--prune-unknown`).
//!
//! Counting wraps the sink and findings stay out of the `Action` enum, so the
//! action contract and the exact-`Vec<Action>` tests are untouched.
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

/// How many report-only sample lines the summary prints, bounding the memory the
/// [`Findings`] accumulator holds.
const FINDINGS_SAMPLE_CAP: usize = 20;

/// Lock-free per-category tally of every applied (or, under `-d`, would-be
/// applied) [`Action`]. Each variant maps to one bucket (see
/// [`ActionTally::bucket`]).
///
/// Counts only work flowing through the [`ActionSink`]; the structural `--jobs`
/// reconcile mutates the job store directly and reports its own totals.
#[derive(Default)]
pub struct ActionTally {
    /// `DeleteOrphanManifest`.
    pub orphan_manifests: AtomicU64,
    /// `DeleteTag` + `DeleteInvalidTag`.
    pub tags: AtomicU64,
    /// `DeleteExpiredUpload`.
    pub uploads: AtomicU64,
    /// `AbortMultipartUpload`.
    pub multipart: AtomicU64,
    /// `DeleteOrphanJob` (config-drift orphan-job deletions from
    /// `--replication-orphans` / `--cache-orphans`). The structural `--jobs`
    /// reconcile does not flow through the sink and is not counted here.
    pub jobs: AtomicU64,
    /// `DeleteInvalidNamespace` + `DeleteInvalidUploadNamespace`.
    pub namespaces_pruned: AtomicU64,
    /// `EnqueueReplicationPush` + `EnqueueReplicationDelete`.
    pub replication_enqueued: AtomicU64,
    /// Every categorized action, so an all-clean run prints a single `total: 0`.
    pub total: AtomicU64,
    /// Applies that returned `Err` and left the fault in place. Bumped instead of
    /// a per-category counter so the summary can flag unresolved faults.
    pub failed: AtomicU64,
    /// Actions a closed per-category gate skipped in a mutate-mode run (e.g. GC
    /// actions under the deprecated `--retention` alias without `--commit`).
    pub suppressed: AtomicU64,
}

impl ActionTally {
    /// Categorize one action and bump it (and `total`). Test-only; production
    /// counts via [`ActionTally::bucket`] so the [`CountingSink`] bumps only after
    /// a successful apply.
    #[cfg(test)]
    pub fn record(&self, action: &Action) {
        self.bucket(action).bump();
    }

    /// Resolve the [`TallyBucket`] one action belongs to, without bumping it. The
    /// exhaustive `match` forces a future [`Action`] variant to be given a bucket.
    pub fn bucket(&self, action: &Action) -> TallyBucket<'_> {
        let counter = match action {
            Action::DeleteOrphanManifest { .. } => &self.orphan_manifests,
            Action::DeleteTag { .. } | Action::DeleteInvalidTag { .. } => &self.tags,
            Action::DeleteExpiredUpload { .. } => &self.uploads,
            Action::AbortMultipartUpload { .. } => &self.multipart,
            Action::DeleteOrphanJob { .. } => &self.jobs,
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

/// A resolved per-category counter plus the `total`, so a deferred `bump` hits
/// both atomics together. Returned by [`ActionTally::bucket`] to choose the
/// bucket before deciding whether to count it.
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

/// Decorator over the inner sink that tallies an action once the inner apply
/// succeeds; a failed apply is counted under [`ActionTally::failed`].
pub struct CountingSink {
    inner: Box<dyn ActionSink + Send>,
    tally: Arc<ActionTally>,
}

#[async_trait]
impl ActionSink for CountingSink {
    async fn apply(&mut self, action: Action) -> Result<(), Error> {
        // Resolve the bucket before the action moves into the inner apply.
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

/// Wrap `inner` in a [`CountingSink`] tallying into `tally`.
pub fn counting_sink(
    inner: Box<dyn ActionSink + Send>,
    tally: Arc<ActionTally>,
) -> Box<dyn ActionSink + Send> {
    Box::new(CountingSink { inner, tally })
}

/// Per-category counts of report-only findings plus a bounded printable sample.
/// Observations, not mutations, so they never become an [`Action`].
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
    /// Push a sample line, built lazily and only while under the cap.
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

/// Concurrency-safe accumulator of report-only findings, shared across the
/// parallel nodes and the three commands via the `Ctx`. A `Mutex` (not atomics)
/// because findings carry sample data; it is independent of the sink mutex.
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

    /// Record one invalid-named namespace skipped because `--prune-unknown` is off.
    pub async fn record_skipped_invalid_namespace(&self, name: &str, reason: &str) {
        let mut inner = self.0.lock().await;
        inner.skipped_invalid_namespaces += 1;
        inner.sample(|| format!("skipped invalid namespace '{name}': {reason}"));
    }

    /// Clone the accumulated findings out from under the lock.
    pub async fn snapshot(&self) -> FindingsInner {
        self.0.lock().await.clone()
    }
}

/// Emit the end-of-run summary as a single multi-line `info!` event.
///
/// `run_label` is the command name; `mutate` selects the `would`/`done` verb;
/// `prune_unknown_supported` gates the "(run --prune-unknown to delete)" hint.
/// Zero-count rows are suppressed; the `mode:` and `total:` lines always print,
/// and a `failed:` line prints when any apply failed. Counts cover only
/// sink-applied actions (see [`ActionTally`]).
pub fn log_summary(
    run_label: &str,
    mutate: bool,
    prune_unknown_supported: bool,
    tally: &ActionTally,
    findings: &FindingsInner,
) {
    let mode = if mutate { "mutate" } else { "report-only" };
    let verb = if mutate { "done" } else { "would" };

    let mut body = String::new();
    // `write!` to a `String` is infallible, so every `Result` is dropped.
    let _ = writeln!(body, "{} SUMMARY (mode: {mode})", run_label.to_uppercase());
    let _ = writeln!(body, "  Actions ({verb}):");

    write_action_rows(&mut body, tally);
    let _ = writeln!(body, "    total: {}", tally.total.load(Ordering::Relaxed));
    let failed = tally.failed.load(Ordering::Relaxed);
    if failed > 0 {
        let _ = writeln!(body, "    failed: {failed} (left in place, see error log)");
    }
    let suppressed = tally.suppressed.load(Ordering::Relaxed);
    if suppressed > 0 {
        let _ = writeln!(body, "    skipped (needs --commit): {suppressed}");
    }

    if findings.total() > 0 {
        write_findings(&mut body, findings, prune_unknown_supported);
    }

    info!("{}", body.trim_end());
}

/// Append the non-zero per-category action rows.
fn write_action_rows(body: &mut String, tally: &ActionTally) {
    let mut row = |label: &str, count: u64| {
        if count == 0 {
            return;
        }
        let _ = writeln!(body, "    {label}: {count}");
    };

    row(
        "orphan manifests",
        tally.orphan_manifests.load(Ordering::Relaxed),
    );
    row("tags", tally.tags.load(Ordering::Relaxed));
    row("expired uploads", tally.uploads.load(Ordering::Relaxed));
    row("multipart aborts", tally.multipart.load(Ordering::Relaxed));
    row("jobs", tally.jobs.load(Ordering::Relaxed));
    row(
        "namespaces pruned",
        tally.namespaces_pruned.load(Ordering::Relaxed),
    );
    row(
        "replication enqueued",
        tally.replication_enqueued.load(Ordering::Relaxed),
    );
}

/// Append the report-only findings block (counts plus the bounded sample).
/// `prune_unknown_supported` gates the "(run --prune-unknown to delete)" hint.
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
        oci::{Digest, Namespace, Tag},
        registry::{
            blob_store,
            job_store::{JobState, Queue},
        },
    };

    fn digest(hex: &str) -> Digest {
        Digest::from_str(&format!("sha256:{hex}")).unwrap()
    }

    fn ns() -> Namespace {
        Namespace::new("test-repo/app").unwrap()
    }

    /// One representative of every `Action` variant.
    fn one_of_each_action() -> Vec<Action> {
        let d = digest("0000000000000000000000000000000000000000000000000000000000000000");
        vec![
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

        assert_eq!(tally.orphan_manifests.load(Ordering::Relaxed), 1);
        assert_eq!(tally.tags.load(Ordering::Relaxed), 2); // DeleteTag + DeleteInvalidTag
        assert_eq!(tally.uploads.load(Ordering::Relaxed), 1);
        assert_eq!(tally.multipart.load(Ordering::Relaxed), 1);
        assert_eq!(tally.jobs.load(Ordering::Relaxed), 1);
        assert_eq!(tally.namespaces_pruned.load(Ordering::Relaxed), 2); // Namespace + UploadNamespace
        assert_eq!(tally.replication_enqueued.load(Ordering::Relaxed), 2); // Push + Delete

        assert_eq!(
            tally.total.load(Ordering::Relaxed),
            actions.len() as u64,
            "total must equal the number of recorded actions"
        );
    }

    /// The `CountingSink` forwards every action to the inner sink and counts them.
    #[tokio::test]
    async fn counting_sink_forwards_unchanged_and_counts() {
        let tally = Arc::new(ActionTally::default());

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
        sink.apply(Action::DeleteOrphanManifest {
            namespace: ns(),
            digest: d.clone(),
        })
        .await
        .unwrap();

        assert_eq!(tally.total.load(Ordering::Relaxed), 2);
        assert_eq!(tally.tags.load(Ordering::Relaxed), 1);
        assert_eq!(tally.orphan_manifests.load(Ordering::Relaxed), 1);
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
