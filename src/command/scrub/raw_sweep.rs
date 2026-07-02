//! Raw-enumeration sweep.
//!
//! Enumerates each owned storage root, classifies every key through
//! [`classify_key`], and logs a per-domain and per-unrecognized tally. The
//! default path is report-only: it computes the same candidate sets as a
//! committed run and deletes nothing. Under `--commit` the sweep deletes.
//!
//! Every GC reap category is age-gated: a candidate is deleted only when its
//! backend `last_modified` is older than the maintenance grace, and an object
//! whose `last_modified` is unavailable is never reaped. De-configured
//! namespace handling is config-driven, not age-gated, and touches only the
//! namespace's own marker subtrees (a child namespace is a sibling path
//! segment inside the same directory, never deleted with its parent).
//! Byte-adjacent removals additionally re-check their precondition under the
//! per-digest blob-data lock at delete time.
//!
//! Backend model: the metadata backend owns the `v2/repositories/**` link
//! shapes, `_jobs/**`, and the `v2/blobs/**` grant shards (`refs/<ns>.json`
//! plus the legacy `index.json`); the blob backend owns the `v2/blobs/**/data`
//! byte keys and the `v2/repositories/<ns>/_uploads/**` staging. In a shared
//! topology both handles reach one physical backend; a split topology adds
//! classification-only walks over the other handle's roots so no key goes
//! unswept. Deletes always route through the handle owning the key's domain.
//! Grant triage keys off enumerated `data` keys, so a shard whose blob bytes
//! are entirely absent is never triaged and persists (a leak, not a loss).
//!
//! Memory is bounded: resident state is the namespace-name list, the
//! de-configured names, the fatal set, one list page per active walk, and per
//! namespace the defect-local candidates (missing bodies, orphan links, seen
//! link targets) for up to `fanout` namespaces in flight. Nothing scales with
//! total key count.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use angos_tx_engine::{StorageError, store::Store};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{StreamExt, stream};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    command::scrub::{
        check::list_all,
        error::Error,
        executor::{
            ApplyOutcome, delete_orphan_blob_locked, orphan_missing_body_locked,
            remove_orphan_link_locked, remove_stale_grant_locked, revoke_bare_self_grant_locked,
        },
        sweep_sink::{MarkerStatus, MarkerTallies},
    },
    oci::{Digest, Namespace},
    registry::{
        blob_store::{self, BlobStore},
        key_classifier::{
            KeyClass, KeyDomain, UnrecognizedReason, blob_digest_from_key, classify_key,
            legacy_index_digest_from_key,
        },
        metadata_store::{
            BlobIndex, Error as MetadataError, LinkKind, LinkOperation, MetadataStore,
        },
        path_builder,
        repository_resolver::RepositoryResolver,
    },
};

const LIST_PAGE: u16 = 1000;
const UNRECOGNIZED_SAMPLE_CAP: usize = 20;
const DECONFIGURED_SAMPLE_CAP: usize = 20;

/// The subtrees holding a namespace's OWN data inside its directory. The
/// config-ownership pass deletes exactly these for a de-configured namespace,
/// never the recursive namespace prefix: child namespaces live as sibling path
/// segments inside the same directory (`team/app1` is the `app1` child of
/// `team`'s directory), so a prefix delete of a de-configured parent would
/// wipe a configured child's metadata.
const NAMESPACE_MARKER_SUBTREES: [&str; 5] =
    ["_manifests", "_layers", "_config", "_uploads", "_blobs"];

/// Which backend enumerated a pending unrecognized key, so the delete routes
/// through the same store that listed it.
#[derive(Clone, Copy)]
enum UnrecognizedBackend {
    Blob,
    Metadata,
}

/// An unrecognized key collected for deletion.
struct PendingUnrecognized {
    key: String,
    reason: UnrecognizedReason,
    backend: UnrecognizedBackend,
}

/// Per-category counts accumulated while classifying every enumerated key.
/// Under report-only, the removal/reap counters hold the would-remove counts.
#[derive(Default)]
pub struct SweepTally {
    owned: BTreeMap<&'static str, u64>,
    out_of_domain: u64,
    unrecognized: BTreeMap<&'static str, u64>,
    unrecognized_sample: Vec<String>,
    total: u64,
    /// Unrecognized keys deleted (would-delete under report-only with
    /// `--delete-unrecognized`).
    deleted: u64,
    deconfigured_namespaces: u64,
    /// De-configured namespaces whose metadata subtree was fully deleted.
    deconfigured_deleted: u64,
    /// De-configured namespaces where at least one key delete failed, so the
    /// subtree is only partially removed.
    deconfigured_partial: u64,
    deconfigured_sample: Vec<String>,
    /// Blob-ownership grant shards (`refs/<ns>.json`) revoked for a de-configured
    /// namespace, making its blobs reap-eligible once nothing else references them.
    deconfigured_grants_revoked: u64,
    /// Grant shards whose delete failed, keeping their blobs reference-pinned.
    deconfigured_grants_partial: u64,
    /// De-configured in-flight upload keys deleted on the blob backend, which
    /// the metadata-subtree delete misses in a split topology.
    deconfigured_uploads_deleted: u64,
    deconfigured_uploads_partial: u64,
    /// Deletes skipped because the lock's cancellation token fired. Under
    /// streaming this counts collected-but-unattempted items, a lower bound:
    /// not-yet-enumerated candidates are unknowable.
    token_rejected: u64,
    orphan_reaped: u64,
    /// Stale grants removed: an entry in `refs/<ns>.json` (tracked
    /// `Layer`/`Config`/`Manifest`, or non-tracked `Tag`/`Digest`/`Referrer`)
    /// whose backing link file is gone. Removing the last reference makes the
    /// blob byte-reap-eligible the same run.
    stale_grants_removed: u64,
    /// Obsolete bare `Blob` self-grants revoked: the namespace holds no link
    /// file to the digest and the grant shard is older than the grace.
    self_grants_revoked: u64,
    /// Orphan derived links removed: an orphan subject `Referrer` link, or a
    /// phantom `referenced_by` entry on a layer/config link. Removing a link's
    /// last referrer cascades to reclaiming it.
    orphan_links_removed: u64,
    /// Dangling revisions removed: a revision or tag whose target manifest has no
    /// body in the blob store. Removal drops its back-links so the orphan-link
    /// pass can reclaim them the same run.
    missing_bodies_removed: u64,
    /// Reap candidates kept because the backend reported no `last_modified`;
    /// such objects are never reaped.
    age_unknown_kept: u64,
    /// Gated deletes that returned an error and left the object in place.
    failed_deletes: u64,
    /// Legacy `index.json` files converged into `refs/` shards this run.
    converged: u64,
    /// Legacy convergence attempts that failed; degrades the run.
    convergence_failed: u64,
    /// A root or namespace enumeration failed, so parts of the store were never
    /// classified; degrades the run.
    enumeration_failed: bool,
}

impl SweepTally {
    fn record(&mut self, key: &str, class: &KeyClass) {
        self.total += 1;
        match class {
            KeyClass::Owned(domain) => {
                *self.owned.entry(domain_label(*domain)).or_default() += 1;
            }
            KeyClass::OutOfDomain(_) => self.out_of_domain += 1,
            KeyClass::Unrecognized(reason) => {
                let label = reason.label();
                *self.unrecognized.entry(label).or_default() += 1;
                if self.unrecognized_sample.len() < UNRECOGNIZED_SAMPLE_CAP {
                    self.unrecognized_sample.push(format!("{key} ({label})"));
                }
            }
        }
    }

    fn unrecognized_count(&self) -> u64 {
        self.unrecognized.values().sum()
    }

    /// Project this tally into the marker's tally shape. The `action_failed`
    /// counter lives outside the sweep, so it is left zero here for the caller
    /// to merge in.
    pub fn to_marker_tallies(&self) -> MarkerTallies {
        MarkerTallies {
            total: self.total,
            deleted: self.deleted,
            orphan_reaped: self.orphan_reaped,
            token_rejected: self.token_rejected,
            converged: self.converged,
            failed: self.convergence_failed,
            action_failed: 0,
            deconfigured_namespaces: self.deconfigured_namespaces,
            deconfigured_deleted: self.deconfigured_deleted,
            deconfigured_partial: self.deconfigured_partial,
            deconfigured_grants_revoked: self.deconfigured_grants_revoked,
            deconfigured_grants_partial: self.deconfigured_grants_partial,
            deconfigured_uploads_deleted: self.deconfigured_uploads_deleted,
            deconfigured_uploads_partial: self.deconfigured_uploads_partial,
            stale_grants_removed: self.stale_grants_removed,
            self_grants_revoked: self.self_grants_revoked,
            orphan_links_removed: self.orphan_links_removed,
            missing_bodies_removed: self.missing_bodies_removed,
            unrecognized: self.unrecognized_count(),
            age_unknown_kept: self.age_unknown_kept,
            failed_deletes: self.failed_deletes,
        }
    }

    /// Map this tally to the ran-path exit code and status, with precedence
    /// aborted (3) over degraded (2) over clean (0). `failed_namespaces`
    /// (walk/rebuild failures), `action_failed` (sink applies that errored),
    /// and `walk_complete` are threaded in from outside the sweep and
    /// participate in the degraded gate alongside the tally's own signals.
    pub fn ran_exit_code(
        &self,
        failed_namespaces: u64,
        action_failed: u64,
        walk_complete: bool,
    ) -> (i32, MarkerStatus) {
        let status = if self.token_rejected > 0 {
            MarkerStatus::Aborted
        } else if failed_namespaces > 0
            || action_failed > 0
            || !walk_complete
            || self.convergence_failed > 0
            || self.failed_deletes > 0
            || self.enumeration_failed
            || self.deconfigured_partial > 0
            || self.deconfigured_grants_partial > 0
            || self.deconfigured_uploads_partial > 0
        {
            MarkerStatus::Degraded
        } else {
            MarkerStatus::Clean
        };
        (status.exit_code(), status)
    }

    fn log(&self, committed: bool) {
        info!(
            total = self.total,
            owned = self.owned.values().sum::<u64>(),
            out_of_domain = self.out_of_domain,
            unrecognized = self.unrecognized_count(),
            deleted = self.deleted,
            "scrub raw sweep: classified {} keys, deleted {} unrecognized",
            self.total,
            self.deleted
        );
        for (domain, count) in &self.owned {
            info!(domain, count, "scrub raw sweep: owned keys by domain");
        }
        if self.unrecognized_count() > 0 {
            warn!(
                unrecognized = self.unrecognized_count(),
                deleted = self.deleted,
                "scrub raw sweep: unrecognized keys present"
            );
            for (reason, count) in &self.unrecognized {
                warn!(
                    reason,
                    count, "scrub raw sweep: unrecognized keys by reason"
                );
            }
            for sample in &self.unrecognized_sample {
                warn!(sample = %sample, "scrub raw sweep: unrecognized key sample");
            }
            if !committed {
                warn!(
                    unrecognized = self.unrecognized_count(),
                    "scrub raw sweep: unrecognized keys older than the grace would be deleted \
                     under --commit --delete-unrecognized"
                );
            }
        }
        if self.token_rejected > 0 {
            warn!(
                token_rejected = self.token_rejected,
                "scrub raw sweep: lock cancelled (max-hold reached or ownership lost); aborted \
                 further deletes"
            );
        }
        if self.age_unknown_kept > 0 {
            warn!(
                age_unknown_kept = self.age_unknown_kept,
                "scrub raw sweep: reap candidates kept because the backend reports no \
                 last_modified; nothing is reaped without an age"
            );
        }
        if self.failed_deletes > 0 {
            warn!(
                failed_deletes = self.failed_deletes,
                "scrub raw sweep: some deletes failed and were left in place; the run is degraded"
            );
        }
        if self.converged > 0 || self.convergence_failed > 0 {
            info!(
                converged = self.converged,
                failed = self.convergence_failed,
                "scrub raw sweep: drained legacy blob indexes into refs/"
            );
        }
        self.log_missing_body(committed);
        self.log_orphan_links(committed);
        self.log_orphan_blobs(committed);
        self.log_stale_grants(committed);
        self.log_self_grants(committed);
        self.log_deconfigured(committed);
    }

    /// Emit the missing-body category count, warning the would-remove count under
    /// report-only.
    fn log_missing_body(&self, committed: bool) {
        if self.missing_bodies_removed == 0 {
            return;
        }
        info!(
            missing_bodies_removed = self.missing_bodies_removed,
            "scrub raw sweep: dangling missing-body revisions classified"
        );
        if !committed {
            warn!(
                missing_bodies_removed = self.missing_bodies_removed,
                "scrub raw sweep: dangling missing-body revisions would be removed under --commit"
            );
        }
    }

    /// Emit the orphan-link category count, warning the would-remove count under
    /// report-only.
    fn log_orphan_links(&self, committed: bool) {
        if self.orphan_links_removed == 0 {
            return;
        }
        info!(
            orphan_links_removed = self.orphan_links_removed,
            "scrub raw sweep: orphan derived metadata links classified"
        );
        if !committed {
            warn!(
                orphan_links_removed = self.orphan_links_removed,
                "scrub raw sweep: orphan derived metadata links would be removed under --commit"
            );
        }
    }

    /// Emit the orphan-blob category count, warning the would-reap count under
    /// report-only.
    fn log_orphan_blobs(&self, committed: bool) {
        if self.orphan_reaped == 0 {
            return;
        }
        info!(
            orphan_reaped = self.orphan_reaped,
            "scrub raw sweep: orphan blobs classified"
        );
        if !committed {
            warn!(
                orphan_reaped = self.orphan_reaped,
                "scrub raw sweep: orphan blobs would be hard-deleted under --commit"
            );
        }
    }

    /// Emit the stale-grant category count, warning the would-remove count under
    /// report-only.
    fn log_stale_grants(&self, committed: bool) {
        if self.stale_grants_removed == 0 {
            return;
        }
        info!(
            stale_grants_removed = self.stale_grants_removed,
            "scrub raw sweep: stale tracked blob-ownership grants classified"
        );
        if !committed {
            warn!(
                stale_grants_removed = self.stale_grants_removed,
                "scrub raw sweep: stale tracked blob-ownership grants would be removed under --commit"
            );
        }
    }

    /// Emit the bare self-grant category count, warning the would-revoke count
    /// under report-only.
    fn log_self_grants(&self, committed: bool) {
        if self.self_grants_revoked == 0 {
            return;
        }
        info!(
            self_grants_revoked = self.self_grants_revoked,
            "scrub raw sweep: obsolete bare blob self-grants classified"
        );
        if !committed {
            warn!(
                self_grants_revoked = self.self_grants_revoked,
                "scrub raw sweep: obsolete bare blob self-grants would be revoked under --commit"
            );
        }
    }

    /// Emit the config-ownership category counts, warning and listing a bounded
    /// sample of would-delete namespaces under report-only.
    fn log_deconfigured(&self, committed: bool) {
        if self.deconfigured_namespaces == 0 {
            return;
        }
        info!(
            deconfigured_namespaces = self.deconfigured_namespaces,
            deconfigured_deleted = self.deconfigured_deleted,
            deconfigured_partial = self.deconfigured_partial,
            deconfigured_grants_revoked = self.deconfigured_grants_revoked,
            deconfigured_grants_partial = self.deconfigured_grants_partial,
            deconfigured_uploads_deleted = self.deconfigured_uploads_deleted,
            deconfigured_uploads_partial = self.deconfigured_uploads_partial,
            "scrub raw sweep: de-configured namespaces classified"
        );
        if self.deconfigured_partial > 0 {
            warn!(
                deconfigured_partial = self.deconfigured_partial,
                "scrub raw sweep: de-configured namespaces only partially deleted; \
                 some keys could not be deleted and remain at their origin"
            );
        }
        if self.deconfigured_grants_partial > 0 {
            warn!(
                deconfigured_grants_partial = self.deconfigured_grants_partial,
                "scrub raw sweep: some de-configured blob-ownership grant shards could not be \
                 deleted, keeping those blobs reference-pinned"
            );
        }
        if self.deconfigured_uploads_partial > 0 {
            warn!(
                deconfigured_uploads_partial = self.deconfigured_uploads_partial,
                "scrub raw sweep: some de-configured namespace upload keys could not be deleted \
                 on the blob backend"
            );
        }
        if !committed {
            warn!(
                deconfigured_namespaces = self.deconfigured_namespaces,
                "scrub raw sweep: de-configured namespaces would be deleted under --commit"
            );
            for sample in &self.deconfigured_sample {
                warn!(namespace = %sample, "scrub raw sweep: de-configured namespace sample");
            }
        }
    }
}

fn domain_label(domain: KeyDomain) -> &'static str {
    match domain {
        KeyDomain::BlobBytes => "blob-bytes",
        KeyDomain::BlobIndexGrant => "blob-index-grant",
        KeyDomain::MetadataLink => "metadata-link",
        KeyDomain::Upload => "upload",
        KeyDomain::Job => "job",
    }
}

/// Run-level sweep parameters, captured once for the whole run.
// The bool fields are independent run gates, not a state machine, so an enum would
// obscure rather than clarify.
#[allow(clippy::struct_excessive_bools)]
pub struct SweepParams {
    /// Run identity and age anchor: names the run in the marker and is the
    /// instant every age probe measures against.
    pub run_epoch: DateTime<Utc>,
    /// Global deletion gate. Without it the whole sweep is report-only.
    pub commit: bool,
    /// Unrecognized deletion opt-in, gated on `commit`.
    pub delete_unrecognized: bool,
    /// The raw enumerated names of namespaces whose metadata walk or rebuild
    /// errored. The per-namespace metadata passes skip these (keep over reap).
    pub fatal_namespaces: Arc<HashSet<String>>,
    /// Whether any namespace failed. When true the global digest-keyed byte
    /// reap, the stale tracked-grant removal, and the self-grant revocation are
    /// skipped whole, since a co-owned blob cannot be gated per-namespace.
    pub any_rebuild_fatal: bool,
    /// The maintenance grace: the minimum quiescence age before any GC category
    /// may reap an object.
    pub grace: ChronoDuration,
    /// Whether the blob and metadata stores share one physical backend. A split
    /// topology adds classification-only walks over the other handle's roots.
    pub shared_backend: bool,
    /// Concurrency budget for fanned-out probes and page deletes
    /// (`max_concurrent_scrub_tasks`, clamped).
    pub fanout: usize,
    /// Whether encountered legacy `index.json` files are converged into `refs/`
    /// shards (suppressed under `--dry-run`, which only reports them).
    pub converge: bool,
}

/// Verdict of the per-object age probe gating every GC reap.
enum AgeVerdict {
    /// Older than the grace; eligible for reaping.
    Reapable,
    /// Younger than the grace; kept this run.
    Fresh,
    /// The backend reported no `last_modified`; never reaped.
    Unknown,
    /// The object is already gone; nothing to reap.
    Gone,
}

/// Probe `key`'s age on `store` against the maintenance grace. Every GC
/// category runs this before collecting a candidate; only `Reapable` proceeds.
/// A failed probe keeps the object (never reap on a failed read).
async fn probe_age(store: &Store, key: &str, params: &SweepParams) -> AgeVerdict {
    match store.head(key).await {
        Ok(meta) => match meta.last_modified {
            Some(modified) if params.run_epoch.signed_duration_since(modified) >= params.grace => {
                AgeVerdict::Reapable
            }
            Some(_) => AgeVerdict::Fresh,
            None => AgeVerdict::Unknown,
        },
        Err(StorageError::NotFound) => AgeVerdict::Gone,
        Err(error) => {
            warn!(key, %error, "scrub raw sweep: age probe failed; keeping");
            AgeVerdict::Fresh
        }
    }
}

/// Age verdict for a namespace's grant on `digest`, anchored on the shard
/// object's `last_modified` as a conservative grant-age proxy; a legacy-only
/// grant falls back to the legacy `index.json`.
async fn grant_age(
    metadata_store: &MetadataStore,
    digest: &Digest,
    namespace: &Namespace,
    params: &SweepParams,
) -> AgeVerdict {
    let shard = path_builder::blob_index_shard_path(digest, namespace);
    match probe_age(metadata_store.store(), &shard, params).await {
        AgeVerdict::Gone => {
            probe_age(
                metadata_store.store(),
                &path_builder::blob_index_path(digest),
                params,
            )
            .await
        }
        verdict => verdict,
    }
}

/// Record a global abort: bump `token_rejected` by the unattempted count and log
/// once. Called the first time the cancellation token fires in an apply loop.
fn abort_remaining(tally: &mut SweepTally, remaining: u64) {
    tally.token_rejected += remaining;
    warn!(
        remaining,
        "scrub: lock cancelled (max-hold reached or ownership lost); aborting all further deletes"
    );
}

// De-configured namespaces (config-ownership pass; config-driven, not age-gated).

/// Collect the de-configured namespace names (`resolve == None`). Names are
/// recorded in both modes so the exclusion sets, grant would-counts, and
/// samples match between report-only and `--commit`.
///
/// Hard guard: when the resolver is empty the pass collects nothing even under
/// `--commit`, so an empty or mis-loaded config never wipes every namespace.
fn collect_deconfigured_namespaces(
    resolver: &RepositoryResolver,
    namespaces: &[String],
    tally: &mut SweepTally,
) -> Vec<String> {
    let mut deconfigured = Vec::new();
    if resolver.len() == 0 {
        warn!(
            "scrub config-ownership sweep: no repositories configured; refusing to delete \
             any namespace (an empty or mis-loaded config must never wipe the registry). \
             Skipping the config-ownership category."
        );
        return deconfigured;
    }
    for namespace in namespaces {
        if resolver.resolve(namespace).is_some() {
            continue;
        }
        tally.deconfigured_namespaces += 1;
        if tally.deconfigured_sample.len() < DECONFIGURED_SAMPLE_CAP {
            tally.deconfigured_sample.push(namespace.clone());
        }
        deconfigured.push(namespace.clone());
    }
    deconfigured
}

/// Delete every de-configured namespace's OWN data: exactly its
/// [`NAMESPACE_MARKER_SUBTREES`], streamed page by page through the metadata
/// handle with `fanout` concurrent deletes per page. The recursive namespace
/// prefix is never deleted, so a configured child namespace nested inside the
/// de-configured directory survives untouched. A namespace counts
/// `deconfigured_deleted` only when every key deleted, else
/// `deconfigured_partial`. After a clean delete the emptied marker directory
/// scaffolding is pruned so the namespace stops re-enumerating on FS.
async fn delete_deconfigured_subtrees(
    metadata_store: &MetadataStore,
    deconfigured: &[String],
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if !params.commit || deconfigured.is_empty() {
        return;
    }
    let store = metadata_store.store();
    for (index, namespace) in deconfigured.iter().enumerate() {
        if cancel.is_cancelled() {
            abort_remaining(tally, (deconfigured.len() - index) as u64);
            return;
        }
        let Some(dir) = path_builder::namespace_dir(namespace) else {
            warn!(
                namespace = %namespace,
                "scrub config-ownership sweep: unsafe namespace name; skipping"
            );
            continue;
        };
        let mut deleted = 0u64;
        let mut failures = 0u64;
        for marker in NAMESPACE_MARKER_SUBTREES {
            let marker_dir = format!("{dir}/{marker}");
            let (marker_deleted, marker_failures) =
                delete_prefix_pages(store, &marker_dir, params, tally, cancel).await;
            deleted += marker_deleted;
            failures += marker_failures;
            if marker_failures == 0 {
                // Prefix delete after the per-key deletes only removes empty
                // directory scaffolding (a no-op on S3), so the namespace stops
                // re-enumerating as de-configured on FS. Scoped to the marker
                // subtree; the namespace directory itself is never
                // prefix-deleted (child namespaces live inside it).
                if let Err(error) = store.delete_prefix(&marker_dir).await {
                    warn!(namespace = %namespace, marker, %error, "scrub config-ownership sweep: failed to prune the emptied marker directory");
                }
            }
        }
        if failures == 0 {
            tally.deconfigured_deleted += 1;
        } else {
            tally.deconfigured_partial += 1;
        }
        info!(
            namespace = %namespace,
            keys = deleted,
            failures,
            "scrub config-ownership sweep: deleted de-configured namespace's own marker subtrees"
        );
    }
}

/// Delete every key under `prefix` on `store`, one page at a time with
/// `fanout` concurrent deletes per page. Returns `(deleted, failures)`; a list
/// failure counts as a failure so the caller records the namespace partial.
async fn delete_prefix_pages(
    store: &Store,
    prefix: &str,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) -> (u64, u64) {
    let mut deleted = 0u64;
    let mut failures = 0u64;
    let mut token = None;
    loop {
        let page = match store.list(prefix, LIST_PAGE, token).await {
            Ok(page) => page,
            Err(error) => {
                warn!(prefix, %error, "scrub raw sweep: failed to list subtree; the delete is partial");
                failures += 1;
                break;
            }
        };
        if cancel.is_cancelled() {
            abort_remaining(tally, page.items.len() as u64);
            failures += 1;
            break;
        }
        let results = stream::iter(&page.items)
            .map(|item| {
                let key = format!("{prefix}/{item}");
                async move { store.delete(&key).await.map_err(|error| (key, error)) }
            })
            .buffer_unordered(params.fanout)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            match result {
                Ok(()) => deleted += 1,
                Err((key, error)) => {
                    failures += 1;
                    warn!(key = %key, %error, "scrub raw sweep: failed to delete key");
                }
            }
        }
        token = page.next_token;
        if token.is_none() {
            break;
        }
    }
    (deleted, failures)
}

/// Delete the de-configured namespaces' in-flight upload keys on the blob
/// backend. Runs only in a split topology; on a shared backend the metadata
/// subtree pass already deletes the physically identical keys.
async fn delete_deconfigured_uploads(
    blob_store: &BlobStore,
    deconfigured: &[String],
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if !params.commit || deconfigured.is_empty() {
        return;
    }
    let store = blob_store.store.as_ref();
    for namespace in deconfigured {
        if cancel.is_cancelled() {
            return;
        }
        let Some(dir) = path_builder::namespace_dir(namespace) else {
            warn!(
                namespace = %namespace,
                "scrub config-ownership sweep: unsafe namespace name; skipping upload clear"
            );
            continue;
        };
        let uploads_root = format!("{dir}/_uploads");
        let (deleted, failures) =
            delete_prefix_pages(store, &uploads_root, params, tally, cancel).await;
        tally.deconfigured_uploads_deleted += deleted;
        tally.deconfigured_uploads_partial += failures;
        if failures == 0
            && let Err(error) = store.delete_prefix(&uploads_root).await
        {
            warn!(namespace = %namespace, %error, "scrub config-ownership sweep: failed to prune the emptied uploads directory");
        }
        info!(
            namespace = %namespace,
            keys = deleted,
            failures,
            "scrub config-ownership sweep: deleted de-configured namespace in-flight uploads on \
             the blob backend"
        );
    }
}

// Per-namespace metadata scan: missing-body revisions and orphan derived links.

/// Which kind of orphan derived link was collected, carried into the log reason.
#[derive(Clone, Copy)]
enum OrphanLinkKind {
    /// A subject `Referrer` link whose referrer manifest is no longer a current
    /// revision (its `Digest` link reads `NotFound`).
    OrphanReferrer,
    /// A phantom `referenced_by` entry on a layer/config link whose referring
    /// revision's `Digest` link is gone. Removing the last referrer cascades to
    /// reclaiming the layer/config link.
    PhantomLink,
}

/// An orphan derived metadata link collected for removal. `referrer` is the
/// referring manifest digest whose absent revision makes it an orphan, the
/// under-lock re-check anchor.
struct PendingOrphanLink {
    link: LinkKind,
    referrer: Digest,
    kind: OrphanLinkKind,
}

/// The per-namespace scan's tally deltas, merged into the run tally after the
/// fanned-out namespace tasks complete.
#[derive(Default)]
struct NamespaceReapOutcome {
    missing_bodies_removed: u64,
    orphan_links_removed: u64,
    age_unknown_kept: u64,
    failed_deletes: u64,
    token_rejected: u64,
}

/// Run the merged per-namespace metadata scan over every eligible namespace,
/// fanned out at `params.fanout`. Namespaces in `deconfigured` are excluded
/// (the config-ownership pass owns their whole subtree); fatal namespaces are
/// excluded (keep over reap); invalid names form no typed links and are owned
/// by the invalid-name sweep.
async fn scan_namespaces(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    namespaces: &[String],
    deconfigured: &[String],
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    let eligible: Vec<Namespace> = namespaces
        .iter()
        .filter(|raw| !params.fatal_namespaces.contains(raw.as_str()))
        .filter(|raw| !deconfigured.iter().any(|name| name == *raw))
        .filter_map(|raw| Namespace::new(raw).ok())
        .collect();
    let outcomes = stream::iter(eligible)
        .map(|namespace| scan_namespace(metadata_store, blob_store, namespace, params, cancel))
        .buffer_unordered(params.fanout)
        .collect::<Vec<NamespaceReapOutcome>>()
        .await;
    for outcome in outcomes {
        tally.missing_bodies_removed += outcome.missing_bodies_removed;
        tally.orphan_links_removed += outcome.orphan_links_removed;
        tally.age_unknown_kept += outcome.age_unknown_kept;
        tally.failed_deletes += outcome.failed_deletes;
        tally.token_rejected += outcome.token_rejected;
    }
}

/// Scan one namespace's revisions ONCE (feeding both the missing-body probe and
/// the orphan-referrer listing), then its tags, then its layer/config links,
/// and apply the collected candidates in the order missing-body then
/// orphan-link (so removing a body-less revision drops its back-links for the
/// orphan-link cascade the same run). Report-only counts the same candidates.
#[allow(clippy::too_many_lines)]
async fn scan_namespace(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    namespace: Namespace,
    params: &SweepParams,
    cancel: &CancellationToken,
) -> NamespaceReapOutcome {
    let mut out = NamespaceReapOutcome::default();
    if cancel.is_cancelled() {
        return out;
    }
    let mut missing_bodies: Vec<Digest> = Vec::new();
    let mut seen_targets: HashSet<Digest> = HashSet::new();
    let mut orphan_links: Vec<PendingOrphanLink> = Vec::new();

    let mut revisions = list_all::revisions(metadata_store, &namespace);
    while let Some(result) = revisions.next().await {
        let revision = match result {
            Ok(revision) => revision,
            Err(error) => {
                warn!(namespace = %namespace, %error, "scrub raw sweep: failed to enumerate revisions; keeping the namespace's remainder");
                break;
            }
        };
        // A revision self-link's target is the revision digest itself.
        collect_missing_body_candidate(
            metadata_store,
            blob_store,
            &namespace,
            &LinkKind::Digest(revision.clone()),
            &revision,
            params,
            &mut seen_targets,
            &mut missing_bodies,
            &mut out,
        )
        .await;
        collect_orphan_referrers_for_revision(
            metadata_store,
            &namespace,
            &revision,
            params,
            &mut orphan_links,
            &mut out,
        )
        .await;
    }
    drop(revisions);

    let mut tags = list_all::tags(metadata_store, &namespace);
    while let Some(result) = tags.next().await {
        let tag = match result {
            Ok(tag) => tag,
            Err(error) => {
                warn!(namespace = %namespace, %error, "scrub raw sweep: failed to enumerate tags; keeping the namespace's remainder");
                break;
            }
        };
        let link = LinkKind::Tag(tag);
        let target = match metadata_store.read_link(&namespace, &link).await {
            Ok(metadata) => metadata.target,
            Err(MetadataError::ReferenceNotFound) => continue,
            Err(error) => {
                warn!(namespace = %namespace, link = %link, %error, "scrub raw sweep: tag read failed for missing-body scan; keeping");
                continue;
            }
        };
        collect_missing_body_candidate(
            metadata_store,
            blob_store,
            &namespace,
            &link,
            &target,
            params,
            &mut seen_targets,
            &mut missing_bodies,
            &mut out,
        )
        .await;
    }
    drop(tags);

    collect_phantom_links(
        metadata_store,
        &namespace,
        params,
        &mut orphan_links,
        &mut out,
    )
    .await;

    apply_missing_bodies(
        metadata_store,
        blob_store,
        &namespace,
        missing_bodies,
        params,
        &mut out,
        cancel,
    )
    .await;
    apply_orphan_links(
        metadata_store,
        &namespace,
        orphan_links,
        params,
        &mut out,
        cancel,
    )
    .await;
    out
}

/// Probe the manifest body for one revision/tag target and, when it is absent
/// and the dangling link is older than the grace, collect a missing-body
/// candidate. A present body is live and is cached too, so a target referenced
/// by many tags is probed once per namespace; a failed probe keeps the link and
/// is not cached (retried on the next reference). One cascade per (namespace,
/// target) per run via `seen_targets`.
#[allow(clippy::too_many_arguments)]
async fn collect_missing_body_candidate(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    namespace: &Namespace,
    anchor: &LinkKind,
    target: &Digest,
    params: &SweepParams,
    seen_targets: &mut HashSet<Digest>,
    pending: &mut Vec<Digest>,
    out: &mut NamespaceReapOutcome,
) {
    if seen_targets.contains(target) {
        return;
    }
    match blob_store.size(target).await {
        Ok(_) => {
            // A present body is live; cache it so a digest referenced by many
            // tags is probed once per namespace, not once per referencing tag.
            seen_targets.insert(target.clone());
            return;
        }
        Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound) => {}
        Err(error) => {
            warn!(namespace = %namespace, target = %target, %error, "scrub raw sweep: missing-body probe failed; keeping");
            return;
        }
    }
    seen_targets.insert(target.clone());
    // Age anchored on the dangling link file itself.
    let anchor_path = path_builder::link_path(anchor, namespace);
    match probe_age(metadata_store.store(), &anchor_path, params).await {
        AgeVerdict::Reapable => pending.push(target.clone()),
        AgeVerdict::Unknown => {
            out.age_unknown_kept += 1;
            warn!(key = %anchor_path, "scrub raw sweep: no last_modified for the dangling link; kept");
        }
        AgeVerdict::Fresh | AgeVerdict::Gone => {}
    }
}

/// For one subject revision, collect its referrer links whose referrer manifest
/// is no longer a current revision (its `Digest` link reads `NotFound`).
async fn collect_orphan_referrers_for_revision(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    subject: &Digest,
    params: &SweepParams,
    pending: &mut Vec<PendingOrphanLink>,
    out: &mut NamespaceReapOutcome,
) {
    let referrers = match metadata_store
        .list_referrer_digests(namespace, subject)
        .await
    {
        Ok(referrers) => referrers,
        Err(error) => {
            warn!(namespace = %namespace, subject = %subject, %error, "scrub raw sweep: failed to list referrer digests; keeping");
            return;
        }
    };
    for referrer in referrers {
        match metadata_store
            .read_link(namespace, &LinkKind::Digest(referrer.clone()))
            .await
        {
            Ok(_) => continue,
            Err(MetadataError::ReferenceNotFound) => {}
            Err(error) => {
                warn!(namespace = %namespace, referrer = %referrer, %error, "scrub raw sweep: referrer revision read failed; keeping");
                continue;
            }
        }
        let link = LinkKind::Referrer(subject.clone(), referrer.clone());
        collect_orphan_link_candidate(
            metadata_store,
            namespace,
            link,
            referrer,
            OrphanLinkKind::OrphanReferrer,
            params,
            pending,
            out,
        )
        .await;
    }
}

/// Collect phantom `referenced_by` entries on every layer and config link in the
/// namespace whose referring revision's `Digest` link is gone.
async fn collect_phantom_links(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    params: &SweepParams,
    pending: &mut Vec<PendingOrphanLink>,
    out: &mut NamespaceReapOutcome,
) {
    let mut layers = list_all::layer_links(metadata_store, namespace);
    while let Some(result) = layers.next().await {
        match result {
            Ok(digest) => {
                collect_phantom_referrers_for_link(
                    metadata_store,
                    namespace,
                    LinkKind::Layer(digest),
                    params,
                    pending,
                    out,
                )
                .await;
            }
            Err(error) => {
                warn!(namespace = %namespace, %error, "scrub raw sweep: failed to enumerate layer links; keeping");
                break;
            }
        }
    }
    drop(layers);
    let mut configs = list_all::config_links(metadata_store, namespace);
    while let Some(result) = configs.next().await {
        match result {
            Ok(digest) => {
                collect_phantom_referrers_for_link(
                    metadata_store,
                    namespace,
                    LinkKind::Config(digest),
                    params,
                    pending,
                    out,
                )
                .await;
            }
            Err(error) => {
                warn!(namespace = %namespace, %error, "scrub raw sweep: failed to enumerate config links; keeping");
                break;
            }
        }
    }
}

/// Read one layer/config link and, for each `referenced_by` entry whose `Digest`
/// revision link is gone, collect a phantom-link candidate.
async fn collect_phantom_referrers_for_link(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    link: LinkKind,
    params: &SweepParams,
    pending: &mut Vec<PendingOrphanLink>,
    out: &mut NamespaceReapOutcome,
) {
    let metadata = match metadata_store.read_link(namespace, &link).await {
        Ok(metadata) => metadata,
        Err(MetadataError::ReferenceNotFound) => return,
        Err(error) => {
            warn!(namespace = %namespace, link = %link, %error, "scrub raw sweep: link read failed for phantom scan; keeping");
            return;
        }
    };
    for stale in metadata.referenced_by {
        match metadata_store
            .read_link(namespace, &LinkKind::Digest(stale.clone()))
            .await
        {
            Ok(_) => continue,
            Err(MetadataError::ReferenceNotFound) => {}
            Err(error) => {
                warn!(namespace = %namespace, referrer = %stale, %error, "scrub raw sweep: phantom referrer revision read failed; keeping");
                continue;
            }
        }
        collect_orphan_link_candidate(
            metadata_store,
            namespace,
            link.clone(),
            stale,
            OrphanLinkKind::PhantomLink,
            params,
            pending,
            out,
        )
        .await;
    }
}

/// Age-gate one orphan derived link on its link file and collect it. The
/// binding re-check runs under the blob-data lock at removal time.
#[allow(clippy::too_many_arguments)]
async fn collect_orphan_link_candidate(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    link: LinkKind,
    referrer: Digest,
    kind: OrphanLinkKind,
    params: &SweepParams,
    pending: &mut Vec<PendingOrphanLink>,
    out: &mut NamespaceReapOutcome,
) {
    let link_path = path_builder::link_path(&link, namespace);
    match probe_age(metadata_store.store(), &link_path, params).await {
        AgeVerdict::Reapable => pending.push(PendingOrphanLink {
            link,
            referrer,
            kind,
        }),
        AgeVerdict::Unknown => {
            out.age_unknown_kept += 1;
            warn!(key = %link_path, "scrub raw sweep: no last_modified for the orphan link; kept");
        }
        AgeVerdict::Fresh | AgeVerdict::Gone => {}
    }
}

/// Apply (or would-count) one namespace's missing-body candidates through the
/// blob-data-locked body-absent re-check, which keeps a revision whose body
/// reappeared and deletes dangling tags even when the self-link is already gone.
async fn apply_missing_bodies(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    namespace: &Namespace,
    pending: Vec<Digest>,
    params: &SweepParams,
    out: &mut NamespaceReapOutcome,
    cancel: &CancellationToken,
) {
    if !params.commit {
        out.missing_bodies_removed += pending.len() as u64;
        return;
    }
    for (index, target) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            out.token_rejected += (pending.len() - index) as u64;
            return;
        }
        match orphan_missing_body_locked(metadata_store, blob_store, namespace, target, cancel)
            .await
        {
            Ok(ApplyOutcome::Applied) => out.missing_bodies_removed += 1,
            Ok(ApplyOutcome::Kept) => {}
            Err(Error::Cancelled) => {
                out.token_rejected += (pending.len() - index) as u64;
                return;
            }
            Err(error) => {
                out.failed_deletes += 1;
                warn!(
                    namespace = %namespace, target = %target, %error,
                    "scrub raw sweep: failed to remove dangling missing-body revision"
                );
            }
        }
    }
}

/// Apply (or would-count) one namespace's orphan derived-link candidates through
/// the blob-data-locked revision re-check, which keeps a link whose referring
/// revision reappeared.
async fn apply_orphan_links(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
    pending: Vec<PendingOrphanLink>,
    params: &SweepParams,
    out: &mut NamespaceReapOutcome,
    cancel: &CancellationToken,
) {
    if !params.commit {
        out.orphan_links_removed += pending.len() as u64;
        return;
    }
    for (index, orphan) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            out.token_rejected += (pending.len() - index) as u64;
            return;
        }
        // A phantom-link entry removes only the stale referrer (the executor
        // cascades a link delete when the last referrer goes); an orphan-referrer
        // removes the whole `Referrer` link.
        let op = match orphan.kind {
            OrphanLinkKind::OrphanReferrer => LinkOperation::delete(orphan.link.clone()),
            OrphanLinkKind::PhantomLink => {
                LinkOperation::delete_with_referrer(orphan.link.clone(), orphan.referrer.clone())
            }
        };
        match remove_orphan_link_locked(metadata_store, namespace, &orphan.referrer, op, cancel)
            .await
        {
            Ok(ApplyOutcome::Applied) => out.orphan_links_removed += 1,
            Ok(ApplyOutcome::Kept) => {}
            Err(Error::Cancelled) => {
                out.token_rejected += (pending.len() - index) as u64;
                return;
            }
            Err(error) => {
                out.failed_deletes += 1;
                warn!(
                    namespace = %namespace, link = %orphan.link, %error,
                    "scrub raw sweep: failed to remove orphan derived metadata link"
                );
            }
        }
    }
}

// Blob walk: per-digest grant/orphan work, unrecognized triage, legacy convergence.

/// A stale tracked grant collected for removal; the removal targets the
/// `refs/<ns>.json` shard through the locked helper.
struct PendingStaleGrant {
    digest: Digest,
    namespace: Namespace,
    link: LinkKind,
}

/// An obsolete bare `Blob` self-grant collected for revocation.
struct PendingSelfGrant {
    digest: Digest,
    namespace: Namespace,
}

/// A de-configured namespace's grant shard collected for deletion on the
/// metadata backend. `digest` feeds the orphan re-check after the revoke.
struct PendingGrantShard {
    digest: Digest,
    shard_key: String,
}

/// One digest's collected candidates and tally deltas from the fanned-out
/// per-digest work.
#[derive(Default)]
struct DigestOutcome {
    stale_grants: Vec<PendingStaleGrant>,
    self_grants: Vec<PendingSelfGrant>,
    deconfigured_shards: Vec<PendingGrantShard>,
    orphan: Option<Digest>,
    age_unknown_kept: u64,
}

/// Run every blob-index-derived collector for one digest from a single index
/// read. `Err(ReferenceNotFound)` (which already folds the legacy `index.json`
/// fallback) IS the orphan verdict; an owned index feeds the stale-grant,
/// self-grant, and de-configured-grant collectors. A failed index read keeps
/// the blob (never reap on a failed read).
async fn per_digest_blob_work(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    digest: Digest,
    deconfigured: &[String],
    params: &SweepParams,
) -> DigestOutcome {
    let mut out = DigestOutcome::default();
    if params.any_rebuild_fatal && deconfigured.is_empty() {
        // Every consumer of the index read is globally skipped this run.
        return out;
    }
    match metadata_store.read_blob_index(&digest).await {
        Ok(index) => {
            triage_index_grants(
                metadata_store,
                &digest,
                &index,
                deconfigured,
                params,
                &mut out,
            )
            .await;
        }
        Err(MetadataError::ReferenceNotFound) => {
            // Unowned bytes: the orphan verdict, age-gated on the data key. The
            // under-lock `has_blob_references` re-check is the delete-time
            // authority.
            if !params.any_rebuild_fatal {
                match probe_age(
                    blob_store.store.as_ref(),
                    &path_builder::blob_path(&digest),
                    params,
                )
                .await
                {
                    AgeVerdict::Reapable => out.orphan = Some(digest),
                    AgeVerdict::Unknown => {
                        out.age_unknown_kept += 1;
                        warn!(digest = %digest, "scrub raw sweep: no last_modified for the orphan blob; kept");
                    }
                    AgeVerdict::Fresh | AgeVerdict::Gone => {}
                }
            }
        }
        Err(error) => {
            warn!(
                digest = %digest, %error,
                "scrub raw sweep: failed to read blob index; keeping"
            );
        }
    }
    out
}

/// Triage one blob's already-read index per owning namespace: de-configured
/// namespaces contribute their whole shard to the config-ownership revoke;
/// other namespaces run the stale tracked-grant and bare self-grant triage
/// unless a rebuild-fatal run skips them whole.
async fn triage_index_grants(
    metadata_store: &Arc<MetadataStore>,
    digest: &Digest,
    index: &BlobIndex,
    deconfigured: &[String],
    params: &SweepParams,
    out: &mut DigestOutcome,
) {
    for (namespace, links) in &index.namespace {
        if deconfigured
            .iter()
            .any(|name| name.as_str() == namespace.as_ref())
        {
            out.deconfigured_shards.push(PendingGrantShard {
                digest: digest.clone(),
                shard_key: path_builder::blob_index_shard_path(digest, namespace),
            });
            continue;
        }
        if params.any_rebuild_fatal {
            continue;
        }
        triage_namespace_grants(metadata_store, digest, namespace, links, params, out).await;
    }
}

/// Triage one namespace's grant entries: any entry (tracked
/// Layer/Config/Manifest, or non-tracked Tag/Digest/Referrer) whose backing
/// link file reads `ReferenceNotFound` is a stale-grant candidate, so a stale
/// non-tracked entry can no longer pin bytes or phantom-protect a manifest
/// from retention forever; a bare `Blob` self-grant whose namespace holds no
/// link file to the digest is a revocation candidate (its own dedicated
/// pass). All pass the grant age gate, anchored on the shard object's
/// `last_modified`, and the removal re-checks the backing link under the
/// blob-data lock. Any transient probe error keeps everything (never reap on
/// a failed read).
async fn triage_namespace_grants(
    metadata_store: &Arc<MetadataStore>,
    digest: &Digest,
    namespace: &Namespace,
    links: &HashSet<LinkKind>,
    params: &SweepParams,
    out: &mut DigestOutcome,
) {
    let mut any_live = false;
    let mut probe_failed = false;
    let mut stale: Vec<LinkKind> = Vec::new();
    for link in links {
        if matches!(link, LinkKind::Blob(_)) {
            continue;
        }
        match metadata_store.read_link(namespace, link).await {
            Ok(_) => any_live = true,
            Err(MetadataError::ReferenceNotFound) => stale.push(link.clone()),
            Err(error) => {
                probe_failed = true;
                warn!(
                    digest = %digest, namespace = %namespace, link = %link, %error,
                    "scrub raw sweep: stale-grant link probe failed; keeping (never reap on a \
                     transient error)"
                );
            }
        }
    }
    let mut self_grant = false;
    if links.contains(&LinkKind::Blob(digest.clone())) && !any_live && !probe_failed {
        self_grant = namespace_holds_no_links(metadata_store, digest, namespace).await;
    }
    if stale.is_empty() && !self_grant {
        return;
    }
    match grant_age(metadata_store, digest, namespace, params).await {
        AgeVerdict::Reapable => {
            for link in stale {
                out.stale_grants.push(PendingStaleGrant {
                    digest: digest.clone(),
                    namespace: namespace.clone(),
                    link,
                });
            }
            if self_grant {
                out.self_grants.push(PendingSelfGrant {
                    digest: digest.clone(),
                    namespace: namespace.clone(),
                });
            }
        }
        AgeVerdict::Unknown => {
            out.age_unknown_kept += 1;
            warn!(
                digest = %digest, namespace = %namespace,
                "scrub raw sweep: no last_modified for the grant shard; kept"
            );
        }
        AgeVerdict::Fresh | AgeVerdict::Gone => {}
    }
}

/// Whether `namespace` holds no link file of any kind referencing `digest`.
/// Any live link, or any transient probe error, keeps the self-grant. Tags
/// are not probed directly: a live tag implies a live `Digest` revision link
/// (the rebuild heals a lost one before the sweep, and a rebuild-fatal
/// namespace never reaches this triage), so the `Digest` probe covers them
/// transitively.
async fn namespace_holds_no_links(
    metadata_store: &MetadataStore,
    digest: &Digest,
    namespace: &Namespace,
) -> bool {
    for link in [
        LinkKind::Blob(digest.clone()),
        LinkKind::Layer(digest.clone()),
        LinkKind::Config(digest.clone()),
        LinkKind::Digest(digest.clone()),
    ] {
        match metadata_store.read_link(namespace, &link).await {
            Ok(_) => return false,
            Err(MetadataError::ReferenceNotFound) => {}
            Err(error) => {
                warn!(
                    digest = %digest, namespace = %namespace, link = %link, %error,
                    "scrub raw sweep: self-grant link probe failed; keeping"
                );
                return false;
            }
        }
    }
    true
}

/// Converge this page's legacy `index.json` files into `refs/` shards, before
/// the page's per-digest index reads. Suppressed when `params.converge` is
/// false (`--dry-run`), where the count is reported instead.
async fn converge_legacy_page(
    metadata_store: &Arc<MetadataStore>,
    legacy: Vec<Digest>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if legacy.is_empty() || cancel.is_cancelled() {
        return;
    }
    if !params.converge {
        info!(
            count = legacy.len(),
            "scrub raw sweep: legacy blob indexes present; convergence suppressed under --dry-run"
        );
        return;
    }
    let results = stream::iter(legacy)
        .map(|digest| async move {
            let result = metadata_store.migrate_blob_index(&digest).await;
            (digest, result)
        })
        .buffer_unordered(params.fanout)
        .collect::<Vec<_>>()
        .await;
    for (digest, result) in results {
        match result {
            Ok(()) => tally.converged += 1,
            Err(error) => {
                tally.convergence_failed += 1;
                warn!(digest = %digest, %error, "scrub raw sweep: failed to migrate legacy blob index; continuing");
            }
        }
    }
}

/// Walk `v2/blobs` on the blob handle one page at a time: classify every key,
/// converge this page's legacy indexes, run the per-digest work with `fanout`
/// concurrency, then apply the page's candidates in dependency order
/// (unrecognized, stale grants, self-grants, de-configured shards, byte reaps).
/// Each grant removal can drop a blob's last reference, so the byte reap runs
/// last and de-configured shard deletes feed their digests into it.
#[allow(clippy::too_many_lines)]
async fn sweep_blob_root(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    deconfigured: &[String],
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    let store = blob_store.store.as_ref();
    let root = path_builder::blobs_root_dir();
    let mut token = None;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let page = match store.list(root, LIST_PAGE, token).await {
            Ok(page) => page,
            Err(error) => {
                warn!(root, %error, "scrub raw sweep: failed to list blob root");
                tally.enumeration_failed = true;
                return;
            }
        };
        let mut digests: Vec<Digest> = Vec::new();
        let mut legacy: Vec<Digest> = Vec::new();
        let mut unrecognized: Vec<PendingUnrecognized> = Vec::new();
        for item in &page.items {
            // `list` returns prefix-relative keys; rebuild the absolute key.
            let key = format!("{root}/{item}");
            let class = classify_key(&key);
            tally.record(&key, &class);
            match class {
                KeyClass::Unrecognized(reason) => unrecognized.push(PendingUnrecognized {
                    key,
                    reason,
                    backend: UnrecognizedBackend::Blob,
                }),
                KeyClass::Owned(KeyDomain::BlobBytes) => {
                    if let Some(digest) = blob_digest_from_key(&key) {
                        digests.push(digest);
                    } else {
                        // A BlobBytes key always yields a digest; skip defensively.
                        warn!(
                            key,
                            "scrub raw sweep: blob-bytes key yielded no digest; skipping"
                        );
                    }
                }
                KeyClass::Owned(KeyDomain::BlobIndexGrant) => {
                    // In a shared topology this walk sees the physical grant
                    // shards; in a split one they live on the metadata backend
                    // and its own walk converges them.
                    if params.shared_backend
                        && let Some(digest) = legacy_index_digest_from_key(&key)
                    {
                        legacy.push(digest);
                    }
                }
                _ => {}
            }
        }
        converge_legacy_page(metadata_store, legacy, params, tally, cancel).await;
        let outcomes = stream::iter(digests)
            .map(|digest| {
                per_digest_blob_work(metadata_store, blob_store, digest, deconfigured, params)
            })
            .buffer_unordered(params.fanout)
            .collect::<Vec<DigestOutcome>>()
            .await;
        let mut stale_grants = Vec::new();
        let mut self_grants = Vec::new();
        let mut shards = Vec::new();
        let mut orphans = Vec::new();
        for outcome in outcomes {
            tally.age_unknown_kept += outcome.age_unknown_kept;
            stale_grants.extend(outcome.stale_grants);
            self_grants.extend(outcome.self_grants);
            shards.extend(outcome.deconfigured_shards);
            if let Some(digest) = outcome.orphan {
                orphans.push(digest);
            }
        }
        apply_unrecognized(
            metadata_store,
            blob_store,
            unrecognized,
            params,
            tally,
            cancel,
        )
        .await;
        apply_stale_grants(
            metadata_store,
            blob_store,
            stale_grants,
            params,
            tally,
            cancel,
        )
        .await;
        apply_self_grants(
            metadata_store,
            blob_store,
            self_grants,
            params,
            tally,
            cancel,
        )
        .await;
        apply_deconfigured_shards(
            metadata_store,
            blob_store,
            shards,
            params,
            tally,
            cancel,
            &mut orphans,
        )
        .await;
        apply_orphan_blobs(metadata_store, blob_store, orphans, params, tally, cancel).await;
        token = page.next_token;
        if token.is_none() {
            return;
        }
    }
}

/// Delete (or would-count) this page's unrecognized keys, each age-gated on the
/// enumerating backend and deleted through the same handle. Runs only with
/// `--delete-unrecognized`; deletion additionally requires `--commit`.
async fn apply_unrecognized(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    pending: Vec<PendingUnrecognized>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if !params.delete_unrecognized || pending.is_empty() {
        return;
    }
    for (index, item) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            abort_remaining(tally, (pending.len() - index) as u64);
            return;
        }
        let store = match item.backend {
            UnrecognizedBackend::Blob => blob_store.store.as_ref(),
            UnrecognizedBackend::Metadata => metadata_store.store(),
        };
        match probe_age(store, &item.key, params).await {
            AgeVerdict::Reapable => {}
            AgeVerdict::Unknown => {
                tally.age_unknown_kept += 1;
                warn!(key = %item.key, "scrub raw sweep: no last_modified for the unrecognized key; kept");
                continue;
            }
            AgeVerdict::Fresh | AgeVerdict::Gone => continue,
        }
        if !params.commit {
            tally.deleted += 1;
            continue;
        }
        match store.delete(&item.key).await {
            Ok(()) => {
                tally.deleted += 1;
                info!(key = %item.key, reason = item.reason.label(), "scrub raw sweep: deleted unrecognized key");
            }
            Err(error) => {
                tally.failed_deletes += 1;
                warn!(key = %item.key, %error, "scrub raw sweep: failed to delete unrecognized key");
            }
        }
    }
}

/// Remove (or would-count) this page's stale tracked grants through the
/// blob-data-locked link re-check, which also reclaims the bytes when it drops
/// the blob's last reference.
async fn apply_stale_grants(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    pending: Vec<PendingStaleGrant>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if !params.commit {
        tally.stale_grants_removed += pending.len() as u64;
        return;
    }
    for (index, grant) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            abort_remaining(tally, (pending.len() - index) as u64);
            return;
        }
        match remove_stale_grant_locked(
            metadata_store,
            blob_store,
            &grant.namespace,
            &grant.digest,
            &grant.link,
            cancel,
        )
        .await
        {
            Ok(ApplyOutcome::Applied) => tally.stale_grants_removed += 1,
            Ok(ApplyOutcome::Kept) => {}
            Err(Error::Cancelled) => {
                abort_remaining(tally, (pending.len() - index) as u64);
                return;
            }
            Err(error) => {
                tally.failed_deletes += 1;
                warn!(
                    digest = %grant.digest, namespace = %grant.namespace, link = %grant.link, %error,
                    "scrub raw sweep: failed to remove stale tracked blob-ownership grant"
                );
            }
        }
    }
}

/// Revoke (or would-count) this page's obsolete bare self-grants through the
/// blob-data-locked link re-probe, reclaiming the bytes when the revocation
/// drops the last reference.
async fn apply_self_grants(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    pending: Vec<PendingSelfGrant>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if !params.commit {
        tally.self_grants_revoked += pending.len() as u64;
        return;
    }
    for (index, grant) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            abort_remaining(tally, (pending.len() - index) as u64);
            return;
        }
        match revoke_bare_self_grant_locked(
            metadata_store,
            blob_store,
            &grant.namespace,
            &grant.digest,
            params.run_epoch,
            params.grace,
            cancel,
        )
        .await
        {
            Ok(ApplyOutcome::Applied) => tally.self_grants_revoked += 1,
            Ok(ApplyOutcome::Kept) => {}
            Err(Error::Cancelled) => {
                abort_remaining(tally, (pending.len() - index) as u64);
                return;
            }
            Err(error) => {
                tally.failed_deletes += 1;
                warn!(
                    digest = %grant.digest, namespace = %grant.namespace, %error,
                    "scrub raw sweep: failed to revoke obsolete bare self-grant"
                );
            }
        }
    }
}

/// Delete (or would-count) this page's de-configured grant shards through the
/// metadata handle, which owns them in every topology. A revoked shard's digest
/// feeds the orphan byte reap when the now-unreferenced bytes pass the age
/// gate, so a blob whose last owner was de-configured is reclaimed the same
/// run. On a rebuild-fatal run the byte reap is skipped whole (a fatal
/// namespace's grants may be missing rather than absent), so the revoked
/// digests are not fed; the shard revoke itself stays config-driven.
async fn apply_deconfigured_shards(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    pending: Vec<PendingGrantShard>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
    orphans: &mut Vec<Digest>,
) {
    if !params.commit {
        tally.deconfigured_grants_revoked += pending.len() as u64;
        return;
    }
    for (index, shard) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            abort_remaining(tally, (pending.len() - index) as u64);
            return;
        }
        match metadata_store.store().delete(&shard.shard_key).await {
            Ok(()) => {
                tally.deconfigured_grants_revoked += 1;
                if params.any_rebuild_fatal {
                    // The global byte reap is skipped this run (keep over
                    // reap); the unpinned bytes converge on a later clean run.
                    continue;
                }
                match probe_age(
                    blob_store.store.as_ref(),
                    &path_builder::blob_path(&shard.digest),
                    params,
                )
                .await
                {
                    AgeVerdict::Reapable => orphans.push(shard.digest.clone()),
                    AgeVerdict::Unknown => {
                        tally.age_unknown_kept += 1;
                        warn!(digest = %shard.digest, "scrub raw sweep: no last_modified for the unpinned blob; kept");
                    }
                    AgeVerdict::Fresh | AgeVerdict::Gone => {}
                }
            }
            Err(error) => {
                tally.deconfigured_grants_partial += 1;
                warn!(
                    key = %shard.shard_key, %error,
                    "scrub config-ownership sweep: failed to delete blob-ownership grant shard"
                );
            }
        }
    }
}

/// Reap (or would-count) this page's orphan blobs through the blob-data-locked
/// `has_blob_references` re-check, which keeps a blob whose reference reappeared.
async fn apply_orphan_blobs(
    metadata_store: &MetadataStore,
    blob_store: &BlobStore,
    pending: Vec<Digest>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    if !params.commit {
        tally.orphan_reaped += pending.len() as u64;
        return;
    }
    for (index, digest) in pending.iter().enumerate() {
        if cancel.is_cancelled() {
            abort_remaining(tally, (pending.len() - index) as u64);
            return;
        }
        match delete_orphan_blob_locked(metadata_store, blob_store, digest, cancel).await {
            Ok(ApplyOutcome::Applied) => tally.orphan_reaped += 1,
            Ok(ApplyOutcome::Kept) => {}
            Err(Error::Cancelled) => {
                abort_remaining(tally, (pending.len() - index) as u64);
                return;
            }
            Err(error) => {
                tally.failed_deletes += 1;
                warn!(digest = %digest, %error, "scrub raw sweep: failed to reap orphan blob");
            }
        }
    }
}

// Unrecognized root scans.

/// One owned root to enumerate for unrecognized keys.
struct ScanRoot<'a> {
    store: &'a Store,
    root: String,
    backend: UnrecognizedBackend,
    /// Key prefixes excluded from classification: keys another pass owns (the
    /// de-configured subtree or upload deletes), applied in both modes so
    /// report-only counts match `--commit`.
    skip_prefixes: Vec<String>,
    /// Trigger legacy `index.json` convergence for owned grant keys seen on
    /// this walk (set on the walk over the physical metadata backend).
    converge_legacy: bool,
}

/// Enumerate `target.root` one page at a time, classifying every key,
/// converging legacy indexes where this walk owns them, and deleting the
/// page's unrecognized keys through the enumerating handle.
async fn sweep_scan_root(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    target: &ScanRoot<'_>,
    params: &SweepParams,
    tally: &mut SweepTally,
    cancel: &CancellationToken,
) {
    let mut token = None;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let page = match target.store.list(&target.root, LIST_PAGE, token).await {
            Ok(page) => page,
            Err(error) => {
                warn!(root = %target.root, %error, "scrub raw sweep: failed to list root");
                tally.enumeration_failed = true;
                return;
            }
        };
        let mut pending: Vec<PendingUnrecognized> = Vec::new();
        let mut legacy: Vec<Digest> = Vec::new();
        for item in &page.items {
            // `list` returns prefix-relative keys; rebuild the absolute key.
            let key = format!("{}/{item}", target.root);
            if target
                .skip_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix.as_str()))
            {
                continue;
            }
            let class = classify_key(&key);
            tally.record(&key, &class);
            match class {
                KeyClass::Unrecognized(reason) => pending.push(PendingUnrecognized {
                    key,
                    reason,
                    backend: target.backend,
                }),
                KeyClass::Owned(KeyDomain::BlobIndexGrant) if target.converge_legacy => {
                    if let Some(digest) = legacy_index_digest_from_key(&key) {
                        legacy.push(digest);
                    }
                }
                _ => {}
            }
        }
        converge_legacy_page(metadata_store, legacy, params, tally, cancel).await;
        apply_unrecognized(metadata_store, blob_store, pending, params, tally, cancel).await;
        token = page.next_token;
        if token.is_none() {
            return;
        }
    }
}

/// The marker-subtree prefixes the config-ownership pass owns for the
/// de-configured namespaces, excluded from the unrecognized classification in
/// both modes. Only the marker subtrees are owned; a key under a nested
/// (possibly configured) child namespace is not.
fn deconfigured_marker_prefixes(deconfigured: &[String]) -> Vec<String> {
    deconfigured
        .iter()
        .filter_map(|name| path_builder::namespace_dir(name))
        .flat_map(|dir| {
            NAMESPACE_MARKER_SUBTREES
                .iter()
                .map(move |marker| format!("{dir}/{marker}/"))
        })
        .collect()
}

/// Warn, naming the rebuild-fatal namespaces, that the global byte reap and the
/// grant removals are skipped this run, along with the fatal namespaces'
/// per-namespace missing-body and orphan-link reaping.
fn warn_rebuild_fatal(params: &SweepParams) {
    let mut names: Vec<&str> = params.fatal_namespaces.iter().map(String::as_str).collect();
    names.sort_unstable();
    warn!(
        failed_namespaces = %names.join(", "),
        "scrub raw sweep: one or more namespaces failed their metadata walk; skipping the \
         global byte reap and grant removals this run, and skipping their per-namespace \
         missing-body and orphan-link reaping (keep over reap)"
    );
}

/// Classify every key under the owned roots and log the tally. The default path
/// is report-only: identical classification and would-counts, no mutation.
/// Under `--commit` the sweep deletes: unrecognized keys with
/// `--delete-unrecognized`, de-configured namespaces (own marker subtrees,
/// grants, uploads; refused when the resolver is empty), and the age-gated GC
/// categories
/// (orphan bytes, stale grants, bare self-grants, orphan links, missing-body
/// revisions).
///
/// `cancel` is the scrub lock's cancellation token: once it fires no further
/// deletes are dispatched and the run exits aborted.
pub async fn run_sweep(
    metadata_store: &Arc<MetadataStore>,
    blob_store: &BlobStore,
    resolver: &RepositoryResolver,
    params: &SweepParams,
    cancel: &CancellationToken,
) -> SweepTally {
    if params.any_rebuild_fatal {
        warn_rebuild_fatal(params);
    }
    info!(
        grace_secs = params.grace.num_seconds(),
        shared_backend = params.shared_backend,
        "scrub raw sweep: maintenance grace is the reap precondition for every GC category"
    );
    let mut tally = SweepTally::default();

    // One tree walk feeds every namespace-driven pass. On failure the run is
    // degraded and those passes are skipped (an empty de-configured set is the
    // conservative direction).
    let namespaces = match metadata_store.collect_all_namespaces().await {
        Ok(namespaces) => namespaces,
        Err(error) => {
            warn!(%error, "scrub raw sweep: namespace enumeration failed; skipping every namespace-driven pass (keep over reap)");
            tally.enumeration_failed = true;
            Vec::new()
        }
    };
    let deconfigured = collect_deconfigured_namespaces(resolver, &namespaces, &mut tally);
    let deconfigured_dirs = deconfigured_marker_prefixes(&deconfigured);

    delete_deconfigured_subtrees(metadata_store, &deconfigured, params, &mut tally, cancel).await;
    if !params.shared_backend {
        delete_deconfigured_uploads(blob_store, &deconfigured, params, &mut tally, cancel).await;
    }

    scan_namespaces(
        metadata_store,
        blob_store,
        &namespaces,
        &deconfigured,
        params,
        &mut tally,
        cancel,
    )
    .await;

    sweep_blob_root(
        metadata_store,
        blob_store,
        &deconfigured,
        params,
        &mut tally,
        cancel,
    )
    .await;

    // Metadata-handle scans; in a split topology the other handle's roots are
    // walked too so no coverage gap remains.
    let mut targets = vec![
        ScanRoot {
            store: metadata_store.store(),
            root: path_builder::repository_dir().to_string(),
            backend: UnrecognizedBackend::Metadata,
            skip_prefixes: deconfigured_dirs.clone(),
            converge_legacy: false,
        },
        ScanRoot {
            store: metadata_store.store(),
            root: path_builder::jobs_root_dir().to_string(),
            backend: UnrecognizedBackend::Metadata,
            skip_prefixes: Vec::new(),
            converge_legacy: false,
        },
    ];
    if !params.shared_backend {
        // The grant shards (and legacy index.json) physically live here.
        targets.push(ScanRoot {
            store: metadata_store.store(),
            root: path_builder::blobs_root_dir().to_string(),
            backend: UnrecognizedBackend::Metadata,
            skip_prefixes: Vec::new(),
            converge_legacy: true,
        });
        // Upload staging lives on the blob backend; only the de-configured
        // `_uploads` prefixes are excluded (that pass owns them), so junk under
        // a de-configured namespace elsewhere stays delete-unrecognized-eligible.
        targets.push(ScanRoot {
            store: blob_store.store.as_ref(),
            root: path_builder::repository_dir().to_string(),
            backend: UnrecognizedBackend::Blob,
            skip_prefixes: deconfigured_dirs
                .iter()
                .filter(|dir| dir.ends_with("/_uploads/"))
                .cloned()
                .collect(),
            converge_legacy: false,
        });
    }
    for target in &targets {
        sweep_scan_root(
            metadata_store,
            blob_store,
            target,
            params,
            &mut tally,
            cancel,
        )
        .await;
    }

    tally.log(params.commit);
    tally
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::Path};

    use angos_storage::{MemoryObjectStore, ObjectStore};
    use bytes::Bytes;
    use uuid::Uuid;

    use super::*;
    use crate::{
        command::scrub::test_support::AgedObjectStore,
        oci::Tag,
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            Repository,
            metadata_store::{
                BlobIndexOperation, LinkMetadata,
                tests::{legacy_blob_index_with, put_legacy_index},
            },
            test_utils::{
                FSRegistryTestCase, RegistryTestCase, S3RegistryTestCase, build_store,
                locked_executor_over, metadata_store_over, put_blob_direct,
            },
        },
    };

    const HASH_256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// A clean tally maps to exit 0 / Clean.
    #[test]
    fn ran_exit_code_clean() {
        let tally = SweepTally::default();
        assert_eq!(tally.ran_exit_code(0, 0, true), (0, MarkerStatus::Clean));
    }

    /// Every degraded signal maps to exit 2 / Degraded: partial de-configured
    /// deletes, a convergence failure, a failed delete, a failed enumeration, a
    /// failed namespace, a failed action apply, or an incomplete walk.
    #[test]
    fn ran_exit_code_degraded() {
        for tally in [
            SweepTally {
                deconfigured_partial: 1,
                ..Default::default()
            },
            SweepTally {
                deconfigured_grants_partial: 1,
                ..Default::default()
            },
            SweepTally {
                deconfigured_uploads_partial: 1,
                ..Default::default()
            },
            SweepTally {
                convergence_failed: 1,
                ..Default::default()
            },
            SweepTally {
                failed_deletes: 1,
                ..Default::default()
            },
            SweepTally {
                enumeration_failed: true,
                ..Default::default()
            },
        ] {
            assert_eq!(tally.ran_exit_code(0, 0, true), (2, MarkerStatus::Degraded));
        }

        // A failed namespace degrades on its own.
        let tally = SweepTally::default();
        assert_eq!(tally.ran_exit_code(1, 0, true), (2, MarkerStatus::Degraded));

        // A failed action apply degrades on its own.
        let tally = SweepTally::default();
        assert_eq!(tally.ran_exit_code(0, 1, true), (2, MarkerStatus::Degraded));

        // An incomplete namespace enumeration degrades on its own.
        let tally = SweepTally::default();
        assert_eq!(
            tally.ran_exit_code(0, 0, false),
            (2, MarkerStatus::Degraded)
        );
    }

    /// A token-rejected tally maps to exit 3 / Aborted, dominating a concurrent
    /// degraded signal including a failed-namespace count.
    #[test]
    fn ran_exit_code_aborted_dominates_degraded() {
        let tally = SweepTally {
            token_rejected: 1,
            deconfigured_partial: 1,
            ..Default::default()
        };
        assert_eq!(tally.ran_exit_code(0, 0, true), (3, MarkerStatus::Aborted));

        let tally = SweepTally {
            token_rejected: 1,
            ..Default::default()
        };
        assert_eq!(tally.ran_exit_code(2, 1, false), (3, MarkerStatus::Aborted));
    }

    /// `to_marker_tallies` mirrors the sweep counters and leaves `action_failed`
    /// zero for the caller to overlay.
    #[test]
    fn to_marker_tallies_mirrors_counters() {
        let mut tally = SweepTally {
            total: 7,
            deleted: 2,
            orphan_reaped: 3,
            deconfigured_namespaces: 4,
            deconfigured_deleted: 3,
            deconfigured_partial: 1,
            self_grants_revoked: 2,
            age_unknown_kept: 5,
            failed_deletes: 1,
            converged: 6,
            convergence_failed: 1,
            ..Default::default()
        };
        *tally.unrecognized.entry("no-minter-match").or_default() += 5;

        let marker = tally.to_marker_tallies();
        assert_eq!(marker.total, 7);
        assert_eq!(marker.deleted, 2);
        assert_eq!(marker.orphan_reaped, 3);
        assert_eq!(marker.deconfigured_namespaces, 4);
        assert_eq!(marker.deconfigured_deleted, 3);
        assert_eq!(marker.deconfigured_partial, 1);
        assert_eq!(marker.self_grants_revoked, 2);
        assert_eq!(marker.age_unknown_kept, 5);
        assert_eq!(marker.failed_deletes, 1);
        assert_eq!(marker.converged, 6);
        assert_eq!(marker.failed, 1);
        assert_eq!(marker.unrecognized, 5);
        // The action counter is left zero here for the caller to overlay.
        assert_eq!(marker.action_failed, 0);
    }

    /// A resolver configured with the given repository prefixes (minimal repos).
    fn resolver(prefixes: &[&str]) -> Arc<RepositoryResolver> {
        let mut repositories = HashMap::new();
        for prefix in prefixes {
            repositories.insert(
                (*prefix).to_string(),
                Repository {
                    name: Namespace::new(prefix).unwrap(),
                    upstreams: Vec::new(),
                    replication: Vec::new(),
                    retention_policy: RetentionPolicy::new(
                        &RetentionPolicyConfig::default(),
                        Arc::new(SystemClock),
                    ),
                    immutable_tags: false,
                    immutable_tags_exclusions: Vec::new(),
                },
            );
        }
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    /// Sweep params for one run. `run_epoch` is stamped at call time, so seeds
    /// planted before this call read as at least age zero.
    fn params(commit: bool, delete_unrecognized: bool, grace: ChronoDuration) -> SweepParams {
        SweepParams {
            run_epoch: Utc::now(),
            commit,
            delete_unrecognized,
            fatal_namespaces: Arc::new(HashSet::new()),
            any_rebuild_fatal: false,
            grace,
            shared_backend: true,
            fanout: 8,
            converge: true,
        }
    }

    /// Seed a namespace with a revision and a tag pointing at it, the shape a
    /// normal manifest push leaves.
    async fn seed_image(metadata_store: &Arc<MetadataStore>, namespace: &Namespace) {
        let digest = put_blob_direct(metadata_store.store(), b"deconfigured manifest").await;
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
    }

    /// Seed a blob-ownership grant shard: byte data plus a `LinkKind::Blob`
    /// self-grant, the end-state of a pushed-but-untagged blob. Returns the digest.
    async fn seed_grant(metadata_store: &Arc<MetadataStore>, namespace: &Namespace) -> Digest {
        let blob = put_blob_direct(metadata_store.store(), b"grant bytes").await;
        metadata_store
            .update_blob_index(
                namespace,
                &blob,
                BlobIndexOperation::Insert(LinkKind::Blob(blob.clone())),
            )
            .await
            .unwrap();
        blob
    }

    /// Seed a grant a live manifest references: the blob's self-grant plus a
    /// tracked layer link, making it a live reference the sweep never revokes.
    async fn seed_referenced_grant(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> Digest {
        let blob = seed_grant(metadata_store, namespace).await;
        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create(
                    LinkKind::Layer(blob.clone()),
                    blob.clone(),
                )],
            )
            .await
            .unwrap();
        blob
    }

    /// Seed a stale tracked grant: a tracked `LinkKind::Layer` grant entry in
    /// `refs/<ns>.json` with no backing layer link file, so its grant is stale.
    /// Returns the digest.
    async fn seed_stale_tracked_grant(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> Digest {
        let blob = put_blob_direct(metadata_store.store(), b"stale tracked grant bytes").await;
        metadata_store
            .update_blob_index(
                namespace,
                &blob,
                BlobIndexOperation::Insert(LinkKind::Layer(blob.clone())),
            )
            .await
            .unwrap();
        blob
    }

    /// A junk key under `v2/blobs` whose hash tail fails digest validation, so the
    /// classifier reports it unrecognized.
    fn unrecognized_blob_key(tag: &str) -> String {
        format!("{}/sha256/aa/{tag}/foreign", path_builder::blobs_root_dir())
    }

    /// Seed an orphan blob: byte data only, no grant shard, so `read_blob_index`
    /// reads `ReferenceNotFound` and the orphan pass may reap it.
    async fn seed_orphan_blob(metadata_store: &Arc<MetadataStore>, content: &[u8]) -> Digest {
        put_blob_direct(metadata_store.store(), content).await
    }

    /// Seed a referenced blob: byte data plus a layer link, so the blob index is
    /// owned and the orphan pass must keep it.
    async fn seed_referenced_blob(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
        content: &[u8],
    ) -> Digest {
        let digest = put_blob_direct(metadata_store.store(), content).await;
        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::create(
                    LinkKind::Layer(digest.clone()),
                    digest.clone(),
                )],
            )
            .await
            .unwrap();
        digest
    }

    /// Run a grace-0 sweep configured for GC reaping: unrecognized and the config
    /// pass are off, only `commit` drives the reap categories.
    async fn run_orphan_sweep(
        metadata_store: &Arc<MetadataStore>,
        blob_store: &BlobStore,
        commit: bool,
    ) -> SweepTally {
        run_sweep(
            metadata_store,
            blob_store,
            &resolver(&["keep"]),
            &params(commit, false, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await
    }

    /// A referenced blob survives a committed grace-0 sweep: a blob any namespace
    /// references is never reaped.
    #[tokio::test]
    async fn referenced_blob_survives_committed_orphan_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_referenced_blob(&metadata_store, &namespace, b"referenced layer").await;

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            blob_store.read(&digest).await.is_ok(),
            "a referenced blob must survive a committed sweep, even at grace 0"
        );

        case.cleanup().await;
    }

    /// An orphan blob older than the grace is hard-deleted under --commit; the
    /// verdict is derived from the single `read_blob_index` outcome.
    #[tokio::test]
    async fn orphan_blob_hard_deleted_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let digest = seed_orphan_blob(&metadata_store, b"aged orphan bytes").await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            matches!(
                blob_store.read(&digest).await,
                Err(blob_store::Error::BlobNotFound)
            ),
            "an orphan blob past the grace must be hard-deleted under --commit"
        );
        assert_eq!(tally.orphan_reaped, 1);

        case.cleanup().await;
    }

    /// Report-only leaves the orphan in place and holds the would-reap count.
    #[tokio::test]
    async fn report_only_leaves_orphan_blob_in_place() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let digest = seed_orphan_blob(&metadata_store, b"report-only orphan bytes").await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, false).await;

        assert!(
            blob_store.read(&digest).await.is_ok(),
            "report-only must leave the orphan blob in place"
        );
        assert_eq!(
            tally.orphan_reaped, 1,
            "report-only must hold the would-reap count"
        );

        case.cleanup().await;
    }

    /// The locked re-check keeps a blob with a live reference, pinning the
    /// under-lock `has_blob_references` re-verify.
    #[tokio::test]
    async fn orphan_sweep_keeps_blob_with_live_reference() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/svc").unwrap();
        let digest =
            seed_referenced_blob(&metadata_store, &namespace, b"live-reference layer").await;

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            blob_store.read(&digest).await.is_ok(),
            "the locked re-check must keep a blob with a live reference"
        );

        case.cleanup().await;
    }

    /// A bare self-grant past the grace whose namespace holds no links to the
    /// digest is revoked under --commit, reclaiming the bytes it pinned.
    #[tokio::test]
    async fn aged_bare_self_grant_revoked_and_bytes_reclaimed() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_grant(&metadata_store, &namespace).await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &digest)
                .await
                .is_err(),
            "an obsolete bare self-grant past the grace must be revoked under --commit"
        );
        assert!(
            matches!(
                blob_store.read(&digest).await,
                Err(blob_store::Error::BlobNotFound)
            ),
            "revoking the last reference must reclaim the bytes"
        );
        assert_eq!(tally.self_grants_revoked, 1);

        case.cleanup().await;
    }

    /// A live tracked grant (its backing layer link file exists) keeps both the
    /// tracked grant and the co-resident self-grant.
    #[tokio::test]
    async fn live_tracked_grant_survives_committed_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        // `seed_referenced_grant` creates the self-grant, the layer link file,
        // and the matching tracked grant.
        let digest = seed_referenced_grant(&metadata_store, &namespace).await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &digest)
                .await
                .is_ok(),
            "a live tracked grant must survive a committed sweep, even at grace 0"
        );
        assert!(
            blob_store.read(&digest).await.is_ok(),
            "a referenced blob's bytes must survive"
        );
        assert_eq!(tally.self_grants_revoked, 0);
        assert_eq!(tally.stale_grants_removed, 0);

        case.cleanup().await;
    }

    /// A stale tracked grant is removed under --commit and its now-unreferenced
    /// bytes reclaimed the same run (it was the blob's last reference).
    #[tokio::test]
    async fn stale_tracked_grant_removed_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_stale_tracked_grant(&metadata_store, &namespace).await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            matches!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await,
                Err(MetadataError::ReferenceNotFound)
            ),
            "a stale tracked grant past the grace must be removed under --commit"
        );
        assert!(
            matches!(
                blob_store.read(&digest).await,
                Err(blob_store::Error::BlobNotFound)
            ),
            "removing the last reference must reclaim the blob's bytes the same run"
        );
        assert_eq!(tally.stale_grants_removed, 1);

        case.cleanup().await;
    }

    /// A stale non-tracked grant entry (a `Tag` entry whose backing tag link
    /// file is gone) is removed under --commit and its now-unreferenced bytes
    /// reclaimed: stale non-tracked entries no longer pin bytes forever or
    /// phantom-protect a manifest from retention.
    #[tokio::test]
    async fn stale_non_tracked_grant_removed_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        let blob = put_blob_direct(metadata_store.store(), b"stale tag entry bytes").await;
        metadata_store
            .update_blob_index(
                &namespace,
                &blob,
                BlobIndexOperation::Insert(LinkKind::Tag(Tag::new("phantom").unwrap())),
            )
            .await
            .unwrap();

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            matches!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &blob)
                    .await,
                Err(MetadataError::ReferenceNotFound)
            ),
            "a stale non-tracked grant entry past the grace must be removed under --commit"
        );
        assert!(
            matches!(
                blob_store.read(&blob).await,
                Err(blob_store::Error::BlobNotFound)
            ),
            "removing the last (phantom) reference must reclaim the bytes"
        );
        assert_eq!(tally.stale_grants_removed, 1);

        case.cleanup().await;
    }

    /// A live non-tracked entry (its backing tag link file exists) survives a
    /// committed grace-0 sweep: existence of the backing link keeps the entry.
    #[tokio::test]
    async fn live_non_tracked_grant_survives_committed_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        // A tagged manifest: the tag link file backs the shard's Tag entry.
        let digest = seed_present_body_revision(&metadata_store, &namespace).await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &digest)
                .await
                .is_ok(),
            "a non-tracked entry with a live backing link must survive"
        );
        assert!(blob_store.read(&digest).await.is_ok(), "bytes must survive");
        assert_eq!(tally.stale_grants_removed, 0);

        case.cleanup().await;
    }

    /// The grace gates non-tracked stale entries exactly like tracked ones: a
    /// huge grace keeps the collection empty.
    #[tokio::test]
    async fn fresh_stale_non_tracked_entry_kept_within_grace() {
        let case = S3RegistryTestCase::new();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        let blob = put_blob_direct(metadata_store.store(), b"fresh tag entry bytes").await;
        metadata_store
            .update_blob_index(
                &namespace,
                &blob,
                BlobIndexOperation::Insert(LinkKind::Tag(Tag::new("fresh").unwrap())),
            )
            .await
            .unwrap();
        let index = metadata_store.read_blob_index(&blob).await.unwrap();
        let links = index.namespace.get(&namespace).unwrap();

        let fresh = params(true, false, ChronoDuration::hours(48));
        let mut out = DigestOutcome::default();
        triage_namespace_grants(&metadata_store, &blob, &namespace, links, &fresh, &mut out).await;

        assert!(
            out.stale_grants.is_empty(),
            "a stale non-tracked entry within the grace must be kept"
        );

        case.cleanup().await;
    }

    /// Report-only leaves a stale tracked grant in place (would-remove tallied,
    /// nothing mutated); a committed run then removes it.
    #[tokio::test]
    async fn report_only_then_commit_stale_tracked_grant() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_stale_tracked_grant(&metadata_store, &namespace).await;

        let tally = run_orphan_sweep(&metadata_store, &blob_store, false).await;

        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &digest)
                .await
                .is_ok(),
            "report-only must leave the stale tracked grant in place"
        );
        assert_eq!(
            tally.stale_grants_removed, 1,
            "report-only must hold the would-remove count"
        );

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            matches!(
                metadata_store
                    .read_blob_index_namespace(&namespace, &digest)
                    .await,
                Err(MetadataError::ReferenceNotFound)
            ),
            "--commit must remove the stale tracked grant"
        );

        case.cleanup().await;
    }

    /// With any namespace fatal, the grant passes reap nothing even under
    /// --commit, so a stale tracked grant and its bytes survive.
    #[tokio::test]
    async fn stale_tracked_grant_not_reaped_when_any_fatal() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_stale_tracked_grant(&metadata_store, &namespace).await;

        run_sweep_with_fatal(&metadata_store, &blob_store, &["fatal/app"]).await;

        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &digest)
                .await
                .is_ok(),
            "the stale tracked grant must survive: a fatal run skips the global grant removals"
        );
        assert!(
            blob_store.read(&digest).await.is_ok(),
            "the blob's bytes must survive too (byte reap gated globally with the grant passes)"
        );

        case.cleanup().await;
    }

    /// Triage-level pin on the stale-grant predicate: a tracked grant whose
    /// backing link is gone is collected once past the grace, and a huge grace
    /// keeps it (fresh-kept / aged-reaped pair for the collection itself).
    #[tokio::test]
    async fn triage_namespace_grants_respects_the_grace() {
        let case = S3RegistryTestCase::new();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_stale_tracked_grant(&metadata_store, &namespace).await;
        let index = metadata_store.read_blob_index(&digest).await.unwrap();
        let links = index.namespace.get(&namespace).unwrap();

        // Past the grace: collected.
        let aged = params(true, false, ChronoDuration::zero());
        let mut out = DigestOutcome::default();
        triage_namespace_grants(&metadata_store, &digest, &namespace, links, &aged, &mut out).await;
        assert_eq!(
            out.stale_grants.len(),
            1,
            "a stale tracked grant past the grace must be collected"
        );

        // Within the grace: kept.
        let fresh = params(true, false, ChronoDuration::hours(48));
        let mut out = DigestOutcome::default();
        triage_namespace_grants(
            &metadata_store,
            &digest,
            &namespace,
            links,
            &fresh,
            &mut out,
        )
        .await;
        assert!(
            out.stale_grants.is_empty(),
            "a stale tracked grant within the grace must be kept"
        );

        case.cleanup().await;
    }

    /// A stale tracked grant present only in a legacy `index.json` (no shard) is
    /// still collected: the grant age falls back to the legacy file's mtime.
    #[tokio::test]
    async fn legacy_only_stale_tracked_grant_collected_for_removal() {
        let case = S3RegistryTestCase::new();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        let blob = put_blob_direct(metadata_store.store(), b"legacy-only stale grant").await;
        let legacy = legacy_blob_index_with(vec![(
            namespace.as_ref(),
            vec![LinkKind::Layer(blob.clone())],
        )]);
        put_legacy_index(metadata_store.as_ref(), &blob, &legacy).await;

        let index = metadata_store.read_blob_index(&blob).await.unwrap();
        let links = index.namespace.get(&namespace).unwrap();
        let aged = params(true, false, ChronoDuration::zero());
        let mut out = DigestOutcome::default();
        triage_namespace_grants(&metadata_store, &blob, &namespace, links, &aged, &mut out).await;

        assert_eq!(
            out.stale_grants.len(),
            1,
            "a legacy-only stale tracked grant must be collected via the legacy age fallback"
        );

        case.cleanup().await;
    }

    /// Under-lock re-check: a tracked link re-established (a concurrent push)
    /// between classification and reap keeps the grant, so `remove_stale_grant_locked`
    /// skips the removal.
    #[tokio::test]
    async fn remove_stale_grant_locked_skips_when_link_reappears() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_stale_tracked_grant(&metadata_store, &namespace).await;
        let link = LinkKind::Layer(digest.clone());

        // A concurrent push re-establishes the layer link between classify and apply.
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(link.clone(), digest.clone())],
            )
            .await
            .unwrap();

        let outcome = remove_stale_grant_locked(
            &metadata_store,
            &blob_store,
            &namespace,
            &digest,
            &link,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(outcome, ApplyOutcome::Kept);
        assert!(
            metadata_store.read_link(&namespace, &link).await.is_ok(),
            "the tracked link must be kept when it reappeared under the lock"
        );
        assert!(
            blob_store.read(&digest).await.is_ok(),
            "the blob's bytes must be kept when the tracked link reappeared under the lock"
        );

        case.cleanup().await;
    }

    /// Seed an orphan referrer: a subject revision plus a `Referrer` link whose
    /// referrer manifest is not a current revision (its `Digest` link is deleted).
    /// Returns `(subject, referrer)`.
    async fn seed_orphan_referrer(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> (Digest, Digest) {
        let subject = put_blob_direct(metadata_store.store(), b"orphan subject").await;
        let referrer = put_blob_direct(metadata_store.store(), b"orphan referrer manifest").await;
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(subject.clone()), subject.clone()),
                    LinkOperation::create(LinkKind::Digest(referrer.clone()), referrer.clone()),
                    LinkOperation::create(
                        LinkKind::Referrer(subject.clone(), referrer.clone()),
                        referrer.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        // Delete the referrer's own revision link so the `Referrer` link orphans.
        metadata_store
            .update_links(
                namespace,
                &[LinkOperation::delete(LinkKind::Digest(referrer.clone()))],
            )
            .await
            .unwrap();
        (subject, referrer)
    }

    /// Seed a live referrer: a subject revision plus a `Referrer` link whose
    /// referrer manifest is a current revision (both `Digest` links present).
    /// Returns `(subject, referrer)`.
    async fn seed_live_referrer(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> (Digest, Digest) {
        let subject = put_blob_direct(metadata_store.store(), b"live subject").await;
        let referrer = put_blob_direct(metadata_store.store(), b"live referrer manifest").await;
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(subject.clone()), subject.clone()),
                    LinkOperation::create(LinkKind::Digest(referrer.clone()), referrer.clone()),
                    LinkOperation::create(
                        LinkKind::Referrer(subject.clone(), referrer.clone()),
                        referrer.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        (subject, referrer)
    }

    /// Seed a phantom link: a layer link whose sole `referenced_by` is a phantom
    /// digest with no `Digest` revision link. The sweep removes the phantom
    /// referrer and cascade-reclaims the layer link. Returns `(layer, phantom)`.
    async fn seed_phantom_link(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> (Digest, Digest) {
        let layer = put_blob_direct(metadata_store.store(), b"phantom layer").await;
        let phantom =
            Digest::sha256("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                .unwrap();
        metadata_store
            .seed_links(
                namespace,
                &[LinkOperation::create_with_referrer(
                    LinkKind::Layer(layer.clone()),
                    layer.clone(),
                    phantom.clone(),
                )],
            )
            .await
            .unwrap();
        (layer, phantom)
    }

    /// A live referrer (its referrer manifest still a current revision) survives a
    /// committed orphan-link sweep; only links whose referrer revision is gone are
    /// removed.
    #[tokio::test]
    async fn live_referrer_survives_committed_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let (subject, referrer) = seed_live_referrer(&metadata_store, &namespace).await;

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Referrer(subject, referrer))
                .await
                .is_ok(),
            "a live referrer link must survive a committed sweep, even at grace 0"
        );

        case.cleanup().await;
    }

    /// A layer link whose sole `referenced_by` resolves to a live revision survives
    /// a committed orphan-link sweep; only phantom back-references are removed.
    #[tokio::test]
    async fn live_phantom_free_link_survives_committed_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        // A revision that references the layer via a live back-link.
        let revision = put_blob_direct(metadata_store.store(), b"live revision").await;
        let layer = put_blob_direct(metadata_store.store(), b"live layer").await;
        metadata_store
            .seed_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(revision.clone()), revision.clone()),
                    LinkOperation::create_with_referrer(
                        LinkKind::Layer(layer.clone()),
                        layer.clone(),
                        revision.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Layer(layer.clone()))
                .await
                .is_ok(),
            "a layer link whose back-links all resolve to live revisions must survive"
        );

        case.cleanup().await;
    }

    /// An orphan referrer past the grace is removed under --commit.
    #[tokio::test]
    async fn orphan_referrer_removed_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let (subject, referrer) = seed_orphan_referrer(&metadata_store, &namespace).await;
        let link = LinkKind::Referrer(subject, referrer);

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            matches!(
                metadata_store.read_link(&namespace, &link).await,
                Err(MetadataError::ReferenceNotFound)
            ),
            "an orphan referrer link past the grace must be removed under --commit"
        );
        assert_eq!(tally.orphan_links_removed, 1);

        case.cleanup().await;
    }

    /// A phantom layer link is cascade-reclaimed under --commit: the phantom
    /// back-reference is removed and, being the last referrer, the layer link is
    /// deleted.
    #[tokio::test]
    async fn phantom_link_removed_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let (layer, _phantom) = seed_phantom_link(&metadata_store, &namespace).await;
        let link = LinkKind::Layer(layer.clone());

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store.read_link(&namespace, &link).await.is_err(),
            "a phantom layer link with no live referrer must be cascade-reclaimed"
        );

        case.cleanup().await;
    }

    /// Report-only leaves an orphan referrer in place and holds the would-remove
    /// count.
    #[tokio::test]
    async fn report_only_leaves_orphan_referrer_in_place() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let (subject, referrer) = seed_orphan_referrer(&metadata_store, &namespace).await;
        let link = LinkKind::Referrer(subject, referrer);

        let tally = run_orphan_sweep(&metadata_store, &blob_store, false).await;

        assert!(
            metadata_store.read_link(&namespace, &link).await.is_ok(),
            "report-only must leave the orphan referrer in place"
        );
        assert_eq!(
            tally.orphan_links_removed, 1,
            "report-only must hold the would-remove count"
        );

        case.cleanup().await;
    }

    /// Seed a present-body revision: a manifest body, its `Digest` self-link, and a
    /// `latest` tag. The missing-body sweep must keep this one. Returns the digest.
    async fn seed_present_body_revision(
        metadata_store: &Arc<MetadataStore>,
        namespace: &Namespace,
    ) -> Digest {
        let digest = put_blob_direct(metadata_store.store(), b"present manifest body").await;
        metadata_store
            .update_links(
                namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                    LinkOperation::create(
                        LinkKind::Tag(Tag::new("latest").unwrap()),
                        digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        digest
    }

    /// Seed a missing-body revision: a present-body revision, then delete the
    /// manifest body so the `Digest` self-link and `latest` tag remain but the body
    /// is gone. Returns the digest.
    async fn seed_missing_body_revision(
        metadata_store: &Arc<MetadataStore>,
        blob_store: &BlobStore,
        namespace: &Namespace,
    ) -> Digest {
        let digest = seed_present_body_revision(metadata_store, namespace).await;
        blob_store.delete_blob(&digest).await.unwrap();
        digest
    }

    /// A present-body revision (and its body) survives a committed missing-body
    /// sweep; only revisions whose manifest body is gone are removed.
    #[tokio::test]
    async fn present_body_revision_survives_committed_missing_body_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_present_body_revision(&metadata_store, &namespace).await;

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(digest.clone()))
                .await
                .is_ok(),
            "a present-body revision must survive a committed missing-body sweep, even at grace 0"
        );
        assert!(
            blob_store.read(&digest).await.is_ok(),
            "a present manifest body must survive"
        );

        case.cleanup().await;
    }

    /// A live digest is probed once per namespace: the first present-body probe
    /// caches the target in `seen_targets`, so a second reference (the common
    /// latest/vX multi-tag case) short-circuits before probing the blob store.
    #[tokio::test]
    async fn present_body_target_probed_once_per_namespace() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_present_body_revision(&metadata_store, &namespace).await;
        let sweep_params = params(false, false, ChronoDuration::zero());

        let mut seen_targets = HashSet::new();
        let mut missing_bodies = Vec::new();
        let mut out = NamespaceReapOutcome::default();
        collect_missing_body_candidate(
            &metadata_store,
            &blob_store,
            &namespace,
            &LinkKind::Digest(digest.clone()),
            &digest,
            &sweep_params,
            &mut seen_targets,
            &mut missing_bodies,
            &mut out,
        )
        .await;
        assert!(
            seen_targets.contains(&digest),
            "the first present-body probe must cache the target so a multi-tag digest is probed once"
        );

        // Delete the body, then re-collect the same target with the same set. A
        // cached target short-circuits and is never re-probed, so it is not
        // collected even though its body is now gone.
        blob_store.delete_blob(&digest).await.unwrap();
        collect_missing_body_candidate(
            &metadata_store,
            &blob_store,
            &namespace,
            &LinkKind::Tag(Tag::new("latest").unwrap()),
            &digest,
            &sweep_params,
            &mut seen_targets,
            &mut missing_bodies,
            &mut out,
        )
        .await;
        assert!(
            missing_bodies.is_empty(),
            "a cached present-body target must not be re-probed by a second referencing tag"
        );

        case.cleanup().await;
    }

    /// A missing-body revision past the grace is removed under --commit: the
    /// `Digest` self-link and every tag pointing at it are gone.
    #[tokio::test]
    async fn missing_body_revision_removed_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_missing_body_revision(&metadata_store, &blob_store, &namespace).await;
        let link = LinkKind::Digest(digest.clone());

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            metadata_store.read_link(&namespace, &link).await.is_err(),
            "a missing-body revision self-link past the grace must be removed under --commit"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("latest").unwrap()))
                .await
                .is_err(),
            "a tag pointing at the missing-body digest must be removed too"
        );
        assert_eq!(tally.missing_bodies_removed, 1);

        case.cleanup().await;
    }

    /// Report-only leaves a missing-body revision in place and holds the
    /// would-remove count.
    #[tokio::test]
    async fn report_only_leaves_missing_body_revision() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        let digest = seed_missing_body_revision(&metadata_store, &blob_store, &namespace).await;
        let link = LinkKind::Digest(digest);

        let tally = run_orphan_sweep(&metadata_store, &blob_store, false).await;

        assert!(
            metadata_store.read_link(&namespace, &link).await.is_ok(),
            "report-only must leave the missing-body revision in place"
        );
        assert_eq!(
            tally.missing_bodies_removed, 1,
            "report-only must hold the would-remove count"
        );

        case.cleanup().await;
    }

    /// A tag-only dangling entry (tag with no revision link, body absent) is
    /// deleted on the first committed run and the tally converges to zero on the
    /// second run instead of recounting forever.
    #[tokio::test]
    async fn missing_body_tag_only_entry_converges() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        let target = put_blob_direct(metadata_store.store(), b"tag-only dangling body").await;
        metadata_store
            .update_links(
                &namespace,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("dangling").unwrap()),
                    target.clone(),
                )],
            )
            .await
            .unwrap();
        blob_store.delete_blob(&target).await.unwrap();

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;
        assert_eq!(
            tally.missing_bodies_removed, 1,
            "the first committed run must delete the dangling tag"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Tag(Tag::new("dangling").unwrap()))
                .await
                .is_err(),
            "the dangling tag must be gone"
        );

        let tally = run_orphan_sweep(&metadata_store, &blob_store, true).await;
        assert_eq!(
            tally.missing_bodies_removed, 0,
            "the second run must count zero: state converged"
        );

        case.cleanup().await;
    }

    /// Under `--commit --delete-unrecognized` an unrecognized key past the grace
    /// is deleted from its original location.
    #[tokio::test]
    async fn commit_delete_unrecognized_deletes_unrecognized() {
        let key = unrecognized_blob_key(HASH_256);
        assert!(
            matches!(classify_key(&key), KeyClass::Unrecognized(_)),
            "the planted key must classify as unrecognized"
        );

        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(true, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                blob_store.store.head(&key).await,
                Err(StorageError::NotFound)
            ),
            "the unrecognized key must be deleted from its original location"
        );
        assert_eq!(tally.deleted, 1);

        case.cleanup().await;
    }

    /// Without `--commit` the sweep is report-only: a planted unrecognized
    /// survives in place, and `deleted` holds the would-delete count so the
    /// preview matches a committed run.
    #[tokio::test]
    async fn report_only_leaves_unrecognized_in_place() {
        let key = unrecognized_blob_key(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );

        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(false, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            blob_store.store.head(&key).await.is_ok(),
            "report-only must leave the unrecognized in place"
        );
        assert_eq!(
            tally.deleted, 1,
            "report-only must hold the would-delete count"
        );

        case.cleanup().await;
    }

    /// A fresh unrecognized key within the grace is kept even under
    /// `--commit --delete-unrecognized`.
    #[tokio::test]
    async fn fresh_unrecognized_kept_within_grace() {
        let key = unrecognized_blob_key(HASH_256);
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(true, true, ChronoDuration::hours(48)),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            blob_store.store.head(&key).await.is_ok(),
            "an unrecognized key within the grace must be kept"
        );
        assert_eq!(tally.deleted, 0);

        case.cleanup().await;
    }

    /// The `deleted` tally counts every deleted unrecognized key across roots.
    #[tokio::test]
    async fn deleted_tally_counts_every_unrecognized_delete() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        let blob_junk = unrecognized_blob_key(HASH_256);
        let jobs_junk = "_jobs/pending/bogus/x.json".to_string();
        blob_store
            .store
            .put(&blob_junk, Bytes::from_static(b"x"))
            .await
            .unwrap();
        metadata_store
            .store()
            .put(&jobs_junk, Bytes::from_static(b"x"))
            .await
            .unwrap();

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(true, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(
            tally.deleted, 2,
            "both unrecognized keys must be counted as deleted"
        );
        assert!(matches!(
            blob_store.store.head(&blob_junk).await,
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            metadata_store.store().head(&jobs_junk).await,
            Err(StorageError::NotFound)
        ));

        case.cleanup().await;
    }

    /// A committed unrecognized sweep never touches owned keys: an owned tag link
    /// survives `--commit --delete-unrecognized`. Guards against the classify path
    /// misclassifying owned keys as unrecognized.
    #[tokio::test]
    async fn commit_does_not_delete_owned_keys() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("nginx").unwrap();
        // A present manifest body so the tag is live, not a dangling entry.
        let digest = put_blob_direct(metadata_store.store(), b"owned tag body").await;

        metadata_store
            .update_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(digest.clone()), digest.clone()),
                    LinkOperation::create(LinkKind::Tag(Tag::new("v1").unwrap()), digest.clone()),
                ],
            )
            .await
            .expect("create owned tag link");

        let tag_link = path_builder::link_path(&LinkKind::Tag(Tag::new("v1").unwrap()), &namespace);
        assert!(
            matches!(classify_key(&tag_link), KeyClass::Owned(_)),
            "the tag link must classify as owned"
        );

        run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(true, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            metadata_store.store().head(&tag_link).await.is_ok(),
            "owned metadata link must survive a committed sweep"
        );

        case.cleanup().await;
    }

    /// A configured namespace's content, in-flight uploads, and referenced blob
    /// grants all survive a committed sweep: a namespace resolving to a configured
    /// repository is never deleted and its referenced grants never revoked.
    #[tokio::test]
    async fn configured_namespace_survives_committed_config_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        let grant_blob = seed_referenced_grant(&metadata_store, &namespace).await;
        let upload_uuid = Uuid::new_v4().to_string();
        blob_store
            .create_upload(&namespace, &upload_uuid)
            .await
            .unwrap();

        let tag_link =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &namespace);
        let upload_key = path_builder::upload_start_date_path(&namespace, &upload_uuid);

        run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(true, false, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            metadata_store.store().head(&tag_link).await.is_ok(),
            "a configured namespace's content must survive a committed config sweep"
        );
        assert!(
            metadata_store.store().head(&upload_key).await.is_ok(),
            "a configured namespace's in-flight upload must survive"
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &grant_blob)
                .await
                .is_ok(),
            "a configured namespace's blob grant must survive"
        );

        case.cleanup().await;
    }

    /// A de-configured namespace under --commit has its metadata subtree deleted,
    /// its grants revoked on the metadata backend, and its now-unreferenced blob
    /// bytes reaped the same run; the namespace drops out of the catalog.
    #[tokio::test]
    async fn deconfigured_namespace_deleted_under_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        let grant_blob = seed_grant(&metadata_store, &namespace).await;
        let upload_uuid = Uuid::new_v4().to_string();
        blob_store
            .create_upload(&namespace, &upload_uuid)
            .await
            .unwrap();

        let tag_link =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &namespace);
        let upload_key = path_builder::upload_start_date_path(&namespace, &upload_uuid);
        let grant_shard = path_builder::blob_index_shard_path(&grant_blob, &namespace);

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(true, false, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                metadata_store.store().head(&tag_link).await,
                Err(StorageError::NotFound)
            ),
            "the de-configured namespace's metadata must be deleted"
        );
        assert!(
            matches!(
                metadata_store.store().head(&upload_key).await,
                Err(StorageError::NotFound)
            ),
            "the de-configured namespace's in-flight upload must be deleted"
        );
        assert!(
            matches!(
                metadata_store.store().head(&grant_shard).await,
                Err(StorageError::NotFound)
            ),
            "the grant shard must be deleted from the metadata backend"
        );
        assert!(
            matches!(
                blob_store.read(&grant_blob).await,
                Err(blob_store::Error::BlobNotFound)
            ),
            "the now-unreferenced blob's bytes must be reaped the same run"
        );
        assert_eq!(tally.deconfigured_deleted, 1);
        assert_eq!(tally.deconfigured_partial, 0);
        assert!(tally.deconfigured_grants_revoked >= 1);

        // The emptied namespace drops out of the catalog.
        let (catalog, _) = metadata_store.list_namespaces(100, None).await.unwrap();
        assert!(
            !catalog.contains(&"ghost/app".to_string()),
            "the de-configured namespace must drop out of the catalog; got: {catalog:?}"
        );

        case.cleanup().await;
    }

    /// Live-reproduced bug pin: de-configuring parent `team` while
    /// `team/app1` stays configured must never touch the nested child. The
    /// parent's own marker subtrees are deleted; the child's metadata, grants,
    /// and bytes survive the committed run AND the follow-up committed run
    /// (whose stale-grant pass would otherwise reap the wiped child's bytes).
    #[tokio::test]
    async fn deconfigured_parent_never_deletes_nested_configured_child() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let parent = Namespace::new("team").unwrap();
        let child = Namespace::new("team/app1").unwrap();
        seed_image(&metadata_store, &parent).await;
        seed_image(&metadata_store, &child).await;
        let child_grant = seed_referenced_grant(&metadata_store, &child).await;

        let parent_tag =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &parent);
        let child_tag =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &child);

        for run in 0..2 {
            let tally = run_sweep(
                &metadata_store,
                &blob_store,
                &resolver(&["team/app1"]),
                &params(true, false, ChronoDuration::zero()),
                &CancellationToken::new(),
            )
            .await;

            assert!(
                matches!(
                    metadata_store.store().head(&parent_tag).await,
                    Err(StorageError::NotFound)
                ),
                "run {run}: the de-configured parent's own markers must be deleted"
            );
            assert!(
                metadata_store.store().head(&child_tag).await.is_ok(),
                "run {run}: the configured child's metadata must survive"
            );
            assert!(
                metadata_store
                    .read_blob_index_namespace(&child, &child_grant)
                    .await
                    .is_ok(),
                "run {run}: the configured child's grant must survive"
            );
            assert!(
                blob_store.read(&child_grant).await.is_ok(),
                "run {run}: the configured child's bytes must survive"
            );
            assert_eq!(
                tally.deconfigured_partial, 0,
                "run {run}: the marker-subtree delete must be clean"
            );
        }

        case.cleanup().await;
    }

    /// A rebuild-fatal run still revokes a de-configured namespace's grant
    /// shards (config-driven) but must not feed the unpinned digests into the
    /// byte reap: the bytes survive until a clean run (keep over reap).
    #[tokio::test]
    async fn deconfigured_byte_reap_skipped_when_any_fatal() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        let grant_blob = seed_grant(&metadata_store, &namespace).await;
        let grant_shard = path_builder::blob_index_shard_path(&grant_blob, &namespace);

        run_sweep_with_fatal(&metadata_store, &blob_store, &["fatal/app"]).await;

        assert!(
            matches!(
                metadata_store.store().head(&grant_shard).await,
                Err(StorageError::NotFound)
            ),
            "the de-configured grant shard revoke is config-driven and still applies"
        );
        assert!(
            blob_store.read(&grant_blob).await.is_ok(),
            "the unpinned bytes must survive a rebuild-fatal run (global byte-reap skip)"
        );

        run_sweep_with_fatal(&metadata_store, &blob_store, &[]).await;

        assert!(
            matches!(
                blob_store.read(&grant_blob).await,
                Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound)
            ),
            "a clean re-run reaps the now-orphan bytes (self-healing next run)"
        );

        case.cleanup().await;
    }

    /// Report-only leaves a de-configured namespace fully in place while still
    /// recording its name and grant would-counts, so the preview matches --commit.
    #[tokio::test]
    async fn report_only_leaves_deconfigured_namespace_in_place() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        let grant_blob = seed_grant(&metadata_store, &namespace).await;

        let tag_link =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &namespace);

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(false, false, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            metadata_store.store().head(&tag_link).await.is_ok(),
            "report-only must leave the de-configured namespace's content in place"
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &grant_blob)
                .await
                .is_ok(),
            "report-only must leave the blob grant in place"
        );
        assert_eq!(tally.deconfigured_namespaces, 1);
        assert_eq!(tally.deconfigured_deleted, 0);
        assert!(
            tally.deconfigured_grants_revoked >= 1,
            "the de-configured grant would-count must be recorded for preview parity"
        );

        case.cleanup().await;
    }

    /// The empty-resolver hard guard prevents any config-ownership deletion even
    /// under --commit: with no repositories configured, a namespace's content,
    /// uploads, and grants all survive.
    #[tokio::test]
    async fn empty_resolver_guard_prevents_config_deletion() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        let grant_blob = seed_referenced_grant(&metadata_store, &namespace).await;
        let upload_uuid = Uuid::new_v4().to_string();
        blob_store
            .create_upload(&namespace, &upload_uuid)
            .await
            .unwrap();

        let tag_link =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &namespace);
        let upload_key = path_builder::upload_start_date_path(&namespace, &upload_uuid);

        run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(true, false, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            metadata_store.store().head(&tag_link).await.is_ok(),
            "an empty resolver must never delete a namespace, even under --commit"
        );
        assert!(
            metadata_store.store().head(&upload_key).await.is_ok(),
            "the empty-resolver guard must leave in-flight uploads in place"
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &grant_blob)
                .await
                .is_ok(),
            "the empty-resolver guard must leave blob grants in place"
        );

        case.cleanup().await;
    }

    /// A key both unrecognized and inside a de-configured namespace is handled
    /// once: the subtree pass deletes it, the unrecognized scan skips it in both
    /// modes (no double count, no spurious degraded exit).
    #[tokio::test]
    async fn overlap_key_deleted_once_with_no_double_count() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        // Junk under the de-configured namespace: unrecognized shape AND owned by
        // the config-ownership subtree pass.
        let junk = "v2/repositories/ghost/app/_uploads/uuid/junk".to_string();
        metadata_store
            .store()
            .put(&junk, Bytes::from_static(b"x"))
            .await
            .unwrap();

        // Report-only first: the overlap key is excluded from the unrecognized
        // classification in both modes.
        let report = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(false, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(
            report.unrecognized_count(),
            0,
            "the overlap key must not be counted unrecognized in report-only"
        );

        let commit = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(true, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                metadata_store.store().head(&junk).await,
                Err(StorageError::NotFound)
            ),
            "the overlap key must be deleted (once, by the subtree pass)"
        );
        assert_eq!(
            commit.unrecognized_count(),
            0,
            "the overlap key must not be double-counted unrecognized under --commit"
        );
        assert_eq!(commit.failed_deletes, 0);
        assert_eq!(
            commit.ran_exit_code(0, 0, true),
            (0, MarkerStatus::Clean),
            "the overlap must not degrade the run"
        );

        case.cleanup().await;
    }

    /// Report-only and --commit compute identical per-category counts on the same
    /// seed (preview parity).
    #[tokio::test]
    async fn preview_parity_report_only_counts_match_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        seed_orphan_blob(&metadata_store, b"parity orphan").await;
        seed_stale_tracked_grant(&metadata_store, &namespace).await;
        seed_missing_body_revision(&metadata_store, &blob_store, &namespace).await;
        // An orphan referrer whose referrer manifest was never stored as a blob,
        // so the commit-mode link cascade cannot orphan extra bytes and the
        // per-category counts stay comparable across modes.
        let subject = put_blob_direct(metadata_store.store(), b"parity subject").await;
        let referrer = Digest::sha256_of_bytes(b"parity referrer never stored");
        metadata_store
            .update_links(
                &namespace,
                &[
                    LinkOperation::create(LinkKind::Digest(subject.clone()), subject.clone()),
                    LinkOperation::create(
                        LinkKind::Referrer(subject.clone(), referrer.clone()),
                        referrer.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
        let junk = unrecognized_blob_key(HASH_256);
        blob_store
            .store
            .put(&junk, Bytes::from_static(b"x"))
            .await
            .unwrap();

        let report = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(false, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;
        let commit = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(true, true, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(report.orphan_reaped, commit.orphan_reaped);
        assert_eq!(report.stale_grants_removed, commit.stale_grants_removed);
        assert_eq!(report.missing_bodies_removed, commit.missing_bodies_removed);
        assert_eq!(report.orphan_links_removed, commit.orphan_links_removed);
        assert_eq!(report.deleted, commit.deleted);
        assert_eq!(report.orphan_reaped, 1);
        assert_eq!(report.stale_grants_removed, 1);
        assert_eq!(report.missing_bodies_removed, 1);
        assert_eq!(report.orphan_links_removed, 1);
        assert_eq!(report.deleted, 1);

        case.cleanup().await;
    }

    /// An already-cancelled token prevents every delete: an unrecognized key and
    /// an orphan blob survive a committed sweep.
    #[tokio::test]
    async fn cancelled_token_prevents_all_deletes() {
        let key = unrecognized_blob_key(HASH_256);
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        blob_store
            .store
            .put(&key, Bytes::from_static(b"x"))
            .await
            .expect("plant unrecognized");
        let orphan = seed_orphan_blob(&metadata_store, b"cancelled orphan").await;

        let cancel = CancellationToken::new();
        cancel.cancel();
        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &params(true, true, ChronoDuration::zero()),
            &cancel,
        )
        .await;

        assert!(
            blob_store.store.head(&key).await.is_ok(),
            "a cancelled token must abort the unrecognized delete"
        );
        assert!(
            blob_store.read(&orphan).await.is_ok(),
            "a cancelled token must abort the orphan reap"
        );
        assert_eq!(tally.deleted, 0);
        assert_eq!(tally.orphan_reaped, 0);

        case.cleanup().await;
    }

    /// Run a committed grace-0 sweep with an explicit rebuild-fatal set.
    async fn run_sweep_with_fatal(
        metadata_store: &Arc<MetadataStore>,
        blob_store: &BlobStore,
        fatal_namespaces: &[&str],
    ) {
        let fatal: HashSet<String> = fatal_namespaces.iter().map(|s| (*s).to_string()).collect();
        let any_rebuild_fatal = !fatal.is_empty();
        run_sweep(
            metadata_store,
            blob_store,
            &resolver(&["keep", "fatal"]),
            &SweepParams {
                fatal_namespaces: Arc::new(fatal),
                any_rebuild_fatal,
                ..params(true, false, ChronoDuration::zero())
            },
            &CancellationToken::new(),
        )
        .await;
    }

    /// A rebuild-fatal namespace seeded with an orphan blob, a self-grant, an
    /// orphan link, and a missing-body revision has nothing reaped under a committed
    /// sweep (keep over reap).
    #[tokio::test]
    async fn fatal_namespace_blobs_links_grants_not_reaped_at_grace0_commit() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("fatal/app").unwrap();

        let orphan_blob = seed_orphan_blob(&metadata_store, b"fatal orphan bytes").await;
        let grant_blob = seed_grant(&metadata_store, &namespace).await;
        let (subject, referrer) = seed_orphan_referrer(&metadata_store, &namespace).await;
        let missing = seed_missing_body_revision(&metadata_store, &blob_store, &namespace).await;

        run_sweep_with_fatal(&metadata_store, &blob_store, &["fatal/app"]).await;

        assert!(
            blob_store.read(&orphan_blob).await.is_ok(),
            "the orphan blob's bytes must survive: the fatal run skips the global byte reap"
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&namespace, &grant_blob)
                .await
                .is_ok(),
            "the self-grant must survive: the fatal run skips the global grant passes"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Referrer(subject, referrer))
                .await
                .is_ok(),
            "the orphan link must survive: the fatal namespace's orphan-link pass is skipped"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(missing.clone()))
                .await
                .is_ok(),
            "the missing-body revision link must survive: the fatal namespace's pass is skipped"
        );

        case.cleanup().await;
    }

    /// With no fatal namespace the same seed reaps normally: the orphan blob is
    /// hard-deleted and the missing-body revision and orphan link are removed. Shows
    /// the fatal-run keep is a gate, not an over-block.
    #[tokio::test]
    async fn non_fatal_namespace_still_reaps_normally() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("fatal/app").unwrap();

        let orphan_blob = seed_orphan_blob(&metadata_store, b"reapable orphan bytes").await;
        let (subject, referrer) = seed_orphan_referrer(&metadata_store, &namespace).await;
        let missing = seed_missing_body_revision(&metadata_store, &blob_store, &namespace).await;

        run_sweep_with_fatal(&metadata_store, &blob_store, &[]).await;

        assert!(
            matches!(
                blob_store.read(&orphan_blob).await,
                Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound)
            ),
            "with no fatal namespace the orphan blob must be hard-deleted"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Referrer(subject, referrer))
                .await
                .is_err(),
            "with no fatal namespace the orphan link must be removed"
        );
        assert!(
            metadata_store
                .read_link(&namespace, &LinkKind::Digest(missing.clone()))
                .await
                .is_err(),
            "with no fatal namespace the missing-body revision must be removed"
        );

        case.cleanup().await;
    }

    /// Co-ownership: a healthy namespace's orphan blob is kept while a separate
    /// namespace is fatal (the global byte reap is skipped); a re-run with no fatal
    /// namespace reaps it.
    #[tokio::test]
    async fn global_byte_reap_skipped_when_any_fatal() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        let orphan_blob = seed_orphan_blob(&metadata_store, b"healthy-owned orphan bytes").await;

        run_sweep_with_fatal(&metadata_store, &blob_store, &["fatal/app"]).await;

        assert!(
            blob_store.read(&orphan_blob).await.is_ok(),
            "a healthy-owned orphan blob is KEPT on a run where any namespace is fatal \
             (global byte-reap skip; converges next run)"
        );

        run_sweep_with_fatal(&metadata_store, &blob_store, &[]).await;

        assert!(
            matches!(
                blob_store.read(&orphan_blob).await,
                Err(blob_store::Error::BlobNotFound | blob_store::Error::ReferenceNotFound)
            ),
            "the same orphan blob is reaped on a clean re-run (self-healing next run)"
        );

        case.cleanup().await;
    }

    /// Per-namespace passes are gated per-name, not globally: with one namespace
    /// fatal, a separate healthy namespace still has its missing-body revision and
    /// orphan link reaped, while the fatal namespace's stay kept.
    #[tokio::test]
    async fn healthy_namespace_still_reaps_per_namespace_when_other_fatal() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let healthy = Namespace::new("keep/app").unwrap();
        let fatal = Namespace::new("fatal/app").unwrap();

        let (healthy_subject, healthy_referrer) =
            seed_orphan_referrer(&metadata_store, &healthy).await;
        let healthy_missing =
            seed_missing_body_revision(&metadata_store, &blob_store, &healthy).await;
        let (fatal_subject, fatal_referrer) = seed_orphan_referrer(&metadata_store, &fatal).await;
        let fatal_missing = seed_missing_body_revision(&metadata_store, &blob_store, &fatal).await;

        run_sweep_with_fatal(&metadata_store, &blob_store, &["fatal/app"]).await;

        assert!(
            metadata_store
                .read_link(
                    &healthy,
                    &LinkKind::Referrer(healthy_subject, healthy_referrer)
                )
                .await
                .is_err(),
            "the healthy namespace's orphan link must be reaped: the per-namespace pass is \
             per-name, not blocked by another namespace being fatal"
        );
        assert!(
            metadata_store
                .read_link(&healthy, &LinkKind::Digest(healthy_missing.clone()))
                .await
                .is_err(),
            "the healthy namespace's missing-body revision must be reaped while another \
             namespace is fatal (no over-block)"
        );
        assert!(
            metadata_store
                .read_link(&fatal, &LinkKind::Referrer(fatal_subject, fatal_referrer))
                .await
                .is_ok(),
            "the fatal namespace's orphan link must survive: its per-namespace pass is skipped"
        );
        assert!(
            metadata_store
                .read_link(&fatal, &LinkKind::Digest(fatal_missing.clone()))
                .await
                .is_ok(),
            "the fatal namespace's missing-body revision must survive (keep over reap)"
        );

        case.cleanup().await;
    }

    /// Guards the legacy read path: a blob whose only reference is a legacy
    /// single-file `index.json` (no `refs/` shard) is not reaped by a committed
    /// sweep. The sweep converges the legacy file to a shard and the live layer
    /// link keeps the grant and bytes.
    #[tokio::test]
    async fn legacy_only_referenced_blob_not_reaped_by_committed_sweep() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("keep/app").unwrap();

        let digest = put_blob_direct(metadata_store.store(), b"legacy-only bytes").await;

        // Seed the backing layer link file so the tracked grant is live; without it
        // the stale-grant pass would correctly reap the grant.
        let link = LinkKind::Layer(digest.clone());
        metadata_store
            .write_link_reference(
                &namespace,
                &link,
                &LinkMetadata::from_digest(digest.clone()),
            )
            .await
            .expect("seed backing layer link file");

        let legacy = legacy_blob_index_with(vec![(
            namespace.as_ref(),
            vec![LinkKind::Layer(digest.clone())],
        )]);
        put_legacy_index(metadata_store.as_ref(), &digest, &legacy).await;

        // No refs/ shard exists before the run, so candidacy can only come from
        // the legacy state.
        let shard_path = path_builder::blob_index_shard_path(&digest, &namespace);
        assert!(
            matches!(
                metadata_store.store().head(&shard_path).await,
                Err(StorageError::NotFound)
            ),
            "the guardrail seed must have NO refs/ shard, only the legacy index.json"
        );

        run_orphan_sweep(&metadata_store, &blob_store, true).await;

        assert!(
            blob_store.read(&digest).await.is_ok(),
            "a legacy-only-referenced blob must survive a committed grace-0 sweep"
        );

        case.cleanup().await;
    }

    /// The sweep converges an encountered legacy `index.json` into a `refs/`
    /// shard when `converge` is set (the command sets it only under
    /// `--commit`), and a second run counts zero.
    #[tokio::test]
    async fn sweep_converges_legacy_blob_index() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ns/app").unwrap();

        let digest = put_blob_direct(metadata_store.store(), b"legacy convergence blob").await;
        let legacy = legacy_blob_index_with(vec![(
            namespace.as_ref(),
            vec![LinkKind::Blob(digest.clone())],
        )]);
        put_legacy_index(metadata_store.as_ref(), &digest, &legacy).await;
        let legacy_path = path_builder::blob_index_path(&digest);
        let shard_path = path_builder::blob_index_shard_path(&digest, &namespace);

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["ns"]),
            &params(false, false, ChronoDuration::hours(48)),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                metadata_store.store().head(&legacy_path).await,
                Err(StorageError::NotFound)
            ),
            "the sweep must drain the encountered legacy index.json"
        );
        metadata_store
            .store()
            .head(&shard_path)
            .await
            .expect("the sweep must write the sharded ref");
        assert_eq!(tally.converged, 1);
        assert_eq!(tally.convergence_failed, 0);

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["ns"]),
            &params(false, false, ChronoDuration::hours(48)),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(tally.converged, 0, "a converged store counts zero");

        case.cleanup().await;
    }

    /// Under `--dry-run` (`converge == false`) the legacy `index.json` is left in
    /// place: convergence issues real mutations, so a read-only run only reports.
    #[tokio::test]
    async fn dry_run_reports_but_keeps_legacy_blob_index() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ns/app").unwrap();

        let digest = put_blob_direct(metadata_store.store(), b"dry-run legacy blob").await;
        let legacy = legacy_blob_index_with(vec![(
            namespace.as_ref(),
            vec![LinkKind::Blob(digest.clone())],
        )]);
        put_legacy_index(metadata_store.as_ref(), &digest, &legacy).await;
        let legacy_path = path_builder::blob_index_path(&digest);

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["ns"]),
            &SweepParams {
                converge: false,
                ..params(false, false, ChronoDuration::hours(48))
            },
            &CancellationToken::new(),
        )
        .await;

        metadata_store
            .store()
            .head(&legacy_path)
            .await
            .expect("a read-only run must leave the legacy index.json in place");
        assert_eq!(tally.converged, 0);

        case.cleanup().await;
    }

    // Age-gate pins over the in-memory backend with fabricated mtimes.

    /// A registry over one in-memory object store, every `head` backdated by
    /// `backdate` (`None` leaves `MemoryObjectStore`'s `last_modified: None`).
    fn memory_registry(backdate: Option<ChronoDuration>) -> (Arc<MetadataStore>, BlobStore) {
        let inner = Arc::new(MemoryObjectStore::new());
        let object: Arc<dyn ObjectStore> = match backdate {
            Some(backdate) => Arc::new(AgedObjectStore::new(inner, backdate)),
            None => inner,
        };
        let metadata_store =
            metadata_store_over(object.clone(), locked_executor_over(object.clone()));
        let blob_store = BlobStore::new(build_store(object.clone(), locked_executor_over(object)));
        (metadata_store, blob_store)
    }

    /// Every reapable GC shape seeded on one registry: orphan blob, stale grant,
    /// bare self-grant, missing-body revision + tag, orphan referrer, and an
    /// unrecognized key.
    struct GcSeeds {
        namespace: Namespace,
        orphan_blob: Digest,
        stale_grant: Digest,
        self_grant: Digest,
        missing_body: Digest,
        referrer_link: LinkKind,
        junk_key: String,
    }

    async fn seed_gc_shapes(
        metadata_store: &Arc<MetadataStore>,
        blob_store: &BlobStore,
    ) -> GcSeeds {
        let namespace = Namespace::new("keep/app").unwrap();
        let orphan_blob = seed_orphan_blob(metadata_store, b"age-gate orphan").await;
        let stale_grant = seed_stale_tracked_grant(metadata_store, &namespace).await;
        let self_grant = seed_grant(metadata_store, &namespace).await;
        let missing_body = seed_missing_body_revision(metadata_store, blob_store, &namespace).await;
        let (subject, referrer) = seed_orphan_referrer(metadata_store, &namespace).await;
        let junk_key = unrecognized_blob_key(HASH_256);
        metadata_store
            .store()
            .put(&junk_key, Bytes::from_static(b"x"))
            .await
            .unwrap();
        GcSeeds {
            namespace,
            orphan_blob,
            stale_grant,
            self_grant,
            missing_body,
            referrer_link: LinkKind::Referrer(subject, referrer),
            junk_key,
        }
    }

    async fn run_gc_sweep(
        metadata_store: &Arc<MetadataStore>,
        blob_store: &BlobStore,
        grace: ChronoDuration,
    ) -> SweepTally {
        run_sweep(
            metadata_store,
            blob_store,
            &resolver(&["keep"]),
            &params(true, true, grace),
            &CancellationToken::new(),
        )
        .await
    }

    /// Fresh candidates (age zero, one-hour grace) are all kept: no GC category
    /// reaps inside the grace.
    #[tokio::test]
    async fn fresh_candidates_kept_within_grace() {
        let (metadata_store, blob_store) = memory_registry(Some(ChronoDuration::zero()));
        let seeds = seed_gc_shapes(&metadata_store, &blob_store).await;

        let tally = run_gc_sweep(&metadata_store, &blob_store, ChronoDuration::hours(1)).await;

        assert!(blob_store.read(&seeds.orphan_blob).await.is_ok());
        assert!(
            metadata_store
                .read_blob_index_namespace(&seeds.namespace, &seeds.stale_grant)
                .await
                .is_ok()
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&seeds.namespace, &seeds.self_grant)
                .await
                .is_ok()
        );
        assert!(
            metadata_store
                .read_link(
                    &seeds.namespace,
                    &LinkKind::Digest(seeds.missing_body.clone())
                )
                .await
                .is_ok()
        );
        assert!(
            metadata_store
                .read_link(&seeds.namespace, &seeds.referrer_link)
                .await
                .is_ok()
        );
        assert!(metadata_store.store().head(&seeds.junk_key).await.is_ok());
        assert_eq!(tally.orphan_reaped, 0);
        assert_eq!(tally.stale_grants_removed, 0);
        assert_eq!(tally.self_grants_revoked, 0);
        assert_eq!(tally.missing_bodies_removed, 0);
        assert_eq!(tally.orphan_links_removed, 0);
        assert_eq!(tally.deleted, 0);
    }

    /// Aged candidates (three hours old, one-hour grace) are all reaped: every GC
    /// category deletes once past the grace.
    #[tokio::test]
    async fn aged_candidates_reaped_past_grace() {
        let (metadata_store, blob_store) = memory_registry(Some(ChronoDuration::hours(3)));
        let seeds = seed_gc_shapes(&metadata_store, &blob_store).await;

        run_gc_sweep(&metadata_store, &blob_store, ChronoDuration::hours(1)).await;

        assert!(
            blob_store.read(&seeds.orphan_blob).await.is_err(),
            "the aged orphan blob must be reaped"
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&seeds.namespace, &seeds.stale_grant)
                .await
                .is_err(),
            "the aged stale tracked grant must be removed"
        );
        assert!(
            blob_store.read(&seeds.stale_grant).await.is_err(),
            "the stale grant's bytes must be reclaimed"
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&seeds.namespace, &seeds.self_grant)
                .await
                .is_err(),
            "the aged bare self-grant must be revoked"
        );
        assert!(
            blob_store.read(&seeds.self_grant).await.is_err(),
            "the revoked self-grant's bytes must be reclaimed"
        );
        assert!(
            metadata_store
                .read_link(
                    &seeds.namespace,
                    &LinkKind::Digest(seeds.missing_body.clone())
                )
                .await
                .is_err(),
            "the aged missing-body revision must be removed"
        );
        assert!(
            metadata_store
                .read_link(&seeds.namespace, &seeds.referrer_link)
                .await
                .is_err(),
            "the aged orphan referrer link must be removed"
        );
        assert!(
            matches!(
                metadata_store.store().head(&seeds.junk_key).await,
                Err(StorageError::NotFound)
            ),
            "the aged unrecognized key must be deleted"
        );
    }

    /// On a backend reporting no `last_modified`, nothing is ever reaped, and
    /// each kept candidate is counted.
    #[tokio::test]
    async fn missing_last_modified_never_reaped() {
        let (metadata_store, blob_store) = memory_registry(None);
        let seeds = seed_gc_shapes(&metadata_store, &blob_store).await;

        let tally = run_gc_sweep(&metadata_store, &blob_store, ChronoDuration::zero()).await;

        assert!(blob_store.read(&seeds.orphan_blob).await.is_ok());
        assert!(
            metadata_store
                .read_blob_index_namespace(&seeds.namespace, &seeds.stale_grant)
                .await
                .is_ok()
        );
        assert!(
            metadata_store
                .read_blob_index_namespace(&seeds.namespace, &seeds.self_grant)
                .await
                .is_ok()
        );
        assert!(
            metadata_store
                .read_link(
                    &seeds.namespace,
                    &LinkKind::Digest(seeds.missing_body.clone())
                )
                .await
                .is_ok()
        );
        assert!(
            metadata_store
                .read_link(&seeds.namespace, &seeds.referrer_link)
                .await
                .is_ok()
        );
        assert!(metadata_store.store().head(&seeds.junk_key).await.is_ok());
        assert!(
            tally.age_unknown_kept >= 5,
            "every kept candidate must be counted; got {}",
            tally.age_unknown_kept
        );
        assert_eq!(tally.orphan_reaped, 0);
        assert_eq!(tally.deleted, 0);
    }

    // Split-backend topology pins.

    /// Split params: distinct physical backends, grace 0.
    fn split_params(commit: bool, delete_unrecognized: bool) -> SweepParams {
        SweepParams {
            shared_backend: false,
            ..params(commit, delete_unrecognized, ChronoDuration::zero())
        }
    }

    /// A de-configured namespace's grant shard lives on the METADATA backend; the
    /// committed sweep revokes it there and reaps the now-unreferenced bytes from
    /// the blob backend the same run.
    #[tokio::test]
    async fn split_grant_revocation_targets_metadata_backend() {
        let case = FSRegistryTestCase::with_split_backends();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        // Make the namespace enumerable on the metadata backend.
        seed_image(&metadata_store, &namespace).await;

        // Bytes on the BLOB backend, the grant shard on the METADATA backend.
        let content = b"split grant bytes";
        let digest = Digest::sha256_of_bytes(content);
        blob_store
            .put_blob(&digest, Bytes::from_static(content))
            .await
            .unwrap();
        metadata_store
            .update_blob_index(
                &namespace,
                &digest,
                BlobIndexOperation::Insert(LinkKind::Blob(digest.clone())),
            )
            .await
            .unwrap();
        let shard_key = path_builder::blob_index_shard_path(&digest, &namespace);
        metadata_store
            .store()
            .head(&shard_key)
            .await
            .expect("the shard must live on the metadata backend");
        assert!(
            matches!(
                blob_store.store.head(&shard_key).await,
                Err(StorageError::NotFound)
            ),
            "the shard must not exist on the blob backend"
        );

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &split_params(true, false),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                metadata_store.store().head(&shard_key).await,
                Err(StorageError::NotFound)
            ),
            "the grant shard must be revoked on the METADATA backend"
        );
        assert!(
            tally.deconfigured_grants_revoked >= 1,
            "the revoke must be counted; got {}",
            tally.deconfigured_grants_revoked
        );
        assert_eq!(tally.deconfigured_grants_partial, 0);
        assert!(
            matches!(
                blob_store.read(&digest).await,
                Err(blob_store::Error::BlobNotFound)
            ),
            "the now-unreferenced blob's bytes must be reaped from the blob backend the same run"
        );

        case.cleanup().await;
    }

    /// Junk under `v2/blobs` on the METADATA backend only is covered by the split
    /// topology's extra walk and deleted through the metadata handle.
    #[tokio::test]
    async fn split_junk_on_metadata_blobs_root_deleted() {
        let case = FSRegistryTestCase::with_split_backends();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        let junk = unrecognized_blob_key(HASH_256);
        metadata_store
            .store()
            .put(&junk, Bytes::from_static(b"x"))
            .await
            .unwrap();

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &split_params(true, true),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                metadata_store.store().head(&junk).await,
                Err(StorageError::NotFound)
            ),
            "the metadata-backend junk must be deleted through the metadata handle"
        );
        assert!(tally.unrecognized_count() >= 1);
        assert_eq!(tally.deleted, 1);

        case.cleanup().await;
    }

    /// Junk under `v2/repositories` on the BLOB backend is covered by the split
    /// topology's extra walk and deleted through the blob handle.
    #[tokio::test]
    async fn split_junk_under_blob_repositories_deleted() {
        let case = FSRegistryTestCase::with_split_backends();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        let junk = "v2/repositories/keep/app/_uploads/uuid/junk".to_string();
        blob_store
            .store
            .put(&junk, Bytes::from_static(b"x"))
            .await
            .unwrap();

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &split_params(true, true),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                blob_store.store.head(&junk).await,
                Err(StorageError::NotFound)
            ),
            "the blob-backend junk must be deleted through the blob handle"
        );
        assert_eq!(tally.deleted, 1);

        case.cleanup().await;
    }

    /// Split-topology: a de-configured namespace's uploads live on the blob
    /// backend, unreachable by the metadata subtree delete. The committed sweep
    /// still deletes them via the dedicated blob-backend pass.
    #[tokio::test]
    async fn deconfigured_uploads_deleted_on_blob_backend_in_split_topology() {
        let case = FSRegistryTestCase::with_split_backends();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();
        let namespace = Namespace::new("ghost/app").unwrap();
        seed_image(&metadata_store, &namespace).await;
        let upload_uuid = Uuid::new_v4().to_string();
        blob_store
            .create_upload(&namespace, &upload_uuid)
            .await
            .unwrap();

        let tag_link =
            path_builder::link_path(&LinkKind::Tag(Tag::new("latest").unwrap()), &namespace);
        let upload_key = path_builder::upload_start_date_path(&namespace, &upload_uuid);

        assert!(
            blob_store.store.head(&upload_key).await.is_ok(),
            "the upload must live on the blob backend before the sweep"
        );
        assert!(
            matches!(
                metadata_store.store().head(&upload_key).await,
                Err(StorageError::NotFound)
            ),
            "the upload must not be on the metadata backend in a split topology"
        );

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&["keep"]),
            &split_params(true, false),
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                metadata_store.store().head(&tag_link).await,
                Err(StorageError::NotFound)
            ),
            "the metadata subtree must be deleted on the metadata backend"
        );
        assert!(
            matches!(
                blob_store.store.head(&upload_key).await,
                Err(StorageError::NotFound)
            ),
            "the de-configured namespace's blob-backend upload must be deleted under --commit"
        );
        assert!(tally.deconfigured_uploads_deleted >= 1);
        assert_eq!(tally.deconfigured_uploads_partial, 0);

        case.cleanup().await;
    }

    /// Shared topology walks `v2/blobs` once: one junk key counts exactly once.
    #[tokio::test]
    async fn shared_topology_counts_junk_once() {
        let case = S3RegistryTestCase::new();
        let blob_store = case.blob_store();
        let metadata_store = case.metadata_store();

        let junk = unrecognized_blob_key(HASH_256);
        blob_store
            .store
            .put(&junk, Bytes::from_static(b"x"))
            .await
            .unwrap();

        let tally = run_sweep(
            &metadata_store,
            &blob_store,
            &resolver(&[]),
            &params(false, false, ChronoDuration::zero()),
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(
            tally.unrecognized_count(),
            1,
            "one junk key must count exactly once on a shared backend"
        );

        case.cleanup().await;
    }

    /// The removed trash prefix appears nowhere in src/ (the needle is
    /// assembled so this test does not match itself).
    #[test]
    fn scrub_trash_prefix_is_gone_from_the_codebase() {
        let needle = format!(".scrub-{}", "trash");
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut stack = vec![src];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    let body = fs::read_to_string(&path).unwrap();
                    assert!(
                        !body.contains(&needle),
                        "{} still references {}",
                        path.display(),
                        needle
                    );
                }
            }
        }
    }
}
