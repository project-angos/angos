//! Builds the scrub DAG node set from the enabled flags and supplies each
//! node's body.
//!
//! The node bodies REUSE the existing checker structs verbatim: each body drives
//! the relevant `NamespaceChecker` / `StoreChecker` / `TagChecker` over the right
//! enumeration. Only the orchestration moved here — the per-checker logic is
//! untouched. The DAG edges encode main's observable execution order:
//!
//! ```text
//!   migrate            deps: []
//!   metadata           deps: []
//!   orphan-namespaces  deps: []
//!   blob               deps: [metadata, orphan-namespaces, migrate]
//!   orphan-grants      deps: [blob]
//!   multipart          deps: []
//!   orphan-jobs        deps: []
//!   jobs               deps: []
//!   replicate          deps: [orphan-jobs, metadata]
//! ```
//!
//! - `metadata -> blob` and `orphan-namespaces -> blob`: metadata link
//!   deletions and orphan-namespace clearing are persisted before the blob GC
//!   reads the reverse index, so "referenced" is accurate (the correctness
//!   barrier).
//! - `migrate -> blob`: when `--migrate` is set, the legacy `index.json` ->
//!   sharded `refs/` rewrite completes before the blob GC reads the reverse
//!   index, preserving main's strict migrate-before-GC ordering. The edge drops
//!   when `--migrate` is absent (the common case), so routine scrubs run the GC
//!   exactly as before.
//! - `blob -> orphan-grants`: grants are applied after blob reclaim, as on main.
//! - `orphan-jobs -> replicate`: the in-process replication drain runs after the
//!   orphan-job sweep, as on main.
//! - `multipart` is dep-free: it shares no state with the other store sweeps
//!   (pure S3 abort), so its action set is independent of ordering.
//!
//! Edges to a disabled (absent) node are dropped by the scheduler, so a
//! dependent then runs against persisted state — exactly as the sequential loop
//! did when a flag was off.

use std::sync::Arc;

use futures_util::StreamExt;
use tracing::{error, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{BlobChecker, NamespaceChecker, StoreChecker, TagChecker, list_all},
        context::Ctx,
        scheduler::Node,
    },
    oci::{Namespace, Tag},
};

/// The `metadata` node body. One uniform walk over `list_all_namespaces`, fanned
/// out up to `fanout` namespaces. Per namespace, run the enabled steps in main's
/// order: tags walk first, then each namespace checker in `setup::namespace_checkers`
/// push order. Each checker call locks the sink so the applies stay serialized
/// (the action set is identical to the serial loop on main).
async fn metadata_node(
    ctx: Arc<Ctx>,
    namespace_checkers: Arc<Vec<Box<dyn NamespaceChecker>>>,
    tag_checkers: Arc<Option<Vec<Box<dyn TagChecker>>>>,
) {
    let metadata_store = ctx.metadata_store.clone();
    list_all::all_namespaces(&metadata_store)
        .for_each_concurrent(ctx.opts.fanout, |namespace| {
            let ctx = ctx.clone();
            let namespace_checkers = namespace_checkers.clone();
            let tag_checkers = tag_checkers.clone();
            async move {
                // `ctx.sem` is the shared cross-node fan-out budget. Within this
                // node it is functionally redundant with the `for_each_concurrent`
                // bound above (both are `DEFAULT_FANOUT`, so this permit never
                // actually gates), but it is the single budget the deferred blob
                // fan-out will draw from the same pool, keeping total in-flight
                // work bounded once blob-level parallelism lands. Held for the
                // task's lifetime.
                let _permit = ctx.sem.clone().acquire_owned().await;
                let namespace = match namespace {
                    Ok(name) => match Namespace::new(&name) {
                        Ok(namespace) => namespace,
                        Err(e) => {
                            // An invalid-named namespace cannot form typed
                            // links, so the per-namespace checkers can't run on
                            // it. Under `--prune-unknown` reclaim its subtrees;
                            // otherwise preserve main's behavior (warn + skip,
                            // no deletion).
                            prune_invalid_namespace(&ctx, &name, &e.to_string()).await;
                            return;
                        }
                    },
                    Err(e) => {
                        warn!("Failed to enumerate namespace: {e}");
                        return;
                    }
                };

                scrub_tags(&ctx, &namespace, &tag_checkers).await;

                for checker in namespace_checkers.iter() {
                    let mut guard = ctx.sink.lock().await;
                    if let Err(e) = checker.check(&namespace, guard.as_mut()).await {
                        warn!("Scrub checker failed for namespace '{namespace}': {e}");
                    }
                }
            }
        })
        .await;
}

/// Handle a namespace whose raw on-disk name fails `Namespace` validation.
///
/// Without `--prune-unknown` this is report-only: it logs a warning and emits
/// no `Action`, exactly as main did (main `warn!`d and continued). With
/// `--prune-unknown` it reclaims both possible footprints — the manifest
/// repository subtree ([`Action::DeleteInvalidNamespace`]) and the upload
/// subtree ([`Action::DeleteInvalidUploadNamespace`]) — because
/// `list_all_namespaces` enumerates a namespace marked by *either* subtree.
/// Each delete is a best-effort prefix removal, so emitting both when only one
/// exists is harmless (an absent prefix is a no-op in the executor).
async fn prune_invalid_namespace(ctx: &Arc<Ctx>, name: &str, reason: &str) {
    if !ctx.opts.prune_unknown {
        warn!(
            "Skipping invalid enumerated namespace '{name}': {reason} (run with --prune-unknown to delete it)"
        );
        return;
    }
    warn!("Deleting invalid namespace directory '{name}': {reason}");
    for action in [
        Action::DeleteInvalidNamespace {
            name: name.to_string(),
        },
        Action::DeleteInvalidUploadNamespace {
            name: name.to_string(),
        },
    ] {
        let mut guard = ctx.sink.lock().await;
        if let Err(e) = guard.apply(action).await {
            error!("Failed to delete invalid namespace directory '{name}': {e}");
        }
    }
}

/// Walks a namespace's tags once: an invalid name is deleted and skipped, a
/// valid tag is dispatched to each enabled per-tag checker. Moved verbatim from
/// the former `Command::scrub_tags`; runs before the aggregate namespace
/// checkers. No-op when `--tags` is off.
async fn scrub_tags(
    ctx: &Arc<Ctx>,
    namespace: &Namespace,
    tag_checkers: &Arc<Option<Vec<Box<dyn TagChecker>>>>,
) {
    let Some(tag_checkers) = tag_checkers.as_ref() else {
        return;
    };
    let mut names = list_all::unparsed_tags(&ctx.metadata_store, namespace);
    while let Some(name) = names.next().await {
        let name = match name {
            Ok(name) => name,
            Err(e) => {
                warn!("Tag enumeration failed for namespace '{namespace}': {e}");
                break;
            }
        };
        let tag = match Tag::try_from(name.as_str()) {
            Ok(tag) => tag,
            Err(reason) => {
                warn!("Deleting invalid tag directory '{namespace}:{name}': {reason}");
                let mut guard = ctx.sink.lock().await;
                if let Err(e) = guard
                    .apply(Action::DeleteInvalidTag {
                        namespace: namespace.clone(),
                        tag: name,
                    })
                    .await
                {
                    error!("Failed to delete invalid tag directory in '{namespace}': {e}");
                }
                continue;
            }
        };
        for checker in tag_checkers {
            let mut guard = ctx.sink.lock().await;
            if let Err(e) = checker.check_tag(namespace, &tag, guard.as_mut()).await {
                error!("Tag check failed for '{namespace}:{tag}': {e}");
            }
        }
    }
}

/// A node body that drives a single store-wide checker's `check_all`. Logs and
/// continues past a failed checker (scrub is best-effort).
async fn store_node(ctx: &Ctx, label: &'static str, checker: Box<dyn StoreChecker>) {
    let mut guard = ctx.sink.lock().await;
    if let Err(e) = checker.check_all(guard.as_mut()).await {
        warn!("Store scrub checker '{label}' failed: {e}");
    }
}

/// The `blob` node body. `BlobChecker` owns its own blob walk, so this just
/// drives `check_all`; blob-level fan-out is deferred (keeping `-b` behavior
/// byte-identical to main).
async fn blob_node(ctx: &Ctx, checker: BlobChecker) {
    let mut guard = ctx.sink.lock().await;
    if let Err(e) = checker.check_all(guard.as_mut()).await {
        warn!("Store scrub checker 'blobs' failed: {e}");
    }
}

/// Build the enabled-node set from the parsed flags. The checker structs are
/// constructed once here (reusing the unchanged `setup::*` builders, passed in)
/// and moved into the node closures. Returns the `Vec<Node>` to hand to
/// `scheduler::run_dag`.
///
/// One flat declarative node table; the length is the node count, not branching
/// complexity, so the line-count lint is allowed rather than fragmenting it.
#[allow(clippy::too_many_lines)]
pub(crate) fn build_nodes(ctx: &Arc<Ctx>, parts: NodeParts) -> Vec<Node> {
    let NodeParts {
        layout_checker,
        namespace_checkers,
        tag_checkers,
        blob_checker,
        multipart_checker,
        orphan_grant_checker,
        orphan_namespace_checker,
        orphan_job_checkers,
        job_checker,
        replication_drain,
    } = parts;

    let mut nodes: Vec<Node> = Vec::new();

    // migrate: independent one-time layout migration, gated by --migrate.
    if let Some(layout_checker) = layout_checker {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "migrate",
            deps: &[],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    store_node(&ctx, "migrate", Box::new(layout_checker)).await;
                })
            }),
        });
    }

    // metadata: the per-namespace walk. Built whenever any namespace-scoped step
    // is enabled. The tag walk is driven separately from the namespace checkers
    // to preserve main's "tags before per-namespace checkers" order.
    if ctx.opts.metadata_enabled() {
        let ctx = ctx.clone();
        let namespace_checkers = Arc::new(namespace_checkers);
        let tag_checkers = Arc::new(tag_checkers);
        nodes.push(Node {
            id: "metadata",
            deps: &[],
            run: Box::new(move |_sem| {
                Box::pin(metadata_node(ctx, namespace_checkers, tag_checkers))
            }),
        });
    }

    // orphan-namespaces: store-wide; its link deletions free manifest bytes the
    // blob GC reclaims, so it must precede `blob` (encoded as a blob dep).
    if let Some(checker) = orphan_namespace_checker {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "orphan-namespaces",
            deps: &[],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    store_node(&ctx, "orphan-namespaces", Box::new(checker)).await;
                })
            }),
        });
    }

    // blob: GC after metadata + orphan-namespaces are persisted, and after the
    // layout migration. The `migrate` edge restores main's strict
    // migrate-before-blob-GC ordering: when `--migrate` rewrites legacy
    // index.json -> sharded refs/, that completes before the GC reads the reverse
    // index. The scheduler drops the edge when `--migrate` is absent (the common
    // case), so routine scrubs are unaffected.
    if let Some(checker) = blob_checker {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "blob",
            deps: &["metadata", "orphan-namespaces", "migrate"],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    blob_node(&ctx, checker).await;
                })
            }),
        });
    }

    // orphan-grants: applied after blob reclaim, as on main.
    if let Some(checker) = orphan_grant_checker {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "orphan-grants",
            deps: &["blob"],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    store_node(&ctx, "orphan-grants", Box::new(checker)).await;
                })
            }),
        });
    }

    // multipart: independent S3 sweep, shares no state with the other sweeps.
    if let Some(checker) = multipart_checker {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "multipart",
            deps: &[],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    store_node(&ctx, "multipart", Box::new(checker)).await;
                })
            }),
        });
    }

    // orphan-jobs: independent; must precede the replication drain.
    if !orphan_job_checkers.is_empty() {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "orphan-jobs",
            deps: &[],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    for checker in orphan_job_checkers {
                        store_node(&ctx, "orphan-jobs", Box::new(checker)).await;
                    }
                })
            }),
        });
    }

    // jobs: structural reconcile of the durable job store (dangling lock-key
    // indexes + unknown-queue removal under --prune-unknown). Independent of the
    // config-drift orphan-jobs sweep and of metadata/blob.
    if let Some(checker) = job_checker {
        let ctx = ctx.clone();
        nodes.push(Node {
            id: "jobs",
            deps: &[],
            run: Box::new(move |_sem| {
                Box::pin(async move {
                    store_node(&ctx, "jobs", Box::new(checker)).await;
                })
            }),
        });
    }

    // replicate: the in-process drain, after orphan-jobs and metadata. The
    // ReplicationChecker's enqueue step already ran inside the metadata node.
    //
    // The drain may overlap the blob GC, orphan-grants, and multipart sweeps
    // (none is a dep), unlike main where the drain ran strictly after the whole
    // store sweep. This is action-set-neutral: the drain only pushes
    // live-referenced tags/blobs, and the blob GC re-checks every reference under
    // the per-digest blob data lock before classifying anything as orphan, so the
    // overlap can never make GC delete content the drain still needs.
    if let Some(drain) = replication_drain {
        nodes.push(Node {
            id: "replicate",
            deps: &["orphan-jobs", "metadata"],
            run: Box::new(move |_sem| Box::pin(drain.drain())),
        });
    }

    nodes
}

/// The pre-built checkers and drain handed to [`build_nodes`]. Constructed in
/// `Command::new` via the unchanged `setup::*` builders.
pub(crate) struct NodeParts {
    pub layout_checker: Option<crate::command::scrub::check::LayoutChecker>,
    pub namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    pub tag_checkers: Option<Vec<Box<dyn TagChecker>>>,
    pub blob_checker: Option<BlobChecker>,
    pub multipart_checker: Option<crate::command::scrub::check::MultipartChecker>,
    pub orphan_grant_checker: Option<crate::command::scrub::check::OrphanGrantChecker>,
    pub orphan_namespace_checker: Option<crate::command::scrub::check::OrphanNamespaceChecker>,
    pub orphan_job_checkers: Vec<crate::command::scrub::check::OrphanJobChecker>,
    pub job_checker: Option<crate::command::scrub::check::JobChecker>,
    pub replication_drain: Option<super::command::ReplicationDrain>,
}
