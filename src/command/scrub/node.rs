//! Supplies each maintenance body and the panic-tolerance guard the command
//! drivers share.
//!
//! Each body drives a `NamespaceChecker` / `StoreChecker` over the right
//! enumeration. Every body is wrapped in [`guard`], so a panic is caught and
//! logged and the sequence continues.

use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::StreamExt;
use tracing::{error, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, StoreChecker, list_all},
        context::Ctx,
        executor::{ActionSink, SharedSink},
    },
    oci::{Namespace, Tag},
};

/// Namespaces whose walk or rebuild errored, keyed on the raw enumerated name.
/// The sweep excludes them from its destructive passes (keep over reap) and the
/// run marker lists them. Shared across the fanned-out namespace tasks.
pub type FailedNamespaces = Arc<Mutex<BTreeSet<String>>>;

/// Record one failed namespace against the shared set.
pub fn record_failed(failed: &FailedNamespaces, name: &str) {
    let mut set = match failed.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    set.insert(name.to_string());
}

/// Snapshot the failed set, called once after the phase barrier.
pub fn failed_snapshot(failed: &FailedNamespaces) -> BTreeSet<String> {
    match failed.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

/// Run one body on its own task, catching a panic (surfaced as a logged
/// `JoinError`). Returns `None` on panic so the caller sees the loss instead of
/// silently continuing.
pub async fn guard<T: Send + 'static>(
    id: &'static str,
    body: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    match tokio::spawn(body).await {
        Ok(value) => Some(value),
        Err(err) => {
            warn!("maintenance node {id} failed: {err}");
            None
        }
    }
}

/// The `metadata` body. One walk over every namespace, fanned out up to
/// `fanout` namespaces. Per namespace: the tag walk first (which deletes
/// invalid-named tag directories unconditionally), then each namespace checker
/// in vec order (the rebuild first). Each drives a per-task `SharedSink` locking
/// around each apply, so mutations serialize while reads fan out.
///
/// Returns whether the walk completed: `false` when the namespace enumeration
/// errored (the stream fuses) or the run was cancelled mid-walk, so the caller
/// must not run destructive passes against the partial view. A checker `Err`
/// records its namespace into `failed` and skips the namespace's remaining
/// checkers instead.
pub async fn metadata_node(
    ctx: Arc<Ctx>,
    namespace_checkers: Arc<Vec<Box<dyn NamespaceChecker>>>,
    failed: FailedNamespaces,
) -> bool {
    let metadata_store = ctx.metadata_store.clone();
    let complete = AtomicBool::new(true);
    list_all::all_namespaces(&metadata_store)
        .for_each_concurrent(ctx.opts.fanout, |namespace| {
            let ctx = ctx.clone();
            let namespace_checkers = namespace_checkers.clone();
            let failed = failed.clone();
            let complete = &complete;
            async move {
                if ctx.cancel.is_cancelled() {
                    complete.store(false, Ordering::Relaxed);
                    return;
                }
                let namespace = match namespace {
                    Ok(name) => match Namespace::new(&name) {
                        Ok(namespace) => namespace,
                        Err(e) => {
                            // An invalid-named namespace cannot form typed links,
                            // so the checkers can't run on it: prune under
                            // `--prune-unknown`, else warn and skip.
                            prune_invalid_namespace(&ctx, &name, &e.to_string()).await;
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Failed to enumerate namespaces; the walk is incomplete: {e}");
                        complete.store(false, Ordering::Relaxed);
                        return;
                    }
                };

                scrub_tags(&ctx, &namespace).await;

                for checker in namespace_checkers.iter() {
                    let mut sink = SharedSink::new(ctx.sink.clone());
                    if let Err(e) = checker.check(&namespace, &mut sink).await {
                        warn!("Scrub checker failed for namespace '{namespace}': {e}");
                        record_failed(&failed, namespace.as_ref());
                        // A failed namespace skips its remaining checkers: a
                        // later checker (retention in particular) must not act
                        // on state an earlier one (the rebuild) could not
                        // verify (keep over reap).
                        break;
                    }
                }
            }
        })
        .await;
    complete.load(Ordering::Relaxed) && !ctx.cancel.is_cancelled()
}

/// Handle a namespace whose raw on-disk name fails `Namespace` validation.
///
/// Without `--prune-unknown` this is report-only (warn, no `Action`). With it,
/// reclaim both possible footprints, the manifest repository subtree
/// ([`Action::DeleteInvalidNamespace`]) and the upload subtree
/// ([`Action::DeleteInvalidUploadNamespace`]), since either can mark the
/// namespace; emitting both is harmless (an absent prefix is a no-op).
async fn prune_invalid_namespace(ctx: &Arc<Ctx>, name: &str, reason: &str) {
    if !ctx.opts.prune_unknown {
        // Only `scrub` exposes `--prune-unknown`, so only steer operators at it
        // when the subcommand accepts it.
        if ctx.prune_unknown_supported {
            warn!(
                "Skipping invalid enumerated namespace '{name}': {reason} (run with --prune-unknown to delete it)"
            );
        } else {
            warn!("Skipping invalid enumerated namespace '{name}': {reason}");
        }
        ctx.findings
            .record_skipped_invalid_namespace(name, reason)
            .await;
        return;
    }
    // The sink logs the outcome (applied, or skipped without --commit).
    warn!("Invalid namespace directory '{name}': {reason}");
    let mut sink = SharedSink::new(ctx.sink.clone());
    for action in [
        Action::DeleteInvalidNamespace {
            name: name.to_string(),
        },
        Action::DeleteInvalidUploadNamespace {
            name: name.to_string(),
        },
    ] {
        if let Err(e) = sink.apply(action).await {
            error!("Failed to delete invalid namespace directory '{name}': {e}");
        }
    }
}

/// Walks a namespace's tags once, deleting every invalid-named tag directory.
/// Runs unconditionally whenever the metadata node runs (a name-grammar
/// structural repair), before the namespace checkers.
async fn scrub_tags(ctx: &Arc<Ctx>, namespace: &Namespace) {
    let mut names = list_all::unparsed_tags(&ctx.metadata_store, namespace);
    while let Some(name) = names.next().await {
        let name = match name {
            Ok(name) => name,
            Err(e) => {
                warn!("Tag enumeration failed for namespace '{namespace}': {e}");
                break;
            }
        };
        if Tag::try_from(name.as_str()).is_ok() {
            continue;
        }
        // The sink logs the outcome (applied, or skipped without --commit).
        warn!("Invalid tag directory '{namespace}:{name}'");
        let mut sink = SharedSink::new(ctx.sink.clone());
        if let Err(e) = sink
            .apply(Action::DeleteInvalidTag {
                namespace: namespace.clone(),
                tag: name,
            })
            .await
        {
            error!("Failed to delete invalid tag directory in '{namespace}': {e}");
        }
    }
}

/// A body that drives a single store-wide checker's `check_all`. Logs and
/// continues past a failed checker (scrub is best-effort).
pub async fn store_node(ctx: &Ctx, label: &'static str, checker: Box<dyn StoreChecker>) {
    let mut sink = SharedSink::new(ctx.sink.clone());
    if let Err(e) = checker.check_all(&mut sink).await {
        warn!("Store scrub checker '{label}' failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::guard;

    /// A later step still runs after an earlier body panics: `guard` catches the
    /// panic (returning `None`) rather than propagating it, and a clean body's
    /// value comes back as `Some`.
    #[tokio::test]
    async fn guard_tolerates_a_panic_and_the_sequence_continues() {
        let after_ran = Arc::new(AtomicBool::new(false));
        let panicked = guard("boom", async { panic!("node body panicked") }).await;
        assert!(panicked.is_none(), "a panicked body must surface as None");
        let flag = after_ran.clone();
        let value = guard("after", async move {
            flag.store(true, Ordering::SeqCst);
            42
        })
        .await;
        assert_eq!(value, Some(42), "a clean body's value must come back");
        assert!(
            after_ran.load(Ordering::SeqCst),
            "a later step must run even after an earlier body panicked"
        );
    }
}
