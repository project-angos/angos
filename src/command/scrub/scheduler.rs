//! A small, generic dependency-DAG scheduler for the scrub pass.
//!
//! Execution is modelled as a dependency DAG rather than a fixed loop: each
//! [`Node`] declares the ids of the nodes it depends on, and the scheduler
//! ([`run_dag`]) starts a node only once every one of its *present* deps has
//! completed. Independent nodes therefore overlap; each node body bounds its own
//! fan-out internally (the `metadata` node via `for_each_concurrent`).
//!
//! Wired into the live scrub flow by [`super::command::Command::run`], which
//! builds the enabled-node set ([`super::node::build_nodes`]) and runs it here.

use std::{collections::HashMap, future::Future, pin::Pin};

use tokio::task::JoinSet;
use tracing::warn;

/// Stable, human-readable identifier for a scheduler node.
pub(crate) type NodeId = &'static str;

/// A node body: produces the future that performs the node's work. Boxed so
/// heterogeneous closures share one type.
pub(crate) type RunFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// One unit of work plus the ids it must wait for before starting.
pub(crate) struct Node {
    /// This node's id; must be unique within a single [`run_dag`] call.
    pub id: NodeId,
    /// Ids whose completion gates this node. Edges to ids absent from the node
    /// set are dropped, so the node runs without waiting on them.
    pub deps: &'static [NodeId],
    /// Builds the node's work future.
    pub run: RunFn,
}

/// Run every node exactly once, honouring `deps`, and return only after the
/// last node has completed.
///
/// Semantics:
/// - A node starts only after **all** of its present deps have completed.
/// - Ready nodes run concurrently; the scheduler does not cap how many run at
///   once. Each node body bounds its own fan-out internally.
/// - A dep is satisfied by **completion, not success**: a panicking node is
///   logged and its dependents are still released (ordering, not success, is
///   the contract); a [`JoinError`](tokio::task::JoinError) never aborts the
///   run or strands dependents.
/// - Edges to a `NodeId` not present in `nodes` are dropped.
pub(crate) async fn run_dag(nodes: Vec<Node>) {
    let present: std::collections::HashSet<NodeId> = nodes.iter().map(|n| n.id).collect();

    // in-degree over present deps only, plus the reverse (dependents) edges.
    let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
    let mut dependents: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    let mut pending: HashMap<NodeId, RunFn> = HashMap::new();

    for node in nodes {
        let deps: Vec<NodeId> = node
            .deps
            .iter()
            .copied()
            .filter(|d| present.contains(d))
            .collect();
        in_degree.insert(node.id, deps.len());
        for dep in deps {
            dependents.entry(dep).or_default().push(node.id);
        }
        pending.insert(node.id, node.run);
    }

    let mut tasks: JoinSet<NodeId> = JoinSet::new();
    let spawn = |tasks: &mut JoinSet<NodeId>, pending: &mut HashMap<NodeId, RunFn>, id: NodeId| {
        if let Some(run) = pending.remove(id) {
            // Run the body on an inner task so a panic surfaces as a
            // `JoinError` *here* (logged) while the outer JoinSet task always
            // returns the id: a dep is satisfied by completion, not success,
            // so dependents must be released even when the body panics.
            tasks.spawn(async move {
                let inner = tokio::spawn(run());
                if let Err(err) = inner.await {
                    warn!("scrub scheduler: node {id} failed: {err}");
                }
                id
            });
        }
    };

    // Seed with every in-degree-0 node, then drain the JoinSet, releasing
    // dependents as each node completes.
    let ready: Vec<NodeId> = in_degree
        .iter()
        .filter(|&(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();
    for id in ready {
        spawn(&mut tasks, &mut pending, id);
    }

    while let Some(joined) = tasks.join_next().await {
        // The outer task never panics (the body's panic is caught above and
        // reported through the inner JoinHandle), so this always yields the id.
        let id = match joined {
            Ok(id) => id,
            Err(err) => {
                warn!("scrub scheduler: scheduler task failed unexpectedly: {err}");
                continue;
            }
        };
        for next in dependents.remove(id).into_iter().flatten() {
            if let Some(deg) = in_degree.get_mut(next) {
                *deg -= 1;
                if *deg == 0 {
                    spawn(&mut tasks, &mut pending, next);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use tokio::sync::Notify;

    use super::{Node, NodeId, run_dag};

    /// Records the order in which nodes begin and end.
    type Log = Arc<Mutex<Vec<String>>>;

    fn record(log: &Log, entry: impl Into<String>) {
        log.lock().unwrap().push(entry.into());
    }

    /// A node that just records `"{id}"` when it runs.
    fn marker(id: NodeId, deps: &'static [NodeId], log: Log) -> Node {
        Node {
            id,
            deps,
            run: Box::new(move || {
                Box::pin(async move {
                    record(&log, id);
                })
            }),
        }
    }

    #[tokio::test]
    async fn linear_chain_runs_in_order() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let nodes = vec![
            marker("c", &["b"], log.clone()),
            marker("a", &[], log.clone()),
            marker("b", &["a"], log.clone()),
        ];

        run_dag(nodes).await;

        assert_eq!(*log.lock().unwrap(), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn diamond_waits_for_both_and_overlaps_middle() {
        // a -> {b, c} -> d. d must observe both b and c done; b and c must
        // overlap. We park b and c on a shared barrier so neither can finish
        // until both have started, proving genuine concurrency.
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let both_started = Arc::new(Notify::new());

        let middle = |id: NodeId| {
            let log = log.clone();
            let started = started.clone();
            let both_started = both_started.clone();
            Node {
                id,
                deps: &["a"],
                run: Box::new(move || {
                    Box::pin(async move {
                        record(&log, format!("start {id}"));
                        if started.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                            both_started.notify_waiters();
                        } else {
                            both_started.notified().await;
                        }
                        record(&log, format!("end {id}"));
                    })
                }),
            }
        };

        let nodes = vec![
            marker("a", &[], log.clone()),
            middle("b"),
            middle("c"),
            marker("d", &["b", "c"], log.clone()),
        ];

        run_dag(nodes).await;

        let log = log.lock().unwrap();
        let pos = |s: &str| log.iter().position(|e| e == s).unwrap();
        // a before both middles.
        assert!(pos("start b") > pos("a") && pos("start c") > pos("a"));
        // b and c overlapped: both started before either ended.
        assert!(pos("start b") < pos("end c") && pos("start c") < pos("end b"));
        // d waited for both.
        assert!(pos("d") > pos("end b") && pos("d") > pos("end c"));
    }

    #[tokio::test]
    async fn two_independent_nodes_overlap() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let both_started = Arc::new(Notify::new());

        let independent = |id: NodeId| {
            let log = log.clone();
            let started = started.clone();
            let both_started = both_started.clone();
            Node {
                id,
                deps: &[],
                run: Box::new(move || {
                    Box::pin(async move {
                        record(&log, format!("start {id}"));
                        if started.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                            both_started.notify_waiters();
                        } else {
                            both_started.notified().await;
                        }
                        record(&log, format!("end {id}"));
                    })
                }),
            }
        };

        run_dag(vec![independent("x"), independent("y")]).await;

        let log = log.lock().unwrap();
        let pos = |s: &str| log.iter().position(|e| e == s).unwrap();
        // Both started before either ended => genuine overlap.
        assert!(pos("start x") < pos("end y") && pos("start y") < pos("end x"));
    }

    #[tokio::test]
    async fn panicking_node_still_releases_dependents() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let nodes = vec![
            Node {
                id: "boom",
                deps: &[],
                run: Box::new(|| Box::pin(async { panic!("boom") })),
            },
            marker("after", &["boom"], log.clone()),
        ];

        run_dag(nodes).await;

        assert_eq!(*log.lock().unwrap(), vec!["after"]);
    }

    #[tokio::test]
    async fn edge_to_absent_node_is_dropped() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        // "lonely" depends on "ghost", which is not in the node set.
        let nodes = vec![marker("lonely", &["ghost"], log.clone())];

        run_dag(nodes).await;

        assert_eq!(*log.lock().unwrap(), vec!["lonely"]);
    }

    #[tokio::test]
    async fn all_nodes_run_exactly_once_and_returns() {
        let counts: Arc<Mutex<HashMap<NodeId, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        let counting = |id: NodeId, deps: &'static [NodeId]| {
            let counts = counts.clone();
            Node {
                id,
                deps,
                run: Box::new(move || {
                    Box::pin(async move {
                        *counts.lock().unwrap().entry(id).or_insert(0) += 1;
                    })
                }),
            }
        };

        let nodes = vec![
            counting("a", &[]),
            counting("b", &["a"]),
            counting("c", &["a"]),
            counting("d", &["b", "c"]),
            counting("e", &[]),
        ];

        run_dag(nodes).await;

        let counts = counts.lock().unwrap();
        assert_eq!(counts.len(), 5);
        for id in ["a", "b", "c", "d", "e"] {
            assert_eq!(counts.get(id), Some(&1), "{id} should run exactly once");
        }
    }
}
