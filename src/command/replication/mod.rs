//! The `angos replication` subcommand: a front-end over the same `Ctx` +
//! scheduler + `metadata_node` + `ReplicationChecker` + `ReplicationDrain` that
//! `scrub` drives. Replication reconcile is a **sync operation**, not policy
//! enforcement, so it lives in its own subcommand rather than under
//! `angos policy`.
//!
//! It runs exactly the replication reconcile: a per-namespace metadata walk with
//! only the `ReplicationChecker` enabled (enqueueing a push for each diverging
//! or missing tag, and, for a `prune = true` downstream, a delete for each
//! downstream-only tag), followed by the in-process `ReplicationDrain` that
//! drains the enqueued jobs to convergence. It reuses the existing
//! `command::scrub` infrastructure verbatim: no checker bodies, scheduler, node
//! body, drain, or DAG edges are duplicated here.

mod command;

pub use command::{Command, Options};

/// The replication subcommand's failures are exactly the same store/bootstrap
/// errors `scrub` raises, so we re-export `scrub::Error` rather than mirror a
/// parallel enum.
pub use crate::command::scrub::Error;
