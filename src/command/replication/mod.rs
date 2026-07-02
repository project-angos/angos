//! The `angos replication` subcommand: a per-namespace metadata walk with only
//! the `ReplicationChecker` enabled (enqueueing a push for each diverging or
//! missing tag, and, for a `prune = true` downstream, a delete for each
//! downstream-only tag), followed by the in-process `ReplicationDrain` that
//! drains the enqueued jobs to convergence. It reuses the `command::scrub`
//! infrastructure rather than duplicating any checker, runner, node, or drain.

mod command;

pub use command::{Command, Options};

/// Replication raises the same store/bootstrap errors as `scrub`, so it re-exports them.
pub use crate::command::scrub::Error;
