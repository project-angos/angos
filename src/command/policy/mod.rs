//! The `angos policy` subcommand: a second front-end over the same `Ctx` +
//! scheduler + `metadata_node` + checker structs that `scrub` drives. It hosts
//! the storage-policy enforcement operation (**retention**) that used to live
//! on `scrub`, reusing the existing `command::scrub` infrastructure verbatim:
//! no checker bodies, scheduler, node body, or DAG edges are duplicated here.
//!
//! Replication reconcile is a sync operation, not policy enforcement, so it is
//! **not** here: it lives in `crate::command::replication` (`angos replication`).
//!
//! The Rust module lives at `crate::command::policy` (the argh subcommand name
//! is the literal `"policy"`) to avoid clashing with the existing
//! `crate::policy` retention/CEL engine.

mod command;

pub use command::{Command, Options};

/// The policy subcommand's failures are exactly the same store/bootstrap errors
/// `scrub` raises, so we re-export `scrub::Error` rather than mirror a parallel
/// enum.
pub use crate::command::scrub::Error;
