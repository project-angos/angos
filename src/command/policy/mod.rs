//! The `angos policy` subcommand: storage-policy enforcement (retention) over the
//! same `Ctx`, maintenance sequence, and checkers that `scrub` drives. It reuses
//! the `command::scrub` infrastructure rather than duplicating any checker,
//! runner, or node body.
//!
//! The Rust module is `crate::command::policy` (argh name `"policy"`) to avoid
//! clashing with the `crate::policy` retention/CEL engine.

mod command;

pub use command::{Command, Options};

/// Policy raises the same store/bootstrap errors as `scrub`, so it re-exports them.
pub use crate::command::scrub::Error;
