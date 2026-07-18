//! The maintenance seam shared by scrub, prune, and replicate: the walked-store
//! key vocabulary (actions, categories), the action executor, and the
//! namespace/store walk drivers.

pub mod action;
pub mod categorize;
pub mod check;
mod error;
pub mod executor;
pub mod walk;

pub use error::Error;
