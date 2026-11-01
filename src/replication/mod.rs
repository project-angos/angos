//! Bi-directional replication of OCI artifacts to per-repository downstreams.

// Single queue carries every replication job; the downstream is encoded in the
// `lock_key` and payload.

/// Push a manifest (and everything it references) to a downstream.
pub const REPLICATION_PUSH_MANIFEST_KIND: &str = "replication.push_manifest";
/// Delete a manifest on a downstream.
pub const REPLICATION_DELETE_MANIFEST_KIND: &str = "replication.delete_manifest";

mod config;
mod downstream;
mod error;
mod handler;
mod pipeline;

pub use crate::replication::config::ReplicationDownstreamConfig;
pub use crate::replication::downstream::{ReplicationDownstream, ReplicationMode};
pub use crate::replication::error::Error;
pub use crate::replication::handler::{
    ReplicationJob, ReplicationJobHandler, ReplicationTarget, build_envelope,
    build_prune_delete_envelope, record_reconcile_outcome,
};
