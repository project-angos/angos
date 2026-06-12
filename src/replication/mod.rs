//! Bi-directional replication of OCI artifacts to per-repository downstreams.

mod config;
mod downstream;
mod error;
mod handler;
mod pipeline;
mod wire;

pub use crate::replication::config::ReplicationDownstreamConfig;
pub use crate::replication::downstream::{ReplicationDownstream, ReplicationMode};
pub use crate::replication::error::Error;
pub use crate::replication::handler::{
    REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND, REPLICATION_QUEUE,
    ReplicationJobHandler, ReplicationPushPayload, build_envelope, build_prune_delete_envelope,
};
pub use crate::replication::wire::{
    REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP, manifest_accept_types,
};
