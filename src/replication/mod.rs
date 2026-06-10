//! Bi-directional replication of OCI artifacts to per-repository downstreams.
//!
//! This module holds the replication configuration DTOs ([`config`]), the
//! runtime downstream type ([`downstream`]), the LWW wire constants ([`wire`]),
//! the replication [`Error`], the push [`pipeline`], and the [`handler`]
//! ([`ReplicationJobHandler`]) that drives it off the job queue.

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
    ReplicationJobHandler, ReplicationPushPayload, build_envelope,
};
pub use crate::replication::wire::{
    REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP, manifest_accept_types,
};
