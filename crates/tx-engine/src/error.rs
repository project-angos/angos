//! Engine-level error type.

use crate::lock;

/// Errors produced by the transactional engine.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] angos_storage::Error),

    /// A lock operation failed.
    #[error("lock error: {0}")]
    Lock(#[from] lock::Error),

    /// Two concurrent transactions conflict; the caller should retry.
    #[error("transaction conflict")]
    Conflict,

    /// A CAS precondition was not met and the transaction cannot be applied.
    #[error("precondition failed")]
    Precondition,

    /// A CAS transaction was partially committed (at least one mutation landed)
    /// but a subsequent mutation encountered true contention: the live body does
    /// not match the staged body. The intent is left in `.tx-log/` for the
    /// recovery loop to converge.
    #[error("partial commit: true contention on a mid-apply mutation")]
    PartialCommit,

    /// An intent record or mutation body could not be (de)serialised.
    #[error("serialisation error: {0}")]
    Serde(#[from] serde_json::Error),

    /// A builder was finalised without all required fields, or a factory
    /// could not construct an engine component from operator inputs.
    #[error("builder error: {0}")]
    Build(String),
}

impl Error {
    /// `true` for the outcomes a retry loop should re-attempt against a fresh
    /// read: [`Error::Conflict`] and [`Error::Precondition`].
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        matches!(self, Error::Conflict | Error::Precondition)
    }
}
