use std::fmt;

use crate::{
    oci::{Digest, Namespace, Tag},
    registry::job_store::{JobState, Queue},
};

/// The mutate gate an [`Action`] rides: `Policy` actions are enabled by the
/// dedicated `policy`/`replication` commands (and scrub's deprecated
/// `-r`/`--replicate` aliases); every GC/repair action mutates only under
/// scrub's `--commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionCategory {
    Policy,
    Gc,
}

/// A single mutation a scrub checker has decided to perform, produced via an
/// `ActionSink` and applied (or skipped in dry-run) by the `Executor`.
pub enum Action {
    DeleteTag {
        namespace: Namespace,
        tag: Tag,
    },
    DeleteInvalidTag {
        namespace: Namespace,
        tag: String,
    },
    /// Reclaim a manifest namespace whose raw on-disk name fails `Namespace`
    /// validation, removing its repository subtree by prefix.
    DeleteInvalidNamespace {
        name: String,
    },
    /// Reclaim an upload-only namespace whose raw on-disk name fails `Namespace`
    /// validation, removing its upload subtree by prefix.
    DeleteInvalidUploadNamespace {
        name: String,
    },
    DeleteOrphanManifest {
        namespace: Namespace,
        digest: Digest,
    },
    DeleteExpiredUpload {
        namespace: Namespace,
        uuid: String,
    },
    AbortMultipartUpload {
        key: String,
        upload_id: String,
    },
    /// Enqueue a replication push job for a tag diverging from or absent on a
    /// downstream. Enqueued rather than pushed inline, so scrub-discovered
    /// divergences get the event path's durable retry/backoff/coalescing.
    EnqueueReplicationPush {
        downstream: String,
        namespace: Namespace,
        tag: Tag,
        digest: Digest,
    },
    /// Enqueue a replication delete job for a downstream-only tag. Emitted only
    /// for a `prune = true` downstream (one-way mirror): absence-driven deletion
    /// would destroy an active-active peer's not-yet-replicated newer tag.
    EnqueueReplicationDelete {
        downstream: String,
        namespace: Namespace,
        tag: Tag,
    },
    /// Delete a queued job (replication or cache) whose payload no longer
    /// resolves to configured state, so it can never succeed usefully again.
    /// `reason` carries the per-queue explanation for display.
    DeleteOrphanJob {
        queue: Queue,
        state: JobState,
        storage_key: String,
        reason: String,
    },
}

impl Action {
    /// The per-category mutate gate this action rides. Exhaustive so a future
    /// variant is forced to pick a category.
    pub fn category(&self) -> ActionCategory {
        match self {
            Action::DeleteTag { .. }
            | Action::DeleteOrphanManifest { .. }
            | Action::EnqueueReplicationPush { .. }
            | Action::EnqueueReplicationDelete { .. } => ActionCategory::Policy,
            Action::DeleteInvalidTag { .. }
            | Action::DeleteInvalidNamespace { .. }
            | Action::DeleteInvalidUploadNamespace { .. }
            | Action::DeleteExpiredUpload { .. }
            | Action::AbortMultipartUpload { .. }
            | Action::DeleteOrphanJob { .. } => ActionCategory::Gc,
        }
    }
}

impl fmt::Display for Action {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::DeleteTag { namespace, tag } => {
                write!(f, "delete tag '{namespace}:{tag}' (policy)")
            }
            Action::DeleteInvalidTag { namespace, tag } => {
                write!(f, "delete invalid tag directory '{namespace}:{tag}'")
            }
            Action::DeleteInvalidNamespace { name } => {
                write!(f, "delete invalid namespace directory '{name}'")
            }
            Action::DeleteInvalidUploadNamespace { name } => {
                write!(f, "delete invalid upload namespace directory '{name}'")
            }
            Action::DeleteOrphanManifest { namespace, digest } => {
                write!(f, "delete orphan manifest '{namespace}@{digest}' (policy)")
            }
            Action::DeleteExpiredUpload { namespace, uuid } => {
                write!(f, "delete expired upload '{namespace}/{uuid}'")
            }
            Action::AbortMultipartUpload { key, upload_id } => {
                write!(f, "abort orphan multipart upload '{key}' ({upload_id})")
            }
            Action::EnqueueReplicationPush {
                downstream,
                namespace,
                tag,
                digest,
            } => {
                write!(
                    f,
                    "enqueue replication push of '{namespace}:{tag}' ({digest}) to downstream '{downstream}'"
                )
            }
            Action::EnqueueReplicationDelete {
                downstream,
                namespace,
                tag,
            } => {
                write!(
                    f,
                    "enqueue replication delete of '{namespace}:{tag}' on downstream '{downstream}'"
                )
            }
            Action::DeleteOrphanJob {
                queue,
                state,
                storage_key,
                reason,
            } => {
                let partition = match state {
                    JobState::Pending => "pending",
                    JobState::Failed => "failed",
                };
                write!(
                    f,
                    "delete the {partition} {queue} job '{storage_key}' because {reason}"
                )
            }
        }
    }
}
