use std::fmt;

use crate::{
    jobs::{JobState, Queue},
    oci::{Digest, Namespace, Tag},
    registry::metadata_store::LinkKind,
};

/// A single mutation that a scrub checker has decided to perform.
///
/// Checkers produce `Action` values via their `ActionSink`; the `Executor`
/// applies them (or skips them in dry-run mode) in one place.
pub enum Action {
    DeleteOrphanBlob(Digest),
    RemoveBlobIndexLink {
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    },
    /// Re-add a blob-index grant the index is missing relative to a manifest that
    /// still references the blob (the additive half of a blob-index reconcile).
    /// Idempotent: re-inserting a present link is a no-op.
    GrantBlobIndexLink {
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    },
    /// Revoke a namespace's orphaned blob-ownership grant (no manifest in the
    /// namespace references the blob), reclaiming the bytes when it was the last
    /// reference anywhere.
    RemoveOrphanBlobGrant {
        namespace: Namespace,
        blob: Digest,
    },
    RecreateLink {
        namespace: Namespace,
        link: LinkKind,
        target: Digest,
    },
    AddReferrer {
        namespace: Namespace,
        link: LinkKind,
        target: Digest,
        referrer: Digest,
    },
    RemoveReferrer {
        namespace: Namespace,
        link: LinkKind,
        referrer: Digest,
    },
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
    DeleteOrphanReferrer {
        namespace: Namespace,
        subject: Digest,
        referrer: Digest,
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

impl fmt::Display for Action {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::DeleteOrphanBlob(digest) => {
                write!(f, "delete orphan blob '{digest}'")
            }
            Action::RemoveBlobIndexLink {
                namespace,
                blob,
                link,
            } => {
                write!(
                    f,
                    "remove invalid link from blob index '{namespace}/{blob}': '{link}'"
                )
            }
            Action::GrantBlobIndexLink {
                namespace,
                blob,
                link,
            } => {
                write!(
                    f,
                    "grant missing blob-index entry '{namespace}/{blob}': '{link}'"
                )
            }
            Action::RemoveOrphanBlobGrant { namespace, blob } => {
                write!(
                    f,
                    "revoke orphaned blob-ownership grant '{namespace}/{blob}' (no manifest references it)"
                )
            }
            Action::RecreateLink {
                namespace,
                link,
                target,
            } => {
                write!(
                    f,
                    "recreate invalid link from namespace '{namespace}': '{link}' -> '{target}'"
                )
            }
            Action::AddReferrer {
                namespace,
                link,
                referrer,
                ..
            } => {
                write!(
                    f,
                    "add referrer {referrer} to link {link} in namespace '{namespace}'"
                )
            }
            Action::RemoveReferrer {
                namespace,
                link,
                referrer,
            } => {
                write!(
                    f,
                    "remove referrer {referrer} from link {link} in namespace '{namespace}'"
                )
            }
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
            Action::DeleteOrphanReferrer {
                namespace,
                subject,
                referrer,
            } => {
                write!(
                    f,
                    "delete orphan referrer '{namespace}': subject {subject} <- {referrer}"
                )
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
