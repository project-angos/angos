use std::fmt;

use crate::{
    oci::{Digest, Tag},
    registry::{
        job_store::{JobState, Queue},
        metadata_store::LinkKind,
    },
};

/// A single mutation that a scrub checker has decided to perform.
///
/// Checkers produce `Action` values via their `ActionSink`; the `Executor`
/// applies them (or skips them in dry-run mode) in one place.
pub enum Action {
    MigrateBlobIndex(Digest),
    /// Delete the dead namespace-registry objects (`_registry/`) left by the
    /// pre-1.3 maintained catalog index; the catalog is now derived from content.
    PruneLegacyNamespaceRegistry,
    DeleteOrphanBlob(Digest),
    RemoveBlobIndexLink {
        namespace: String,
        blob: Digest,
        link: LinkKind,
    },
    /// Revoke a namespace's orphaned blob-ownership grant (no manifest in the
    /// namespace references the blob), reclaiming the bytes when it was the last
    /// reference anywhere.
    RemoveOrphanBlobGrant {
        namespace: String,
        blob: Digest,
    },
    RecreateLink {
        namespace: String,
        link: LinkKind,
        target: Digest,
    },
    AddReferrer {
        namespace: String,
        link: LinkKind,
        target: Digest,
        referrer: Digest,
    },
    RemoveReferrer {
        namespace: String,
        link: LinkKind,
        referrer: Digest,
    },
    SetMediaType {
        namespace: String,
        link: LinkKind,
        target: Digest,
        media_type: String,
        display_name: String,
    },
    DeleteTag {
        namespace: String,
        tag: Tag,
    },
    DeleteInvalidTag {
        namespace: String,
        tag: String,
    },
    DeleteOrphanManifest {
        namespace: String,
        digest: Digest,
    },
    DeleteExpiredUpload {
        namespace: String,
        uuid: String,
    },
    DeleteOrphanReferrer {
        namespace: String,
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
        namespace: String,
        tag: Tag,
        digest: Digest,
    },
    /// Enqueue a replication delete job for a downstream-only tag. Emitted only
    /// for a `prune = true` downstream (one-way mirror): absence-driven deletion
    /// would destroy an active-active peer's not-yet-replicated newer tag.
    EnqueueReplicationDelete {
        downstream: String,
        namespace: String,
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
            Action::MigrateBlobIndex(digest) => {
                write!(f, "migrate blob index layout for '{digest}'")
            }
            Action::PruneLegacyNamespaceRegistry => {
                write!(
                    f,
                    "prune the legacy namespace-registry index ('_registry/')"
                )
            }
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
            Action::SetMediaType {
                namespace,
                media_type,
                display_name,
                ..
            } => {
                write!(
                    f,
                    "set media_type '{media_type}' on {display_name} in namespace '{namespace}'"
                )
            }
            Action::DeleteTag { namespace, tag } => {
                write!(f, "delete tag '{namespace}:{tag}' (policy)")
            }
            Action::DeleteInvalidTag { namespace, tag } => {
                write!(f, "delete invalid tag directory '{namespace}:{tag}'")
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
