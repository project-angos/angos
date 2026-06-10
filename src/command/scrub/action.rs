use std::fmt;

use crate::{oci::Digest, registry::metadata_store::link_kind::LinkKind};

/// A single mutation that a scrub checker has decided to perform.
///
/// Checkers produce `Action` values via their `ActionSink`; the `Executor`
/// applies them (or skips them in dry-run mode) in one place.
pub enum Action {
    MigrateNamespaceRegistry,
    MigrateBlobIndex(Digest),
    DeleteOrphanBlob(Digest),
    RemoveBlobIndexLink {
        namespace: String,
        blob: Digest,
        link: LinkKind,
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
    /// Enqueue a replication push job for a tag that diverges from (or is
    /// absent on) a downstream. Applied by the `Executor` via `JobStore::enqueue`,
    /// never an inline network push (so scrub-discovered divergences get the
    /// same durable retry/backoff/coalescing as the event path).
    EnqueueReplicationPush {
        downstream: String,
        namespace: String,
        tag: String,
        digest: Digest,
    },
    /// Enqueue a replication delete job for a tag that exists on a downstream but
    /// not locally. Emitted ONLY for a downstream marked `prune = true` (an
    /// authoritative one-way mirror); pruning is OFF BY DEFAULT, because for an
    /// active-active peer deleting a tag merely because it is absent locally would
    /// destroy the peer's legitimately-newer tag that has not yet replicated back.
    /// Applied by the `Executor` via `JobStore::enqueue`, like
    /// [`Action::EnqueueReplicationPush`].
    EnqueueReplicationDelete {
        downstream: String,
        namespace: String,
        tag: String,
    },
}

impl fmt::Display for Action {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::MigrateNamespaceRegistry => {
                write!(f, "migrate namespace registry layout")
            }
            Action::MigrateBlobIndex(digest) => {
                write!(f, "migrate blob index layout for '{digest}'")
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
        }
    }
}
