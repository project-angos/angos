use std::fmt;

use angos_oci::{Digest, Namespace, Tag, UploadSessionId};

use crate::{
    jobs::{JobState, Queue},
    registry::{blob_store::OrphanMultipartUpload, metadata_store::LinkKind},
};
use crate::{layer::IndexLayerPayload, scan::ScanImagePayload};

/// Root prefix quarantined keys are moved under, preserving their original
/// path below it. The walk knows the prefix, so quarantined objects are never
/// re-processed; emptying it is the operator's job.
pub const LOST_AND_FOUND_PREFIX: &str = "_lost_and_found";

/// Which physical object store a walked key was found in. The two stores may
/// be distinct buckets, so a raw-key action must name the one it targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkedStore {
    Blob,
    Metadata,
}

impl fmt::Display for WalkedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalkedStore::Blob => write!(f, "blob store"),
            WalkedStore::Metadata => write!(f, "metadata store"),
        }
    }
}

/// A single mutation a maintenance check decided to perform, produced through
/// an `ActionSink` and applied (or skipped, in dry-run) by the `Executor`.
pub enum Action {
    DeleteOrphanBlob(Digest),
    RemoveBlobIndexLink {
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    },
    /// Re-add a blob-index grant missing for a manifest that still references
    /// the blob; re-inserting a present link is a no-op.
    GrantBlobIndexLink {
        namespace: Namespace,
        blob: Digest,
        link: LinkKind,
    },
    /// Demote one superseded tag entry to the `!hist/` prefix, hist key first
    /// so an interrupted demotion duplicates rather than loses.
    DemoteTagEntry {
        namespace: Namespace,
        tag: Tag,
        entry_name: String,
    },
    /// Write a namespace's missing catalog index key.
    EnsureCatalogIndex {
        namespace: Namespace,
    },
    /// Revoke a namespace's blob-ownership grant no manifest references,
    /// reclaiming the bytes when it was the last reference anywhere.
    RemoveOrphanBlobGrant {
        namespace: Namespace,
        blob: Digest,
    },
    RecreateLink {
        namespace: Namespace,
        link: LinkKind,
        target: Digest,
    },
    DeleteTag {
        namespace: Namespace,
        tag: Tag,
        /// The digest the tag pointed at when it was judged. The executor
        /// re-reads the tag and skips when it points elsewhere by then, so a
        /// re-push racing the sweep is not deleted. `None` deletes whatever the
        /// tag points at, which is what clearing an orphan namespace means.
        target: Option<Digest>,
    },
    /// Remove by prefix the upload subtree of a namespace whose raw on-disk
    /// name fails `Namespace` validation.
    DeleteInvalidUploadNamespace {
        name: String,
    },
    DeleteOrphanManifest {
        namespace: Namespace,
        digest: Digest,
    },
    DeleteExpiredUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
    DeleteOrphanReferrer {
        namespace: Namespace,
        subject: Digest,
        referrer: Digest,
    },
    AbortMultipartUpload {
        upload: OrphanMultipartUpload,
    },
    /// Enqueue a replication push for a tag diverging from or absent on a
    /// downstream, rather than pushing inline, so it gets the event path's
    /// durable retry, backoff, and coalescing.
    EnqueueReplicationPush {
        downstream: String,
        namespace: Namespace,
        tag: Tag,
        digest: Digest,
    },
    /// Enqueue a scan of an image manifest; a forced one scans it again even
    /// when a report already hangs off it.
    EnqueueScan(ScanImagePayload),
    /// Enqueue the filesystem indexing of a layer; a forced one walks it
    /// again even when a listing exists.
    EnqueueIndex(IndexLayerPayload),
    /// Delete a layer's listing and checkpoints: no `index = true` repository
    /// holds an image using the layer, so its listing lives on demand only.
    ReclaimListing(Digest),
    /// Enqueue a replication delete for a downstream-only tag, only on a
    /// `prune = true` downstream: absence-driven deletion would destroy an
    /// active-active peer's not-yet-replicated newer tag.
    EnqueueReplicationDelete {
        downstream: String,
        namespace: Namespace,
        tag: Tag,
    },
    /// Delete a queued job whose payload resolves to no configured state, so
    /// it can never succeed usefully again.
    DeleteOrphanJob {
        queue: Queue,
        state: JobState,
        storage_key: String,
        reason: String,
    },
    /// Delete a job dedup index naming a pending job that does not exist, left
    /// by a crash between the index write and the pending write. The executor
    /// re-checks both the absence and the index's age.
    DeleteOrphanJobIndex {
        queue: Queue,
        key: String,
        storage_key: String,
    },
    /// Move a key matching no known angos layout to its store's lost-and-found
    /// prefix, preserving its bytes for inspection.
    QuarantineKey {
        store: WalkedStore,
        key: String,
    },
    /// Discard a key matching no known angos layout, emitted instead of
    /// [`Action::QuarantineKey`] under `scrub --delete-unknown`.
    DeleteUnknownKey {
        store: WalkedStore,
        key: String,
    },
    /// Delete an expected-shape object whose content does not parse, so it can
    /// never be read successfully again.
    DeleteCorruptObject {
        store: WalkedStore,
        key: String,
    },
    /// Delete a superseded access-time entry past the audit window; already
    /// age-gated by the walk.
    RetireAtimeKey {
        key: String,
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
            Action::DemoteTagEntry {
                namespace,
                tag,
                entry_name,
            } => {
                write!(
                    f,
                    "demote superseded tag entry '{namespace}:{tag}' ({entry_name}) to history"
                )
            }
            Action::EnsureCatalogIndex { namespace } => {
                write!(f, "write missing catalog index key for '{namespace}'")
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
            Action::DeleteTag {
                namespace,
                tag,
                target,
            } => match target {
                Some(target) => write!(f, "delete tag '{namespace}:{tag}' -> '{target}' (policy)"),
                None => write!(f, "delete tag '{namespace}:{tag}' (policy)"),
            },
            Action::DeleteInvalidUploadNamespace { name } => {
                write!(f, "delete invalid upload namespace directory '{name}'")
            }
            Action::DeleteOrphanManifest { namespace, digest } => {
                write!(f, "delete orphan manifest '{namespace}@{digest}' (policy)")
            }
            Action::DeleteExpiredUpload {
                namespace,
                session_id,
            } => {
                write!(f, "delete expired upload '{namespace}/{session_id}'")
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
            Action::AbortMultipartUpload { upload } => {
                write!(
                    f,
                    "abort orphan multipart upload '{}' ({})",
                    upload.key, upload.upload_id
                )
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
            Action::EnqueueIndex(index) => {
                let forced = if index.force { " (forced)" } else { "" };
                write!(
                    f,
                    "enqueue index of layer '{}@{}'{forced}",
                    index.namespace, index.digest
                )
            }
            Action::ReclaimListing(digest) => write!(f, "reclaim listing of layer '{digest}'"),
            Action::EnqueueScan(scan) => {
                let forced = if scan.force { " (forced)" } else { "" };
                write!(
                    f,
                    "enqueue scan of '{}@{}'{forced}",
                    scan.namespace, scan.digest
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
            Action::DeleteOrphanJobIndex {
                queue,
                key,
                storage_key,
            } => {
                write!(
                    f,
                    "delete orphan {queue} job index '{key}' (pending job '{storage_key}' does not exist)"
                )
            }
            Action::QuarantineKey { store, key } => {
                write!(f, "quarantine unrecognized {store} key '{key}'")
            }
            Action::DeleteUnknownKey { store, key } => {
                write!(f, "delete unrecognized {store} key '{key}'")
            }
            Action::DeleteCorruptObject { store, key } => {
                write!(f, "delete corrupt {store} object '{key}'")
            }
            Action::RetireAtimeKey { key } => {
                write!(f, "retire access-time key '{key}'")
            }
        }
    }
}
