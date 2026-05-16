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
    SetMediaType {
        namespace: String,
        link: LinkKind,
        target: Digest,
        media_type: String,
        display_name: String,
    },
    AbortMultipartUpload {
        key: String,
        upload_id: String,
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
}

impl fmt::Display for Action {
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
            Action::AbortMultipartUpload { key, .. } => {
                write!(f, "abort orphan multipart upload {key}")
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
        }
    }
}
