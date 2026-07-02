//! Public link-mutation operation type.

use crate::{
    oci::{Descriptor, Digest, MediaType},
    registry::metadata_store::LinkKind,
};

/// A single link mutation submitted to [`crate::registry::metadata_store::MetadataStore::update_links`].
/// `Create` carries the target digest and optional referrer / media-type /
/// descriptor metadata; `Delete` carries an optional referrer qualification so
/// tracked links (layers, configs) decrement their reference count rather than
/// being removed outright, plus an optional expected target so a cascade delete
/// planned against one target skips a link concurrently re-pointed at another.
#[derive(Debug, Clone)]
pub enum LinkOperation {
    Create {
        link: LinkKind,
        target: Digest,
        referrer: Option<Digest>,
        media_type: Option<MediaType>,
        descriptor: Option<Box<Descriptor>>,
    },
    Delete {
        link: LinkKind,
        referrer: Option<Digest>,
        expected_target: Option<Digest>,
    },
}

impl LinkOperation {
    /// Creates a link with no referrer, media type, or descriptor.
    pub fn create(link: LinkKind, target: Digest) -> Self {
        Self::Create {
            link,
            target,
            referrer: None,
            media_type: None,
            descriptor: None,
        }
    }

    /// Creates a link carrying a parent `referrer` digest.
    pub fn create_with_referrer(link: LinkKind, target: Digest, referrer: Digest) -> Self {
        Self::Create {
            link,
            target,
            referrer: Some(referrer),
            media_type: None,
            descriptor: None,
        }
    }

    /// Creates a link carrying an optional `media_type`.
    pub fn create_with_media_type(
        link: LinkKind,
        target: Digest,
        media_type: Option<MediaType>,
    ) -> Self {
        Self::Create {
            link,
            target,
            referrer: None,
            media_type,
            descriptor: None,
        }
    }

    /// Creates a link carrying a pre-computed `Descriptor` (referrer index entry).
    pub fn create_with_descriptor(
        link: LinkKind,
        target: Digest,
        descriptor: Box<Descriptor>,
    ) -> Self {
        Self::Create {
            link,
            target,
            referrer: None,
            media_type: None,
            descriptor: Some(descriptor),
        }
    }

    /// Deletes a link with no referrer qualification.
    pub fn delete(link: LinkKind) -> Self {
        Self::Delete {
            link,
            referrer: None,
            expected_target: None,
        }
    }

    /// Deletes a link qualified by a parent `referrer` digest.
    pub fn delete_with_referrer(link: LinkKind, referrer: Digest) -> Self {
        Self::Delete {
            link,
            referrer: Some(referrer),
            expected_target: None,
        }
    }

    /// Deletes a link only while it still targets `expected_target`: a link
    /// concurrently re-pointed at another digest is kept (the concurrent
    /// writer wins). Cascade deletes planned from "tags pointing at digest"
    /// ride this so a racing push is never untagged.
    pub fn delete_if_targets(link: LinkKind, expected_target: Digest) -> Self {
        Self::Delete {
            link,
            referrer: None,
            expected_target: Some(expected_target),
        }
    }
}
