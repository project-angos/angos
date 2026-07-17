use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::oci::{Descriptor, Digest, MediaType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkMetadata {
    pub target: Digest,
    pub created_at: Option<DateTime<Utc>>,
    pub accessed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub referenced_by: HashSet<Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<MediaType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<Descriptor>,
}

impl LinkMetadata {
    /// Test convenience for `from_digest_at(target, now())`.
    #[cfg(test)]
    pub fn from_digest(target: Digest) -> Self {
        Self::from_digest_at(target, Utc::now())
    }

    /// Construct with an explicit creation time. Replicated writes pass the
    /// originating `source_ts` so the tag's last-writer-wins timestamp tracks
    /// the author's write time, not each receiver's clock.
    pub fn from_digest_at(target: Digest, created_at: DateTime<Utc>) -> Self {
        Self {
            target,
            created_at: Some(created_at),
            accessed_at: None,
            referenced_by: HashSet::new(),
            media_type: None,
            descriptor: None,
        }
    }

    /// Build a link carrying no creation timestamp. Used by `angos migrate`
    /// to rebuild a pre-JSON `distribution` link (a bare digest file) as JSON.
    /// A missing `created_at` never wins last-writer-wins (a synthesised
    /// `now()` would re-stamp fresher on every read and block replication to
    /// the tag), so a migrated legacy link stays subordinate to any real write.
    pub fn without_timestamp(target: Digest) -> Self {
        Self {
            target,
            created_at: None,
            accessed_at: None,
            referenced_by: HashSet::new(),
            media_type: None,
            descriptor: None,
        }
    }

    pub fn add_referrer(&mut self, digest: Digest) {
        self.referenced_by.insert(digest);
    }

    pub fn remove_referrer(&mut self, digest: &Digest) {
        self.referenced_by.remove(digest);
    }

    pub fn has_references(&self) -> bool {
        !self.referenced_by.is_empty()
    }

    /// `Some(created_at)` iff this link strictly supersedes an incoming write
    /// authored at `source_ts`: newer `created_at`, or equal with a target
    /// digest ordering above `incoming_digest` (the tie-break that stops
    /// equal-timestamp peers swapping digests forever; deletes carry no digest
    /// and keep plain strictly-greater). `None` when the link has no
    /// `created_at` or loses.
    pub fn supersedes(
        &self,
        source_ts: DateTime<Utc>,
        incoming_digest: Option<&Digest>,
    ) -> Option<DateTime<Utc>> {
        let created_at = self.created_at?;
        if created_at > source_ts {
            return Some(created_at);
        }
        if created_at == source_ts
            && let Some(incoming) = incoming_digest
            && self.target > *incoming
        {
            return Some(created_at);
        }
        None
    }

    pub fn with_media_type(mut self, media_type: Option<MediaType>) -> Self {
        self.media_type = media_type;
        self
    }

    pub fn with_descriptor(mut self, descriptor: Option<Descriptor>) -> Self {
        self.descriptor = descriptor;
        self
    }

    pub fn accessed(mut self) -> Self {
        self.accessed_at = Some(Utc::now());
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::registry::test_utils::media_type;

    const VALID_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_HASH: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    fn digest() -> Digest {
        Digest::sha256(VALID_HASH).unwrap()
    }

    fn other_digest() -> Digest {
        Digest::sha256(OTHER_HASH).unwrap()
    }

    fn minimal_descriptor() -> Descriptor {
        Descriptor {
            media_type: media_type("application/vnd.oci.image.manifest.v1+json"),
            digest: digest(),
            size: 42,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        }
    }

    #[test]
    fn from_digest_at_uses_explicit_timestamp() {
        let ts = Utc::now() - chrono::Duration::hours(2);
        let meta = LinkMetadata::from_digest_at(digest(), ts);
        assert_eq!(meta.target, digest());
        assert_eq!(meta.created_at, Some(ts));
        assert!(meta.accessed_at.is_none());
        assert!(meta.referenced_by.is_empty());
        assert!(meta.media_type.is_none());
        assert!(meta.descriptor.is_none());
    }

    #[test]
    fn link_metadata_survives_json_round_trip() {
        let mut meta = LinkMetadata::from_digest(digest());
        meta.add_referrer(other_digest());
        let meta = meta
            .with_media_type(Some(media_type(
                "application/vnd.oci.image.manifest.v1+json",
            )))
            .with_descriptor(Some(minimal_descriptor()));

        let bytes = serde_json::to_vec(&meta).unwrap();
        let parsed: LinkMetadata = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed.target, meta.target);
        assert_eq!(parsed.referenced_by, meta.referenced_by);
        assert_eq!(parsed.media_type, meta.media_type);
        assert_eq!(parsed.descriptor, meta.descriptor);
    }

    #[test]
    fn add_referrer_inserts_unique_digest() {
        let mut meta = LinkMetadata::from_digest(digest());
        meta.add_referrer(other_digest());
        assert!(meta.referenced_by.contains(&other_digest()));
        assert!(meta.has_references());
    }

    #[test]
    fn add_referrer_is_idempotent() {
        let mut meta = LinkMetadata::from_digest(digest());
        meta.add_referrer(other_digest());
        meta.add_referrer(other_digest());
        assert_eq!(meta.referenced_by.len(), 1);
    }

    #[test]
    fn remove_referrer_eliminates_existing_digest() {
        let mut meta = LinkMetadata::from_digest(digest());
        meta.add_referrer(other_digest());
        meta.remove_referrer(&other_digest());
        assert!(meta.referenced_by.is_empty());
        assert!(!meta.has_references());
    }

    #[test]
    fn remove_referrer_unknown_digest_is_noop() {
        let mut meta = LinkMetadata::from_digest(digest());
        meta.remove_referrer(&other_digest());
        assert!(meta.referenced_by.is_empty());
    }

    #[test]
    fn has_references_reflects_set_state() {
        let mut meta = LinkMetadata::from_digest(digest());
        assert!(!meta.has_references());
        meta.add_referrer(other_digest());
        assert!(meta.has_references());
        meta.remove_referrer(&other_digest());
        assert!(!meta.has_references());
    }

    #[test]
    fn accessed_sets_accessed_at() {
        let before = Utc::now();
        let meta = LinkMetadata::from_digest(digest()).accessed();
        assert!(meta.accessed_at.is_some());
        let accessed_at = meta.accessed_at.unwrap();
        assert!(accessed_at >= before);
        assert!(meta.created_at.is_some());
        assert_eq!(meta.target, digest());
    }

    #[test]
    fn accessed_does_not_mutate_referrer_list() {
        let mut meta = LinkMetadata::from_digest(digest());
        meta.add_referrer(other_digest());
        let meta = meta.accessed();
        assert_eq!(meta.referenced_by.len(), 1);
        assert!(meta.referenced_by.contains(&other_digest()));
    }

    #[test]
    fn with_media_type_assigns_field() {
        let meta = LinkMetadata::from_digest(digest())
            .with_media_type(Some(media_type("application/vnd.foo")));
        assert_eq!(meta.media_type, Some(media_type("application/vnd.foo")));
    }

    #[test]
    fn with_media_type_none_clears_field() {
        let meta = LinkMetadata::from_digest(digest())
            .with_media_type(Some(media_type("application/vnd.foo")))
            .with_media_type(None);
        assert!(meta.media_type.is_none());
    }

    #[test]
    fn with_descriptor_assigns_field() {
        let desc = minimal_descriptor();
        let meta = LinkMetadata::from_digest(digest()).with_descriptor(Some(desc.clone()));
        assert_eq!(meta.descriptor, Some(desc));
    }
}
