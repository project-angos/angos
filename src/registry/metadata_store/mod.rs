//! The metadata store: every key angos writes outside the blob store, and
//! the readers over them. A key's layout and the one writer that composes it
//! live here; which keys a request writes, and in what order, is the OCI
//! service's to decide.

use std::{
    fmt::{self, Display, Formatter},
    num::NonZeroUsize,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, de::Error as DeError};
use tracing::instrument;

use angos_oci::{Descriptor, Digest, MediaType, Namespace, Reference, Tag};
use angos_storage::ObjectStore;

use crate::registry::{Error, keys::REPOS_ROOT, pagination};

pub mod access_time;
mod blob_index;
mod catalog;
mod gc;
mod record;
pub mod tag;

pub use access_time::AccessEntry;
pub use blob_index::BlobIndex;

/// What a key records a namespace holding. A push pins every digest it
/// references through [`LinkKind::ReferencedBy`]; the per-role keys an older
/// angos wrote instead (`r/layer`, `r/config`, `r/idx.*`) no longer parse, so
/// scrub quarantines them.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum LinkKind {
    Blob(Digest),
    Tag(Tag),
    Digest(Digest),
    /// A referrer back-link: the manifest `referrer` names `subject` in its
    /// `subject` field.
    Referrer {
        subject: Digest,
        referrer: Digest,
    },
    /// A per-referrer reference entry: the manifest with this digest
    /// references the blob the entry lives under, and is backed while that
    /// manifest's revision still resolves in the namespace.
    ReferencedBy(Digest),
}

impl LinkKind {
    /// The link a client names by tag or by digest.
    pub fn from_reference(reference: &Reference) -> Self {
        match reference {
            Reference::Tag(s) => LinkKind::Tag(s.clone()),
            Reference::Digest(d) => LinkKind::Digest(d.clone()),
        }
    }
}

impl Display for LinkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LinkKind::Blob(d) => write!(f, "blob:{d}"),
            LinkKind::Tag(s) => write!(f, "tag:{s}"),
            LinkKind::Digest(d) => write!(f, "digest:{d}"),
            LinkKind::Referrer { subject, referrer } => {
                write!(f, "referrer:{subject}-{referrer}")
            }
            LinkKind::ReferencedBy(referrer) => write!(f, "referenced-by:{referrer}"),
        }
    }
}

/// Reads a recorded media type, dropping the parameter section an angos before
/// 1.5.0 copied verbatim out of the pushed `Content-Type`. Refusing such a
/// value would have scrub delete the record as corrupt.
fn stored_media_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<MediaType>, D::Error> {
    let Some(recorded) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };

    MediaType::from_content_type(&recorded)
        .map(Some)
        .map_err(DeError::custom)
}

/// What a link read resolves to, assembled in memory from the stored record.
#[derive(Debug, Clone)]
pub struct LinkMetadata {
    pub target: Digest,
    pub created_at: Option<DateTime<Utc>>,
    pub media_type: Option<MediaType>,
    pub descriptor: Option<Descriptor>,
}

impl LinkMetadata {
    /// `Some(created_at)` iff this link supersedes an incoming write authored
    /// at `source_ts`: newer `created_at`, or equal with a target digest
    /// ordering above `incoming_digest`. That tie-break stops equal-timestamp
    /// peers swapping digests forever, and a delete carries no digest so it
    /// keeps plain strictly-greater.
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
}

/// Every key angos writes outside the blob store, over one object store.
/// Cheap to clone: the handle is shared.
#[derive(Clone)]
pub struct MetadataStore {
    object: Arc<dyn ObjectStore>,
    namespace_walk_concurrency: NonZeroUsize,
    /// How long fresh blob data and fresh reference keys are unconditionally
    /// live, and how long a collector's range marker outlives its last
    /// refresh. Writers and collectors over the same store share the value.
    pub gc_grace_secs: u64,
    /// How long superseded access entries are kept as pull history before
    /// scrub collects them.
    pub atime_audit_window_secs: u64,
    /// How long a released reclamation marker keeps blocking writers.
    release_linger_ms: i64,
}

/// Keys fetched per page when the store walks a directory it does not bound
/// by a caller's page size.
pub const LIST_PAGE: u16 = 1000;

/// Default reclamation grace period, which only has to exceed the widest
/// adjacent-request gap on a write path plus clock skew.
pub const DEFAULT_GC_GRACE_SECS: u64 = 300;

/// Default retention for superseded access entries.
pub const DEFAULT_ATIME_AUDIT_WINDOW_SECS: u64 = 3600;

/// Default linger of a released reclamation marker, which has to outlast a
/// writer's whole backoff budget plus one listing.
pub const DEFAULT_RELEASE_LINGER_MS: i64 = 5_000;

/// What the store reads off its configuration. [`Settings::default`] is what a
/// test or an offline run gets; the server bootstrap fills all three from the
/// operator's configuration.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Concurrent directory-scan fan-out for catalog namespace walks.
    pub namespace_walk_concurrency: NonZeroUsize,
    /// The reclamation grace period, in seconds; tests and offline maintenance
    /// runs shrink it to exercise reclamation immediately.
    pub gc_grace_secs: u64,
    /// How long superseded access entries are retained as pull history.
    pub atime_audit_window_secs: u64,
    /// How long a released reclamation marker keeps blocking writers; tests
    /// shrink it so they do not sleep out the real one.
    pub release_linger_ms: i64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            namespace_walk_concurrency: pagination::NAMESPACE_WALK_CONCURRENCY,
            gc_grace_secs: DEFAULT_GC_GRACE_SECS,
            atime_audit_window_secs: DEFAULT_ATIME_AUDIT_WINDOW_SECS,
            release_linger_ms: DEFAULT_RELEASE_LINGER_MS,
        }
    }
}

impl MetadataStore {
    /// A store over `object`, the one object store all its reads and writes
    /// flow through.
    #[must_use]
    pub fn new(object: Arc<dyn ObjectStore>, settings: Settings) -> Self {
        Self {
            object,
            namespace_walk_concurrency: settings.namespace_walk_concurrency,
            gc_grace_secs: settings.gc_grace_secs,
            atime_audit_window_secs: settings.atime_audit_window_secs,
            release_linger_ms: settings.release_linger_ms,
        }
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object
    }

    /// Write one link of any kind: the repair path's "make this link exist". The reference key lands first and is
    /// checked against collector runs before the record it pins, the same
    /// order a push keeps across its whole batch; a kind that lives as a
    /// reference key alone is done once the key is.
    pub async fn write_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
        target: &Digest,
    ) -> Result<(), Error> {
        self.pin_references(namespace, &[(target.clone(), link.clone())])
            .await?;
        match link {
            LinkKind::Tag(tag) => {
                self.put_tag_entry(namespace, tag, target, None).await?;
            }
            LinkKind::Digest(digest) => {
                self.put_revision(namespace, digest, None, None).await?;
            }
            LinkKind::Referrer { subject, referrer } => {
                self.put_referrer(namespace, subject, referrer, None)
                    .await?;
            }
            LinkKind::Blob(_) | LinkKind::ReferencedBy(_) => {}
        }
        Ok(())
    }

    /// Read the stored [`LinkMetadata`] for `link` within `namespace`: a tag
    /// from its ordered entries, a revision or referrer from its record. Every
    /// other kind is stored as a reference key carrying no metadata of its own.
    #[instrument(skip(self))]
    pub async fn read_link(
        &self,
        namespace: &Namespace,
        link: &LinkKind,
    ) -> Result<LinkMetadata, Error> {
        match link {
            LinkKind::Tag(tag) => self.resolve_tag(namespace, tag).await,
            LinkKind::Digest(digest) => self.resolve_revision(namespace, digest).await,
            LinkKind::Referrer { subject, referrer } => {
                self.resolve_referrer(namespace, subject, referrer).await
            }
            _ => Err(Error::NotFound),
        }
    }

    /// One bounded listing probes the backend; readiness must not walk the
    /// namespace tree.
    pub async fn check_ready(&self) -> Result<(), Error> {
        self.object
            .list_children(REPOS_ROOT, 1, None, None)
            .await
            .map_err(|e| Error::Internal(format!("storage backend not ready: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use angos_oci::{MediaType, Namespace, Tag};

    use crate::registry::{
        Error,
        metadata_store::{LinkKind, record::RevisionRecord},
        test_utils::{create_link, drop_links, for_each_backend, put_blob_direct, seed_links},
    };

    /// A tag and a revision written by `write_link` read back with the target
    /// and the stamp the write recorded, and stop resolving once the record is
    /// gone.
    #[tokio::test]
    async fn a_written_link_reads_back_and_stops_resolving_once_dropped() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = &Namespace::new("test-update-links").unwrap();
            let digest1 = put_blob_direct(m.object_store(), b"content1").await;
            let digest2 = put_blob_direct(m.object_store(), b"content2").await;

            let tag = LinkKind::Tag(Tag::new("v1").unwrap());
            let revision = LinkKind::Digest(digest2.clone());

            seed_links(
                &m,
                namespace,
                &[
                    (tag.clone(), digest1.clone()),
                    (revision.clone(), digest2.clone()),
                ],
            )
            .await
            .unwrap();

            for (link, target) in [(&tag, &digest1), (&revision, &digest2)] {
                let meta = m.read_link(namespace, link).await.unwrap();
                assert_eq!(&meta.target, target);
                assert!(
                    meta.created_at.is_some(),
                    "{link} must read back the stamp its write recorded"
                );
            }

            drop_links(&m, namespace, &[tag.clone(), revision.clone()])
                .await
                .unwrap();

            for link in [&tag, &revision] {
                assert!(
                    matches!(m.read_link(namespace, link).await, Err(Error::NotFound)),
                    "{link} must stop resolving once its record is gone"
                );
            }
        })
        .await;
    }

    /// A revision keeps the media type its push recorded; a tag seeded without
    /// one reads as `None` rather than as a default.
    #[tokio::test]
    async fn a_recorded_media_type_round_trips_and_its_absence_reads_as_none() {
        for_each_backend(async |test_case| {
            let m = test_case.metadata_store();
            let namespace = Namespace::new("media-type-test").unwrap();
            let digest = put_blob_direct(m.object_store(), b"test content").await;
            let recorded =
                MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap();

            m.put_revision(&namespace, &digest, Some(recorded.clone()), None)
                .await
                .unwrap();
            let revision = m
                .read_link(&namespace, &LinkKind::Digest(digest.clone()))
                .await
                .unwrap();
            assert_eq!(revision.media_type, Some(recorded));
            assert_eq!(revision.target, digest);

            let tag = LinkKind::Tag(Tag::new("latest").unwrap());
            create_link(&m, &namespace, &tag, &digest).await;
            let tag = m.read_link(&namespace, &tag).await.unwrap();
            assert_eq!(tag.media_type, None);
            assert_eq!(tag.target, digest);
        })
        .await;
    }

    /// An angos before 1.5.0 copied the pushed `Content-Type` into the record
    /// verbatim, parameter section included. The revision record must keep
    /// reading such a value, since refusing it has scrub delete the record as
    /// corrupt.
    #[test]
    fn a_stored_media_type_keeps_reading_when_it_carries_parameters() {
        let body = br#"{"media_type":"application/vnd.oci.image.manifest.v1+json; charset=utf-8"}"#;

        let revision: RevisionRecord = serde_json::from_slice(body).unwrap();
        assert_eq!(
            revision.media_type,
            Some(MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap())
        );
    }
}
