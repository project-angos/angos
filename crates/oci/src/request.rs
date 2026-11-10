//! One struct per distribution-API operation, shared by both ends. Only the
//! receiving side names a session by [`UploadSessionId`]; a client follows the
//! opaque URL the server assigned.

use std::slice;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};

use crate::types::http_range::{ByteWindow, RequestRange};
use crate::types::{
    Algorithm, Digest, MediaRange, MediaType, Namespace, Reference, Tag, UploadSessionId,
};

/// What a manifest PUT writes: one tag, or a digest plus any `?tag=` tags.
#[derive(Clone, Debug)]
pub enum ManifestPutTarget {
    Tag(Tag),
    Digest { digest: Digest, tags: Vec<Tag> },
}

impl ManifestPutTarget {
    #[must_use]
    pub fn reference(&self) -> Reference {
        match self {
            Self::Tag(tag) => Reference::Tag(tag.clone()),
            Self::Digest { digest, .. } => Reference::Digest(digest.clone()),
        }
    }

    #[must_use]
    pub fn created_tags(&self) -> &[Tag] {
        match self {
            Self::Tag(tag) => slice::from_ref(tag),
            Self::Digest { tags, .. } => tags,
        }
    }

    /// The reference and the extra `?tag=` tags, empty for a by-tag push.
    #[must_use]
    pub fn into_parts(self) -> (Reference, Vec<Tag>) {
        match self {
            Self::Tag(tag) => (Reference::Tag(tag), Vec::new()),
            Self::Digest { digest, tags } => (Reference::Digest(digest), tags),
        }
    }
}

impl Serialize for ManifestPutTarget {
    /// The policy input: every created tag, plus the digest of a by-digest push.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Self::Digest { digest, .. } = self {
            map.serialize_entry("digest", digest)?;
        }
        map.serialize_entry("tags", self.created_tags())?;
        map.end()
    }
}

#[derive(Debug)]
pub struct ListTagsRequest {
    pub namespace: Namespace,
    pub n: Option<u16>,
    pub last: Option<String>,
}

/// `last` is the cursor a registry mints inside the `Link` it must advertise
/// when the listing does not fit one page; the spec defines no page-size
/// parameter here, so the serving side alone sizes the page.
#[derive(Debug)]
pub struct GetReferrersRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    pub artifact_type: Option<MediaType>,
    pub last: Option<String>,
}

/// An empty `accepted_types` is the presence-only probe: nothing is negotiated
/// and the status alone answers.
#[derive(Debug)]
pub struct HeadBlobRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    pub accepted_types: Vec<MediaRange>,
}

#[derive(Debug)]
pub struct GetBlobRequest {
    pub namespace: Namespace,
    pub digest: Digest,
    pub accepted_types: Vec<MediaRange>,
    pub range: Option<RequestRange>,
}

#[derive(Debug)]
pub struct DeleteBlobRequest {
    pub namespace: Namespace,
    pub digest: Digest,
}

#[derive(Debug)]
pub struct HeadManifestRequest {
    pub namespace: Namespace,
    pub reference: Reference,
    pub accepted_types: Vec<MediaRange>,
}

#[derive(Debug)]
pub struct GetManifestRequest {
    pub namespace: Namespace,
    pub reference: Reference,
    pub accepted_types: Vec<MediaRange>,
}

/// The manifest body is passed separately, as a stream on the receiving side
/// and as bytes on the sending one. `source_ts` is the origin timestamp of a
/// replication write, which settles last-writer-wins on the receiver.
#[derive(Debug)]
pub struct PutManifestRequest {
    pub namespace: Namespace,
    pub reference: Reference,
    pub content_type: Option<MediaType>,
    /// The `?tag=` parameters a push by digest binds in the same operation.
    /// Read by the receiving side only: an outbound client pushes one reference
    /// per request, so it leaves this empty.
    pub tags: Vec<Tag>,
    pub source_ts: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct DeleteManifestRequest {
    pub namespace: Namespace,
    pub reference: Reference,
    pub source_ts: Option<DateTime<Utc>>,
}

/// An OCI cross-repository blob mount request
/// (`POST /v2/<ns>/blobs/uploads/?mount=<digest>[&from=<repo>]`).
/// An unsatisfiable mount falls back to a normal upload session rather than
/// failing, per the distribution spec.
#[derive(Debug)]
pub struct BlobMount {
    pub digest: Digest,
    pub from: Option<Namespace>,
}

/// The `?digest=` target of an upload POST: the digest the client names, plus
/// the length the POST declares, which is the single-request upload. A declared
/// zero is the empty blob, not an absent body; only a POST declaring no length
/// at all opens a session instead. A body without a digest is not a target,
/// since there would be nothing to verify it against.
#[derive(Debug)]
pub struct StartUploadTarget {
    pub digest: Digest,
    pub content_length: Option<u64>,
}

#[derive(Debug)]
pub struct StartUploadRequest {
    pub namespace: Namespace,
    /// The algorithm the client says it will close the upload with, so the
    /// session hashes under that one alone.
    pub digest_algorithm: Option<Algorithm>,
    /// Set only by a single-request upload, which carries the blob in the POST.
    /// An outbound client always opens a session and streams into it, so it
    /// leaves this empty.
    pub target: Option<StartUploadTarget>,
}

#[derive(Debug)]
pub struct MountBlobRequest {
    pub namespace: Namespace,
    pub mount: BlobMount,
}

#[derive(Debug)]
pub struct GetUploadRequest {
    pub namespace: Namespace,
    pub session_id: UploadSessionId,
}

#[derive(Debug)]
pub struct DeleteUploadRequest {
    pub namespace: Namespace,
    pub session_id: UploadSessionId,
}

/// The chunk is passed separately, as a stream; a missing `content_length` is
/// a chunked upload, which docker push sends.
#[derive(Debug)]
pub struct PatchUploadRequest {
    pub namespace: Namespace,
    pub session_id: UploadSessionId,
    pub content_range: Option<ByteWindow>,
    pub content_length: Option<u64>,
}

/// The body is passed separately, as a stream.
#[derive(Debug)]
pub struct CompleteUploadRequest {
    pub namespace: Namespace,
    pub session_id: UploadSessionId,
    pub digest: Digest,
    pub content_range: Option<ByteWindow>,
    pub content_length: Option<u64>,
}
