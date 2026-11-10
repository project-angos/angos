//! The OCI Distribution service contract: the spec's routes and typed
//! responses, so the registry cannot construct an answer the protocol forbids.
//! Each response type carries exactly what its spec headers require; with the
//! `hyper` feature it renders itself, so the transport never hand-builds a
//! header.
//!
//! REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md>

use angos_oci::response::TagsListResponse;
use angos_oci::types::http_range::ResponseRange;
use angos_oci::{Digest, Manifest, MediaType, Namespace, Reference, Tag, UploadSessionId};

pub mod endpoint;
pub use endpoint::{
    Endpoint, ManifestEndpoint, UploadEndpoint, is_invalid_referrers_request, parse,
};

#[cfg(feature = "hyper")]
pub mod render;

/// The `200` answer to `GET /v2/`.
#[derive(Debug)]
pub struct ApiVersion {
    /// The non-standard `X-Powered-By` header, omitted when `None`.
    pub powered_by: Option<String>,
}

#[derive(Debug)]
pub struct Accepted;

#[derive(Debug)]
pub struct NoContent;

/// The digest and media type ride both arms: the spec's `Docker-Content-Digest`
/// and `Content-Type` headers answer a redirect too.
#[derive(Debug)]
pub enum ManifestGet {
    Content {
        digest: Digest,
        media_type: Option<MediaType>,
        bytes: Vec<u8>,
    },
    Redirect {
        digest: Digest,
        media_type: Option<MediaType>,
        location: String,
    },
}

impl ManifestGet {
    #[must_use]
    pub fn digest(&self) -> &Digest {
        match self {
            ManifestGet::Content { digest, .. } | ManifestGet::Redirect { digest, .. } => digest,
        }
    }
}

#[derive(Debug)]
pub struct ManifestDescriptor {
    pub digest: Digest,
    pub media_type: Option<MediaType>,
    pub length: u64,
}

#[derive(Debug)]
pub struct ManifestWritten {
    pub namespace: Namespace,
    pub reference: Reference,
    pub digest: Digest,
    pub subject: Option<Digest>,
    pub created_tags: Vec<Tag>,
    /// Whether local state moved, for replication; not a wire concern.
    pub changed: bool,
}

/// A `Some` range makes the response `206`, the reader carrying exactly that
/// window.
pub struct BlobStream<R> {
    pub digest: Digest,
    pub total_length: u64,
    pub range: Option<ResponseRange>,
    pub reader: R,
}

pub enum BlobGet<R> {
    Content(BlobStream<R>),
    Redirect { digest: Digest, location: String },
}

#[derive(Debug)]
pub struct BlobDescriptor {
    pub digest: Digest,
    pub size: u64,
}

#[derive(Debug)]
pub struct UploadSession {
    pub namespace: Namespace,
    pub session_id: UploadSessionId,
    pub received: u64,
}

/// The answer to a mount and to a single-request or completed upload.
#[derive(Debug)]
pub struct BlobWritten {
    pub namespace: Namespace,
    pub digest: Digest,
}

/// `Completed` when the request carried a `?digest=` and finished in one shot.
#[derive(Debug)]
pub enum StartUpload {
    Session(UploadSession),
    Completed(Box<BlobWritten>),
}

#[derive(Debug)]
pub struct Tags {
    pub list: TagsListResponse,
    pub next: Option<String>,
}

#[derive(Debug)]
pub struct Referrers {
    pub index: Manifest,
    /// Whether an `artifactType` filter applied, which the response advertises.
    pub filtered: bool,
    pub next: Option<String>,
}
