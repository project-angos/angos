//! The OCI Distribution service contract: the routes the OCI Distribution
//! Specification defines and their typed responses, so the registry cannot
//! construct an answer the protocol forbids. The registry crate supplies the
//! behaviour.
//!
//! REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md>
//!
//! # Non-representable illegal states
//!
//! Requests arrive already validated: a [`Namespace`], [`Digest`],
//! [`Reference`] or [`MediaType`] cannot hold a value the grammar rejects, so a
//! handler never re-checks its inputs. Responses are shaped the same way. A
//! content GET answers with a two-arm enum that is *either* the bytes *or* a
//! redirect, never both and never neither, and the redirect still carries the
//! digest the spec's `Docker-Content-Digest` header needs. Each response type
//! carries exactly what its spec headers require, and no more.
//!
//! # The `hyper` feature
//!
//! With `hyper` enabled, every response type renders itself to an
//! `http::Response` whose status, headers and body are exactly what the spec
//! prescribes, via [`angos_oci::server`] and [`angos_transport`]. The
//! transport layer calls `into_response` and never hand-builds a header.

use angos_oci::response::TagsListResponse;
use angos_oci::types::http_range::ResponseRange;
use angos_oci::{Digest, Manifest, MediaType, Namespace, Reference, Tag, UploadSessionId};

pub mod endpoint;
pub use endpoint::{Endpoint, is_invalid_referrers_request, parse};

#[cfg(feature = "hyper")]
pub mod render;

/// The `200` answer to `GET /v2/`: the endpoint speaks the v2 API.
#[derive(Debug)]
pub struct ApiVersion {
    /// A value for the non-standard `X-Powered-By` header, or `None` to omit
    /// it. Lets a deployment brand, replace or hide it without the transport
    /// touching the spec's own headers.
    pub powered_by: Option<String>,
}

/// A `202 Accepted`: a delete the registry took. No body.
#[derive(Debug)]
pub struct Accepted;

/// A `204 No Content`: a cancelled upload session, or a retried or removed
/// job. No body, no headers.
#[derive(Debug)]
pub struct NoContent;

// ---- Manifests ------------------------------------------------------------

/// A manifest GET: the bytes inline, or a redirect to where they live. The
/// digest and media type ride both arms, since the spec's `Docker-Content-Digest`
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
    /// The digest served, which the pull event records.
    #[must_use]
    pub fn digest(&self) -> &Digest {
        match self {
            ManifestGet::Content { digest, .. } | ManifestGet::Redirect { digest, .. } => digest,
        }
    }
}

/// What a manifest HEAD answers: the descriptor, no body.
#[derive(Debug)]
pub struct ManifestDescriptor {
    pub digest: Digest,
    pub media_type: Option<MediaType>,
    pub length: u64,
}

/// What a manifest PUT committed, and everything its `201` headers name: where
/// it landed ([`Self::namespace`]/[`Self::reference`]), the digest stored, the
/// subject it refers to, and the tags the push created.
#[derive(Debug)]
pub struct ManifestWritten {
    pub namespace: Namespace,
    pub reference: Reference,
    pub digest: Digest,
    pub subject: Option<Digest>,
    pub created_tags: Vec<Tag>,
    /// Whether local state moved, for the caller's replication decision. Not a
    /// wire concern.
    pub changed: bool,
}

// ---- Blobs ----------------------------------------------------------------

/// A blob body being streamed out. `range` is `None` for a whole blob and the
/// response is `200`; `Some` makes it `206` and the reader carries exactly that
/// window.
pub struct BlobStream<R> {
    pub digest: Digest,
    pub total_length: u64,
    pub range: Option<ResponseRange>,
    pub reader: R,
}

/// A blob GET: the stream inline, or a redirect carrying the digest.
pub enum BlobGet<R> {
    Content(BlobStream<R>),
    Redirect { digest: Digest, location: String },
}

/// What a blob HEAD answers: the descriptor, no body.
#[derive(Debug)]
pub struct BlobDescriptor {
    pub digest: Digest,
    pub size: u64,
    pub media_type: Option<MediaType>,
}

// ---- Uploads --------------------------------------------------------------

/// An open upload session: the id to continue it under, the namespace it lives
/// in, and the count of bytes received so far. A fresh session has received
/// zero, which is `0` rather than an absent value.
#[derive(Debug)]
pub struct UploadSession {
    pub namespace: Namespace,
    pub session_id: UploadSessionId,
    pub received: u64,
}

/// A blob now stored, and the namespace whose `201` `Location` names it. The
/// answer to a mount and to a single-request or completed upload.
#[derive(Debug)]
pub struct BlobWritten {
    pub namespace: Namespace,
    pub digest: Digest,
}

/// What opening an upload produced: an open session (`202`), or a blob when the
/// request carried a `?digest=` and completed in one shot (`201`).
#[derive(Debug)]
pub enum StartUpload {
    Session(UploadSession),
    Completed(Box<BlobWritten>),
}

// ---- Discovery ------------------------------------------------------------

/// A page of a tag listing and the cursor to resume after, `None` once the
/// listing is exhausted.
#[derive(Debug)]
pub struct Tags {
    pub list: TagsListResponse,
    pub next: Option<String>,
}

/// The referrers of a subject: the OCI image index the endpoint serves, whether
/// the listing honoured an `artifactType` filter (which the response advertises
/// so a client can tell a filtered index from a complete one), and the cursor
/// to resume after.
#[derive(Debug)]
pub struct Referrers {
    pub index: Manifest,
    pub filtered: bool,
    pub next: Option<String>,
}
