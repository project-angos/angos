//! The OCI Distribution service as a trait.
//!
//! This crate is the *skeleton* of the registry's HTTP API: the set of
//! operations the OCI Distribution Specification defines, expressed as a trait
//! with typed requests and typed responses, so an implementor cannot construct
//! an answer the protocol forbids. The registry crate supplies the behaviour.
//!
//! REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md>
//!
//! # Non-representable illegal states
//!
//! Requests arrive already validated: a [`Namespace`], [`Digest`],
//! [`Reference`] or [`MediaType`] cannot hold a value the grammar rejects, so a
//! method never re-checks its inputs. Responses are shaped the same way. A
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

use async_trait::async_trait;
use tokio::io::AsyncRead;

use angos_oci::request::{
    CompleteUploadRequest, DeleteBlobRequest, DeleteManifestRequest, DeleteUploadRequest,
    GetBlobRequest, GetManifestRequest, GetReferrersRequest, GetUploadRequest, HeadBlobRequest,
    HeadManifestRequest, ListTagsRequest, MountBlobRequest, PatchUploadRequest, PutManifestRequest,
    StartUploadRequest,
};
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

/// A `204 No Content`: a cancelled upload session, dropped. No body, no headers.
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

// ---- The service ----------------------------------------------------------

/// The OCI Distribution operations, each an endpoint of the v1.1 HTTP API.
///
/// `Actor` is the already-authenticated caller the transport resolved; the
/// service never authenticates. `Body` is an incoming, streamed request body.
/// `Error` is the implementor's error, which the transport maps to an OCI
/// [`angos_oci::response::ErrorCode`] and HTTP status at the boundary.
#[async_trait]
pub trait OciService: Send + Sync {
    /// The authenticated caller, resolved by the transport before dispatch.
    type Actor: Send + Sync;
    /// An incoming, streamed request body.
    type Body: AsyncRead + Send + Unpin;
    /// The implementor's error; the transport renders it as an OCI error.
    type Error;

    /// `GET /v2/`, end-1. Answers whether the endpoint speaks the v2 API.
    async fn check_version(&self, actor: &Self::Actor) -> Result<ApiVersion, Self::Error>;

    /// `GET /v2/<name>/manifests/<reference>`, end-3.
    async fn get_manifest(
        &self,
        actor: &Self::Actor,
        request: GetManifestRequest,
        allow_redirect: bool,
    ) -> Result<ManifestGet, Self::Error>;

    /// `HEAD /v2/<name>/manifests/<reference>`, end-3.
    async fn head_manifest(
        &self,
        actor: &Self::Actor,
        request: HeadManifestRequest,
    ) -> Result<ManifestDescriptor, Self::Error>;

    /// `PUT /v2/<name>/manifests/<reference>`, end-7.
    async fn put_manifest(
        &self,
        actor: &Self::Actor,
        request: PutManifestRequest,
        body: Self::Body,
    ) -> Result<ManifestWritten, Self::Error>;

    /// `DELETE /v2/<name>/manifests/<reference>`, end-9.
    async fn delete_manifest(
        &self,
        actor: &Self::Actor,
        request: DeleteManifestRequest,
    ) -> Result<Accepted, Self::Error>;

    /// `GET /v2/<name>/blobs/<digest>`, end-2.
    async fn get_blob(
        &self,
        actor: &Self::Actor,
        request: GetBlobRequest,
        allow_redirect: bool,
    ) -> Result<BlobGet<Self::Body>, Self::Error>;

    /// `HEAD /v2/<name>/blobs/<digest>`, end-2.
    async fn head_blob(
        &self,
        actor: &Self::Actor,
        request: HeadBlobRequest,
    ) -> Result<BlobDescriptor, Self::Error>;

    /// `DELETE /v2/<name>/blobs/<digest>`, end-10.
    async fn delete_blob(
        &self,
        actor: &Self::Actor,
        request: DeleteBlobRequest,
    ) -> Result<Accepted, Self::Error>;

    /// `POST /v2/<name>/blobs/uploads/`, end-4a/4b.
    async fn start_upload(
        &self,
        actor: &Self::Actor,
        request: StartUploadRequest,
        body: Self::Body,
    ) -> Result<StartUpload, Self::Error>;

    /// `POST /v2/<name>/blobs/uploads/?mount=<digest>&from=<name>`, end-11.
    /// An unsatisfiable mount falls back to a fresh session, hence [`StartUpload`].
    ///
    /// `source` is the namespace the transport authorized the caller to mount
    /// from, resolved before dispatch (the caller must not be handed bytes it
    /// could not otherwise read); `None` degrades to an ordinary session.
    async fn mount_blob(
        &self,
        actor: &Self::Actor,
        request: MountBlobRequest,
        source: Option<Namespace>,
    ) -> Result<StartUpload, Self::Error>;

    /// `GET /v2/<name>/blobs/uploads/<session>`, end-13.
    async fn upload_status(
        &self,
        actor: &Self::Actor,
        request: GetUploadRequest,
    ) -> Result<UploadSession, Self::Error>;

    /// `PATCH /v2/<name>/blobs/uploads/<session>`, end-5.
    async fn patch_upload(
        &self,
        actor: &Self::Actor,
        request: PatchUploadRequest,
        body: Self::Body,
    ) -> Result<UploadSession, Self::Error>;

    /// `PUT /v2/<name>/blobs/uploads/<session>?digest=<digest>`, end-6.
    async fn complete_upload(
        &self,
        actor: &Self::Actor,
        request: CompleteUploadRequest,
        body: Self::Body,
    ) -> Result<BlobWritten, Self::Error>;

    /// `DELETE /v2/<name>/blobs/uploads/<session>`. Cancels an open session.
    async fn cancel_upload(
        &self,
        actor: &Self::Actor,
        request: DeleteUploadRequest,
    ) -> Result<NoContent, Self::Error>;

    /// `GET /v2/<name>/tags/list`, end-8.
    async fn list_tags(
        &self,
        actor: &Self::Actor,
        request: ListTagsRequest,
    ) -> Result<Tags, Self::Error>;

    /// `GET /v2/<name>/referrers/<digest>`, end-12.
    async fn get_referrers(
        &self,
        actor: &Self::Actor,
        request: GetReferrersRequest,
    ) -> Result<Referrers, Self::Error>;
}
