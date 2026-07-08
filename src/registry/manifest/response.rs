use crate::oci::{Digest, MediaType, Namespace, Reference, Tag};

pub struct ManifestMeta {
    pub media_type: Option<MediaType>,
    pub digest: Digest,
    pub size: u64,
}

pub struct ManifestBody {
    pub media_type: Option<MediaType>,
    pub digest: Digest,
    pub content: Vec<u8>,
}

/// The facts a manifest GET resolved to; the handler turns each variant into its
/// wire response (redirect or full body) and headers.
pub enum GetManifestResponse {
    Redirect {
        redirect_url: String,
        digest: Digest,
        media_type: Option<MediaType>,
    },
    Body {
        media_type: Option<MediaType>,
        digest: Digest,
        content: Vec<u8>,
    },
}

impl GetManifestResponse {
    /// The digest of the manifest being served, for pull-event reporting.
    pub fn digest(&self) -> &Digest {
        match self {
            GetManifestResponse::Redirect { digest, .. }
            | GetManifestResponse::Body { digest, .. } => digest,
        }
    }
}

/// The facts a manifest HEAD resolved to; the handler builds the
/// `Docker-Content-Digest`, `Content-Length`, and optional `Content-Type`
/// headers from them.
pub struct HeadManifestResponse {
    pub media_type: Option<MediaType>,
    pub digest: Digest,
    pub size: u64,
}

/// The facts a manifest PUT committed; the handler rebuilds the `Location`,
/// optional `OCI-Subject`, and optional `OCI-Tag` headers from them.
pub struct PutManifestResponse {
    pub namespace: Namespace,
    pub reference: Reference,
    pub digest: Digest,
    /// The manifest's `subject` back-link target, surfaced as `OCI-Subject`.
    pub subject: Option<Digest>,
    /// Tags created via `?tag=` query parameters, surfaced as `OCI-Tag`.
    pub created_tags: Vec<Tag>,
    /// Whether the write changed local state, as validated by the committed
    /// link transaction itself (no racy pre-read); gates the replication
    /// re-dispatch.
    pub changed: bool,
}
