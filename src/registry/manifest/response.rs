use http::HeaderMap;

use angos_oci::{Digest, MediaType};

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

/// What a manifest GET resolved to, before it becomes a response: the pull
/// event needs the served digest, which the response itself does not hand back.
pub enum GetManifestResponse {
    Redirect {
        digest: Digest,
        headers: HeaderMap,
    },
    Body {
        digest: Digest,
        content: Vec<u8>,
        headers: HeaderMap,
    },
}

impl GetManifestResponse {
    pub fn digest(&self) -> &Digest {
        match self {
            GetManifestResponse::Redirect { digest, .. }
            | GetManifestResponse::Body { digest, .. } => digest,
        }
    }
}

/// What a manifest PUT committed: the headers to serve plus what the
/// replication decision needs.
pub struct PutManifestResponse {
    pub digest: Digest,
    /// Whether the write changed local state, per the commit's recorded
    /// prior targets; gates the replication re-dispatch.
    pub changed: bool,
    /// A plain image manifest, the only kind a `scan = true` repository
    /// sends to the scanner.
    pub scan_subject: bool,
    /// The walkable layers of a plain image, for the filesystem indexer;
    /// empty for anything else.
    pub layers: Vec<Digest>,
    pub headers: HeaderMap,
}
