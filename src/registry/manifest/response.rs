use crate::{
    event_webhook::event::Event,
    oci::{Digest, Namespace, Reference, Tag},
    registry::{HeaderMap, OCI_SUBJECT, OCI_TAG, ResponseHeaders},
};

pub struct ManifestMeta {
    pub media_type: Option<String>,
    pub digest: Digest,
    pub size: u64,
}

pub struct ManifestBody {
    pub media_type: Option<String>,
    pub digest: Digest,
    pub content: Vec<u8>,
}

pub enum GetManifestResponse {
    Redirect {
        headers: HeaderMap,
    },
    Body {
        headers: HeaderMap,
        content: Vec<u8>,
    },
}

pub struct HeadManifestResponse {
    pub headers: HeaderMap,
}

pub struct PutManifestResponse {
    pub headers: HeaderMap,
    pub digest: Digest,
    pub events: Vec<Event>,
    /// Whether the write changed local state, as validated by the committed
    /// link transaction itself (no racy pre-read); gates the replication
    /// re-dispatch.
    pub changed: bool,
}

pub struct DeleteManifestResponse {
    pub events: Vec<Event>,
}

pub fn head_manifest_headers(meta: &ManifestMeta) -> HeaderMap {
    let headers = ResponseHeaders::new()
        .docker_content_digest(&meta.digest)
        .content_length(meta.size);
    match &meta.media_type {
        Some(media_type) => headers.content_type(media_type).into_inner(),
        None => headers.into_inner(),
    }
}

pub fn get_manifest_body_headers(
    media_type: Option<&str>,
    digest: &Digest,
    content_length: u64,
) -> HeaderMap {
    let headers = ResponseHeaders::new()
        .docker_content_digest(digest)
        .content_length(content_length);
    match media_type {
        Some(media_type) => headers.content_type(media_type).into_inner(),
        None => headers.into_inner(),
    }
}

pub fn get_manifest_redirect_headers(
    url: String,
    digest: &Digest,
    media_type: Option<String>,
) -> HeaderMap {
    let headers = ResponseHeaders::new()
        .location(url)
        .docker_content_digest(digest);
    match media_type {
        Some(media_type) => headers.content_type(media_type).into_inner(),
        None => headers.into_inner(),
    }
}

pub fn put_manifest_headers(
    namespace: &Namespace,
    reference: &Reference,
    digest: &Digest,
    subject: Option<&Digest>,
    created_tags: &[Tag],
) -> HeaderMap {
    let mut headers = ResponseHeaders::new()
        .location(format!("/v2/{namespace}/manifests/{reference}"))
        .docker_content_digest(digest);
    if let Some(subject) = subject {
        headers = headers.with(OCI_SUBJECT, subject.to_string());
    }
    if !created_tags.is_empty() {
        let joined = created_tags
            .iter()
            .map(Tag::as_ref)
            .collect::<Vec<&str>>()
            .join(", ");
        headers = headers.with(OCI_TAG, joined);
    }
    headers.into_inner()
}
