use std::collections::HashMap;

use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};

use crate::{
    event_webhook::event::Event,
    oci::{Digest, Namespace, Reference},
    registry::{DOCKER_CONTENT_DIGEST, OCI_SUBJECT},
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
        headers: HashMap<&'static str, String>,
    },
    Body {
        headers: HashMap<&'static str, String>,
        content: Vec<u8>,
    },
}

pub struct HeadManifestResponse {
    pub headers: HashMap<&'static str, String>,
}

pub struct PutManifestResponse {
    pub headers: HashMap<&'static str, String>,
    pub events: Vec<Event>,
}

pub struct DeleteManifestResponse {
    pub events: Vec<Event>,
}

pub fn head_manifest_headers(meta: &ManifestMeta) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([
        (DOCKER_CONTENT_DIGEST, meta.digest.to_string()),
        (CONTENT_LENGTH.as_str(), meta.size.to_string()),
    ]);
    if let Some(media_type) = meta.media_type.clone() {
        headers.insert(CONTENT_TYPE.as_str(), media_type);
    }
    headers
}

pub fn get_manifest_body_headers(
    media_type: Option<&str>,
    digest: &Digest,
) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([(DOCKER_CONTENT_DIGEST, digest.to_string())]);
    if let Some(media_type) = media_type {
        headers.insert(CONTENT_TYPE.as_str(), media_type.to_string());
    }
    headers
}

pub fn get_manifest_redirect_headers(
    url: String,
    digest: &Digest,
    media_type: Option<String>,
) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([
        (LOCATION.as_str(), url),
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
    ]);
    if let Some(media_type) = media_type {
        headers.insert(CONTENT_TYPE.as_str(), media_type);
    }
    headers
}

pub fn put_manifest_headers(
    namespace: &Namespace,
    reference: &Reference,
    digest: &Digest,
    subject: Option<&Digest>,
) -> HashMap<&'static str, String> {
    let mut headers = HashMap::from([
        (
            LOCATION.as_str(),
            format!("/v2/{namespace}/manifests/{reference}"),
        ),
        (DOCKER_CONTENT_DIGEST, digest.to_string()),
    ]);
    if let Some(subject) = subject {
        headers.insert(OCI_SUBJECT, subject.to_string());
    }
    headers
}
