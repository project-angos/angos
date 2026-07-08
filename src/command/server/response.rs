//! HTTP response presentation: the header builder and header-name constants the
//! handlers use to turn the registry's domain facts into wire responses. The
//! registry returns facts (digests, sizes, locations); header assembly lives
//! here, next to the status-code choice.

use std::collections::HashMap;

use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RANGE};

use crate::{oci::Digest, registry::DOCKER_CONTENT_DIGEST};

pub type HeaderMap = HashMap<&'static str, String>;

// Header names emitted only in server responses. The OCI wire vocabulary shared
// with the transport client (`Docker-Content-Digest`, `Docker-Upload-UUID`,
// `OCI-Subject`, `OCI-Tag`) lives in `registry`; handlers import those directly.
pub const OCI_FILTERS_APPLIED: &str = "OCI-Filters-Applied";
pub const DOCKER_DISTRIBUTION_API_VERSION: &str = "Docker-Distribution-API-Version";
pub const X_POWERED_BY: &str = "X-Powered-By";
pub const APPLICATION_JSON: &str = "application/json";

#[derive(Debug, Default)]
pub struct ResponseHeaders {
    headers: HeaderMap,
}

impl ResponseHeaders {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.insert(name, value.into());
        self
    }

    pub fn docker_content_digest(self, digest: &Digest) -> Self {
        self.with(DOCKER_CONTENT_DIGEST, digest.to_string())
    }

    pub fn content_length(self, length: u64) -> Self {
        self.with(CONTENT_LENGTH.as_str(), length.to_string())
    }

    pub fn content_type(self, media_type: impl Into<String>) -> Self {
        self.with(CONTENT_TYPE.as_str(), media_type)
    }

    pub fn location(self, location: impl Into<String>) -> Self {
        self.with(LOCATION.as_str(), location)
    }

    pub fn range(self, range: impl Into<String>) -> Self {
        self.with(RANGE.as_str(), range)
    }

    pub fn into_inner(self) -> HeaderMap {
        self.headers
    }
}
