use std::collections::HashMap;

use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RANGE};

use crate::{oci::Digest, registry::DOCKER_CONTENT_DIGEST};

pub type HeaderMap = HashMap<&'static str, String>;

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
