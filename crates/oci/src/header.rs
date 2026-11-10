//! The header vocabulary of the distribution API. As `const`s, a malformed
//! `from_static` value fails the build rather than a request.

use http::header::{HeaderName, HeaderValue};

use crate::types::constants::OCI_INDEX_MEDIA_TYPE;

pub const DOCKER_CONTENT_DIGEST: HeaderName = HeaderName::from_static("docker-content-digest");
pub const DOCKER_UPLOAD_UUID: HeaderName = HeaderName::from_static("docker-upload-uuid");
pub const OCI_SUBJECT: HeaderName = HeaderName::from_static("oci-subject");
pub const OCI_TAG: HeaderName = HeaderName::from_static("oci-tag");
pub const OCI_FILTERS_APPLIED: HeaderName = HeaderName::from_static("oci-filters-applied");
/// Echoes the `?ns=` a response was served under, telling a proxying client
/// that the parameter selected an upstream. Absent when it did not.
pub const OCI_NAMESPACE: HeaderName = HeaderName::from_static("oci-namespace");
pub const DOCKER_DISTRIBUTION_API_VERSION: HeaderName =
    HeaderName::from_static("docker-distribution-api-version");
pub const DOCKER_DISTRIBUTION_API_VERSION_V2: HeaderValue =
    HeaderValue::from_static("registry/2.0");

/// The origin timestamp (RFC 3339) of an angos replication write; a receiver
/// holding a strictly newer tag refuses it with
/// [`crate::response::REPLICATION_SUPERSEDED_CODE`].
pub const X_ANGOS_SOURCE_TIMESTAMP: HeaderName =
    HeaderName::from_static("x-angos-source-timestamp");

pub const LINK_REL_NEXT: &str = "rel=\"next\"";

pub const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

pub const OCI_INDEX_CONTENT_TYPE: HeaderValue = HeaderValue::from_static(OCI_INDEX_MEDIA_TYPE);
