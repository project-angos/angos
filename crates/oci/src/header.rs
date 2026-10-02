//! The header names of the distribution API, shared by the registry that emits
//! them and the client that reads them back. `from_static` is a `const fn`
//! requiring lowercase, so a malformed name fails the build rather than a
//! request; HTTP header names are case-insensitive on the wire.

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
/// The only value the version handshake carries: the `/v2/` API this registry
/// speaks, which the protocol pins rather than the deployment choosing.
pub const DOCKER_DISTRIBUTION_API_VERSION_V2: HeaderValue =
    HeaderValue::from_static("registry/2.0");

/// The originating event timestamp (RFC 3339) of an angos replication write;
/// the receiver refuses it with
/// [`crate::response::REPLICATION_SUPERSEDED_CODE`] when its local tag is
/// strictly newer.
pub const X_ANGOS_SOURCE_TIMESTAMP: HeaderName =
    HeaderName::from_static("x-angos-source-timestamp");

/// The `rel` a paginated listing advertises its next page under, written into
/// the `Link` header by the server and matched by the client that follows it.
pub const LINK_REL_NEXT: &str = "rel=\"next\"";

/// The `Content-Type` a listing answer is served under.
pub const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

/// The `Content-Type` an image index is served under, as the value a response
/// builder inserts directly. A `const` binding makes `from_static` reject a
/// malformed value at build time, like the names above.
pub const OCI_INDEX_CONTENT_TYPE: HeaderValue = HeaderValue::from_static(OCI_INDEX_MEDIA_TYPE);
