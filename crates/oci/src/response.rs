//! The answers of the distribution API, shared by both ends.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{Digest, MediaType, Namespace};

/// Body of `GET /v2/<ns>/tags/list`. `tags` stays raw, so one bad name does not
/// lose the whole page.
#[derive(Debug, Deserialize, Serialize)]
pub struct TagsListResponse {
    /// Absent only on a remote that breaks the spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<Namespace>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// The `code` of an error answer. The distribution spec fixes the set a 4xx may
/// use, so a registry picks the closest of these and says the rest in
/// [`ErrorInfo::message`]; a 5xx code is unconstrained and is not one of these.
/// REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md#error-codes>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    BlobUnknown,
    BlobUploadInvalid,
    BlobUploadUnknown,
    DigestInvalid,
    ManifestBlobUnknown,
    ManifestInvalid,
    ManifestUnknown,
    NameInvalid,
    NameUnknown,
    SizeInvalid,
    Unauthorized,
    Denied,
    Unsupported,
    TooManyRequests,
}

impl ErrorCode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::BlobUnknown => "BLOB_UNKNOWN",
            ErrorCode::BlobUploadInvalid => "BLOB_UPLOAD_INVALID",
            ErrorCode::BlobUploadUnknown => "BLOB_UPLOAD_UNKNOWN",
            ErrorCode::DigestInvalid => "DIGEST_INVALID",
            ErrorCode::ManifestBlobUnknown => "MANIFEST_BLOB_UNKNOWN",
            ErrorCode::ManifestInvalid => "MANIFEST_INVALID",
            ErrorCode::ManifestUnknown => "MANIFEST_UNKNOWN",
            ErrorCode::NameInvalid => "NAME_INVALID",
            ErrorCode::NameUnknown => "NAME_UNKNOWN",
            ErrorCode::SizeInvalid => "SIZE_INVALID",
            ErrorCode::Unauthorized => "UNAUTHORIZED",
            ErrorCode::Denied => "DENIED",
            ErrorCode::Unsupported => "UNSUPPORTED",
            ErrorCode::TooManyRequests => "TOOMANYREQUESTS",
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Codes outside the spec's set that angos serves: a 5xx code is unconstrained,
// and the replication code answers an angos-to-angos write only, whose sender
// reads it to settle convergence rather than retry.
pub const INTERNAL_ERROR_CODE: &str = "INTERNAL_ERROR";
pub const PROVIDER_UNAVAILABLE_CODE: &str = "PROVIDER_UNAVAILABLE";
pub const RECLAMATION_IN_PROGRESS_CODE: &str = "RECLAMATION_IN_PROGRESS";
pub const UPSTREAM_UNAVAILABLE_CODE: &str = "UPSTREAM_UNAVAILABLE";
pub const REPLICATION_SUPERSEDED_CODE: &str = "REPLICATION_SUPERSEDED";

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponse {
    pub errors: Vec<ErrorInfo>,
}

impl ErrorResponse {
    #[must_use]
    pub fn first_code(&self) -> Option<&str> {
        self.errors.first().map(|error| error.code.as_str())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorInfo {
    pub code: String,
    /// Always present on the wire, `null` where the answer carries no message,
    /// which a 5xx never does.
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

/// What a manifest `HEAD` reports. The digest is absent when the answer omits
/// `Docker-Content-Digest`, which the spec states as optional; with no body
/// there is nothing to recompute it from, so the caller decides what an unknown
/// digest means.
#[derive(Clone, Debug)]
pub struct ManifestHeadResponse {
    pub media_type: Option<MediaType>,
    pub digest: Option<Digest>,
    pub size: u64,
}

/// A fetched manifest. The digest is absent for the same reason as on
/// [`ManifestHeadResponse`]; here the body is authoritative, so a caller
/// recomputes it under the algorithm its reference asked for.
#[derive(Clone, Debug)]
pub struct ManifestResponse {
    pub media_type: Option<MediaType>,
    pub digest: Option<Digest>,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub enum PutManifestOutcome {
    /// Stored, echoing what the receiver saw.
    Stored {
        /// The `Docker-Content-Digest` echo, absent when the receiver sent none
        /// or an unparseable one.
        digest: Option<Digest>,
        /// The `OCI-Subject` echo. Absent on an OCI-1.0 receiver, which does
        /// not auto-index the subject and so needs the referrers fallback tag.
        subject: Option<String>,
    },
    /// Rejected by last-writer-wins: convergence, not failure.
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteManifestOutcome {
    Deleted,
    /// Already absent: a converged no-op.
    AlreadyAbsent,
    /// Rejected by last-writer-wins: convergence, not failure.
    Superseded,
    /// The receiver does not support deleting by this reference (stock
    /// `distribution` rejects tag deletes this way). Retrying cannot help, so
    /// the caller records it distinctly instead of dead-lettering.
    Unsupported,
}

#[cfg(test)]
mod tests {
    use super::*;

    // The spelling is the wire contract, so each variant is pinned rather than
    // derived from its name.
    #[test]
    fn every_error_code_spells_its_wire_form() {
        let pinned = [
            (ErrorCode::BlobUnknown, "BLOB_UNKNOWN"),
            (ErrorCode::BlobUploadInvalid, "BLOB_UPLOAD_INVALID"),
            (ErrorCode::BlobUploadUnknown, "BLOB_UPLOAD_UNKNOWN"),
            (ErrorCode::DigestInvalid, "DIGEST_INVALID"),
            (ErrorCode::ManifestBlobUnknown, "MANIFEST_BLOB_UNKNOWN"),
            (ErrorCode::ManifestInvalid, "MANIFEST_INVALID"),
            (ErrorCode::ManifestUnknown, "MANIFEST_UNKNOWN"),
            (ErrorCode::NameInvalid, "NAME_INVALID"),
            (ErrorCode::NameUnknown, "NAME_UNKNOWN"),
            (ErrorCode::SizeInvalid, "SIZE_INVALID"),
            (ErrorCode::Unauthorized, "UNAUTHORIZED"),
            (ErrorCode::Denied, "DENIED"),
            (ErrorCode::Unsupported, "UNSUPPORTED"),
            (ErrorCode::TooManyRequests, "TOOMANYREQUESTS"),
        ];
        for (code, wire) in pinned {
            assert_eq!(code.as_str(), wire);
            assert_eq!(code.to_string(), wire);
        }
    }
}
