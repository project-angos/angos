//! The answers of the distribution API: the JSON documents an endpoint serves
//! and a client reads back, and what a write reports. Shared by both ends, like
//! [`crate::request`].
//!
//! What a *transport* hands back is not here: a streamed blob body and an open
//! upload session carry a reader and a credential, which belong to the client
//! that owns them rather than to the protocol.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::types::{Digest, MediaType, Namespace};

/// Body of `GET /v2/<ns>/tags/list`.
///
/// `tags` stays in its wire form: a reader that rejected the whole page over one
/// name it cannot parse would lose every other tag with it, so callers validate
/// entry by entry and decide what a bad one costs them.
#[derive(Debug, Deserialize, Serialize)]
pub struct TagsListResponse {
    /// Absent only on a remote that omits it; the spec requires it and a
    /// registry always serves it.
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
    /// The whole set, for a caller checking that a body names a code the spec
    /// defines.
    pub const ALL: &'static [Self] = &[
        Self::BlobUnknown,
        Self::BlobUploadInvalid,
        Self::BlobUploadUnknown,
        Self::DigestInvalid,
        Self::ManifestBlobUnknown,
        Self::ManifestInvalid,
        Self::ManifestUnknown,
        Self::NameInvalid,
        Self::NameUnknown,
        Self::SizeInvalid,
        Self::Unauthorized,
        Self::Denied,
        Self::Unsupported,
        Self::TooManyRequests,
    ];

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
pub const REPLICATION_SUPERSEDED_CODE: &str = "REPLICATION_SUPERSEDED";

/// Body of any 4xx/5xx answer: the codes the spec defines, each with what the
/// registry chose to say about it.
#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponse {
    pub errors: Vec<ErrorInfo>,
}

impl ErrorResponse {
    /// The code of the first error, which is the one a caller acts on.
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
    pub detail: Option<serde_json::Value>,
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

/// Outcome of a manifest push.
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

/// Outcome of a manifest delete.
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
    use crate::response::*;

    // Both ends read this body, so the shape a registry serves is what a client
    // parses back.
    #[test]
    fn a_tags_list_round_trips() {
        let served = TagsListResponse {
            name: Some(Namespace::new("nginx").unwrap()),
            tags: vec!["v1".to_string(), "latest".to_string()],
        };
        let body = serde_json::to_vec(&served).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({ "name": "nginx", "tags": ["v1", "latest"] })
        );

        let read: TagsListResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(read.tags, served.tags);
    }

    // A remote that serves neither field must still parse: the listing is then
    // empty rather than an error.
    #[test]
    fn a_tags_list_tolerates_a_missing_name_and_tags() {
        let read: TagsListResponse = serde_json::from_slice(b"{}").unwrap();
        assert!(read.name.is_none() && read.tags.is_empty());
    }

    #[test]
    fn an_error_body_round_trips_to_its_first_code() {
        let served = ErrorResponse {
            errors: vec![ErrorInfo {
                code: "MANIFEST_UNKNOWN".to_string(),
                message: Some("not here".to_string()),
                detail: None,
            }],
        };
        let body = serde_json::to_vec(&served).unwrap();
        let read: ErrorResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(read.first_code(), Some("MANIFEST_UNKNOWN"));
    }

    // The spelling is the wire contract, so each variant is pinned rather than
    // derived from its name. The table doubles as the completeness check of
    // `ErrorCode::ALL`, which no `match` forces to stay exhaustive.
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
        assert_eq!(ErrorCode::ALL.len(), pinned.len());

        for (code, wire) in pinned {
            assert!(ErrorCode::ALL.contains(&code));
            assert_eq!(code.as_str(), wire);
            assert_eq!(code.to_string(), wire);
        }
    }

    #[test]
    fn an_error_body_without_entries_names_no_code() {
        let read: ErrorResponse = serde_json::from_slice(br#"{"errors":[]}"#).unwrap();
        assert_eq!(read.first_code(), None);
    }
}
