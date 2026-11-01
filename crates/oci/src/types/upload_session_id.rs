use uuid::Uuid;

use crate::types::Error;

/// A blob upload session identifier, the opaque token the registry issues in the
/// `Location` of a started upload and the client echoes back on every subsequent
/// `/v2/<name>/blobs/uploads/<session>` request. The OCI spec treats it as
/// server-defined; angos issues a version-4 UUID, so the private field is the
/// canonical hyphenated form and the validating constructors reject anything
/// that is not one, which also keeps it safe as a single storage path segment.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct UploadSessionId(String);

impl UploadSessionId {
    /// Mint a fresh session identifier for a newly opened upload.
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// # Errors
    ///
    /// Returns an error when `s` is not a session identifier this registry
    /// issued. Only the canonical hyphenated form is one: the braced, URN and
    /// unhyphenated UUID spellings address a session nobody was ever handed.
    pub fn new(s: &str) -> Result<Self, Error> {
        match Uuid::try_parse(s) {
            Ok(uuid) if uuid.as_hyphenated().to_string() == s => Ok(Self(s.to_owned())),
            _ => Err(Error::UploadSessionId(s.to_string())),
        }
    }
}

str_newtype!(UploadSessionId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_round_trips_through_new() {
        let id = UploadSessionId::generate();
        assert_eq!(UploadSessionId::new(id.as_ref()).unwrap(), id);
    }

    // The registry hands out the canonical hyphenated form and the client
    // echoes it back, so every other UUID spelling names a session that was
    // never opened.
    #[test]
    fn test_non_canonical_forms_rejected() {
        for input in [
            "067E6162-3B6F-4AE2-A171-2470B63DFF00",
            "067e61623b6f4ae2a1712470b63dff00",
            "{067e6162-3b6f-4ae2-a171-2470b63dff00}",
            "urn:uuid:067e6162-3b6f-4ae2-a171-2470b63dff00",
        ] {
            assert!(
                UploadSessionId::new(input).is_err(),
                "'{input}' is not the form the registry issued"
            );
        }
    }

    #[test]
    fn test_invalid_rejected() {
        for input in ["", "not-a-uuid", "../etc/passwd", "067e6162"] {
            assert!(
                UploadSessionId::new(input).is_err(),
                "'{input}' must be rejected"
            );
        }
    }

    #[test]
    fn test_deserialize_invalid_rejected() {
        let result: Result<UploadSessionId, _> = serde_json::from_str("\"nope\"");
        assert!(result.is_err());
    }
}
