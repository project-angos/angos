use uuid::Uuid;

use crate::types::Error;

/// A blob upload session id: a canonical hyphenated v4 UUID, which also keeps
/// it safe as a single storage path segment.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct UploadSessionId(String);

impl UploadSessionId {
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// # Errors
    ///
    /// Returns an error when `s` is not the canonical hyphenated form this
    /// registry issues; other UUID spellings name sessions nobody was handed.
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

    #[test]
    fn test_invalid_and_non_canonical_forms_rejected() {
        for input in [
            "",
            "not-a-uuid",
            "../etc/passwd",
            "067e6162",
            "067E6162-3B6F-4AE2-A171-2470B63DFF00",
            "067e61623b6f4ae2a1712470b63dff00",
            "{067e6162-3b6f-4ae2-a171-2470b63dff00}",
            "urn:uuid:067e6162-3b6f-4ae2-a171-2470b63dff00",
        ] {
            assert!(
                UploadSessionId::new(input).is_err(),
                "'{input}' must be rejected"
            );
        }
    }
}
