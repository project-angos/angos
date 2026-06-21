use std::{
    borrow::Borrow,
    fmt::{Display, Formatter},
    ops::Deref,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::oci::Error;

/// A blob upload session identifier, the opaque token the registry issues in the
/// `Location` of a started upload and the client echoes back on every subsequent
/// `/v2/<name>/blobs/uploads/<session>` request. The OCI spec treats it as
/// server-defined; angos issues a version-4 UUID, so the private field is the
/// canonical hyphenated form and the validating constructors reject anything
/// that is not a UUID, which also keeps it safe as a single storage path segment.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct UploadSessionId(String);

impl UploadSessionId {
    /// Mint a fresh session identifier for a newly opened upload.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn new(s: &str) -> Result<Self, Error> {
        match Uuid::try_parse(s) {
            Ok(uuid) => Ok(Self(uuid.to_string())),
            Err(_) => Err(Error::InvalidUploadSessionId(s.to_string())),
        }
    }
}

impl FromStr for UploadSessionId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for UploadSessionId {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(&s)
    }
}

impl TryFrom<&str> for UploadSessionId {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl Display for UploadSessionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for UploadSessionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for UploadSessionId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for UploadSessionId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for UploadSessionId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl Serialize for UploadSessionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UploadSessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_from(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "067e6162-3b6f-4ae2-a171-2470b63dff00";

    #[test]
    fn test_generate_round_trips_through_new() {
        let id = UploadSessionId::generate();
        assert_eq!(UploadSessionId::new(id.as_ref()).unwrap(), id);
    }

    #[test]
    fn test_valid_hyphenated() {
        let id = UploadSessionId::new(SAMPLE).unwrap();
        assert_eq!(id.as_ref(), SAMPLE);
    }

    // Non-canonical UUID forms are accepted but normalized to the canonical
    // hyphenated representation, so the stored path segment is always path-safe.
    #[test]
    fn test_non_canonical_forms_normalize() {
        for input in [
            "067E6162-3B6F-4AE2-A171-2470B63DFF00",
            "067e61623b6f4ae2a1712470b63dff00",
            "{067e6162-3b6f-4ae2-a171-2470b63dff00}",
            "urn:uuid:067e6162-3b6f-4ae2-a171-2470b63dff00",
        ] {
            assert_eq!(UploadSessionId::new(input).unwrap().as_ref(), SAMPLE);
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
    fn test_from_str_and_try_from() {
        assert_eq!(UploadSessionId::from_str(SAMPLE).unwrap().as_ref(), SAMPLE);
        assert!(UploadSessionId::from_str("bad").is_err());
        assert_eq!(UploadSessionId::try_from(SAMPLE).unwrap().as_ref(), SAMPLE);
        assert_eq!(
            UploadSessionId::try_from(SAMPLE.to_string())
                .unwrap()
                .as_ref(),
            SAMPLE
        );
    }

    #[test]
    fn test_display_and_deref() {
        let id = UploadSessionId::new(SAMPLE).unwrap();
        assert_eq!(id.to_string(), SAMPLE);
        assert_eq!(id.len(), SAMPLE.len());
    }

    #[test]
    fn test_serialize_round_trip() {
        let id = UploadSessionId::new(SAMPLE).unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{SAMPLE}\""));
        let parsed: UploadSessionId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_deserialize_invalid_rejected() {
        let result: Result<UploadSessionId, _> = serde_json::from_str("\"nope\"");
        assert!(result.is_err());
    }
}
