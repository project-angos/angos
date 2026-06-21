use std::{
    borrow::Borrow,
    fmt::{Display, Formatter},
    ops::Deref,
    str::FromStr,
    sync::LazyLock,
};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::oci::Error;

// ASCII-only tag grammar per the OCI Distribution Spec: `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`.
// The regex crate makes `\w` Unicode-aware, which would wrongly accept non-ASCII tags.
static TAG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$").unwrap());

/// A validated OCI tag: a single reference name (not a digest). The private
/// field forces construction through the validating constructors below.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct Tag(String);

impl Tag {
    pub fn new(s: &str) -> Result<Self, Error> {
        if TAG_REGEX.is_match(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::InvalidReference(format!("Invalid tag: '{s}'")))
        }
    }
}

impl FromStr for Tag {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for Tag {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if TAG_REGEX.is_match(&s) {
            Ok(Self(s))
        } else {
            Err(Error::InvalidReference(format!("Invalid tag: '{s}'")))
        }
    }
}

impl TryFrom<&str> for Tag {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Tag {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for Tag {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for Tag {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Tag {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl Serialize for Tag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Tag {
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

    #[test]
    fn test_valid_tag() {
        let tag = Tag::new("latest").unwrap();
        assert_eq!(tag.as_ref(), "latest");
    }

    #[test]
    fn test_invalid_char_exclamation_rejected() {
        assert!(Tag::new("foo!bar").is_err());
    }

    // Edge case: tag starting with '@' is rejected (@ is not a leading word char).
    #[test]
    fn test_leading_at_rejected() {
        assert!(Tag::new("@invalid").is_err());
    }

    // The tag grammar is ASCII only, so Unicode word characters must be rejected.
    #[test]
    fn test_non_ascii_rejected() {
        for tag in ["café", "日本", "Ａ"] {
            assert!(
                Tag::new(tag).is_err(),
                "non-ASCII tag '{tag}' must be rejected"
            );
        }
    }

    // Edge case: tag of exactly 128 chars is valid (1 leading + 127 trailing).
    #[test]
    fn test_128_chars_accepted() {
        let tag = "a".repeat(128);
        assert_eq!(tag.len(), 128);
        assert!(Tag::new(&tag).is_ok());
    }

    // Edge case: tag of 129 chars exceeds the 128-char OCI limit and is rejected.
    #[test]
    fn test_129_chars_rejected() {
        let tag = "a".repeat(129);
        assert_eq!(tag.len(), 129);
        assert!(Tag::new(&tag).is_err());
    }

    #[test]
    fn test_empty_rejected() {
        assert!(Tag::new("").is_err());
    }

    #[test]
    fn test_from_str() {
        assert_eq!(Tag::from_str("v1.0.0").unwrap().as_ref(), "v1.0.0");
    }

    #[test]
    fn test_display() {
        assert_eq!(Tag::new("latest").unwrap().to_string(), "latest");
    }

    #[test]
    fn test_deref() {
        let tag = Tag::new("release").unwrap();
        assert_eq!(tag.len(), 7);
        assert!(tag.starts_with("rel"));
    }

    #[test]
    fn test_partial_eq_str() {
        let tag = Tag::new("latest").unwrap();
        assert_eq!(tag, *"latest");
        assert_ne!(tag, *"other");
    }

    #[test]
    fn test_try_from_str_and_string() {
        assert_eq!(Tag::try_from("valid").unwrap().as_ref(), "valid");
        assert!(Tag::try_from("@bad").is_err());
        assert_eq!(
            Tag::try_from("valid".to_string()).unwrap().as_ref(),
            "valid"
        );
        assert!(Tag::try_from("@bad".to_string()).is_err());
    }

    #[test]
    fn test_serialize() {
        let tag = Tag::new("latest").unwrap();
        assert_eq!(serde_json::to_string(&tag).unwrap(), "\"latest\"");
    }

    #[test]
    fn test_deserialize_round_trip() {
        let tag: Tag = serde_json::from_str("\"v2.1\"").unwrap();
        assert_eq!(tag.as_ref(), "v2.1");
    }

    #[test]
    fn test_deserialize_invalid_rejected() {
        let result: Result<Tag, _> = serde_json::from_str("\"@invalid\"");
        assert!(result.is_err());
    }
}
