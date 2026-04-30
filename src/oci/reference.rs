use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
    sync::LazyLock,
};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::oci::{Digest, Error};

static TAG_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\w[\w.-]{0,127}$").unwrap());

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub enum Reference {
    Tag(String),
    Digest(Digest),
}

impl Reference {
    pub fn as_str(&self) -> &str {
        match self {
            Reference::Tag(s) => s,
            Reference::Digest(d) => d.as_str(),
        }
    }
}

impl FromStr for Reference {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidFormat(
                "Reference cannot be empty".to_string(),
            ));
        }

        if s.contains(':') {
            Ok(Reference::Digest(Digest::try_from(s)?))
        } else if TAG_REGEX.is_match(s) {
            Ok(Reference::Tag(s.to_string()))
        } else {
            Err(Error::InvalidFormat(format!("Invalid reference: '{s}'")))
        }
    }
}

impl TryFrom<String> for Reference {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Reference::Tag(s) => write!(f, "{s}"),
            Reference::Digest(d) => write!(f, "{d}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn test_parse_tag() {
        let Reference::Tag(tag) = Reference::from_str("latest").unwrap() else {
            panic!("Expected tag");
        };
        assert_eq!(tag, "latest");
    }

    #[test]
    fn test_parse_digest() {
        let input = format!("sha256:{VALID_HASH}");
        let Reference::Digest(digest) = Reference::from_str(&input).unwrap() else {
            panic!("Expected digest");
        };
        assert_eq!(digest.algorithm(), "sha256");
    }

    #[test]
    fn test_display() {
        assert_eq!(Reference::Tag("latest".to_string()).to_string(), "latest");
    }

    // Edge case: empty string is rejected
    #[test]
    fn test_empty_string_rejected() {
        assert!(
            Reference::from_str("").is_err(),
            "empty reference must be rejected"
        );
    }

    // Edge case: tag with invalid characters (exclamation mark) is rejected
    #[test]
    fn test_tag_invalid_char_exclamation_rejected() {
        assert!(
            Reference::from_str("foo!bar").is_err(),
            "tag containing '!' must be rejected"
        );
    }

    // Edge case: tag starting with '@' is rejected (@ is not \w)
    #[test]
    fn test_tag_leading_at_rejected() {
        assert!(
            Reference::from_str("@invalid").is_err(),
            "tag starting with '@' must be rejected"
        );
    }

    // Edge case: tag of exactly 128 chars is valid (1 \w + 127 [\w.-])
    #[test]
    fn test_tag_128_chars_accepted() {
        let tag = "a".repeat(128);
        assert_eq!(tag.len(), 128);
        assert!(
            Reference::from_str(&tag).is_ok(),
            "128-char tag must be accepted"
        );
    }

    // Edge case: tag of 129 chars exceeds the 128-char OCI limit and is rejected
    #[test]
    fn test_tag_129_chars_rejected() {
        let tag = "a".repeat(129);
        assert_eq!(tag.len(), 129);
        assert!(
            Reference::from_str(&tag).is_err(),
            "129-char tag must be rejected"
        );
    }

    // Edge case: digest with non-hex hash is rejected
    #[test]
    fn test_digest_non_hex_rejected() {
        assert!(
            Reference::from_str("sha256:NOT_A_HEX_STRING_AT_ALL_XXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                .is_err(),
            "digest with non-hex hash must be rejected"
        );
    }

    // Edge case: bare colon (no algorithm, no hash) is rejected via digest path
    #[test]
    fn test_bare_colon_rejected() {
        assert!(
            Reference::from_str(":").is_err(),
            "bare ':' must be rejected"
        );
    }

    // Edge case: colon-prefixed string (empty tag portion before colon) is rejected
    #[test]
    fn test_colon_prefix_rejected() {
        assert!(
            Reference::from_str(":foo").is_err(),
            "':foo' must be rejected"
        );
    }

    // Edge case: colon-suffixed string (treated as digest with empty hash) is rejected
    #[test]
    fn test_colon_suffix_rejected() {
        assert!(
            Reference::from_str("foo:").is_err(),
            "'foo:' (unsupported algorithm) must be rejected"
        );
    }
}
