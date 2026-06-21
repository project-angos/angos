use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::oci::{Digest, Error, Tag};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub enum Reference {
    Tag(Tag),
    Digest(Digest),
}

impl Reference {
    /// The tag when this is a tag reference, `None` for a digest reference.
    pub fn as_tag(&self) -> Option<&Tag> {
        match self {
            Reference::Tag(tag) => Some(tag),
            Reference::Digest(_) => None,
        }
    }
}

impl FromStr for Reference {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidReference(
                "Reference cannot be empty".to_string(),
            ));
        }

        if s.contains(':') {
            Ok(Reference::Digest(Digest::try_from(s)?))
        } else {
            Tag::from_str(s).map(Reference::Tag)
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
            Reference::Tag(tag) => write!(f, "{tag}"),
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
        assert_eq!(tag, *"latest");
    }

    #[test]
    fn test_parse_digest() {
        let input = format!("sha256:{VALID_HASH}");
        let Reference::Digest(digest) = Reference::from_str(&input).unwrap() else {
            panic!("Expected digest");
        };
        assert_eq!(digest.algorithm().to_string(), "sha256");
    }

    #[test]
    fn test_as_tag() {
        let tag = Reference::from_str("latest").unwrap();
        assert_eq!(tag.as_tag().unwrap().to_string(), "latest");

        let digest = Reference::from_str(&format!("sha256:{VALID_HASH}")).unwrap();
        assert!(digest.as_tag().is_none());
    }

    #[test]
    fn test_display() {
        assert_eq!(
            Reference::Tag(Tag::new("latest").unwrap()).to_string(),
            "latest"
        );
    }

    #[test]
    fn test_display_digest_includes_algorithm_prefix() {
        let input = format!("sha256:{VALID_HASH}");
        let reference = Reference::from_str(&input).unwrap();
        assert_eq!(reference.to_string(), input);
    }

    #[test]
    fn test_empty_string_rejected() {
        assert!(
            Reference::from_str("").is_err(),
            "empty reference must be rejected"
        );
    }

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
