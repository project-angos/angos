use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize, Serializer};

use crate::types::{Digest, Error, Tag};

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "String")]
pub enum Reference {
    Tag(Tag),
    Digest(Digest),
}

/// Written as the plain string it parses from, like [`Tag`] and [`Digest`]. The
/// derived enum impl would emit a tagged object, which CEL access policies see
/// as a map instead of the documented `request.reference` string.
impl Serialize for Reference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl Reference {
    /// The tag when this is a tag reference, `None` for a digest reference.
    #[must_use]
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
            return Err(Error::Reference("Reference cannot be empty".to_string()));
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
    use serde_json::json;

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
    fn test_empty_string_rejected() {
        assert!(Reference::from_str("").is_err());
    }

    #[test]
    fn test_digest_non_hex_rejected() {
        assert!(
            Reference::from_str("sha256:NOT_A_HEX_STRING_AT_ALL_XXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                .is_err()
        );
    }

    // Edge case: bare colon (no algorithm, no hash) is rejected via digest path
    #[test]
    fn test_bare_colon_rejected() {
        assert!(Reference::from_str(":").is_err());
    }

    // Edge case: colon-prefixed string (empty tag portion before colon) is rejected
    #[test]
    fn test_colon_prefix_rejected() {
        assert!(Reference::from_str(":foo").is_err());
    }

    // Edge case: colon-suffixed string (treated as digest with empty hash) is rejected
    #[test]
    fn test_colon_suffix_rejected() {
        assert!(
            Reference::from_str("foo:").is_err(),
            "'foo:' (unsupported algorithm) must be rejected"
        );
    }

    /// `Reference` parses from a plain string, so it has to serialize back to
    /// one. A derived enum impl would emit a tagged object instead, which the
    /// CEL policy context exposes as a map rather than the documented string.
    #[test]
    fn serializes_as_a_plain_string_in_both_directions() {
        let tag: Reference = "latest".parse().unwrap();
        let digest: Reference = format!("sha256:{VALID_HASH}").parse().unwrap();

        assert_eq!(serde_json::to_value(&tag).unwrap(), json!("latest"));
        assert_eq!(
            serde_json::to_value(&digest).unwrap(),
            json!(format!("sha256:{VALID_HASH}"))
        );

        let round_tripped: Reference =
            serde_json::from_value(serde_json::to_value(&tag).unwrap()).unwrap();
        assert_eq!(round_tripped.to_string(), "latest");
    }
}
