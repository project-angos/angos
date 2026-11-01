use crate::types::{Digest, Error};

/// The ASCII-only OCI Distribution Spec tag grammar:
/// `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`.
fn is_tag(s: &str) -> bool {
    let mut bytes = s.bytes();
    s.len() <= 128
        && bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// A validated OCI tag: a single reference name (not a digest). The private
/// field forces construction through the validating constructors below.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct Tag(String);

impl Tag {
    /// # Errors
    ///
    /// Returns an error when `s` breaks the OCI tag grammar.
    pub fn new(s: &str) -> Result<Self, Error> {
        if is_tag(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::Reference(format!("Invalid tag: '{s}'")))
        }
    }
}

// Here rather than in `digest.rs` because the schema is valid by construction,
// so it builds a `Tag` straight from the private field.
impl Digest {
    /// The tag this subject's referrers are recorded under on a registry that
    /// does not implement the Referrers API: the algorithm and the encoded hash
    /// truncated to 64 characters, joined by a hyphen. Infallible by
    /// construction, since a registered algorithm plus lowercase hex only
    /// produces characters the tag grammar admits and the truncation keeps it
    /// inside the length cap, so the spec's substitution rule never applies.
    /// REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md#referrers-tag-schema>
    #[must_use]
    pub fn referrers_fallback_tag(&self) -> Tag {
        let hash = self.hash();

        Tag(format!(
            "{}-{}",
            self.algorithm().as_str(),
            &hash[..hash.len().min(64)]
        ))
    }
}

str_newtype!(Tag);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Algorithm;

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
        assert!(Tag::new(&tag).is_ok());
    }

    // Edge case: tag of 129 chars exceeds the 128-char OCI limit and is rejected.
    #[test]
    fn test_129_chars_rejected() {
        let tag = "a".repeat(129);
        assert!(Tag::new(&tag).is_err());
    }

    // sha256 fits the 64-character cap exactly; sha512 is 128 and must be cut,
    // or the tag would exceed the grammar's 128-character limit outright.
    #[test]
    fn referrers_fallback_tag_truncates_the_encoded_hash() {
        let sha256 = Digest::sha256_of_bytes(b"subject");
        let tag = sha256.referrers_fallback_tag();
        assert_eq!(tag.to_string(), format!("sha256-{}", sha256.hash()));

        let sha512 = Digest::from_bytes(Algorithm::Sha512, b"subject");
        let tag = sha512.referrers_fallback_tag();
        assert_eq!(tag.to_string(), format!("sha512-{}", &sha512.hash()[..64]));
        assert!(
            Tag::new(tag.as_ref()).is_ok(),
            "the truncated schema must stay inside the tag grammar"
        );
    }

    #[test]
    fn test_empty_rejected() {
        assert!(Tag::new("").is_err());
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
