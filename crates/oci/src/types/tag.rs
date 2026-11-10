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

// Here so it can build the `Tag` from the private field.
impl Digest {
    /// The tag this subject's referrers are recorded under on a registry
    /// without the Referrers API: `<algorithm>-<hash truncated to 64>`, which
    /// always satisfies the tag grammar.
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
    fn test_tag_grammar() {
        for tag in ["latest", "_v2.1-rc", "a".repeat(128).as_str()] {
            assert!(Tag::new(tag).is_ok(), "'{tag}' must be accepted");
        }
        // The grammar is ASCII only.
        for tag in [
            "",
            "foo!bar",
            "@invalid",
            ".v1",
            "café",
            "日本",
            "Ａ",
            "a".repeat(129).as_str(),
        ] {
            assert!(Tag::new(tag).is_err(), "'{tag}' must be rejected");
        }
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

    /// Covers the serde half of `str_newtype!` for every newtype.
    #[test]
    fn test_serde_round_trips_and_validates() {
        let tag: Tag = serde_json::from_str("\"v2.1\"").unwrap();
        assert_eq!(serde_json::to_string(&tag).unwrap(), "\"v2.1\"");
        assert!(serde_json::from_str::<Tag>("\"@invalid\"").is_err());
    }
}
