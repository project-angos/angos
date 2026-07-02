use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::oci::{Digest, Reference, Tag};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum LinkKind {
    Blob(Digest),
    Tag(Tag),
    Digest(Digest),
    Layer(Digest),
    Config(Digest),
    Referrer(Digest, Digest),
    Manifest(Digest, Digest),
}

impl LinkKind {
    pub fn is_tracked(&self) -> bool {
        matches!(
            self,
            LinkKind::Layer(_) | LinkKind::Config(_) | LinkKind::Manifest(_, _)
        )
    }

    pub fn from_reference(reference: &Reference) -> Self {
        match reference {
            Reference::Tag(s) => LinkKind::Tag(s.clone()),
            Reference::Digest(d) => LinkKind::Digest(d.clone()),
        }
    }
}

impl Display for LinkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LinkKind::Blob(d) => write!(f, "blob:{d}"),
            LinkKind::Tag(s) => write!(f, "tag:{s}"),
            LinkKind::Digest(d) => write!(f, "digest:{d}"),
            LinkKind::Layer(d) => write!(f, "layer:{d}"),
            LinkKind::Config(d) => write!(f, "config:{d}"),
            LinkKind::Referrer(l, r) => write!(f, "referrer:{l}-{r}"),
            LinkKind::Manifest(index, child) => write!(f, "manifest:{index}-{child}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oci::{Reference, Tag};

    // Valid 64-char lowercase-hex sha256 hashes (the only shape `Digest` accepts).
    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn sha(hash: &str) -> Digest {
        Digest::sha256(hash).unwrap()
    }

    #[test]
    fn test_from_reference() {
        let tag = Reference::Tag(Tag::new("tag").unwrap());
        let tag_link = LinkKind::Tag(Tag::new("tag").unwrap());
        assert_eq!(LinkKind::from_reference(&tag), tag_link);

        let digest = Reference::Digest(sha(HASH_A));
        let digest_link = LinkKind::Digest(sha(HASH_A));
        assert_eq!(LinkKind::from_reference(&digest), digest_link);
    }

    #[test]
    fn is_tracked_returns_true_for_layer() {
        assert!(LinkKind::Layer(sha(HASH_A)).is_tracked());
    }

    #[test]
    fn is_tracked_returns_true_for_config() {
        assert!(LinkKind::Config(sha(HASH_A)).is_tracked());
    }

    #[test]
    fn is_tracked_returns_true_for_manifest() {
        assert!(LinkKind::Manifest(sha(HASH_A), sha(HASH_B)).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_blob() {
        assert!(!LinkKind::Blob(sha(HASH_A)).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_tag() {
        assert!(!LinkKind::Tag(Tag::new("latest").unwrap()).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_digest() {
        assert!(!LinkKind::Digest(sha(HASH_A)).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_referrer() {
        assert!(!LinkKind::Referrer(sha(HASH_A), sha(HASH_B)).is_tracked());
    }

    #[test]
    fn display_renders_expected_string_for_each_variant() {
        let cases = [
            (
                LinkKind::Tag(Tag::new("v1.0.0").unwrap()),
                "tag:v1.0.0".to_string(),
            ),
            (LinkKind::Blob(sha(HASH_A)), format!("blob:sha256:{HASH_A}")),
            (
                LinkKind::Digest(sha(HASH_A)),
                format!("digest:sha256:{HASH_A}"),
            ),
            (
                LinkKind::Layer(sha(HASH_A)),
                format!("layer:sha256:{HASH_A}"),
            ),
            (
                LinkKind::Config(sha(HASH_A)),
                format!("config:sha256:{HASH_A}"),
            ),
            (
                LinkKind::Referrer(sha(HASH_A), sha(HASH_B)),
                format!("referrer:sha256:{HASH_A}-sha256:{HASH_B}"),
            ),
            (
                LinkKind::Manifest(sha(HASH_A), sha(HASH_B)),
                format!("manifest:sha256:{HASH_A}-sha256:{HASH_B}"),
            ),
        ];

        for (link, expected) in cases {
            assert_eq!(link.to_string(), expected);
        }
    }
}
