use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::oci::{Digest, Reference};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum LinkKind {
    Blob(Digest),
    Tag(String),
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    use crate::oci::Reference;

    fn sha(hex: &str) -> Digest {
        Digest::Sha256(hex.into())
    }

    #[test]
    fn test_from_reference() {
        let tag = Reference::Tag("tag".to_string());
        let tag_link = LinkKind::Tag("tag".to_string());
        assert_eq!(LinkKind::from_reference(&tag), tag_link);

        let digest = Reference::Digest(Digest::Sha256("digest".into()));
        let digest_link = LinkKind::Digest(Digest::Sha256("digest".into()));
        assert_eq!(LinkKind::from_reference(&digest), digest_link);
    }

    #[test]
    fn is_tracked_returns_true_for_layer() {
        assert!(LinkKind::Layer(sha("aabb")).is_tracked());
    }

    #[test]
    fn is_tracked_returns_true_for_config() {
        assert!(LinkKind::Config(sha("aabb")).is_tracked());
    }

    #[test]
    fn is_tracked_returns_true_for_manifest() {
        assert!(LinkKind::Manifest(sha("aabb"), sha("ccdd")).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_blob() {
        assert!(!LinkKind::Blob(sha("aabb")).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_tag() {
        assert!(!LinkKind::Tag("latest".to_string()).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_digest() {
        assert!(!LinkKind::Digest(sha("aabb")).is_tracked());
    }

    #[test]
    fn is_tracked_returns_false_for_referrer() {
        assert!(!LinkKind::Referrer(sha("aabb"), sha("ccdd")).is_tracked());
    }

    #[test]
    fn display_renders_expected_string_for_each_variant() {
        let cases = [
            (LinkKind::Tag("v1.0.0".to_string()), "tag:v1.0.0"),
            (LinkKind::Blob(sha("aabb")), "blob:sha256:aabb"),
            (LinkKind::Digest(sha("aabb")), "digest:sha256:aabb"),
            (LinkKind::Layer(sha("aabb")), "layer:sha256:aabb"),
            (LinkKind::Config(sha("aabb")), "config:sha256:aabb"),
            (
                LinkKind::Referrer(sha("aabb"), sha("ccdd")),
                "referrer:sha256:aabb-sha256:ccdd",
            ),
            (
                LinkKind::Manifest(sha("aabb"), sha("ccdd")),
                "manifest:sha256:aabb-sha256:ccdd",
            ),
        ];

        for (link, expected) in cases {
            assert_eq!(link.to_string(), expected);
        }
    }
}
