use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::oci::{Digest, Reference};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum LinkKind {
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
}

impl Display for LinkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkKind::Tag(s) => write!(f, "tag:{s}"),
            LinkKind::Digest(d) => write!(f, "digest:{d}"),
            LinkKind::Layer(d) => write!(f, "layer:{d}"),
            LinkKind::Config(d) => write!(f, "config:{d}"),
            LinkKind::Referrer(l, r) => write!(f, "referrer:{l}-{r}"),
            LinkKind::Manifest(index, child) => write!(f, "manifest:{index}-{child}"),
        }
    }
}

impl From<Reference> for LinkKind {
    fn from(r: Reference) -> Self {
        match r {
            Reference::Tag(s) => LinkKind::Tag(s),
            Reference::Digest(d) => LinkKind::Digest(d),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oci::Reference;

    #[test]
    fn test_from_reference() {
        let tag = Reference::Tag("tag".to_string());
        let tag_link = LinkKind::Tag("tag".to_string());
        assert_eq!(LinkKind::from(tag), tag_link);

        let digest = Reference::Digest(Digest::Sha256("digest".into()));
        let digest_link = LinkKind::Digest(Digest::Sha256("digest".into()));
        assert_eq!(LinkKind::from(digest), digest_link);
    }
}
