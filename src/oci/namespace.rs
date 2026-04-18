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

static NAMESPACE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$").unwrap()
});

#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct Namespace(String);

impl Namespace {
    pub fn new(s: impl Into<String>) -> Result<Self, Error> {
        let s = s.into();
        if NAMESPACE_RE.is_match(&s) {
            Ok(Self(s))
        } else {
            Err(Error::InvalidFormat(format!(
                "Invalid namespace format: '{s}'"
            )))
        }
    }
}

impl FromStr for Namespace {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for Namespace {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<&str> for Namespace {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Namespace {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for Namespace {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for Namespace {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Namespace {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Namespace> for str {
    fn eq(&self, other: &Namespace) -> bool {
        self == other.0
    }
}

impl PartialEq<Namespace> for &str {
    fn eq(&self, other: &Namespace) -> bool {
        *self == other.0
    }
}

impl Borrow<str> for Namespace {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_simple_namespace() {
        let ns = Namespace::new("library").unwrap();
        assert_eq!(ns.as_ref(), "library");
    }

    #[test]
    fn test_valid_nested_namespace() {
        let ns = Namespace::new("myrepo/app").unwrap();
        assert_eq!(ns.as_ref(), "myrepo/app");
    }

    #[test]
    fn test_valid_deeply_nested_namespace() {
        let ns = Namespace::new("org/team/project/app").unwrap();
        assert_eq!(ns.as_ref(), "org/team/project/app");
    }

    #[test]
    fn test_valid_with_special_chars() {
        let ns = Namespace::new("my-repo_v2.0").unwrap();
        assert_eq!(ns.as_ref(), "my-repo_v2.0");
    }

    #[test]
    fn test_invalid_uppercase() {
        assert!(Namespace::new("MyRepo").is_err());
    }

    #[test]
    fn test_invalid_empty() {
        assert!(Namespace::new("").is_err());
    }

    #[test]
    fn test_invalid_special_char_at_start() {
        assert!(Namespace::new("-repo").is_err());
        assert!(Namespace::new("_repo").is_err());
        assert!(Namespace::new(".repo").is_err());
    }

    #[test]
    fn test_invalid_double_slash() {
        assert!(Namespace::new("repo//app").is_err());
    }

    #[test]
    fn test_from_str() {
        let ns = Namespace::from_str("test-repo").unwrap();
        assert_eq!(ns.as_ref(), "test-repo");
    }

    #[test]
    fn test_display() {
        let ns = Namespace::new("test-repo").unwrap();
        assert_eq!(ns.to_string(), "test-repo");
    }

    #[test]
    fn test_as_ref() {
        let ns = Namespace::new("test-repo").unwrap();
        let s: &str = ns.as_ref();
        assert_eq!(s, "test-repo");
    }

    #[test]
    fn test_deref() {
        let ns = Namespace::new("test-repo").unwrap();
        assert_eq!(ns.len(), 9);
        assert!(ns.starts_with("test"));
    }

    #[test]
    fn test_serialize() {
        let ns = Namespace::new("test-repo").unwrap();
        let json = serde_json::to_string(&ns).unwrap();
        assert_eq!(json, "\"test-repo\"");
    }

    #[test]
    fn test_deserialize_valid() {
        let json = "\"test-repo\"";
        let ns: Namespace = serde_json::from_str(json).unwrap();
        assert_eq!(ns.as_ref(), "test-repo");
    }

    #[test]
    fn test_deserialize_invalid() {
        let json = "\"Invalid-Repo\"";
        let result: Result<Namespace, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_partial_eq_str() {
        let ns = Namespace::new("test-repo").unwrap();
        assert_eq!(ns, "test-repo");
        assert_eq!("test-repo", ns);
        assert_ne!(ns, "other-repo");
        assert_ne!("other-repo", ns);
    }

    #[test]
    fn test_partial_eq_ref_str() {
        let ns = Namespace::new("test-repo").unwrap();
        let s: &str = "test-repo";
        assert_eq!(ns, s);
        assert_eq!(s, ns);
    }

    #[test]
    fn test_valid_single_char_letter() {
        let ns = Namespace::new("a").unwrap();
        assert_eq!(ns.as_ref(), "a");
    }

    #[test]
    fn test_valid_single_char_digit() {
        let ns = Namespace::new("0").unwrap();
        assert_eq!(ns.as_ref(), "0");
    }

    #[test]
    fn test_invalid_single_char_uppercase() {
        assert!(Namespace::new("A").is_err());
    }

    #[test]
    fn test_valid_long_namespace() {
        let long = "a".repeat(10_000);
        assert!(Namespace::new(long).is_ok());
    }

    #[test]
    fn test_invalid_space() {
        assert!(Namespace::new("hello world").is_err());
    }

    #[test]
    fn test_invalid_at_symbol() {
        assert!(Namespace::new("user@host").is_err());
    }

    #[test]
    fn test_invalid_hash_symbol() {
        assert!(Namespace::new("repo#1").is_err());
    }

    #[test]
    fn test_invalid_leading_slash() {
        assert!(Namespace::new("/repo").is_err());
    }

    #[test]
    fn test_invalid_trailing_slash() {
        assert!(Namespace::new("repo/").is_err());
    }

    #[test]
    fn test_invalid_triple_slash() {
        assert!(Namespace::new("a///b").is_err());
    }

    #[test]
    fn test_invalid_unicode_accented() {
        assert!(Namespace::new("café").is_err());
    }

    #[test]
    fn test_invalid_unicode_cjk() {
        assert!(Namespace::new("日本").is_err());
    }

    #[test]
    fn test_try_from_str_ref() {
        let ns = Namespace::try_from("valid-repo").unwrap();
        assert_eq!(ns.as_ref(), "valid-repo");
        assert!(Namespace::try_from("INVALID").is_err());
    }

    #[test]
    fn test_try_from_string() {
        let ns = Namespace::try_from("valid-repo".to_string()).unwrap();
        assert_eq!(ns.as_ref(), "valid-repo");
        assert!(Namespace::try_from("INVALID".to_string()).is_err());
    }

    #[test]
    fn test_invalid_trailing_separator() {
        assert!(Namespace::new("repo-").is_err());
        assert!(Namespace::new("app.").is_err());
    }

    #[test]
    fn test_invalid_segment_starts_with_separator() {
        assert!(Namespace::new("a/-b").is_err());
        assert!(Namespace::new("a/_b").is_err());
        assert!(Namespace::new("a/.b").is_err());
    }
}
