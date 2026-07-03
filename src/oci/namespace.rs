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

/// OCI/Docker repository-name cap (Docker `reference.NameTotalLengthMax`).
const MAX_NAMESPACE_LENGTH: usize = 255;

#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct Namespace(String);

impl Namespace {
    pub fn new(s: &str) -> Result<Self, Error> {
        if s.len() <= MAX_NAMESPACE_LENGTH && NAMESPACE_RE.is_match(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::InvalidNamespace(s.to_string()))
        }
    }

    /// Remove the leading whole-segment `prefix`, returning the sub-namespace,
    /// which is still a valid namespace by construction. Errors when `prefix` is
    /// not a segment prefix or matches exactly (which would leave nothing).
    pub fn strip_prefix(&self, prefix: &Namespace) -> Result<Namespace, Error> {
        match self
            .0
            .strip_prefix(prefix.0.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            Some(sub) if !sub.is_empty() => Ok(Namespace(sub.to_owned())),
            _ => Err(Error::InvalidNamespace(format!(
                "'{}' is not a prefix of '{}'",
                prefix.0, self.0
            ))),
        }
    }

    /// Nest this namespace under `prefix`, yielding `prefix/self`. Joining two
    /// valid namespaces always yields valid grammar, so only the total length
    /// cap is checked here, keeping the request path off the full regex.
    pub fn prepend(&self, prefix: &Namespace) -> Result<Namespace, Error> {
        let joined = format!("{}/{}", prefix.0, self.0);
        if joined.len() <= MAX_NAMESPACE_LENGTH {
            Ok(Namespace(joined))
        } else {
            Err(Error::InvalidNamespace(joined))
        }
    }

    /// Map this local namespace to its remote form: drop the `local_namespace`
    /// prefix when set, then nest under `target_namespace` when set. At the
    /// repository root (namespace equals `local_namespace`) the remote is
    /// `target_namespace` alone, or this namespace verbatim when no target is set.
    pub fn remote(
        &self,
        local_namespace: Option<&Namespace>,
        target_namespace: Option<&Namespace>,
    ) -> Result<Namespace, Error> {
        let bare = match local_namespace {
            Some(local_namespace) if self == local_namespace => None,
            Some(local_namespace) => Some(self.strip_prefix(local_namespace)?),
            None => Some(self.clone()),
        };
        Ok(match (bare, target_namespace) {
            (Some(bare), Some(target_namespace)) => bare.prepend(target_namespace)?,
            (Some(bare), None) => bare,
            (None, Some(target_namespace)) => target_namespace.clone(),
            (None, None) => self.clone(),
        })
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
        if s.len() <= MAX_NAMESPACE_LENGTH && NAMESPACE_RE.is_match(&s) {
            Ok(Self(s))
        } else {
            Err(Error::InvalidNamespace(s))
        }
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
        Self::try_from(s).map_err(serde::de::Error::custom)
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

/// Returns `true` when `namespace` belongs to the configured `repository_name`,
/// either as an exact match or as a direct sub-namespace of the form
/// `{repository_name}/...`.
///
/// The `/` separator check is intentional: without it, `"myrepo2"` would
/// spuriously match `"myrepo"` via `starts_with`.
pub fn namespace_belongs_to(namespace: &str, repository_name: &str) -> bool {
    namespace == repository_name
        || namespace
            .strip_prefix(repository_name)
            .is_some_and(|rest| rest.starts_with('/'))
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
    fn test_namespace_length_cap() {
        assert!(Namespace::new(&"a".repeat(MAX_NAMESPACE_LENGTH)).is_ok());
        assert!(Namespace::new(&"a".repeat(MAX_NAMESPACE_LENGTH + 1)).is_err());
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

    #[test]
    fn strip_prefix_removes_a_leading_segment() {
        assert_eq!(
            Namespace::new("repo/app")
                .unwrap()
                .strip_prefix(&Namespace::new("repo").unwrap())
                .unwrap(),
            "app"
        );
    }

    #[test]
    fn strip_prefix_keeps_deeper_sub_namespaces() {
        assert_eq!(
            Namespace::new("repo/team/app")
                .unwrap()
                .strip_prefix(&Namespace::new("repo").unwrap())
                .unwrap(),
            "team/app"
        );
    }

    #[test]
    fn strip_prefix_respects_segment_boundaries() {
        // `repo` is not a whole-segment prefix of `repo2/app`, so it errors.
        assert!(
            Namespace::new("repo2/app")
                .unwrap()
                .strip_prefix(&Namespace::new("repo").unwrap())
                .is_err()
        );
    }

    #[test]
    fn strip_prefix_exact_match_errors() {
        assert!(
            Namespace::new("repo")
                .unwrap()
                .strip_prefix(&Namespace::new("repo").unwrap())
                .is_err()
        );
    }

    #[test]
    fn prepend_nests_under_the_prefix() {
        assert_eq!(
            Namespace::new("app")
                .unwrap()
                .prepend(&Namespace::new("mirror").unwrap())
                .unwrap(),
            "mirror/app"
        );
    }

    #[test]
    fn strip_then_prepend_remaps_the_namespace() {
        let repo = Namespace::new("repo").unwrap();
        let mirror = Namespace::new("mirror").unwrap();
        let ns = Namespace::new("repo/app").unwrap();
        assert_eq!(
            ns.strip_prefix(&repo).unwrap().prepend(&mirror).unwrap(),
            "mirror/app"
        );
    }

    #[test]
    fn remote_passes_through_verbatim() {
        assert_eq!(
            Namespace::new("repo/app")
                .unwrap()
                .remote(None, None)
                .unwrap(),
            "repo/app"
        );
    }

    #[test]
    fn remote_strips_repository_prefix() {
        let repo = Namespace::new("repo").unwrap();
        assert_eq!(
            Namespace::new("repo/app")
                .unwrap()
                .remote(Some(&repo), None)
                .unwrap(),
            "app"
        );
    }

    #[test]
    fn remote_strips_then_prepends() {
        let repo = Namespace::new("repo").unwrap();
        let mirror = Namespace::new("mirror").unwrap();
        assert_eq!(
            Namespace::new("repo/app")
                .unwrap()
                .remote(Some(&repo), Some(&mirror))
                .unwrap(),
            "mirror/app"
        );
    }

    #[test]
    fn remote_keeps_deeper_sub_namespaces() {
        let repo = Namespace::new("repo").unwrap();
        let mirror = Namespace::new("mirror").unwrap();
        assert_eq!(
            Namespace::new("repo/team/app")
                .unwrap()
                .remote(Some(&repo), Some(&mirror))
                .unwrap(),
            "mirror/team/app"
        );
    }

    #[test]
    fn remote_root_maps_to_prefix() {
        // The bug fix: a push at the repository root (namespace == strip) must
        // map to the bare URL-path prefix, not error.
        let repo = Namespace::new("repo").unwrap();
        let mirror = Namespace::new("mirror").unwrap();
        assert_eq!(
            Namespace::new("repo")
                .unwrap()
                .remote(Some(&repo), Some(&mirror))
                .unwrap(),
            "mirror"
        );
    }

    #[test]
    fn remote_root_without_prefix_is_verbatim() {
        let repo = Namespace::new("repo").unwrap();
        assert_eq!(
            Namespace::new("repo")
                .unwrap()
                .remote(Some(&repo), None)
                .unwrap(),
            "repo"
        );
    }

    #[test]
    fn remote_errors_on_non_prefix() {
        let repo = Namespace::new("repo").unwrap();
        let mirror = Namespace::new("mirror").unwrap();
        assert!(
            Namespace::new("other/x")
                .unwrap()
                .remote(Some(&repo), Some(&mirror))
                .is_err()
        );
    }

    #[test]
    fn remote_errors_when_mapped_namespace_exceeds_length_cap() {
        let local = Namespace::new("repo").unwrap();
        let target = Namespace::new(&"t".repeat(MAX_NAMESPACE_LENGTH - 10)).unwrap();
        let sub = "a".repeat(20);
        let ns = Namespace::new(&format!("repo/{sub}")).unwrap();
        assert!(ns.remote(Some(&local), Some(&target)).is_err());
    }

    #[test]
    fn test_namespace_belongs_to_exact_match() {
        assert!(namespace_belongs_to("myrepo", "myrepo"));
    }

    #[test]
    fn test_namespace_belongs_to_prefix_match() {
        assert!(namespace_belongs_to("myrepo/sub", "myrepo"));
        assert!(namespace_belongs_to("myrepo/sub/path", "myrepo"));
    }

    #[test]
    fn test_namespace_belongs_to_distinct_namespace() {
        assert!(!namespace_belongs_to("other", "myrepo"));
        assert!(!namespace_belongs_to("other/sub", "myrepo"));
    }

    #[test]
    fn test_namespace_belongs_to_no_trailing_slash_without_sub() {
        assert!(!namespace_belongs_to("myrepo", "myrepo/"));
        assert!(!namespace_belongs_to("myrepo2", "myrepo"));
        assert!(!namespace_belongs_to("myrepo", "myrepo2"));
    }
}
