use crate::types::Error;

/// One `/`-separated component of the distribution-spec repository-name
/// grammar, `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*`. A separator is a dot, one or
/// two underscores, or a run of dashes, and always sits between alphanumerics.
fn is_path_component(component: &str) -> bool {
    let is_alphanumeric = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let bytes = component.as_bytes();
    bytes.first().is_some_and(is_alphanumeric)
        && bytes.last().is_some_and(is_alphanumeric)
        && component
            .split(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            .all(|separator| {
                matches!(separator, "" | "." | "_" | "__") || separator.bytes().all(|b| b == b'-')
            })
}

/// OCI/Docker repository-name cap (Docker `reference.NameTotalLengthMax`).
const MAX_NAMESPACE_LENGTH: usize = 255;

#[derive(Debug, Clone, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct Namespace(String);

impl Namespace {
    /// # Errors
    ///
    /// Returns an error when `s` breaks the namespace grammar or exceeds the
    /// length cap.
    pub fn new(s: &str) -> Result<Self, Error> {
        if s.len() <= MAX_NAMESPACE_LENGTH && s.split('/').all(is_path_component) {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::Namespace(s.to_string()))
        }
    }

    /// Remove the leading whole-segment `prefix`, returning the sub-namespace,
    /// which is still a valid namespace by construction. Errors when `prefix` is
    /// not a segment prefix or matches exactly (which would leave nothing).
    ///
    /// # Errors
    ///
    /// Returns an error when `prefix` is not a whole-segment prefix of this
    /// namespace, or is the whole of it.
    pub fn strip_prefix(&self, prefix: &Namespace) -> Result<Namespace, Error> {
        match self
            .0
            .strip_prefix(prefix.0.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            Some(sub) if !sub.is_empty() => Ok(Namespace(sub.to_owned())),
            _ => Err(Error::Namespace(format!(
                "'{}' is not a prefix of '{}'",
                prefix.0, self.0
            ))),
        }
    }

    /// Nest this namespace under `prefix`, yielding `prefix/self`. Joining two
    /// valid namespaces always yields valid grammar, so only the total length
    /// cap is checked here, keeping the request path off the full grammar.
    ///
    /// # Errors
    ///
    /// Returns an error when the joined namespace exceeds the length cap.
    pub fn prepend(&self, prefix: &Namespace) -> Result<Namespace, Error> {
        let joined = format!("{}/{}", prefix.0, self.0);
        if joined.len() <= MAX_NAMESPACE_LENGTH {
            Ok(Namespace(joined))
        } else {
            Err(Error::Namespace(joined))
        }
    }

    /// Map this local namespace to its remote form: drop the `local_namespace`
    /// prefix when set, then nest under `target_namespace` when set. At the
    /// repository root (namespace equals `local_namespace`) the remote is
    /// `target_namespace` alone, or this namespace verbatim when no target is set.
    ///
    /// # Errors
    ///
    /// Returns an error when `local_namespace` is not a prefix of this namespace,
    /// or when nesting under `target_namespace` exceeds the length cap.
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

str_newtype!(Namespace);

/// Returns `true` when `namespace` belongs to the configured `repository_name`,
/// either as an exact match or as a direct sub-namespace of the form
/// `{repository_name}/...`.
///
/// The `/` separator check is intentional: without it, `"myrepo2"` would
/// spuriously match `"myrepo"` via `starts_with`.
#[must_use]
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
    fn test_deserialize_invalid() {
        let json = "\"Invalid-Repo\"";
        let result: Result<Namespace, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_namespace_length_cap() {
        assert!(Namespace::new(&"a".repeat(MAX_NAMESPACE_LENGTH)).is_ok());
        assert!(Namespace::new(&"a".repeat(MAX_NAMESPACE_LENGTH + 1)).is_err());
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

    /// The distribution-spec name grammar: lowercase alphanumeric components
    /// joined by `/`, each component's runs separated by `.`, `_`, `__`, or a
    /// run of dashes.
    #[test]
    fn accepts_the_spec_name_grammar() {
        for name in [
            // Components, from one character to a deep path.
            "a",
            "0",
            "library",
            "myrepo/app",
            "org/team/project/app",
            // Every separator the grammar lists, single and doubled.
            "a.b",
            "a_b",
            "a-b",
            "a__b",
            "a--b",
            "a---b",
            "ubuntu--test",
            "my__repo/sub--component",
            "my-repo_v2.0",
        ] {
            assert!(
                Namespace::new(name).is_ok(),
                "{name} is legal under the distribution-spec name grammar"
            );
        }
    }

    /// A component is lowercase ASCII alphanumeric, and a separator must be one
    /// of the four the grammar lists sitting between two alphanumerics.
    #[test]
    fn rejects_what_the_spec_name_grammar_forbids() {
        // Empty, uppercase, and non-ASCII.
        let outside_the_alphabet = ["", "MyRepo", "A", "café", "日本"];
        // Characters the grammar does not list at all.
        let foreign_characters = ["hello world", "user@host", "repo#1"];
        // A separator with nothing on one side of it.
        let dangling_separators = [
            "-repo", "_repo", ".repo", "repo-", "app.", "a/-b", "a/_b", "a/.b", "-ab", "ab-",
            "_ab", "ab_",
        ];
        // Separator runs the grammar does not admit.
        let unlisted_separators = ["a___b", "a._b", "a..b", "a-_b"];
        // Component boundaries.
        let empty_components = ["/repo", "repo/", "repo//app", "a///b", "a//b"];

        let illegal_names = outside_the_alphabet
            .into_iter()
            .chain(foreign_characters)
            .chain(dangling_separators)
            .chain(unlisted_separators)
            .chain(empty_components);
        for name in illegal_names {
            assert!(
                Namespace::new(name).is_err(),
                "{name} is not legal under the distribution-spec name grammar"
            );
        }
    }
}
