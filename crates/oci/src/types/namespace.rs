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

    /// `prefix/self`; joining two valid namespaces only risks the length cap.
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

    /// The remote form: strip `local_namespace`, then nest under
    /// `target_namespace`. The repository root maps to the target alone, or to
    /// itself without one.
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
            Some(local_namespace) => {
                let sub = self
                    .0
                    .strip_prefix(local_namespace.0.as_str())
                    .and_then(|rest| rest.strip_prefix('/'))
                    .ok_or_else(|| {
                        Error::Namespace(format!(
                            "'{}' is not a prefix of '{}'",
                            local_namespace.0, self.0
                        ))
                    })?;
                Some(Namespace(sub.to_owned()))
            }
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

/// Whether `namespace` is `repository_name` or under it; `myrepo2` is not
/// under `myrepo`.
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
    fn test_namespace_length_cap() {
        assert!(Namespace::new(&"a".repeat(MAX_NAMESPACE_LENGTH)).is_ok());
        assert!(Namespace::new(&"a".repeat(MAX_NAMESPACE_LENGTH + 1)).is_err());
    }

    fn ns(s: &str) -> Namespace {
        Namespace::new(s).unwrap()
    }

    #[test]
    fn remote_maps_every_local_and_target_combination() {
        let (repo, mirror) = (ns("repo"), ns("mirror"));
        for (namespace, local, target, expected) in [
            ("repo/app", None, None, "repo/app"),
            ("repo/app", Some(&repo), None, "app"),
            (
                "repo/team/app",
                Some(&repo),
                Some(&mirror),
                "mirror/team/app",
            ),
            // The repository root maps to the bare target.
            ("repo", Some(&repo), Some(&mirror), "mirror"),
            ("repo", Some(&repo), None, "repo"),
        ] {
            assert_eq!(
                ns(namespace).remote(local, target).unwrap(),
                expected,
                "{namespace}"
            );
        }
        assert!(ns("other/x").remote(Some(&repo), Some(&mirror)).is_err());
        // Only a whole-segment prefix strips.
        assert!(ns("repo2/app").remote(Some(&repo), None).is_err());
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
    fn test_namespace_belongs_to() {
        assert!(namespace_belongs_to("myrepo", "myrepo"));
        assert!(namespace_belongs_to("myrepo/sub/path", "myrepo"));
        for namespace in ["other", "other/sub", "myrepo2"] {
            assert!(!namespace_belongs_to(namespace, "myrepo"), "{namespace}");
        }
        assert!(!namespace_belongs_to("myrepo", "myrepo/"));
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
