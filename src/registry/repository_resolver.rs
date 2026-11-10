//! Namespace-to-repository resolution. Overlapping prefixes (`team` and
//! `team/app`) fail at startup rather than resolving by `HashMap` order.

use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use angos_oci::namespace_belongs_to;

use crate::registry::Repository;

/// A repository map that rejects overlapping repository prefixes at
/// construction and provides deterministic namespace resolution.
pub struct RepositoryResolver {
    inner: Arc<HashMap<String, Repository>>,
}

impl fmt::Debug for RepositoryResolver {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepositoryResolver")
            .field("repositories", &self.inner.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Two repositories cannot be told apart: their prefixes overlap, or they
/// claim the same `ns`.
#[derive(Debug, thiserror::Error)]
pub enum OverlapError {
    #[error("repository prefixes overlap: '{first}' and '{second}'")]
    Prefix { first: String, second: String },
    #[error("repositories '{first}' and '{second}' both mirror the namespace '{namespace}'")]
    Namespace {
        first: String,
        second: String,
        namespace: String,
    },
}

impl RepositoryResolver {
    /// # Errors
    ///
    /// Returns [`OverlapError`] for the first pair of repositories found that
    /// cannot be told apart.
    pub fn new(repositories: Arc<HashMap<String, Repository>>) -> Result<Self, OverlapError> {
        // Each key paired with the `ns` it mirrors, so the checks below need no
        // lookup back into the map.
        let entries: Vec<(&str, Option<&str>)> = repositories
            .iter()
            .map(|(key, repository)| (key.as_str(), repository.namespace.as_deref()))
            .collect();
        for i in 0..entries.len() {
            for j in (i + 1)..entries.len() {
                let (a, a_mirrors) = entries[i];
                let (b, b_mirrors) = entries[j];
                if namespace_belongs_to(a, b) || namespace_belongs_to(b, a) {
                    return Err(OverlapError::Prefix {
                        first: a.to_string(),
                        second: b.to_string(),
                    });
                }
                // A `?ns=` value must resolve to one repository, so two
                // claiming the same one is a misconfiguration, not a race.
                if let (Some(left), Some(right)) = (a_mirrors, b_mirrors)
                    && left == right
                {
                    return Err(OverlapError::Namespace {
                        first: a.to_string(),
                        second: b.to_string(),
                        namespace: left.to_string(),
                    });
                }
            }
        }
        Ok(Self {
            inner: repositories,
        })
    }

    /// Resolves the repository mirroring `ns`, the value of a request's proxy
    /// `?ns=` parameter. `None` when no repository claims it, which leaves the
    /// parameter without effect. Matched case-insensitively, `ns` being a host.
    pub fn resolve_ns(&self, ns: &str) -> Option<&Repository> {
        self.inner.values().find(|repository| {
            repository
                .namespace
                .as_deref()
                .is_some_and(|declared| declared.eq_ignore_ascii_case(ns))
        })
    }

    /// The repository whose key is a namespace-prefix of `namespace`; at most
    /// one matches, since overlapping prefixes are rejected at construction.
    pub fn resolve(&self, namespace: &str) -> Option<&Repository> {
        self.inner
            .iter()
            .find(|(key, _)| namespace_belongs_to(namespace, key))
            .map(|(_, repo)| repo)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(String::as_str)
    }

    pub fn values(&self) -> impl Iterator<Item = &Repository> {
        self.inner.values()
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.inner.contains_key(name)
    }

    pub fn get(&self, name: &str) -> Option<&Repository> {
        self.inner.get(name)
    }
}

#[cfg(test)]
mod tests {
    use angos_oci::Namespace;

    use super::*;
    use crate::policy::RetentionPolicy;

    fn repo(name: &str) -> Repository {
        Repository {
            name: Namespace::new(name).unwrap(),
            namespace: None,
            upstreams: Vec::new(),
            replication: Vec::new(),
            retention_policy: RetentionPolicy::new(Vec::new()),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            scan: None,
            index: None,
        }
    }

    fn resolver(pairs: &[(&str, &str)]) -> Result<RepositoryResolver, OverlapError> {
        let mut map = HashMap::new();
        for (key, name) in pairs {
            map.insert(key.to_string(), repo(name));
        }
        RepositoryResolver::new(Arc::new(map))
    }

    #[test]
    fn overlapping_shallow_and_nested_rejected() {
        let result = resolver(&[("team", "team"), ("team/app", "team/app")]);
        let msg = result
            .expect_err("overlapping prefixes team and team/app must be rejected")
            .to_string();
        assert!(
            msg.contains("team") && msg.contains("team/app"),
            "error message must name both prefixes: {msg}"
        );
    }

    /// A `?ns=` must name one repository, so two claiming the same registry
    /// namespace fail at startup rather than resolving by map order.
    #[test]
    fn repositories_mirroring_the_same_namespace_rejected() {
        let mut mirror = repo("docker-hub");
        mirror.namespace = Some("docker.io".to_string());
        let mut second = repo("hub-mirror");
        second.namespace = Some("docker.io".to_string());

        let repositories = Arc::new(HashMap::from([
            ("docker-hub".to_string(), mirror),
            ("hub-mirror".to_string(), second),
        ]));
        let error = RepositoryResolver::new(repositories)
            .expect_err("two repositories mirroring docker.io must be refused");

        let message = error.to_string();
        assert!(
            message.contains("docker.io"),
            "the error must name the namespace both claim: {message}"
        );
    }

    /// `ns` names a host, which is case-insensitive, so a client spelling it
    /// differently still reaches the mirror.
    #[test]
    fn resolve_ns_finds_the_mirroring_repository() {
        let mut mirror = repo("docker-hub");
        mirror.namespace = Some("docker.io".to_string());
        let repositories = Arc::new(HashMap::from([
            ("docker-hub".to_string(), mirror),
            ("internal".to_string(), repo("internal")),
        ]));
        let resolver = RepositoryResolver::new(repositories).unwrap();

        for ns in ["docker.io", "Docker.IO", "DOCKER.IO"] {
            assert_eq!(
                resolver.resolve_ns(ns).map(|r| r.name.to_string()),
                Some("docker-hub".to_string()),
                "'{ns}' names the same host"
            );
        }
        assert!(resolver.resolve_ns("quay.io").is_none());
    }

    #[test]
    fn sibling_prefixes_with_shared_root_accepted() {
        // "team/backend" and "team/frontend" share the "team" root but neither
        // is a namespace-prefix of the other.
        let result = resolver(&[("team/backend", "backend"), ("team/frontend", "frontend")]);
        assert!(
            result.is_ok(),
            "team/backend and team/frontend must be accepted"
        );
    }

    #[test]
    fn resolve_matches_whole_namespace_segments_only() {
        let r = resolver(&[("myrepo", "myrepo"), ("other", "other")]).unwrap();
        let resolved = |namespace| r.resolve(namespace).map(|repo| repo.name.to_string());
        assert_eq!(resolved("myrepo").as_deref(), Some("myrepo"));
        assert_eq!(resolved("myrepo/sub/path").as_deref(), Some("myrepo"));
        assert_eq!(resolved("myrepo2"), None);
        assert_eq!(resolved("completely-different"), None);
    }
}
