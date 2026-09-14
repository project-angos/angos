use std::sync::Arc;

use regex::Regex;
use serde::Deserialize;

use angos_oci::{Error, Namespace};

use crate::registry_client::RegistryClient;

/// Whether a downstream participates in the event-driven push path, the scrub
/// reconciliation path, or both.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Push on local mutations and include in scrub reconciliation.
    #[default]
    #[serde(rename = "event+reconcile")]
    EventReconcile,
    /// Push only on local mutations; excluded from scrub reconciliation.
    #[serde(rename = "event-only")]
    EventOnly,
    /// Excluded from the event path; mirrored only via scrub reconciliation.
    #[serde(rename = "reconcile-only")]
    ReconcileOnly,
}

impl ReplicationMode {
    /// Whether the event-driven path enqueues pushes for this downstream.
    #[must_use]
    pub fn enqueues_on_event(self) -> bool {
        matches!(self, Self::EventReconcile | Self::EventOnly)
    }

    /// Whether the scrub reconciliation checker includes this downstream.
    #[must_use]
    pub fn participates_in_reconcile(self) -> bool {
        matches!(self, Self::EventReconcile | Self::ReconcileOnly)
    }
}

/// Runtime representation of one replication downstream, holding only
/// resolved fields.
#[derive(Debug)]
pub struct ReplicationDownstream {
    pub name: String,
    pub registry_client: Arc<RegistryClient>,
    pub mode: ReplicationMode,
    pub namespace_filter: Vec<Regex>,
    pub max_concurrent_pushes: usize,
    /// When `true`, scrub reconciliation deletes tags present on this downstream
    /// but absent locally. Only safe for a one-way mirror, not an active-active peer.
    pub prune: bool,
    /// Local repository namespace to strip before mapping to the downstream.
    /// `None` for a bare-host downstream (verbatim mirror).
    pub local_namespace: Option<Namespace>,
    /// Target namespace to prepend after stripping. `None` for a bare-host
    /// downstream (verbatim mirror). Applied via [`Namespace::remote`].
    pub target_namespace: Option<Namespace>,
}

impl ReplicationDownstream {
    /// A test downstream with the defaults: event+reconcile mode, a match-all
    /// filter, no prune and a verbatim namespace mapping.
    #[cfg(test)]
    #[must_use]
    pub fn new(
        name: String,
        registry_client: Arc<RegistryClient>,
        max_concurrent_pushes: usize,
    ) -> Self {
        Self {
            name,
            registry_client,
            mode: ReplicationMode::default(),
            namespace_filter: Vec::new(),
            max_concurrent_pushes,
            prune: false,
            local_namespace: None,
            target_namespace: None,
        }
    }

    /// Returns `true` when `namespace` passes this downstream's filter; an
    /// empty filter matches everything.
    #[must_use]
    pub fn matches_namespace(&self, namespace: &str) -> bool {
        self.namespace_filter.is_empty()
            || self
                .namespace_filter
                .iter()
                .any(|pattern| pattern.is_match(namespace))
    }

    /// True when a live mutation in `namespace` enqueues an event push to this
    /// downstream; the exact condition `dispatch_replication` selects on.
    #[must_use]
    pub fn enqueues_for(&self, namespace: &str) -> bool {
        self.mode.enqueues_on_event() && self.matches_namespace(namespace)
    }

    /// Maps `namespace` to its downstream form via [`Namespace::remote`]. For a
    /// routed namespace the strip always succeeds, so an `Err` is effectively
    /// unreachable but callers still handle it.
    pub fn remote(&self, namespace: &Namespace) -> Result<Namespace, Error> {
        namespace.remote(
            self.local_namespace.as_ref(),
            self.target_namespace.as_ref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use regex::Regex;

    use crate::{
        registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        registry_client::RegistryClient,
        replication::{ReplicationDownstream, ReplicationMode},
    };

    fn test_client() -> Arc<RegistryClient> {
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
        Arc::new(RegistryClient::new(
            "https://example.test".to_string(),
            reqwest::Client::new(),
            None,
            cache,
            DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        ))
    }

    #[test]
    fn mode_gating() {
        assert!(ReplicationMode::EventReconcile.enqueues_on_event());
        assert!(ReplicationMode::EventReconcile.participates_in_reconcile());
        assert!(ReplicationMode::EventOnly.enqueues_on_event());
        assert!(!ReplicationMode::EventOnly.participates_in_reconcile());
        assert!(!ReplicationMode::ReconcileOnly.enqueues_on_event());
        assert!(ReplicationMode::ReconcileOnly.participates_in_reconcile());
    }

    #[test]
    fn new_applies_defaults() {
        let downstream = ReplicationDownstream::new("eu-region".to_string(), test_client(), 4);

        assert_eq!(downstream.name, "eu-region");
        assert_eq!(downstream.mode, ReplicationMode::EventReconcile);
        assert!(downstream.namespace_filter.is_empty());
        assert_eq!(downstream.max_concurrent_pushes, 4);
    }

    #[test]
    fn matches_namespace_empty_filter_matches_all() {
        let downstream = ReplicationDownstream::new("d".to_string(), test_client(), 1);
        assert!(downstream.matches_namespace("anything/at-all"));
    }

    #[test]
    fn matches_namespace_honours_filter() {
        let downstream = ReplicationDownstream {
            namespace_filter: vec![Regex::new("^nginx/.*").unwrap()],
            ..ReplicationDownstream::new("d".to_string(), test_client(), 1)
        };
        assert!(downstream.matches_namespace("nginx/foo"));
        assert!(!downstream.matches_namespace("redis/bar"));
    }
}
