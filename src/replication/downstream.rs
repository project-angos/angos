use std::num::NonZeroUsize;
use std::sync::Arc;

use regex::Regex;
use serde::Deserialize;

use angos_oci::{Error, Namespace};

use angos_oci_client::RegistryClient;

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
    #[must_use]
    pub fn enqueues_on_event(self) -> bool {
        matches!(self, Self::EventReconcile | Self::EventOnly)
    }

    #[must_use]
    pub fn participates_in_reconcile(self) -> bool {
        matches!(self, Self::EventReconcile | Self::ReconcileOnly)
    }
}

#[derive(Debug)]
pub struct ReplicationDownstream {
    pub name: String,
    pub registry_client: Arc<RegistryClient>,
    pub mode: ReplicationMode,
    pub namespace_filter: Vec<Regex>,
    pub max_concurrent_pushes: NonZeroUsize,
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
    /// An empty filter matches everything.
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

    /// The strip always succeeds for a routed namespace, so `Err` is effectively unreachable.
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

    use angos_cache::Config as CacheConfig;
    use angos_oci_client::RegistryClient;
    use reqwest::Client;

    use crate::registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES;
    use crate::registry::test_utils::test_downstream;
    use crate::replication::{ReplicationDownstream, ReplicationMode};

    fn test_client() -> Arc<RegistryClient> {
        let cache = CacheConfig::Memory.to_backend().unwrap();
        Arc::new(RegistryClient::new(
            "https://example.test".to_string(),
            Client::new(),
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
    fn matches_namespace_empty_filter_matches_all() {
        let downstream = test_downstream("d".to_string(), test_client());
        assert!(downstream.matches_namespace("anything/at-all"));
    }

    #[test]
    fn matches_namespace_honours_filter() {
        let downstream = ReplicationDownstream {
            namespace_filter: vec![Regex::new("^nginx/.*").unwrap()],
            ..test_downstream("d".to_string(), test_client())
        };
        assert!(downstream.matches_namespace("nginx/foo"));
        assert!(!downstream.matches_namespace("redis/bar"));
    }
}
