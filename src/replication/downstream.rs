use std::sync::Arc;

use regex::Regex;
use serde::Deserialize;

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

/// Runtime representation of one replication downstream. Holds only resolved
/// fields and is constructed exclusively via [`ReplicationDownstream::builder`].
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
}

impl ReplicationDownstream {
    /// Starts building a downstream from individual resolved fields. The local
    /// `name`, the pre-built `registry_client` for the downstream registry and
    /// `max_concurrent_pushes` are required; `mode`, `namespace_filter` and
    /// `prune` are optional fluent setters on the returned builder.
    #[must_use]
    pub fn builder(
        name: String,
        registry_client: Arc<RegistryClient>,
        max_concurrent_pushes: usize,
    ) -> ReplicationDownstreamBuilder {
        ReplicationDownstreamBuilder {
            name,
            registry_client,
            mode: None,
            namespace_filter: None,
            max_concurrent_pushes,
            prune: false,
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
}

/// Builder for [`ReplicationDownstream`]. `name`, `registry_client` and
/// `max_concurrent_pushes` are required and supplied to
/// [`ReplicationDownstream::builder`]; the rest default.
pub struct ReplicationDownstreamBuilder {
    name: String,
    registry_client: Arc<RegistryClient>,
    mode: Option<ReplicationMode>,
    namespace_filter: Option<Vec<Regex>>,
    max_concurrent_pushes: usize,
    prune: bool,
}

impl ReplicationDownstreamBuilder {
    /// Replication mode (defaults to [`ReplicationMode::EventReconcile`]).
    #[must_use]
    pub fn mode(mut self, mode: ReplicationMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Compiled namespace filter (defaults to empty = match-all).
    #[must_use]
    pub fn namespace_filter(mut self, namespace_filter: Vec<Regex>) -> Self {
        self.namespace_filter = Some(namespace_filter);
        self
    }

    /// Whether reconciliation may delete downstream-only tags (defaults to
    /// `false`; only enable for a one-way mirror).
    #[must_use]
    pub fn prune(mut self, prune: bool) -> Self {
        self.prune = prune;
        self
    }

    /// Builds the [`ReplicationDownstream`].
    #[must_use]
    pub fn build(self) -> ReplicationDownstream {
        ReplicationDownstream {
            name: self.name,
            registry_client: self.registry_client,
            mode: self.mode.unwrap_or_default(),
            namespace_filter: self.namespace_filter.unwrap_or_default(),
            max_concurrent_pushes: self.max_concurrent_pushes,
            prune: self.prune,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use regex::Regex;

    use crate::{
        cache,
        registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
        registry_client::RegistryClient,
        replication::{ReplicationDownstream, ReplicationMode},
    };

    fn test_client() -> Arc<RegistryClient> {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let client = RegistryClient::builder(
            "https://example.test".to_string(),
            reqwest::Client::new(),
            cache,
        )
        .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
        .build();
        Arc::new(client)
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
    fn builder_applies_defaults() {
        let downstream =
            ReplicationDownstream::builder("eu-region".to_string(), test_client(), 4).build();

        assert_eq!(downstream.name, "eu-region");
        assert_eq!(downstream.mode, ReplicationMode::EventReconcile);
        assert!(downstream.namespace_filter.is_empty());
        assert_eq!(downstream.max_concurrent_pushes, 4);
    }

    #[test]
    fn matches_namespace_empty_filter_matches_all() {
        let downstream = ReplicationDownstream::builder("d".to_string(), test_client(), 1).build();
        assert!(downstream.matches_namespace("anything/at-all"));
    }

    #[test]
    fn matches_namespace_honours_filter() {
        let downstream = ReplicationDownstream::builder("d".to_string(), test_client(), 1)
            .namespace_filter(vec![Regex::new("^nginx/.*").unwrap()])
            .build();
        assert!(downstream.matches_namespace("nginx/foo"));
        assert!(!downstream.matches_namespace("redis/bar"));
    }
}
