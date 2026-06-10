use std::sync::Arc;

use regex::Regex;
use serde::Deserialize;

use crate::{registry_client::RegistryClient, replication::Error};

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

/// Runtime representation of one replication downstream.
///
/// Holds only resolved fields (an `Arc<RegistryClient>`, compiled regexes,
/// scalars), with no `*Config` field. Constructed exclusively via
/// [`ReplicationDownstream::builder`].
#[derive(Debug)]
pub struct ReplicationDownstream {
    pub name: String,
    pub registry_client: Arc<RegistryClient>,
    pub mode: ReplicationMode,
    pub namespace_filter: Vec<Regex>,
    pub max_concurrent_pushes: usize,
    /// When `true`, scrub reconciliation deletes tags present on this downstream
    /// but absent locally (local is authoritative). Only safe for a one-way
    /// mirror; unsafe for an active-active peer. Defaults to `false`.
    pub prune: bool,
}

impl ReplicationDownstream {
    /// Starts building a downstream from individual resolved fields.
    #[must_use]
    pub fn builder() -> ReplicationDownstreamBuilder {
        ReplicationDownstreamBuilder::default()
    }

    /// Returns `true` when `namespace` passes this downstream's filter.
    ///
    /// An empty filter matches every namespace; otherwise the namespace must
    /// match at least one pattern.
    #[must_use]
    pub fn matches_namespace(&self, namespace: &str) -> bool {
        self.namespace_filter.is_empty()
            || self
                .namespace_filter
                .iter()
                .any(|pattern| pattern.is_match(namespace))
    }

    /// True when a live mutation in `namespace` enqueues an event push to this
    /// downstream: the exact per-downstream condition `dispatch_replication`
    /// selects on.
    #[must_use]
    pub fn enqueues_for(&self, namespace: &str) -> bool {
        self.mode.enqueues_on_event() && self.matches_namespace(namespace)
    }
}

/// Builder for [`ReplicationDownstream`] taking individual resolved fields.
///
/// `name`, `registry_client` and `max_concurrent_pushes` are required; `mode`
/// defaults to [`ReplicationMode::EventReconcile`], `namespace_filter` defaults
/// to empty (match-all), and `prune` defaults to `false`.
#[derive(Default)]
pub struct ReplicationDownstreamBuilder {
    name: Option<String>,
    registry_client: Option<Arc<RegistryClient>>,
    mode: Option<ReplicationMode>,
    namespace_filter: Option<Vec<Regex>>,
    max_concurrent_pushes: Option<usize>,
    prune: bool,
}

impl ReplicationDownstreamBuilder {
    /// Local identifier of the downstream (required).
    #[must_use]
    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Pre-built registry client for the downstream registry (required).
    #[must_use]
    pub fn registry_client(mut self, registry_client: Arc<RegistryClient>) -> Self {
        self.registry_client = Some(registry_client);
        self
    }

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

    /// Maximum concurrent blob pushes for one manifest (required).
    #[must_use]
    pub fn max_concurrent_pushes(mut self, max_concurrent_pushes: usize) -> Self {
        self.max_concurrent_pushes = Some(max_concurrent_pushes);
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
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when a required field is missing.
    pub fn build(self) -> Result<ReplicationDownstream, Error> {
        let name = self.name.ok_or_else(|| {
            Error::Initialization("replication downstream builder requires a name".into())
        })?;
        let registry_client = self.registry_client.ok_or_else(|| {
            Error::Initialization(
                "replication downstream builder requires a registry_client".into(),
            )
        })?;
        let max_concurrent_pushes = self.max_concurrent_pushes.ok_or_else(|| {
            Error::Initialization(
                "replication downstream builder requires max_concurrent_pushes".into(),
            )
        })?;

        Ok(ReplicationDownstream {
            name,
            registry_client,
            mode: self.mode.unwrap_or_default(),
            namespace_filter: self.namespace_filter.unwrap_or_default(),
            max_concurrent_pushes,
            prune: self.prune,
        })
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
        replication::{Error, ReplicationDownstream, ReplicationMode},
    };

    fn test_client() -> Arc<RegistryClient> {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let client = RegistryClient::builder()
            .url("https://example.test".to_string())
            .client(reqwest::Client::new())
            .cache(cache)
            .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .build()
            .unwrap();
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
        let downstream = ReplicationDownstream::builder()
            .name("eu-region".to_string())
            .registry_client(test_client())
            .max_concurrent_pushes(4)
            .build()
            .unwrap();

        assert_eq!(downstream.name, "eu-region");
        assert_eq!(downstream.mode, ReplicationMode::EventReconcile);
        assert!(downstream.namespace_filter.is_empty());
        assert_eq!(downstream.max_concurrent_pushes, 4);
    }

    #[test]
    fn builder_requires_name() {
        let result = ReplicationDownstream::builder()
            .registry_client(test_client())
            .max_concurrent_pushes(4)
            .build();
        assert!(matches!(result, Err(Error::Initialization(_))));
    }

    #[test]
    fn matches_namespace_empty_filter_matches_all() {
        let downstream = ReplicationDownstream::builder()
            .name("d".to_string())
            .registry_client(test_client())
            .max_concurrent_pushes(1)
            .build()
            .unwrap();
        assert!(downstream.matches_namespace("anything/at-all"));
    }

    #[test]
    fn matches_namespace_honours_filter() {
        let downstream = ReplicationDownstream::builder()
            .name("d".to_string())
            .registry_client(test_client())
            .namespace_filter(vec![Regex::new("^nginx/.*").unwrap()])
            .max_concurrent_pushes(1)
            .build()
            .unwrap();
        assert!(downstream.matches_namespace("nginx/foo"));
        assert!(!downstream.matches_namespace("redis/bar"));
    }
}
