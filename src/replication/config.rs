use std::num::NonZeroUsize;

use serde::Deserialize;

use angos_oci_client::RegistryClientConfig;

use crate::configuration::RegexPattern;
use crate::replication::ReplicationMode;

/// Parse-time DTO; the runtime counterpart is [`crate::replication::ReplicationDownstream`].
#[derive(Clone, Debug, Deserialize)]
pub struct ReplicationDownstreamConfig {
    /// Local identifier for this downstream (appears in logs and metrics).
    pub name: String,
    /// A path on the `url` (`http://host:8000/team`) replaces the source
    /// repository prefix on the downstream (`<repo>/x` -> `team/x`); a bare-host
    /// `url` mirrors the namespace verbatim.
    #[serde(flatten)]
    pub client: RegistryClientConfig,
    #[serde(default)]
    pub mode: ReplicationMode,
    /// Namespace patterns this downstream replicates (empty = all).
    #[serde(default)]
    pub namespace_filter: Vec<RegexPattern>,
    pub max_concurrent_pushes: Option<NonZeroUsize>,
    /// When `true`, scrub reconciliation also deletes tags present downstream
    /// but absent locally. Only safe for a one-way mirror; on an active-active
    /// peer it would delete a newer tag that has not yet replicated back.
    #[serde(default)]
    pub prune: bool,
}

#[cfg(test)]
mod tests {
    use crate::replication::{ReplicationDownstreamConfig, ReplicationMode};

    #[test]
    fn downstream_config_minimal_parses_with_defaults() {
        let config: ReplicationDownstreamConfig = toml::from_str(
            r#"
            name = "eu-region"
            url = "https://angos-eu.example.com"
            "#,
        )
        .unwrap();

        assert_eq!(config.mode, ReplicationMode::EventReconcile);
        assert!(config.namespace_filter.is_empty());
        assert_eq!(config.max_concurrent_pushes, None);
        assert!(!config.prune);
    }
}
