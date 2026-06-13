use std::num::NonZeroUsize;

use serde::Deserialize;

use crate::{
    configuration::RegexPattern, registry_client::RegistryClientConfig,
    replication::ReplicationMode,
};

/// Parse-time DTO for one per-repository replication downstream; the runtime
/// counterpart is [`crate::replication::ReplicationDownstream`].
#[derive(Clone, Debug, Deserialize)]
pub struct ReplicationDownstreamConfig {
    /// Local identifier for this downstream (appears in logs and metrics).
    pub name: String,
    /// HTTP/TLS/auth fields shared with an upstream registry, flattened so they
    /// parse from the same TOML table; carries the mTLS-pairing validation.
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
    use std::num::NonZeroUsize;

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

        assert_eq!(config.name, "eu-region");
        assert_eq!(config.client.url, "https://angos-eu.example.com");
        assert_eq!(config.client.max_redirect, 5);
        assert_eq!(config.mode, ReplicationMode::EventReconcile);
        assert!(config.namespace_filter.is_empty());
        assert_eq!(config.max_concurrent_pushes, None);
        assert!(!config.prune, "prune defaults to false");
    }

    #[test]
    fn downstream_config_full_parses() {
        let config: ReplicationDownstreamConfig = toml::from_str(
            r#"
            name = "eu-region"
            url = "https://angos-eu.example.com"
            username = "replicator"
            password = "s3cret"
            mode = "event-only"
            namespace_filter = ["^nginx/.*"]
            max_concurrent_pushes = 8
            prune = true
            "#,
        )
        .unwrap();

        assert_eq!(config.mode, ReplicationMode::EventOnly);
        assert_eq!(config.namespace_filter.len(), 1);
        assert_eq!(config.namespace_filter[0].as_source(), "^nginx/.*");
        assert_eq!(config.max_concurrent_pushes, NonZeroUsize::new(8));
        assert_eq!(config.client.username.as_deref(), Some("replicator"));
        assert_eq!(config.client.password.as_ref().unwrap().expose(), "s3cret");
        assert!(config.prune);
    }

    #[test]
    fn downstream_config_rejects_partial_mtls() {
        let result: Result<ReplicationDownstreamConfig, _> = toml::from_str(
            r#"
            name = "eu-region"
            url = "https://angos-eu.example.com"
            client_certificate = "cert.pem"
            "#,
        );
        assert!(result.is_err(), "partial mTLS pairing must be rejected");
    }

    #[test]
    fn downstream_config_rejects_zero_max_concurrent_pushes() {
        let result: Result<ReplicationDownstreamConfig, _> = toml::from_str(
            r#"
            name = "eu-region"
            url = "https://angos-eu.example.com"
            max_concurrent_pushes = 0
            "#,
        );
        assert!(
            result.is_err(),
            "max_concurrent_pushes = 0 must be rejected at parse time"
        );
    }
}
