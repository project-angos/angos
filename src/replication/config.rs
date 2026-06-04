use regex::Regex;
use serde::Deserialize;

use crate::{
    configuration::RegexPattern, registry_client::RegistryClientConfig,
    replication::ReplicationMode, secret::Secret,
};

/// Parse-time DTO for one per-repository replication downstream.
///
/// Shaped after [`crate::registry_client::RegistryClientConfig`] and reusing its
/// mTLS-pairing `try_from` validation: `client_certificate` and
/// `client_private_key` must be supplied together. This type is parse-only; the
/// runtime counterpart is [`crate::replication::ReplicationDownstream`], built
/// from these fields via its builder.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "ReplicationDownstreamConfigFields")]
pub struct ReplicationDownstreamConfig {
    /// Local identifier for this downstream (appears in logs and metrics).
    pub name: String,
    pub url: String,
    pub max_redirect: u8,
    pub server_ca_bundle: Option<String>,
    /// Note: named `client_certificate` (without `_bundle`) to match the
    /// existing upstream config key.
    pub client_certificate: Option<String>,
    pub client_private_key: Option<String>,
    pub username: Option<String>,
    pub password: Option<Secret<String>>,
    pub mode: ReplicationMode,
    /// Compiled at parse time from the `RegexPattern` strings in the raw fields;
    /// internal state only, so it holds bare `Regex` rather than the
    /// serialize-friendly `RegexPattern` wrapper.
    pub namespace_filter: Vec<Regex>,
    pub max_concurrent_pushes: Option<usize>,
    /// When `true`, scrub reconciliation also deletes tags present on this
    /// downstream but absent locally (treats local as authoritative). Only safe
    /// for a one-way mirror that nothing else writes to; leave `false` for an
    /// active-active peer, where it would delete a peer's legitimately-newer tag
    /// that has not yet replicated back. Defaults to `false`.
    pub prune: bool,
}

#[derive(Deserialize)]
struct ReplicationDownstreamConfigFields {
    name: String,
    url: String,
    #[serde(default = "ReplicationDownstreamConfig::default_max_redirect")]
    max_redirect: u8,
    server_ca_bundle: Option<String>,
    client_certificate: Option<String>,
    client_private_key: Option<String>,
    username: Option<String>,
    password: Option<Secret<String>>,
    #[serde(default)]
    mode: ReplicationMode,
    #[serde(default)]
    namespace_filter: Vec<RegexPattern>,
    max_concurrent_pushes: Option<usize>,
    #[serde(default)]
    prune: bool,
}

impl TryFrom<ReplicationDownstreamConfigFields> for ReplicationDownstreamConfig {
    type Error = String;

    fn try_from(fields: ReplicationDownstreamConfigFields) -> Result<Self, Self::Error> {
        if fields.client_certificate.is_some() != fields.client_private_key.is_some() {
            return Err(
                "both client_certificate and client_private_key are required for mTLS".to_string(),
            );
        }
        Ok(Self {
            name: fields.name,
            url: fields.url,
            max_redirect: fields.max_redirect,
            server_ca_bundle: fields.server_ca_bundle,
            client_certificate: fields.client_certificate,
            client_private_key: fields.client_private_key,
            username: fields.username,
            password: fields.password,
            mode: fields.mode,
            namespace_filter: fields
                .namespace_filter
                .into_iter()
                .map(RegexPattern::into_regex)
                .collect(),
            max_concurrent_pushes: fields.max_concurrent_pushes,
            prune: fields.prune,
        })
    }
}

impl ReplicationDownstreamConfig {
    fn default_max_redirect() -> u8 {
        5
    }

    /// Projects the HTTP/TLS/auth fields shared with an upstream registry into a
    /// [`RegistryClientConfig`], so the downstream's [`crate::registry_client::RegistryClient`]
    /// can be resolved through the same code path as upstreams.
    ///
    /// The mTLS pairing has already been validated by this type's `try_from`.
    #[must_use]
    pub fn to_registry_client_config(&self) -> RegistryClientConfig {
        RegistryClientConfig {
            url: self.url.clone(),
            max_redirect: self.max_redirect,
            server_ca_bundle: self.server_ca_bundle.clone(),
            client_certificate: self.client_certificate.clone(),
            client_private_key: self.client_private_key.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
        }
    }
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

        assert_eq!(config.name, "eu-region");
        assert_eq!(config.url, "https://angos-eu.example.com");
        assert_eq!(config.max_redirect, 5);
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
        assert_eq!(config.namespace_filter[0].as_str(), "^nginx/.*");
        assert_eq!(config.max_concurrent_pushes, Some(8));
        assert_eq!(config.username.as_deref(), Some("replicator"));
        assert_eq!(config.password.as_ref().unwrap().expose(), "s3cret");
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
}
