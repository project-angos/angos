use serde::Deserialize;

use crate::{
    configuration::RegexPattern,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
};

#[derive(Clone, Debug, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default = "default_max_concurrent_cache_jobs")]
    pub max_concurrent_cache_jobs: usize,
    #[serde(default = "default_update_pull_time")]
    pub update_pull_time: bool,
    #[serde(default)]
    pub enable_redirect: Option<bool>,
    #[serde(default)]
    pub enable_blob_redirect: Option<bool>,
    #[serde(default)]
    pub enable_manifest_redirect: Option<bool>,
    #[serde(default)]
    pub access_policy: AccessPolicyConfig,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
}

fn default_max_concurrent_requests() -> usize {
    64
}

fn default_max_concurrent_cache_jobs() -> usize {
    4
}

fn default_update_pull_time() -> bool {
    false
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            max_concurrent_requests: default_max_concurrent_requests(),
            max_concurrent_cache_jobs: default_max_concurrent_cache_jobs(),
            update_pull_time: default_update_pull_time(),
            enable_redirect: None,
            enable_blob_redirect: None,
            enable_manifest_redirect: None,
            access_policy: AccessPolicyConfig::default(),
            retention_policy: RetentionPolicyConfig::default(),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
            authorization_webhook: None,
            event_webhooks: Vec::new(),
        }
    }
}

impl GlobalConfig {
    pub fn resolved_enable_blob_redirect(&self) -> bool {
        self.enable_blob_redirect
            .or(self.enable_redirect)
            .unwrap_or(true)
    }

    pub fn resolved_enable_manifest_redirect(&self) -> bool {
        self.enable_manifest_redirect
            .or(self.enable_redirect)
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::GlobalConfig;

    #[test]
    fn default_values_match_configuration_defaults() {
        let config = GlobalConfig::default();

        assert_eq!(config.max_concurrent_requests, 64);
        assert_eq!(config.max_concurrent_cache_jobs, 4);
        assert!(!config.update_pull_time);
        assert!(!config.immutable_tags);
        assert!(config.immutable_tags_exclusions.is_empty());
        assert!(config.authorization_webhook.is_none());
    }

    #[test]
    fn custom_values_parse() {
        let config = toml::from_str::<GlobalConfig>(
            r#"
            max_concurrent_requests = 10
            max_concurrent_cache_jobs = 8
            update_pull_time = true
            immutable_tags = true
            immutable_tags_exclusions = ["latest", "dev"]
            authorization_webhook = "my-webhook"
            "#,
        )
        .unwrap();

        assert_eq!(config.max_concurrent_requests, 10);
        assert_eq!(config.max_concurrent_cache_jobs, 8);
        assert!(config.update_pull_time);
        assert!(config.immutable_tags);
        assert_eq!(config.immutable_tags_exclusions.len(), 2);
        assert_eq!(config.immutable_tags_exclusions[0].as_source(), "latest");
        assert_eq!(config.immutable_tags_exclusions[1].as_source(), "dev");
        assert_eq!(config.authorization_webhook.as_deref(), Some("my-webhook"));
    }
}
