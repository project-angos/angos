use std::sync::Arc;

use serde::Deserialize;

use crate::{
    auth::webhook,
    configuration::RegexPattern,
    policy::{AccessPolicyConfig, RetentionPolicyConfig},
};

/// Parsed shape of `[global]` before webhook name references are resolved.
#[derive(Clone, Debug, Deserialize)]
pub struct RawGlobalConfig {
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

impl Default for RawGlobalConfig {
    fn default() -> Self {
        RawGlobalConfig {
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

/// Resolved global configuration with webhook references replaced by their definitions.
#[derive(Clone, Debug)]
pub struct GlobalConfig {
    pub max_concurrent_requests: usize,
    pub max_concurrent_cache_jobs: usize,
    pub update_pull_time: bool,
    pub enable_redirect: Option<bool>,
    pub enable_blob_redirect: Option<bool>,
    pub enable_manifest_redirect: Option<bool>,
    pub access_policy: AccessPolicyConfig,
    pub retention_policy: RetentionPolicyConfig,
    pub immutable_tags: bool,
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    pub authorization_webhook: Option<Arc<webhook::Config>>,
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
