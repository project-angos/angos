//! Retention policy evaluation for manifest cleanup.
//!
//! This module provides CEL-based retention policies for automatic manifest cleanup.
//! Policies are pre-compiled at configuration load time for performance.
//!
//! # Policy Evaluation
//!
//! Retention policies determine which manifests should be kept. If any rule
//! matches, the manifest is retained. Otherwise, it is eligible for deletion.
//!
//! # Available Variables
//!
//! CEL expressions have access to:
//! - `image`: Manifest information (`tag`, `pushed_at`, `last_pulled_at`)
//!
//! # Helper Functions
//!
//! - `now()`: Current timestamp in seconds since epoch
//! - `days(n)`: Convert days to seconds
//! - `hours(n)`: Convert hours to seconds
//! - `minutes(n)`: Convert minutes to seconds
//! - `top_pushed(n)`: Check if tag is in top N most recently pushed
//! - `top_pulled(n)`: Check if tag is in top N most recently pulled

use cel_interpreter::{Context, Program, Value};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{configuration::Error, registry::cel};

/// Configuration for retention policies.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct RetentionPolicyConfig {
    #[serde(default)]
    pub rules: Vec<String>,
}

/// Manifest image information used in retention decisions.
#[derive(Debug, Default, Serialize)]
pub struct ManifestImage {
    pub tag: Option<String>,
    pub pushed_at: i64,
    pub last_pulled_at: i64,
}

/// Retention policy engine.
///
/// Evaluates CEL expressions to determine if manifests should be retained.
/// Rules are pre-compiled at configuration time for better performance.
pub struct RetentionPolicy {
    rules: Vec<Program>,
}

impl RetentionPolicy {
    /// Creates a new retention policy from configuration.
    ///
    /// Compiles CEL expressions from the configuration into programs.
    pub fn new(config: &RetentionPolicyConfig) -> Result<Self, Error> {
        let rules =
            cel::compile_rules(&config.rules, "retention policy").map_err(Error::Initialization)?;

        Ok(Self { rules })
    }

    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Evaluates whether a manifest should be retained.
    ///
    /// # Arguments
    /// * `manifest` - The manifest image information
    /// * `last_pushed` - List of recently pushed tags (most recent first)
    /// * `last_pulled` - List of recently pulled tags (most recent first)
    ///
    /// # Returns
    /// * `Ok(true)` if the manifest should be retained
    /// * `Ok(false)` if the manifest can be deleted
    /// * `Err` if policy evaluation fails
    pub fn should_retain(
        &self,
        manifest: &ManifestImage,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<bool, Error> {
        if self.rules.is_empty() {
            return Ok(true);
        }

        let context = Self::build_context(manifest, last_pushed, last_pulled)?;

        for rule in &self.rules {
            match rule.execute(&context) {
                Ok(Value::Bool(true)) => {
                    debug!("Retention rule matched");
                    return Ok(true);
                }
                Ok(Value::Bool(false)) => {}
                _ => {
                    debug!("Retention rule did not evaluate to a boolean, retaining");
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn build_context<'a>(
        manifest: &'a ManifestImage,
        last_pushed: &'a [String],
        last_pulled: &'a [String],
    ) -> Result<Context<'a>, Error> {
        let mut context = Context::default();

        context
            .add_variable("image", manifest)
            .map_err(|e| Error::Initialization(e.to_string()))?;

        context.add_function("now", || Utc::now().timestamp());
        context.add_function("days", |d: i64| d * 86400);
        context.add_function("hours", |h: i64| h * 3600);
        context.add_function("minutes", |m: i64| m * 60);

        let tag_for_pushed = manifest.tag.clone();
        let pushed_list: Vec<String> = last_pushed.to_vec();
        context.add_function("top_pushed", move |count: i64| {
            let Some(ref tag) = tag_for_pushed else {
                return false;
            };
            let limit = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
            pushed_list.iter().take(limit).any(|t| t == tag)
        });

        let tag_for_pulled = manifest.tag.clone();
        let pulled_list: Vec<String> = last_pulled.to_vec();
        context.add_function("top_pulled", move |count: i64| {
            let Some(ref tag) = tag_for_pulled else {
                return false;
            };
            let limit = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
            pulled_list.iter().take(limit).any(|t| t == tag)
        });

        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_pushed_in_top_n() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["top_pushed(3)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            ..Default::default()
        };
        let last_pushed = vec!["v3".to_string(), "v2".to_string(), "v1".to_string()];

        assert!(policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    #[test]
    fn test_top_pushed_not_in_top_n() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["top_pushed(2)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let last_pushed = vec!["v3".to_string(), "v2".to_string(), "v1".to_string()];

        assert!(!policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    #[test]
    fn test_top_pushed_orphan_manifest() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["top_pushed(10)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: None,
            ..Default::default()
        };
        let last_pushed = vec!["v1".to_string()];

        assert!(!policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    #[test]
    fn test_top_pulled_in_top_n() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["top_pulled(2)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let last_pulled = vec!["v1".to_string(), "v2".to_string()];

        assert!(policy.should_retain(&manifest, &[], &last_pulled).unwrap());
    }

    #[test]
    fn test_top_pulled_not_in_top_n() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["top_pulled(1)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            ..Default::default()
        };
        let last_pulled = vec!["v1".to_string(), "v2".to_string()];

        assert!(!policy.should_retain(&manifest, &[], &last_pulled).unwrap());
    }

    #[test]
    fn test_pushed_at_recent() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["image.pushed_at > now() - days(1)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: Utc::now().timestamp(),
            ..Default::default()
        };

        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_pushed_at_old() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["image.pushed_at > now() - days(1)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: Utc::now().timestamp() - 2 * 86400,
            ..Default::default()
        };

        assert!(!policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_last_pulled_at_recent() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["image.last_pulled_at > now() - hours(1)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            last_pulled_at: Utc::now().timestamp(),
            ..Default::default()
        };

        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_last_pulled_at_old() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec!["image.last_pulled_at > now() - hours(1)".to_string()],
        })
        .unwrap();

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            last_pulled_at: Utc::now().timestamp() - 2 * 3600,
            ..Default::default()
        };

        assert!(!policy.should_retain(&manifest, &[], &[]).unwrap());
    }
}
