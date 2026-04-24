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

use cel_interpreter::{Context, Value};
use chrono::Utc;
use serde::{Deserialize, Serialize, Serializer};
use tracing::{debug, warn};

use super::{CelRule, Error};

/// Configuration for retention policies.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct RetentionPolicyConfig {
    #[serde(default)]
    pub rules: Vec<CelRule>,
}

/// Seconds since the Unix epoch, guaranteed non-negative.
///
/// Constructed from `i64` values returned by `chrono::DateTime::timestamp()`.
/// Negative inputs (pre-epoch dates) are saturated to zero.
///
/// Serializes as `i64` so that CEL expressions using integer arithmetic
/// (e.g. `image.pushed_at > now() - days(30)`) continue to work without
/// cross-type coercion.
#[derive(Clone, Copy, Debug, Default)]
pub struct EpochSeconds(u64);

impl EpochSeconds {
    /// Constructs an `EpochSeconds` from a signed timestamp.
    ///
    /// Negative values (pre-epoch) are saturated to zero.
    pub fn from_seconds(s: i64) -> Self {
        Self(u64::try_from(s).unwrap_or(0))
    }
}

impl Serialize for EpochSeconds {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(i64::try_from(self.0).unwrap_or(i64::MAX))
    }
}

/// Manifest image information used in retention decisions.
#[derive(Debug, Default, Serialize)]
pub struct ManifestImage {
    pub tag: Option<String>,
    pub pushed_at: EpochSeconds,
    pub last_pulled_at: EpochSeconds,
}

/// Retention policy engine.
///
/// Evaluates CEL expressions to determine if manifests should be retained.
/// Rules are pre-compiled at configuration time for better performance.
pub struct RetentionPolicy {
    rules: Vec<CelRule>,
}

impl RetentionPolicy {
    /// Creates a new retention policy from configuration.
    ///
    /// Rules are already compiled; this constructor is infallible.
    pub fn new(config: &RetentionPolicyConfig) -> Self {
        Self {
            rules: config.rules.clone(),
        }
    }

    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Evaluates whether a manifest should be retained.
    ///
    /// Rules are evaluated in order; the first matching rule (returning `true`) causes the
    /// manifest to be kept. If no rule matches, the manifest is eligible for deletion.
    ///
    /// # Fail-open semantics (data safety)
    ///
    /// Retention policies are **fail-open**: unexpected rule outcomes default to keeping the
    /// manifest. This is the safer choice — it is better to retain a manifest that should have
    /// been deleted than to silently delete one that should have been kept.
    ///
    /// | Outcome                        | Behaviour          | Log level |
    /// |--------------------------------|--------------------|-----------|
    /// | `bool(true)`                   | retain             | `debug`   |
    /// | `bool(false)`                  | continue to next rule | —      |
    /// | non-boolean value (misconfiguration) | retain (fail-open) | `warn` |
    /// | evaluation error               | retain (fail-open) | `warn`   |
    /// | no rules matched               | delete             | —         |
    ///
    /// # Arguments
    /// * `manifest` - The manifest image information
    /// * `last_pushed` - List of recently pushed tags (most recent first)
    /// * `last_pulled` - List of recently pulled tags (most recent first)
    ///
    /// # Returns
    /// * `Ok(true)` if the manifest should be retained
    /// * `Ok(false)` if the manifest can be deleted
    /// * `Err` if context construction fails (rule execution errors are handled internally)
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

        for (index, rule) in self.rules.iter().enumerate() {
            let rule_index = index + 1;
            match rule.execute(&context) {
                Ok(Value::Bool(true)) => {
                    debug!("Retention rule {rule_index} matched");
                    return Ok(true);
                }
                Ok(Value::Bool(false)) => {}
                Ok(value) => {
                    warn!(
                        "Retention rule {rule_index} returned non-boolean value: {value:?}; \
                         treating as 'retain' (fail-open)"
                    );
                    return Ok(true);
                }
                Err(e) => {
                    warn!(
                        "Retention rule {rule_index} evaluation failed: {e}; \
                         treating as 'retain' (fail-open)"
                    );
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
            .map_err(|e| Error::Evaluation(e.to_string()))?;

        context.add_function("now", || Utc::now().timestamp());
        context.add_function("days", |d: i64| d * 86400);
        context.add_function("hours", |h: i64| h * 3600);
        context.add_function("minutes", |m: i64| m * 60);

        context.add_function(
            "top_pushed",
            Self::build_top_fn(manifest.tag.clone(), last_pushed.to_vec()),
        );
        context.add_function(
            "top_pulled",
            Self::build_top_fn(manifest.tag.clone(), last_pulled.to_vec()),
        );

        Ok(context)
    }

    fn build_top_fn(tag: Option<String>, list: Vec<String>) -> impl Fn(i64) -> bool + Send + Sync {
        move |count: i64| {
            let Some(ref tag) = tag else {
                return false;
            };
            let limit = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
            list.iter().take(limit).any(|t| t == tag)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(s: &str) -> CelRule {
        CelRule::compile(s).unwrap()
    }

    #[test]
    fn test_top_pushed_in_top_n() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("top_pushed(3)")],
        });

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
            rules: vec![rule("top_pushed(2)")],
        });

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
            rules: vec![rule("top_pushed(10)")],
        });

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
            rules: vec![rule("top_pulled(2)")],
        });

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
            rules: vec![rule("top_pulled(1)")],
        });

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
            rules: vec![rule("image.pushed_at > now() - days(1)")],
        });

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: EpochSeconds::from_seconds(Utc::now().timestamp()),
            ..Default::default()
        };

        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_pushed_at_old() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("image.pushed_at > now() - days(1)")],
        });

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: EpochSeconds::from_seconds(Utc::now().timestamp() - 2 * 86400),
            ..Default::default()
        };

        assert!(!policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_last_pulled_at_recent() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("image.last_pulled_at > now() - hours(1)")],
        });

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            last_pulled_at: EpochSeconds::from_seconds(Utc::now().timestamp()),
            ..Default::default()
        };

        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_last_pulled_at_old() {
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("image.last_pulled_at > now() - hours(1)")],
        });

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            last_pulled_at: EpochSeconds::from_seconds(Utc::now().timestamp() - 2 * 3600),
            ..Default::default()
        };

        assert!(!policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn negative_timestamp_saturates_to_zero() {
        let t = EpochSeconds::from_seconds(-100);
        let serialized = serde_json::to_value(t).unwrap();
        assert_eq!(serialized, serde_json::json!(0));
    }

    #[test]
    fn non_boolean_rule_retains_fail_open() {
        // A rule that returns an integer instead of a bool is a misconfiguration.
        // The evaluator must treat this as "retain" (fail-open) without panicking.
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("42")],
        });
        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn non_boolean_rule_string_retains_fail_open() {
        // A rule that returns a string instead of a bool.
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("'keep'")],
        });
        let manifest = ManifestImage::default();
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn failed_rule_evaluation_retains_fail_open() {
        // A rule that references an unbound variable fails at execution time
        // (not compile time).  The evaluator must treat this as "retain" (fail-open).
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("nonexistent_var")],
        });
        let manifest = ManifestImage::default();
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn non_boolean_rule_does_not_shadow_later_rules() {
        // When rule 1 returns a non-bool, the evaluator returns retain immediately
        // (fail-open), so rule 2 is never reached.  Both orderings should retain.
        let policy = RetentionPolicy::new(&RetentionPolicyConfig {
            rules: vec![rule("42"), rule("false")],
        });
        let manifest = ManifestImage::default();
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }
}
