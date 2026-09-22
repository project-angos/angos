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
//! - `image`: Manifest information (`tag`, `pushed_at`, `last_pulled_at`,
//!   `scanned_at`)
//!
//! # Helper Functions
//!
//! - `now()`: Current timestamp in seconds since epoch
//! - `days(n)`: Convert days to seconds
//! - `hours(n)`: Convert hours to seconds
//! - `minutes(n)`: Convert minutes to seconds
//! - `top_pushed(n)`: Check if tag is in top N most recently pushed
//! - `top_pulled(n)`: Check if tag is in top N most recently pulled

use std::sync::Arc;

use cel_interpreter::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::policy::{CelRule, Error, RuleOutcome, clock::Clock, evaluate_rules};

/// Configuration for retention policies.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct RetentionPolicyConfig {
    #[serde(default)]
    pub rules: Vec<CelRule>,
}

/// Whether any of `rules` reads pull-time data, `image.last_pulled_at` or
/// `top_pulled`, which is only recorded when `update_pull_time` is enabled.
pub fn rules_use_pull_time(rules: &[CelRule]) -> bool {
    rules
        .iter()
        .any(|rule| rule.source.contains("last_pulled_at") || rule.source.contains("top_pulled"))
}

/// Manifest image information used in retention decisions.
#[derive(Debug, Default, Serialize)]
pub struct ManifestImage {
    pub tag: Option<String>,
    /// Seconds since the Unix epoch, pre-epoch dates saturated to zero.
    pub pushed_at: i64,
    /// Seconds since the Unix epoch, `0` for an image never pulled.
    pub last_pulled_at: i64,
    /// Seconds since the Unix epoch of the newest scan report, `0` for an
    /// image never scanned. Set by `reconcile scan`; retention reads it as `0`.
    pub scanned_at: i64,
}

impl ManifestImage {
    /// Builds the CEL-facing image from the optional timestamps a link carries.
    /// An unknown push time becomes `now`, so an age rule keeps content of
    /// unknown age instead of deleting it as if pushed at the epoch; an unknown
    /// pull time reaches CEL as `0`.
    #[must_use]
    pub fn new(
        tag: Option<String>,
        pushed_at: Option<DateTime<Utc>>,
        last_pulled_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            tag,
            pushed_at: pushed_at.unwrap_or(now).timestamp().max(0),
            last_pulled_at: last_pulled_at.map_or(0, |t| t.timestamp().max(0)),
            scanned_at: 0,
        }
    }
}

/// Retention policy engine.
///
/// Evaluates CEL expressions to determine if manifests should be retained.
/// Rules are pre-compiled at configuration time for better performance.
pub struct RetentionPolicy {
    rules: Vec<CelRule>,
    clock: Arc<dyn Clock>,
}

impl RetentionPolicy {
    /// Creates a new retention policy from configuration.
    ///
    /// Rules are already compiled; this constructor is infallible.
    pub fn new(config: &RetentionPolicyConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            rules: config.rules.clone(),
            clock,
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
    /// manifest. This is the safer choice: it is better to retain a manifest that should have
    /// been deleted than to silently delete one that should have been kept.
    ///
    /// | Outcome                        | Behaviour          | Log level |
    /// |--------------------------------|--------------------|-----------|
    /// | `bool(true)`                   | retain             | `debug`   |
    /// | `bool(false)`                  | continue to next rule | none   |
    /// | non-boolean value (misconfiguration) | retain (fail-open) | `warn` |
    /// | evaluation error               | retain (fail-open) | `warn`   |
    /// | no rules matched               | delete             | none      |
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

        match self.evaluate(manifest, last_pushed, last_pulled)? {
            RuleOutcome::Matched(index) => {
                debug!("Retention rule {index} matched");
                Ok(true)
            }
            RuleOutcome::NoMatch => Ok(false),
            RuleOutcome::Indeterminate { index, message } => {
                // Fail-open: a misconfigured or failing rule retains (data safety).
                warn!(
                    "Retention rule {index} is indeterminate: {message}; \
                     treating as 'retain' (fail-open)"
                );
                Ok(true)
            }
        }
    }

    /// Runs the rules over `manifest` and answers the raw outcome, for a
    /// caller whose reading of an indeterminate rule differs from retention's.
    pub fn evaluate(
        &self,
        manifest: &ManifestImage,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<RuleOutcome, Error> {
        let context = self.build_context(manifest, last_pushed, last_pulled)?;
        Ok(evaluate_rules(&self.rules, &context))
    }

    fn build_context<'a>(
        &self,
        manifest: &'a ManifestImage,
        last_pushed: &'a [String],
        last_pulled: &'a [String],
    ) -> Result<Context<'a>, Error> {
        let mut context = Context::default();

        context.add_variable("image", manifest)?;

        let clock_for_now = self.clock.clone();
        context.add_function("now", move || clock_for_now.now().timestamp());
        context.add_function("days", days);
        context.add_function("hours", hours);
        context.add_function("minutes", minutes);

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

    /// Builds a `top_pushed`/`top_pulled` CEL function over a tag ranking.
    /// Untagged subjects never match: this is what lets a count-only policy
    /// reclaim the revisions its own tag deletions orphan.
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

fn days(d: i64) -> i64 {
    d.saturating_mul(86400)
}

fn hours(h: i64) -> i64 {
    h.saturating_mul(3600)
}

fn minutes(m: i64) -> i64 {
    m.saturating_mul(60)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::{super::clock::SystemClock, *};

    struct FixedClock(DateTime<Utc>);

    impl Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    /// Builds a policy over compiled CEL rule sources, driven by `clock`.
    fn policy_with_clock(rules: &[&str], clock: Arc<dyn Clock>) -> RetentionPolicy {
        let rules = rules
            .iter()
            .map(|source| CelRule::compile(source).unwrap())
            .collect();
        RetentionPolicy::new(&RetentionPolicyConfig { rules }, clock)
    }

    /// Builds a policy over CEL rule sources, driven by the system clock.
    fn policy(rules: &[&str]) -> RetentionPolicy {
        policy_with_clock(rules, Arc::new(SystemClock))
    }

    /// Builds a policy over CEL rule sources, driven by a clock frozen at `now`.
    fn policy_at(now: DateTime<Utc>, rules: &[&str]) -> RetentionPolicy {
        policy_with_clock(rules, Arc::new(FixedClock(now)))
    }

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    #[test]
    fn test_top_pushed_in_top_n() {
        let policy = policy(&["top_pushed(3)"]);

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            ..Default::default()
        };
        let last_pushed = vec!["v3".to_string(), "v2".to_string(), "v1".to_string()];

        assert!(policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    #[test]
    fn test_top_pushed_not_in_top_n() {
        let policy = policy(&["top_pushed(2)"]);

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let last_pushed = vec!["v3".to_string(), "v2".to_string(), "v1".to_string()];

        assert!(!policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    #[test]
    fn test_top_pushed_orphan_manifest() {
        let policy = policy(&["top_pushed(10)"]);

        let manifest = ManifestImage {
            tag: None,
            ..Default::default()
        };
        let last_pushed = vec!["v1".to_string()];

        assert!(!policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    #[test]
    fn test_top_pulled_in_top_n() {
        let policy = policy(&["top_pulled(2)"]);

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let last_pulled = vec!["v1".to_string(), "v2".to_string()];

        assert!(policy.should_retain(&manifest, &[], &last_pulled).unwrap());
    }

    #[test]
    fn test_top_pulled_not_in_top_n() {
        let policy = policy(&["top_pulled(1)"]);

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            ..Default::default()
        };
        let last_pulled = vec!["v1".to_string(), "v2".to_string()];

        assert!(!policy.should_retain(&manifest, &[], &last_pulled).unwrap());
    }

    #[test]
    fn test_pushed_at_recent() {
        let now = fixed_now();
        let policy = policy_at(now, &["image.pushed_at > now() - days(1)"]);

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: now.timestamp(),
            ..Default::default()
        };

        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_pushed_at_old() {
        let now = fixed_now();
        let policy = policy_at(now, &["image.pushed_at > now() - days(1)"]);

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: now.timestamp() - 2 * 86400,
            ..Default::default()
        };

        assert!(!policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_last_pulled_at_recent() {
        let now = fixed_now();
        let policy = policy_at(now, &["image.last_pulled_at > now() - hours(1)"]);

        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            last_pulled_at: now.timestamp(),
            ..Default::default()
        };

        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn test_last_pulled_at_old() {
        let now = fixed_now();
        let policy = policy_at(now, &["image.last_pulled_at > now() - hours(1)"]);

        let manifest = ManifestImage {
            tag: Some("v2".to_string()),
            last_pulled_at: now.timestamp() - 2 * 3600,
            ..Default::default()
        };

        assert!(!policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn negative_timestamp_saturates_to_zero() {
        let pre_epoch = DateTime::from_timestamp(-100, 0).unwrap();
        let image = ManifestImage::new(None, Some(pre_epoch), Some(pre_epoch), fixed_now());
        assert_eq!((image.pushed_at, image.last_pulled_at), (0, 0));
    }

    #[test]
    fn non_boolean_rule_retains_fail_open() {
        // A rule that returns an integer instead of a bool is a misconfiguration.
        // The evaluator must treat this as "retain" (fail-open) without panicking.
        let policy = policy(&["42"]);
        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn non_boolean_rule_string_retains_fail_open() {
        // A rule that returns a string instead of a bool.
        let policy = policy(&["'keep'"]);
        let manifest = ManifestImage::default();
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn failed_rule_evaluation_retains_fail_open() {
        // A rule that references an unbound variable fails at execution time
        // (not compile time).  The evaluator must treat this as "retain" (fail-open).
        let policy = policy(&["nonexistent_var"]);
        let manifest = ManifestImage::default();
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn non_boolean_rule_does_not_shadow_later_rules() {
        // When rule 1 returns a non-bool, the evaluator returns retain immediately
        // (fail-open), so rule 2 is never reached.  Both orderings should retain.
        let policy = policy(&["42", "false"]);
        let manifest = ManifestImage::default();
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    /// Empty rules list means no retention criteria are configured.
    /// `should_retain` returns `true` immediately so that manifests are never
    /// deleted when the operator has not expressed an intent to delete anything.
    #[test]
    fn empty_rules_retain_all() {
        let policy = policy(&[]);
        let tagged = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let orphan = ManifestImage {
            tag: None,
            ..Default::default()
        };
        assert!(policy.should_retain(&tagged, &[], &[]).unwrap());
        assert!(policy.should_retain(&orphan, &[], &[]).unwrap());
    }

    /// `top_pushed(-n)` clamps the count to 0 via `count.max(0)`, so the
    /// effective window is empty and no tag can ever match.  The manifest is
    /// eligible for deletion rather than being retained.
    #[test]
    fn top_pushed_with_negative_count_clamps_to_zero() {
        let policy = policy(&["top_pushed(-5)"]);
        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let last_pushed = vec!["v1".to_string(), "v2".to_string()];
        assert!(!policy.should_retain(&manifest, &last_pushed, &[]).unwrap());
    }

    /// `top_pulled(-n)` clamps the count to 0 via `count.max(0)`, so the
    /// effective window is empty and no tag can ever match.
    #[test]
    fn top_pulled_with_negative_count_clamps_to_zero() {
        let policy = policy(&["top_pulled(-1)"]);
        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        let last_pulled = vec!["v1".to_string()];
        assert!(!policy.should_retain(&manifest, &[], &last_pulled).unwrap());
    }

    /// Rules are evaluated in OR fashion: the first rule that returns `true`
    /// causes immediate retention.  When rule 1 returns `false` and rule 2
    /// returns `true`, the manifest is still retained.
    #[test]
    fn multiple_rules_first_false_then_true_retains() {
        let policy = policy(&["false", "true"]);
        let manifest = ManifestImage {
            tag: Some("v1".to_string()),
            ..Default::default()
        };
        assert!(policy.should_retain(&manifest, &[], &[]).unwrap());
    }

    #[test]
    fn injected_clock_is_observed_by_now_function() {
        let fixed = fixed_now();
        let policy = policy_at(fixed, &["image.pushed_at == now()"]);

        let matching = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: fixed.timestamp(),
            ..Default::default()
        };
        assert!(
            policy.should_retain(&matching, &[], &[]).unwrap(),
            "manifest pushed exactly at the fixed clock time should be retained"
        );

        let one_second_later = ManifestImage {
            tag: Some("v1".to_string()),
            pushed_at: fixed.timestamp() + 1,
            ..Default::default()
        };
        assert!(
            !policy.should_retain(&one_second_later, &[], &[]).unwrap(),
            "manifest pushed one second after the fixed clock time should not be retained"
        );
    }

    #[test]
    fn days_overflow_saturates() {
        assert_eq!(days(i64::MAX), i64::MAX);
    }

    #[test]
    fn hours_overflow_saturates() {
        assert_eq!(hours(i64::MAX), i64::MAX);
    }

    #[test]
    fn minutes_overflow_saturates() {
        assert_eq!(minutes(i64::MAX), i64::MAX);
    }

    #[test]
    fn days_negative_overflow_saturates() {
        assert_eq!(days(i64::MIN), i64::MIN);
    }

    #[test]
    fn days_normal_value() {
        assert_eq!(days(7), 7 * 86400);
    }

    #[test]
    fn uses_pull_time_detects_pull_dependent_rules() {
        for source in ["top_pulled(5)", "image.last_pulled_at > now() - days(7)"] {
            let config = RetentionPolicyConfig {
                rules: vec![CelRule::compile(source).unwrap()],
            };
            assert!(
                rules_use_pull_time(&config.rules),
                "{source} must be detected"
            );
        }

        let push_only = RetentionPolicyConfig {
            rules: vec![
                CelRule::compile("top_pushed(3)").unwrap(),
                CelRule::compile("image.pushed_at > now() - days(30)").unwrap(),
            ],
        };
        assert!(!rules_use_pull_time(&push_only.rules));
    }

    /// The two absences resolve differently and both are operator-visible:
    /// `doc/reference/cel-expressions.md` documents `last_pulled_at` as "0 if
    /// never pulled", while an unknown push time must not read as 1970.
    #[test]
    fn absent_timestamps_resolve_to_now_and_zero() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

        let never_pulled = ManifestImage::new(None, None, None, now);
        assert_eq!(
            never_pulled.last_pulled_at, 0,
            "never pulled must be an absence, not a timestamp at the epoch"
        );
        let unknown = serde_json::to_value(never_pulled).unwrap();
        assert_eq!(unknown["pushed_at"], now.timestamp());
        assert_eq!(unknown["last_pulled_at"], 0);

        let pushed = Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap();
        let pulled = Utc.with_ymd_and_hms(2025, 7, 1, 0, 0, 0).unwrap();
        let known = serde_json::to_value(ManifestImage::new(None, Some(pushed), Some(pulled), now))
            .unwrap();
        assert_eq!(known["pushed_at"], pushed.timestamp());
        assert_eq!(known["last_pulled_at"], pulled.timestamp());
    }
}
