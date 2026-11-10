//! CEL retention policies: a manifest any rule matches is retained, the rest
//! are eligible for deletion.

use std::sync::Arc;

use cel_interpreter::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use angos_oci::Namespace;

use crate::policy::clock::{Clock, SystemClock};
use crate::policy::{CelRule, Error, RuleOutcome, evaluate_rules};

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

/// The `image` object CEL rules read.
#[derive(Debug, Default, Serialize)]
pub struct ManifestImage {
    pub namespace: String,
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
        namespace: &Namespace,
        tag: Option<String>,
        pushed_at: Option<DateTime<Utc>>,
        last_pulled_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            namespace: namespace.to_string(),
            tag,
            pushed_at: pushed_at.unwrap_or(now).timestamp().max(0),
            last_pulled_at: last_pulled_at.map_or(0, |t| t.timestamp().max(0)),
            scanned_at: 0,
        }
    }
}

pub struct RetentionPolicy {
    rules: Vec<CelRule>,
    /// The `now()` rules read: the system clock, frozen by this module's tests.
    clock: Arc<dyn Clock>,
}

impl RetentionPolicy {
    pub fn new(rules: Vec<CelRule>) -> Self {
        Self {
            rules,
            clock: Arc::new(SystemClock),
        }
    }

    pub fn has_rules(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Whether `manifest` is kept. No rules keep everything, and an
    /// indeterminate rule keeps it too, since keeping a manifest by mistake
    /// beats deleting one. `last_pushed` and `last_pulled` rank tags most
    /// recent first.
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
                warn!(
                    "Retention rule {index} is indeterminate: {message}; \
                     treating as 'retain' (fail-open)"
                );
                Ok(true)
            }
        }
    }

    /// The raw outcome, for a caller reading an indeterminate rule
    /// differently. `Err` only when the context cannot be built.
    pub fn evaluate(
        &self,
        manifest: &ManifestImage,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> Result<RuleOutcome, Error> {
        let mut context = Context::default();
        context.add_variable("image", manifest)?;
        let clock = self.clock.clone();
        context.add_function("now", move || clock.now().timestamp());
        context.add_function("days", |d: i64| d.saturating_mul(86400));
        context.add_function("hours", |h: i64| h.saturating_mul(3600));
        context.add_function("minutes", |m: i64| m.saturating_mul(60));
        context.add_function(
            "top_pushed",
            top_fn(manifest.tag.clone(), last_pushed.to_vec()),
        );
        context.add_function(
            "top_pulled",
            top_fn(manifest.tag.clone(), last_pulled.to_vec()),
        );
        Ok(evaluate_rules(&self.rules, &context))
    }
}

/// A `top_pushed`/`top_pulled` function over a tag ranking. Untagged subjects
/// never match, which lets a count-only policy reclaim the revisions its own
/// tag deletions orphan.
fn top_fn(tag: Option<String>, ranking: Vec<String>) -> impl Fn(i64) -> bool + Send + Sync {
    move |count: i64| {
        let Some(ref tag) = tag else {
            return false;
        };
        let limit = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
        ranking.iter().take(limit).any(|t| t == tag)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::*;

    struct FixedClock(DateTime<Utc>);

    impl Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn policy(rules: &[&str]) -> RetentionPolicy {
        let rules = rules.iter().map(|s| CelRule::compile(s).unwrap()).collect();
        RetentionPolicy {
            clock: Arc::new(FixedClock(fixed_now())),
            ..RetentionPolicy::new(rules)
        }
    }

    fn tagged(tag: Option<&str>) -> ManifestImage {
        ManifestImage {
            tag: tag.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn namespace_is_exposed_to_rules() {
        let policy = policy(&["image.namespace.startsWith('prod/')"]);
        let image = |namespace: &str| {
            ManifestImage::new(
                &Namespace::new(namespace).unwrap(),
                None,
                None,
                None,
                Utc::now(),
            )
        };
        assert!(policy.should_retain(&image("prod/api"), &[], &[]).unwrap());
        assert!(!policy.should_retain(&image("dev/api"), &[], &[]).unwrap());
    }

    #[test]
    fn top_n_matches_the_tag_ranking() {
        let pushed = ["v3".to_string(), "v2".to_string(), "v1".to_string()];
        let pulled = ["v1".to_string(), "v2".to_string()];
        for (rule, tag, retained) in [
            ("top_pushed(2)", Some("v2"), true),
            ("top_pushed(2)", Some("v1"), false),
            ("top_pushed(10)", None, false),
            ("top_pushed(-5)", Some("v3"), false),
            ("top_pulled(1)", Some("v1"), true),
            ("top_pulled(1)", Some("v3"), false),
        ] {
            let kept = policy(&[rule])
                .should_retain(&tagged(tag), &pushed, &pulled)
                .unwrap();
            assert_eq!(kept, retained, "{rule} on {tag:?}");
        }
    }

    #[test]
    fn now_reads_the_injected_clock() {
        let policy = policy(&["image.last_pulled_at > now() - hours(1)"]);
        let pulled_at = |offset: i64| ManifestImage {
            last_pulled_at: fixed_now().timestamp() - offset,
            ..Default::default()
        };
        assert!(policy.should_retain(&pulled_at(0), &[], &[]).unwrap());
        assert!(
            !policy
                .should_retain(&pulled_at(2 * 3600), &[], &[])
                .unwrap()
        );
    }

    #[test]
    fn duration_helpers_saturate() {
        let policy =
            policy(&["days(7) == 604800 && days(9223372036854775807) == 9223372036854775807"]);
        assert!(matches!(
            policy.evaluate(&tagged(None), &[], &[]).unwrap(),
            RuleOutcome::Matched(1)
        ));
    }

    #[test]
    fn rule_outcomes_decide_retention() {
        for (rules, retained) in [
            (&[][..], true),
            (&["false", "true"][..], true),
            (&["false"][..], false),
            // Fail open on a non-boolean or an evaluation error.
            (&["42"][..], true),
            (&["nonexistent_var"][..], true),
        ] {
            let kept = policy(rules)
                .should_retain(&tagged(Some("v1")), &[], &[])
                .unwrap();
            assert_eq!(kept, retained, "{rules:?}");
        }
    }

    #[test]
    fn uses_pull_time_detects_pull_dependent_rules() {
        let compile = |sources: &[&str]| -> Vec<CelRule> {
            sources
                .iter()
                .map(|s| CelRule::compile(s).unwrap())
                .collect()
        };
        assert!(rules_use_pull_time(&compile(&["top_pulled(5)"])));
        assert!(rules_use_pull_time(&compile(&[
            "image.last_pulled_at > now() - days(7)"
        ])));
        assert!(!rules_use_pull_time(&compile(&[
            "top_pushed(3)",
            "image.pushed_at > now() - days(30)"
        ])));
    }

    /// An unknown push time must not read as 1970, and `last_pulled_at` is
    /// documented as `0` if never pulled; pre-epoch dates saturate to `0`.
    #[test]
    fn new_resolves_absent_and_pre_epoch_timestamps() {
        let app = Namespace::new("app").unwrap();
        let absent = ManifestImage::new(&app, None, None, None, fixed_now());
        assert_eq!(
            (absent.pushed_at, absent.last_pulled_at),
            (fixed_now().timestamp(), 0)
        );

        let pre_epoch = DateTime::from_timestamp(-100, 0).unwrap();
        let old = ManifestImage::new(&app, None, Some(pre_epoch), Some(pre_epoch), fixed_now());
        assert_eq!((old.pushed_at, old.last_pulled_at), (0, 0));
    }
}
