//! Per-image policies: the `scan` and `index` tables, a [`PolicyConfig`]
//! judged over an image when it lands and again by `reconcile scan` and
//! `reconcile index`.

use std::sync::Arc;

use chrono::Utc;
use tracing::warn;

use angos_oci::Tag;

use crate::policy::{
    ManifestImage, PolicyConfig, RetentionPolicy, RetentionPolicyConfig, RuleOutcome, SystemClock,
};

/// A compiled `scan` or `index` policy.
pub struct ImagePolicy {
    applies_by_default: bool,
    rules: RetentionPolicy,
}

impl ImagePolicy {
    pub fn new<A: Copy + Into<bool>>(config: &PolicyConfig<A>) -> Self {
        Self {
            applies_by_default: config.default.is_some_and(Into::into),
            rules: RetentionPolicy::new(
                &RetentionPolicyConfig {
                    rules: config.rules.clone(),
                },
                Arc::new(SystemClock),
            ),
        }
    }

    pub fn has_rules(&self) -> bool {
        self.rules.has_rules()
    }

    /// Whether the policy applies to `image`. A rule that cannot be evaluated
    /// applies it: an extra scan or listing costs work, a missed one hides a
    /// finding.
    pub fn applies(
        &self,
        image: &ManifestImage,
        last_pushed: &[String],
        last_pulled: &[String],
    ) -> bool {
        match self.rules.evaluate(image, last_pushed, last_pulled) {
            Ok(RuleOutcome::Matched(_)) => !self.applies_by_default,
            Ok(RuleOutcome::NoMatch) => self.applies_by_default,
            Ok(RuleOutcome::Indeterminate { index, message }) => {
                warn!("Image policy rule {index} is indeterminate: {message}; applying the policy");
                true
            }
            Err(e) => {
                warn!("Image policy rules could not be evaluated: {e}; applying the policy");
                true
            }
        }
    }

    /// The decision for a manifest as it lands: pushed now, never pulled,
    /// never scanned, judged under each of `tags` in turn or untagged with
    /// none, the pushed tags being the whole push ranking.
    pub fn applies_at_push(&self, tags: &[Tag]) -> bool {
        let now = Utc::now();
        if tags.is_empty() {
            return self.applies(&ManifestImage::new(None, Some(now), None, now), &[], &[]);
        }
        let ranking: Vec<String> = tags.iter().map(ToString::to_string).collect();
        tags.iter().any(|tag| {
            let image = ManifestImage::new(Some(tag.to_string()), Some(now), None, now);
            self.applies(&image, &ranking, &[])
        })
    }
}
