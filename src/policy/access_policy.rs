//! Access control policy evaluation for registry operations.
//!
//! This module provides CEL-based access control for registry operations.
//! Policies are pre-compiled at configuration load time for performance.
//!
//! # Policy Evaluation
//!
//! Access policies support two modes:
//! - **Allow**: Access is granted unless explicitly denied by a rule
//! - **Deny**: Access is denied unless explicitly granted by a rule
//!
//! # Available Variables
//!
//! CEL expressions have access to:
//! - `identity`: Client identity information (id, username, certificate details)
//! - `request`: Request details (action, namespace, digest, reference)

use std::fmt;

use cel_interpreter::{Context, Value};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use tracing::{debug, warn};

use crate::identity::{Action, ClientIdentity};
use crate::policy::{CelRule, Error, PolicyDecision, PolicyError};

/// Whether an access policy defaults to allowing or denying requests.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AccessMode {
    /// Access is denied unless a rule explicitly grants it.
    #[default]
    Deny,
    /// Access is granted unless a rule explicitly denies it.
    Allow,
}

/// Configuration for access control policies.
#[derive(Clone, Debug, Default)]
pub struct AccessPolicyConfig {
    pub default: AccessMode,
    pub rules: Vec<CelRule>,
}

impl<'de> Deserialize<'de> for AccessPolicyConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AccessPolicyVisitor;

        impl<'de> Visitor<'de> for AccessPolicyVisitor {
            type Value = AccessPolicyConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("access policy configuration")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut default = None;
                let mut default_allow = None;
                let mut rules = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "default" => assign_once(&mut default, "default", map.next_value()?)?,
                        "default_allow" => {
                            assign_once(&mut default_allow, "default_allow", map.next_value()?)?;
                        }
                        "rules" => assign_once(&mut rules, "rules", map.next_value()?)?,
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                if default_allow.is_some() {
                    warn!("'access_policy.default_allow' is deprecated; use 'default' instead");
                }

                Ok(AccessPolicyConfig {
                    default: default
                        .or_else(|| default_allow.map(AccessMode::from_default_allow))
                        .unwrap_or_default(),
                    rules: rules.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(AccessPolicyVisitor)
    }
}

fn assign_once<T, E>(slot: &mut Option<T>, field: &'static str, value: T) -> Result<(), E>
where
    E: de::Error,
{
    if slot.replace(value).is_some() {
        return Err(de::Error::duplicate_field(field));
    }
    Ok(())
}

impl AccessMode {
    fn from_default_allow(allow: bool) -> Self {
        if allow { Self::Allow } else { Self::Deny }
    }
}

impl From<AccessMode> for PolicyDecision {
    fn from(mode: AccessMode) -> Self {
        match mode {
            AccessMode::Allow => Self::Allow,
            AccessMode::Deny => Self::Deny,
        }
    }
}

/// Access control policy engine.
///
/// Evaluates CEL expressions to determine if a request should be allowed.
/// Rules are pre-compiled at configuration time for better performance.
pub struct AccessPolicy {
    default: AccessMode,
    rules: Vec<CelRule>,
}

impl AccessPolicy {
    /// Creates a new access policy from configuration.
    ///
    /// Rules are already compiled; this constructor is infallible.
    pub fn new(config: AccessPolicyConfig) -> Self {
        Self {
            default: config.default,
            rules: config.rules,
        }
    }

    /// Evaluates the access policy for a given action and identity.
    ///
    /// Rules are evaluated in order; the first matching rule (returning `true`) flips the
    /// default decision.  If no rule matches, the `default` mode determines the outcome.
    ///
    /// # Fail-closed semantics
    ///
    /// Access policies are **fail-closed for non-boolean results and runtime evaluation errors
    /// in both modes**.  A misconfigured rule that returns a non-boolean value, or a rule that
    /// throws at runtime, immediately stops evaluation and returns `Indeterminate`.  Callers
    /// must treat `Indeterminate` as deny.
    ///
    /// ## Allow mode (default-allow; rules are DENY rules)
    ///
    /// | Outcome                             | Decision               | Log level |
    /// |-------------------------------------|------------------------|-----------|
    /// | `bool(true)`  — rule matched        | `Deny` (fail-closed)   | `debug`   |
    /// | `bool(false)` — rule did not match  | continue to next rule  | —         |
    /// | non-boolean value (misconfiguration)| `Indeterminate`        | `warn`    |
    /// | evaluation error                    | `Indeterminate`        | `warn`    |
    /// | no rules matched                    | `Allow` (default)      | —         |
    ///
    /// ## Deny mode (default-deny; rules are ALLOW rules)
    ///
    /// | Outcome                             | Decision               | Log level |
    /// |-------------------------------------|------------------------|-----------|
    /// | `bool(true)`  — rule matched        | `Allow` (fail-open)    | `debug`   |
    /// | `bool(false)` — rule did not match  | continue to next rule  | —         |
    /// | non-boolean value (misconfiguration)| `Indeterminate`        | `warn`    |
    /// | evaluation error                    | `Indeterminate`        | `warn`    |
    /// | no rules matched                    | `Deny` (default)       | —         |
    ///
    /// # Arguments
    /// * `action` - The domain action representing the registry operation
    /// * `identity` - The client identity containing authentication information
    ///
    /// # Returns
    /// `PolicyDecision::Allow`, `PolicyDecision::Deny`, or `PolicyDecision::Indeterminate`.
    /// Context construction failures are reported as `Indeterminate` with rule index 0.
    pub fn evaluate(&self, action: &Action, identity: &ClientIdentity) -> PolicyDecision {
        if self.rules.is_empty() {
            return self.default.into();
        }

        let context = match Self::build_context(action, identity) {
            Ok(ctx) => ctx,
            Err(e) => {
                return PolicyDecision::Indeterminate(PolicyError {
                    rule_index: None,
                    message: e.to_string(),
                });
            }
        };

        let rule_kind = match self.default {
            AccessMode::Allow => "deny",
            AccessMode::Deny => "allow",
        };

        for (index, rule) in self.rules.iter().enumerate() {
            let rule_index = index + 1;
            match rule.execute(&context) {
                Ok(Value::Bool(true)) => {
                    debug!("{rule_kind} rule {rule_index} matched");
                    return match self.default {
                        AccessMode::Allow => PolicyDecision::Deny,
                        AccessMode::Deny => PolicyDecision::Allow,
                    };
                }
                Ok(Value::Bool(false)) => {}
                Ok(value) => {
                    let value_type = value.type_of();
                    warn!(
                        "Access policy {rule_kind} rule {rule_index} returned non-boolean value of type {value_type}"
                    );
                    return PolicyDecision::Indeterminate(PolicyError {
                        rule_index: Some(rule_index),
                        message: format!("non-boolean result of type {value_type}"),
                    });
                }
                Err(e) => {
                    warn!("Access policy {rule_kind} rule {rule_index} evaluation failed: {e}");
                    return PolicyDecision::Indeterminate(PolicyError {
                        rule_index: Some(rule_index),
                        message: e.to_string(),
                    });
                }
            }
        }

        self.default.into()
    }

    fn build_context<'a>(
        action: &'a Action,
        identity: &'a ClientIdentity,
    ) -> Result<Context<'a>, Error> {
        let mut context = Context::default();
        context.add_variable("request", action)?;
        context.add_variable("identity", identity)?;
        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oci::{Digest, Namespace};

    fn rule(s: &str) -> CelRule {
        CelRule::compile(s).unwrap()
    }

    fn is_allow(d: &PolicyDecision) -> bool {
        matches!(d, PolicyDecision::Allow)
    }

    fn is_deny(d: &PolicyDecision) -> bool {
        matches!(d, PolicyDecision::Deny)
    }

    fn is_indeterminate(d: &PolicyDecision) -> bool {
        matches!(d, PolicyDecision::Indeterminate(_))
    }

    #[test]
    fn test_access_policy_allow_mode_no_rules() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_allow(&policy.evaluate(&action, &identity)));
    }

    #[test]
    fn test_access_policy_deny_mode_no_rules() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_deny(&policy.evaluate(&action, &identity)));
    }

    #[test]
    fn test_access_policy_allow_mode_with_deny_rule() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("identity.username == 'forbidden'")],
        });

        let action = Action::ApiVersion;
        let identity = ClientIdentity {
            username: Some("forbidden".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_deny(&policy.evaluate(&action, &identity)));

        let identity = ClientIdentity {
            username: Some("allowed".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_allow(&policy.evaluate(&action, &identity)));
    }

    #[test]
    fn test_access_policy_deny_mode_with_allow_rule() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("identity.username == 'admin'")],
        });

        let action = Action::ApiVersion;
        let identity = ClientIdentity {
            username: Some("admin".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_allow(&policy.evaluate(&action, &identity)));

        let identity = ClientIdentity {
            username: Some("user".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_deny(&policy.evaluate(&action, &identity)));
    }

    /// A mount authorizes as the dedicated `mount-blob` action, independent of
    /// the `start-upload` rules that govern ordinary uploads.
    #[test]
    fn test_access_policy_gates_cross_repo_mount() {
        let namespace = Namespace::new("team/app").unwrap();
        let digest = Digest::try_from(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();

        let normal_upload = Action::StartUpload {
            namespace: namespace.clone(),
            digest: None,
        };
        let mount = Action::MountBlob {
            namespace,
            digest,
            from: Some(Namespace::new("team/base").unwrap()),
        };
        let anyone = ClientIdentity {
            username: Some("alice".to_string()),
            ..ClientIdentity::default()
        };
        let replicator = ClientIdentity {
            id: Some("replicator".to_string()),
            username: Some("svc".to_string()),
            ..ClientIdentity::default()
        };

        let replicator_only = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![
                rule("identity.username != null && request.action == 'start-upload'"),
                rule("identity.id == 'replicator' && request.action == 'mount-blob'"),
            ],
        });
        assert!(is_allow(&replicator_only.evaluate(&normal_upload, &anyone)));
        assert!(is_deny(&replicator_only.evaluate(&mount, &anyone)));
        assert!(is_allow(&replicator_only.evaluate(&mount, &replicator)));

        let deny_non_replicator = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule(
                "request.action == 'mount-blob' && identity.id != 'replicator'",
            )],
        });
        assert!(is_allow(
            &deny_non_replicator.evaluate(&normal_upload, &anyone)
        ));
        assert!(is_deny(&deny_non_replicator.evaluate(&mount, &anyone)));
        assert!(is_allow(&deny_non_replicator.evaluate(&mount, &replicator)));
    }

    /// `request.from` is present only on a `from`-bearing mount, so rules need
    /// `has(request.from)` to handle auto-discovery mounts.
    #[test]
    fn test_access_policy_gates_cross_repo_mount_by_source() {
        let target = Namespace::new("team/app").unwrap();
        let digest = Digest::try_from(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();

        let from_trusted = Action::MountBlob {
            namespace: target.clone(),
            digest: digest.clone(),
            from: Some(Namespace::new("team/base").unwrap()),
        };
        let from_untrusted = Action::MountBlob {
            namespace: target.clone(),
            digest: digest.clone(),
            from: Some(Namespace::new("other/evil").unwrap()),
        };
        let no_from = Action::MountBlob {
            namespace: target,
            digest,
            from: None,
        };
        let client = ClientIdentity {
            username: Some("alice".to_string()),
            ..ClientIdentity::default()
        };

        // The `has()` guard keeps the rule from raising the fail-closed "no
        // such key" error on a from-less mount.
        let only_from_trusted = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule(
                "request.action == 'mount-blob' && has(request.from) && request.from == 'team/base'",
            )],
        });
        assert!(is_allow(
            &only_from_trusted.evaluate(&from_trusted, &client)
        ));
        assert!(is_deny(
            &only_from_trusted.evaluate(&from_untrusted, &client)
        ));
        assert!(is_deny(&only_from_trusted.evaluate(&no_from, &client)));

        let deny_untrusted_source = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule(
                "request.action == 'mount-blob' && has(request.from) && request.from == 'other/evil'",
            )],
        });
        assert!(is_allow(
            &deny_untrusted_source.evaluate(&from_trusted, &client)
        ));
        assert!(is_deny(
            &deny_untrusted_source.evaluate(&from_untrusted, &client)
        ));
        assert!(is_allow(&deny_untrusted_source.evaluate(&no_from, &client)));
    }

    #[test]
    fn test_access_policy_default_toml_allow() {
        let config: AccessPolicyConfig = toml::from_str("default = \"allow\"").unwrap();
        assert_eq!(config.default, AccessMode::Allow);
    }

    #[test]
    fn test_access_policy_default_toml_deny() {
        let config: AccessPolicyConfig = toml::from_str("default = \"deny\"").unwrap();
        assert_eq!(config.default, AccessMode::Deny);
    }

    #[test]
    fn test_access_policy_default_toml_missing_is_deny() {
        let config: AccessPolicyConfig = toml::from_str("").unwrap();
        assert_eq!(config.default, AccessMode::Deny);
    }

    #[test]
    fn test_access_policy_default_toml_unknown_value_fails() {
        let result: Result<AccessPolicyConfig, _> = toml::from_str("default = \"maybe\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_deprecated_default_allow_true_maps_to_allow() {
        let config: AccessPolicyConfig = toml::from_str("default_allow = true").unwrap();
        assert_eq!(config.default, AccessMode::Allow);
    }

    #[test]
    fn test_deprecated_default_allow_false_maps_to_deny() {
        let config: AccessPolicyConfig = toml::from_str("default_allow = false").unwrap();
        assert_eq!(config.default, AccessMode::Deny);
    }

    #[test]
    fn test_default_wins_over_deprecated_default_allow() {
        let toml = r#"
            default = "deny"
            default_allow = true
        "#;
        let config: AccessPolicyConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.default, AccessMode::Deny);
    }

    // --- Non-boolean and error rule behaviour ---

    #[test]
    fn non_boolean_rule_in_allow_mode_denies_fail_closed() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("42")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity)),
            "non-boolean result must be Indeterminate (fail-closed)"
        );
    }

    #[test]
    fn non_boolean_rule_in_deny_mode_denies_fail_closed() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("42")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity)),
            "non-boolean result must be Indeterminate (fail-closed)"
        );
    }

    #[test]
    fn non_boolean_rule_in_deny_mode_short_circuits_subsequent_allow_rules() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("42"), rule("true")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity)),
            "non-boolean rule must short-circuit to Indeterminate, even when a later rule would allow"
        );
    }

    #[test]
    fn failed_rule_in_allow_mode_is_indeterminate_and_denies() {
        // Allow mode: a runtime evaluation error in a DENY rule now produces
        // Indeterminate instead of silently falling through to allow.
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity)),
            "a failing DENY rule in Allow mode must be Indeterminate, not Allow"
        );
    }

    #[test]
    fn failed_rule_in_allow_mode_indeterminate_carries_rule_index() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("false"), rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        let PolicyDecision::Indeterminate(err) = policy.evaluate(&action, &identity) else {
            panic!("expected Indeterminate");
        };
        assert_eq!(err.rule_index, Some(2), "failing rule is the second rule");
    }

    #[test]
    fn failed_rule_in_deny_mode_is_indeterminate() {
        // Deny mode: an evaluation error in an ALLOW rule produces Indeterminate
        // (which callers treat as deny — fail-closed).
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity)),
            "a failing ALLOW rule in Deny mode must be Indeterminate"
        );
    }

    // --- Multi-rule ordering and short-circuit semantics ---

    #[test]
    fn multi_rule_allow_mode_first_match_denies_second_rule_unreached() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![
                rule("true"),  // rule 1: always triggers → Deny
                rule("false"), // rule 2: unreachable
            ],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_deny(&policy.evaluate(&action, &identity)));
    }

    #[test]
    fn multi_rule_deny_mode_first_match_allows_second_rule_unreached() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![
                rule("true"),  // rule 1: always triggers → Allow
                rule("false"), // rule 2: unreachable
            ],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_allow(&policy.evaluate(&action, &identity)));
    }

    #[test]
    fn multi_rule_no_match_falls_through_to_default() {
        let policy_allow = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("false"), rule("false")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();
        assert!(is_allow(&policy_allow.evaluate(&action, &identity)));

        let policy_deny = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("false"), rule("false")],
        });
        assert!(is_deny(&policy_deny.evaluate(&action, &identity)));
    }
}
