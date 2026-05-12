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

use super::{CelRule, Error};
use crate::identity::{Action, ClientIdentity};

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
                            assign_once(&mut default_allow, "default_allow", map.next_value()?)?
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
    /// # Fail-open vs fail-closed semantics
    ///
    /// Access policies are **fail-closed for non-boolean results in both modes**: a misconfigured
    /// rule that returns a non-boolean value immediately denies access. This eliminates the risk
    /// of a typo in an ALLOW rule (Deny mode) silently letting subsequent rules grant access.
    ///
    /// ## Allow mode (default-allow; rules are DENY rules)
    ///
    /// | Outcome                             | Behaviour              | Log level |
    /// |-------------------------------------|------------------------|-----------|
    /// | `bool(true)`  — rule matched        | deny (fail-closed)     | `debug`   |
    /// | `bool(false)` — rule did not match  | continue to next rule  | —         |
    /// | non-boolean value (misconfiguration)| deny (fail-closed)     | `warn`    |
    /// | evaluation error                    | continue (fail-**open**)| `warn`   |
    /// | no rules matched                    | allow (default)        | —         |
    ///
    /// In Allow mode, an evaluation error is the one genuinely fail-open case: a broken DENY
    /// rule is skipped, and the default grants access.  This is a known trade-off; changing it
    /// would require propagating `Err` through all callers.
    ///
    /// ## Deny mode (default-deny; rules are ALLOW rules)
    ///
    /// | Outcome                             | Behaviour              | Log level |
    /// |-------------------------------------|------------------------|-----------|
    /// | `bool(true)`  — rule matched        | allow (fail-open)      | `debug`   |
    /// | `bool(false)` — rule did not match  | continue to next rule  | —         |
    /// | non-boolean value (misconfiguration)| deny (fail-closed)     | `warn`    |
    /// | evaluation error                    | continue (skip rule)   | `warn`    |
    /// | no rules matched                    | deny (default)         | —         |
    ///
    /// In Deny mode, evaluation errors skip the rule and fall through to the default deny.
    /// Non-boolean results immediately deny (matching Allow mode's fail-closed behavior).
    ///
    /// # Arguments
    /// * `action` - The domain action representing the registry operation
    /// * `identity` - The client identity containing authentication information
    ///
    /// # Returns
    /// * `Ok(true)` if access should be granted
    /// * `Ok(false)` if access should be denied
    /// * `Err` if context construction fails (rule execution errors are handled internally)
    pub fn evaluate(&self, action: &Action, identity: &ClientIdentity) -> Result<bool, Error> {
        if self.rules.is_empty() {
            return Ok(self.default == AccessMode::Allow);
        }

        let context = Self::build_context(action, identity)?;
        let rule_kind = match self.default {
            AccessMode::Allow => "deny",
            AccessMode::Deny => "allow",
        };

        for (index, rule) in self.rules.iter().enumerate() {
            let rule_index = index + 1;
            match rule.execute(&context) {
                Ok(Value::Bool(true)) => {
                    debug!("{rule_kind} rule {rule_index} matched");
                    return Ok(self.default == AccessMode::Deny);
                }
                Ok(Value::Bool(false)) => {}
                Ok(value) => {
                    warn!(
                        "Access policy {rule_kind} rule {rule_index} returned non-boolean value: {value:?}"
                    );
                    return Ok(false);
                }
                Err(e) => {
                    warn!("Access policy {rule_kind} rule {rule_index} evaluation failed: {e}");
                }
            }
        }
        Ok(self.default == AccessMode::Allow)
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

    fn rule(s: &str) -> CelRule {
        CelRule::compile(s).unwrap()
    }

    #[test]
    fn test_access_policy_allow_mode_no_rules() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn test_access_policy_deny_mode_no_rules() {
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(!policy.evaluate(&action, &identity).unwrap());
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

        assert!(!policy.evaluate(&action, &identity).unwrap());

        let identity = ClientIdentity {
            username: Some("allowed".to_string()),
            ..ClientIdentity::default()
        };

        assert!(policy.evaluate(&action, &identity).unwrap());
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

        assert!(policy.evaluate(&action, &identity).unwrap());

        let identity = ClientIdentity {
            username: Some("user".to_string()),
            ..ClientIdentity::default()
        };

        assert!(!policy.evaluate(&action, &identity).unwrap());
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
        // Allow mode: rules are DENY rules.  A non-bool result is treated as a
        // match → access is denied (fail-closed).
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("42")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(!policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn non_boolean_rule_in_deny_mode_denies_fail_closed() {
        // Deny mode: rules are ALLOW rules.  A non-bool result is treated as a
        // misconfiguration and immediately denies access (fail-closed).
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("42")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(!policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn non_boolean_rule_in_deny_mode_short_circuits_subsequent_allow_rules() {
        // Deny mode + ALLOW rules: a non-bool result must fail-closed immediately,
        // not silently skip and let a later rule grant access.
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("42"), rule("true")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            !policy.evaluate(&action, &identity).unwrap(),
            "non-boolean rule must short-circuit to deny, even when a later rule would allow"
        );
    }

    #[test]
    fn failed_rule_in_allow_mode_falls_through_to_allow_fail_open() {
        // Allow mode: an evaluation error in a DENY rule is skipped, and the
        // default grants access.  This is the documented fail-open case in Allow mode.
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn failed_rule_in_deny_mode_falls_through_to_deny_fail_closed() {
        // Deny mode: an evaluation error in an ALLOW rule is skipped, and the
        // default denies access (fail-closed).
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(!policy.evaluate(&action, &identity).unwrap());
    }

    // --- Multi-rule ordering and short-circuit semantics ---

    #[test]
    fn multi_rule_allow_mode_first_match_denies_second_rule_unreached() {
        // Allow mode: rules are DENY rules evaluated in order.  Rule 1 always matches
        // (true), so access is denied immediately.  Rule 2 would grant access if it
        // were the active mode, but it is never reached.  The final outcome must be
        // deny, pinning short-circuit behaviour.
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![
                rule("true"),  // rule 1: always triggers → deny
                rule("false"), // rule 2: would not trigger, but is unreachable anyway
            ],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(!policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn multi_rule_deny_mode_first_match_allows_second_rule_unreached() {
        // Deny mode: rules are ALLOW rules evaluated in order.  Rule 1 always matches
        // (true), so access is granted immediately.  Rule 2 is never evaluated.
        // The final outcome must be allow, pinning short-circuit behaviour.
        let policy = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![
                rule("true"),  // rule 1: always triggers → allow
                rule("false"), // rule 2: unreachable
            ],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn multi_rule_no_match_falls_through_to_default() {
        // Both modes: when no rule returns true the default applies.
        // Allow mode with two false rules → allow (default).
        let policy_allow = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("false"), rule("false")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();
        assert!(policy_allow.evaluate(&action, &identity).unwrap());

        // Deny mode with two false rules → deny (default).
        let policy_deny = AccessPolicy::new(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("false"), rule("false")],
        });
        assert!(!policy_deny.evaluate(&action, &identity).unwrap());
    }
}
