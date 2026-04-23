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

use cel_interpreter::{Context, Value};
use serde::Deserialize;
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
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(from = "RawAccessPolicyConfig")]
pub struct AccessPolicyConfig {
    pub default: AccessMode,
    pub rules: Vec<CelRule>,
}

/// Intermediate shape that also accepts the deprecated `default_allow` field.
///
/// `default` takes precedence when both are present. `default_allow` usage
/// emits a deprecation warning and will be removed in a future release.
#[derive(Default, Deserialize)]
#[serde(default)]
struct RawAccessPolicyConfig {
    default: Option<AccessMode>,
    default_allow: Option<bool>,
    rules: Vec<CelRule>,
}

impl From<RawAccessPolicyConfig> for AccessPolicyConfig {
    fn from(raw: RawAccessPolicyConfig) -> Self {
        if raw.default_allow.is_some() {
            warn!("'access_policy.default_allow' is deprecated; use 'default' instead");
        }
        let default = raw
            .default
            .or_else(|| raw.default_allow.map(AccessMode::from_default_allow))
            .unwrap_or_default();
        Self {
            default,
            rules: raw.rules,
        }
    }
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
    pub fn new(config: &AccessPolicyConfig) -> Self {
        Self {
            default: config.default,
            rules: config.rules.clone(),
        }
    }

    /// Evaluates the access policy for a given action and identity.
    ///
    /// # Arguments
    /// * `action` - The domain action representing the registry operation
    /// * `identity` - The client identity containing authentication information
    ///
    /// # Returns
    /// * `Ok(true)` if access should be granted
    /// * `Ok(false)` if access should be denied
    /// * `Err` if policy evaluation fails
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
                    if self.default == AccessMode::Allow {
                        return Ok(false);
                    }
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
        context
            .add_variable("request", action)
            .map_err(|e| Error::Evaluation(e.to_string()))?;
        context
            .add_variable("identity", identity)
            .map_err(|e| Error::Evaluation(e.to_string()))?;
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
        let config = AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![],
        };
        let policy = AccessPolicy::new(&config);
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn test_access_policy_deny_mode_no_rules() {
        let config = AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![],
        };
        let policy = AccessPolicy::new(&config);
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(!policy.evaluate(&action, &identity).unwrap());
    }

    #[test]
    fn test_access_policy_allow_mode_with_deny_rule() {
        let config = AccessPolicyConfig {
            default: AccessMode::Allow,
            rules: vec![rule("identity.username == 'forbidden'")],
        };
        let policy = AccessPolicy::new(&config);

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
        let config = AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![rule("identity.username == 'admin'")],
        };
        let policy = AccessPolicy::new(&config);

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
}
