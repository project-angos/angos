//! Access control policy evaluation for registry operations.
//!
//! This module provides CEL-based access control for registry operations.
//! Policies are pre-compiled at configuration load time for performance.
//!
//! # Policy Evaluation
//!
//! Access policies support two modes:
//! - **Default Allow**: Access is granted unless explicitly denied by a rule
//! - **Default Deny**: Access is denied unless explicitly granted by a rule
//!
//! # Available Variables
//!
//! CEL expressions have access to:
//! - `identity`: Client identity information (id, username, certificate details)
//! - `request`: Request details (action, namespace, digest, reference)

use cel_interpreter::{Context, Program, Value};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{
    identity::{ClientIdentity, Route},
    registry::{Error, cel},
};

/// Configuration for access control policies.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct AccessPolicyConfig {
    #[serde(default)]
    pub default_allow: bool,
    #[serde(default)]
    pub rules: Vec<String>,
}

/// Access control policy engine.
///
/// Evaluates CEL expressions to determine if a request should be allowed.
/// Rules are pre-compiled at configuration time for better performance.
pub struct AccessPolicy {
    default_allow: bool,
    rules: Vec<Program>,
}

impl AccessPolicy {
    /// Creates a new access policy from configuration.
    ///
    /// Compiles CEL expressions from the configuration into programs.
    pub fn new(config: &AccessPolicyConfig) -> Result<Self, Error> {
        let rules =
            cel::compile_rules(&config.rules, "access policy").map_err(Error::Initialization)?;

        Ok(Self {
            default_allow: config.default_allow,
            rules,
        })
    }

    /// Evaluates the access policy for a given request and identity.
    ///
    /// # Arguments
    /// * `request` - The validation info containing action and resource information
    /// * `identity` - The client identity containing authentication information
    ///
    /// # Returns
    /// * `Ok(true)` if access should be granted
    /// * `Ok(false)` if access should be denied
    /// * `Err` if policy evaluation fails
    pub fn evaluate(&self, request: &Route, identity: &ClientIdentity) -> Result<bool, Error> {
        if self.rules.is_empty() {
            return Ok(self.default_allow);
        }

        let context = Self::build_context(request, identity)?;
        let rule_kind = if self.default_allow { "deny" } else { "allow" };

        for (index, rule) in self.rules.iter().enumerate() {
            let rule_index = index + 1;
            match rule.execute(&context) {
                Ok(Value::Bool(true)) => {
                    debug!("{rule_kind} rule {rule_index} matched");
                    return Ok(!self.default_allow);
                }
                Ok(Value::Bool(false)) => {}
                Ok(value) => {
                    warn!(
                        "Access policy {rule_kind} rule {rule_index} returned non-boolean value: {value:?}"
                    );
                    if self.default_allow {
                        return Ok(false);
                    }
                }
                Err(e) => {
                    warn!("Access policy {rule_kind} rule {rule_index} evaluation failed: {e}");
                }
            }
        }
        Ok(self.default_allow)
    }

    fn build_context<'a>(
        request: &'a Route,
        identity: &'a ClientIdentity,
    ) -> Result<Context<'a>, Error> {
        let mut context = Context::default();
        context.add_variable("request", request)?;
        context.add_variable("identity", identity)?;
        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_policy_default_allow_no_rules() {
        let config = AccessPolicyConfig {
            default_allow: true,
            rules: vec![],
        };
        let policy = AccessPolicy::new(&config).unwrap();
        let request = Route::ApiVersion;
        let identity = ClientIdentity::default();

        let result = policy.evaluate(&request, &identity);
        assert!(result.unwrap());
    }

    #[test]
    fn test_access_policy_default_deny_no_rules() {
        let config = AccessPolicyConfig {
            default_allow: false,
            rules: vec![],
        };
        let policy = AccessPolicy::new(&config).unwrap();
        let request = Route::ApiVersion;
        let identity = ClientIdentity::default();

        let result = policy.evaluate(&request, &identity);
        assert!(!result.unwrap());
    }

    #[test]
    fn test_access_policy_default_allow_with_deny_rule() {
        let config = AccessPolicyConfig {
            default_allow: true,
            rules: vec!["identity.username == 'forbidden'".to_string()],
        };
        let policy = AccessPolicy::new(&config).unwrap();

        let request = Route::ApiVersion;
        let identity = ClientIdentity {
            username: Some("forbidden".to_string()),
            ..ClientIdentity::default()
        };

        let result = policy.evaluate(&request, &identity);
        assert!(!result.unwrap());

        let identity = ClientIdentity {
            username: Some("allowed".to_string()),
            ..ClientIdentity::default()
        };

        let result = policy.evaluate(&request, &identity);
        assert!(result.unwrap());
    }

    #[test]
    fn test_access_policy_default_deny_with_allow_rule() {
        let config = AccessPolicyConfig {
            default_allow: false,
            rules: vec!["identity.username == 'admin'".to_string()],
        };
        let policy = AccessPolicy::new(&config).unwrap();

        let request = Route::ApiVersion;
        let identity = ClientIdentity {
            username: Some("admin".to_string()),
            ..ClientIdentity::default()
        };

        let result = policy.evaluate(&request, &identity);
        assert!(result.unwrap());

        let identity = ClientIdentity {
            username: Some("user".to_string()),
            ..ClientIdentity::default()
        };

        let result = policy.evaluate(&request, &identity);
        assert!(!result.unwrap());
    }
}
