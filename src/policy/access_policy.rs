//! Access control policy evaluation for registry operations.
//!
//! Rules are CEL expressions compiled at configuration load time and evaluated
//! against `identity`, `request` and `has_repository_policy()`, which
//! `doc/reference/cel-expressions.md` documents.

use cel_interpreter::Context;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::identity::{Action, ClientIdentity};
use crate::policy::{
    CelRule, Error, PolicyConfig, PolicyDecision, PolicyError, RuleOutcome, evaluate_rules,
};

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
    /// Rules are already compiled, so this constructor is infallible; a
    /// missing `default` denies.
    pub fn new(config: PolicyConfig<AccessMode>) -> Self {
        Self {
            default: config.default.unwrap_or(AccessMode::Deny),
            rules: config.rules,
        }
    }

    /// Rules run in order and the first returning `true` flips the `default`
    /// decision, which otherwise stands. A rule returning a non-boolean or
    /// throwing stops evaluation as `Indeterminate`, which callers must treat as
    /// deny: that is what makes both modes fail closed. A context that fails to
    /// build is `Indeterminate` with no rule index, since no rule ran.
    ///
    /// `has_repository_policy` is exposed to rules as `has_repository_policy()`:
    /// whether a `[repository]` declaring its own access policy decides this
    /// request too.
    pub fn evaluate(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        has_repository_policy: bool,
    ) -> PolicyDecision {
        if self.rules.is_empty() {
            return self.default.into();
        }

        let context = match Self::build_context(action, identity, has_repository_policy) {
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

        match evaluate_rules(&self.rules, &context) {
            RuleOutcome::Matched(index) => {
                debug!("{rule_kind} rule {index} matched");
                match self.default {
                    AccessMode::Allow => PolicyDecision::Deny,
                    AccessMode::Deny => PolicyDecision::Allow,
                }
            }
            RuleOutcome::NoMatch => self.default.into(),
            RuleOutcome::Indeterminate { index, message } => {
                // Fail-closed: a misconfigured or failing rule denies.
                warn!("Access policy {rule_kind} rule {index} is indeterminate: {message}");
                PolicyDecision::Indeterminate(PolicyError {
                    rule_index: Some(index),
                    message,
                })
            }
        }
    }

    fn build_context<'a>(
        action: &'a Action,
        identity: &'a ClientIdentity,
        has_repository_policy: bool,
    ) -> Result<Context<'a>, Error> {
        let mut context = Context::default();
        context.add_variable("request", action)?;
        context.add_variable("identity", identity)?;
        context.add_function("has_repository_policy", move || has_repository_policy);
        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use angos_oci::{Digest, Namespace, Reference, Tag};

    use crate::policy::access_policy::*;

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
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_allow(&policy.evaluate(&action, &identity, false)));
    }

    #[test]
    fn test_access_policy_deny_mode_no_rules() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_deny(&policy.evaluate(&action, &identity, false)));
    }

    #[test]
    fn test_access_policy_allow_mode_with_deny_rule() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule("identity.username == 'forbidden'")],
        });

        let action = Action::ApiVersion;
        let identity = ClientIdentity {
            username: Some("forbidden".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_deny(&policy.evaluate(&action, &identity, false)));

        let identity = ClientIdentity {
            username: Some("allowed".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_allow(&policy.evaluate(&action, &identity, false)));
    }

    #[test]
    fn test_access_policy_deny_mode_with_allow_rule() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("identity.username == 'admin'")],
        });

        let action = Action::ApiVersion;
        let identity = ClientIdentity {
            username: Some("admin".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_allow(&policy.evaluate(&action, &identity, false)));

        let identity = ClientIdentity {
            username: Some("user".to_string()),
            ..ClientIdentity::default()
        };

        assert!(is_deny(&policy.evaluate(&action, &identity, false)));
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
            digest_algorithm: None,
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

        let replicator_only = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![
                rule("identity.username != null && request.action == 'start-upload'"),
                rule("identity.id == 'replicator' && request.action == 'mount-blob'"),
            ],
        });
        assert!(is_allow(&replicator_only.evaluate(
            &normal_upload,
            &anyone,
            false
        )));
        assert!(is_deny(&replicator_only.evaluate(&mount, &anyone, false)));
        assert!(is_allow(&replicator_only.evaluate(
            &mount,
            &replicator,
            false
        )));

        let deny_non_replicator = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule(
                "request.action == 'mount-blob' && identity.id != 'replicator'",
            )],
        });
        assert!(is_allow(&deny_non_replicator.evaluate(
            &normal_upload,
            &anyone,
            false
        )));
        assert!(is_deny(
            &deny_non_replicator.evaluate(&mount, &anyone, false)
        ));
        assert!(is_allow(&deny_non_replicator.evaluate(
            &mount,
            &replicator,
            false
        )));
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
        let only_from_trusted = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule(
                "request.action == 'mount-blob' && has(request.from) && request.from == 'team/base'",
            )],
        });
        assert!(is_allow(&only_from_trusted.evaluate(
            &from_trusted,
            &client,
            false
        )));
        assert!(is_deny(&only_from_trusted.evaluate(
            &from_untrusted,
            &client,
            false
        )));
        assert!(is_deny(
            &only_from_trusted.evaluate(&no_from, &client, false)
        ));

        let deny_untrusted_source = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule(
                "request.action == 'mount-blob' && has(request.from) && request.from == 'other/evil'",
            )],
        });
        assert!(is_allow(&deny_untrusted_source.evaluate(
            &from_trusted,
            &client,
            false
        )));
        assert!(is_deny(&deny_untrusted_source.evaluate(
            &from_untrusted,
            &client,
            false
        )));
        assert!(is_allow(
            &deny_untrusted_source.evaluate(&no_from, &client, false)
        ));
    }

    #[test]
    fn test_access_policy_default_toml_allow() {
        let config: PolicyConfig<AccessMode> = toml::from_str("default = \"allow\"").unwrap();
        assert_eq!(config.default, Some(AccessMode::Allow));
    }

    #[test]
    fn test_access_policy_default_toml_deny() {
        let config: PolicyConfig<AccessMode> = toml::from_str("default = \"deny\"").unwrap();
        assert_eq!(config.default, Some(AccessMode::Deny));
    }

    #[test]
    fn test_access_policy_default_toml_missing_is_deny() {
        let config: PolicyConfig<AccessMode> = toml::from_str("").unwrap();
        assert_eq!(config.default, None, "denies once built");
        assert!(matches!(
            AccessPolicy::new(config).default,
            AccessMode::Deny
        ));
    }

    #[test]
    fn test_access_policy_default_toml_unknown_value_fails() {
        let result: Result<PolicyConfig<AccessMode>, _> = toml::from_str("default = \"maybe\"");
        assert!(result.is_err());
    }

    // Non-boolean and error rule behaviour

    #[test]
    fn non_boolean_rule_in_allow_mode_denies_fail_closed() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule("42")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity, false)),
            "non-boolean result must be Indeterminate (fail-closed)"
        );
    }

    #[test]
    fn non_boolean_rule_in_deny_mode_denies_fail_closed() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("42")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity, false)),
            "non-boolean result must be Indeterminate (fail-closed)"
        );
    }

    #[test]
    fn non_boolean_rule_in_deny_mode_short_circuits_subsequent_allow_rules() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("42"), rule("true")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity, false)),
            "non-boolean rule must short-circuit to Indeterminate, even when a later rule would allow"
        );
    }

    #[test]
    fn failed_rule_in_allow_mode_is_indeterminate_and_denies() {
        // Allow mode: a runtime evaluation error in a DENY rule now produces
        // Indeterminate instead of silently falling through to allow.
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity, false)),
            "a failing DENY rule in Allow mode must be Indeterminate, not Allow"
        );
    }

    #[test]
    fn failed_rule_in_allow_mode_indeterminate_carries_rule_index() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule("false"), rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        let PolicyDecision::Indeterminate(err) = policy.evaluate(&action, &identity, false) else {
            panic!("expected Indeterminate");
        };
        assert_eq!(err.rule_index, Some(2), "failing rule is the second rule");
    }

    #[test]
    fn failed_rule_in_deny_mode_is_indeterminate() {
        // Deny mode: an evaluation error in an ALLOW rule produces Indeterminate
        // (which callers treat as deny, i.e. fail-closed).
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("nonexistent_var")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(
            is_indeterminate(&policy.evaluate(&action, &identity, false)),
            "a failing ALLOW rule in Deny mode must be Indeterminate"
        );
    }

    // Multi-rule ordering and short-circuit semantics

    #[test]
    fn multi_rule_allow_mode_first_match_denies_second_rule_unreached() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![
                rule("true"),  // rule 1: always triggers → Deny
                rule("false"), // rule 2: unreachable
            ],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_deny(&policy.evaluate(&action, &identity, false)));
    }

    #[test]
    fn multi_rule_deny_mode_first_match_allows_second_rule_unreached() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![
                rule("true"),  // rule 1: always triggers → Allow
                rule("false"), // rule 2: unreachable
            ],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();

        assert!(is_allow(&policy.evaluate(&action, &identity, false)));
    }

    #[test]
    fn multi_rule_no_match_falls_through_to_default() {
        let policy_allow = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Allow),
            rules: vec![rule("false"), rule("false")],
        });
        let action = Action::ApiVersion;
        let identity = ClientIdentity::default();
        assert!(is_allow(&policy_allow.evaluate(&action, &identity, false)));

        let policy_deny = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("false"), rule("false")],
        });
        assert!(is_deny(&policy_deny.evaluate(&action, &identity, false)));
    }

    /// `request.reference` is documented as a string, so a rule comparing it to
    /// one has to match. A tagged-object serialization would make it a map and
    /// every such policy would silently never fire.
    #[test]
    fn reference_is_a_string_in_the_policy_context() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("request.reference == 'latest'")],
        });
        let action = Action::GetManifest {
            namespace: Namespace::new("team/app").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
        };

        assert!(is_allow(&policy.evaluate(
            &action,
            &ClientIdentity::default(),
            false
        )));
    }

    /// The global policy hands a namespace over to the repository that declares
    /// its own rules, and keeps deciding for every namespace that has none.
    #[test]
    fn has_repository_policy_defers_to_a_declaring_repository() {
        let policy = AccessPolicy::new(PolicyConfig {
            default: Some(AccessMode::Deny),
            rules: vec![rule("has_repository_policy()")],
        });
        let action = Action::StartUpload {
            namespace: Namespace::new("team/app").unwrap(),
            digest: None,
            digest_algorithm: None,
        };
        let identity = ClientIdentity::default();

        assert!(is_allow(&policy.evaluate(&action, &identity, true)));
        assert!(is_deny(&policy.evaluate(&action, &identity, false)));
    }
}
