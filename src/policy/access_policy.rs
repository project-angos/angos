//! Access policies: CEL rules over `identity`, `request` and
//! `has_repository_policy()`, as `doc/reference/cel-expressions.md` documents.

use cel_interpreter::Context;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::identity::{Action, ClientIdentity};
use crate::policy::{
    CelRule, Error, PolicyConfig, PolicyDecision, PolicyError, RuleOutcome, evaluate_rules,
};

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AccessMode {
    #[default]
    Deny,
    Allow,
}

/// Whether the `[repository]` covering a request declares its own access
/// policy, exposed to rules as `has_repository_policy()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryPolicy {
    Declared,
    Undeclared,
}

impl From<AccessMode> for PolicyDecision {
    fn from(mode: AccessMode) -> Self {
        match mode {
            AccessMode::Allow => Self::Allow,
            AccessMode::Deny => Self::Deny,
        }
    }
}

pub struct AccessPolicy {
    default: AccessMode,
    rules: Vec<CelRule>,
}

impl AccessPolicy {
    /// A missing `default` denies.
    pub fn new(config: PolicyConfig<AccessMode>) -> Self {
        Self {
            default: config.default.unwrap_or(AccessMode::Deny),
            rules: config.rules,
        }
    }

    /// The first rule returning `true` flips `default`. A rule that returns a
    /// non-boolean or errors, or a context that fails to build, is
    /// `Indeterminate`, which callers must treat as deny so both modes fail
    /// closed.
    #[must_use]
    pub fn evaluate(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        repository_policy: RepositoryPolicy,
    ) -> PolicyDecision {
        if self.rules.is_empty() {
            return self.default.into();
        }

        let context = match Self::build_context(action, identity, repository_policy) {
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
        repository_policy: RepositoryPolicy,
    ) -> Result<Context<'a>, Error> {
        let has_repository_policy = repository_policy == RepositoryPolicy::Declared;
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

    use super::*;

    fn policy(default: AccessMode, rules: &[&str]) -> AccessPolicy {
        AccessPolicy::new(PolicyConfig {
            default: Some(default),
            rules: rules.iter().map(|s| CelRule::compile(s).unwrap()).collect(),
        })
    }

    fn decide(policy: &AccessPolicy, action: &Action, identity: &ClientIdentity) -> &'static str {
        match policy.evaluate(action, identity, RepositoryPolicy::Undeclared) {
            PolicyDecision::Allow => "allow",
            PolicyDecision::Deny => "deny",
            PolicyDecision::Indeterminate(_) => "indeterminate",
        }
    }

    #[test]
    fn rules_flip_the_default_and_fail_closed() {
        let cases: [(AccessMode, &[&str], &str); 9] = [
            (AccessMode::Deny, &[], "deny"),
            (AccessMode::Allow, &["false", "false"], "allow"),
            (AccessMode::Deny, &["false", "false"], "deny"),
            (AccessMode::Allow, &["true", "false"], "deny"),
            (AccessMode::Deny, &["false", "true"], "allow"),
            (AccessMode::Allow, &["42"], "indeterminate"),
            // A non-boolean stops evaluation even when a later rule would allow.
            (AccessMode::Deny, &["42", "true"], "indeterminate"),
            (AccessMode::Allow, &["nonexistent_var"], "indeterminate"),
            (AccessMode::Deny, &["nonexistent_var"], "indeterminate"),
        ];
        for (default, rules, expected) in cases {
            let decision = decide(
                &policy(default, rules),
                &Action::ApiVersion,
                &ClientIdentity::default(),
            );
            assert_eq!(decision, expected, "{default:?} with {rules:?}");
        }
    }

    #[test]
    fn indeterminate_carries_the_failing_rule_index() {
        let PolicyDecision::Indeterminate(err) =
            policy(AccessMode::Allow, &["false", "nonexistent_var"]).evaluate(
                &Action::ApiVersion,
                &ClientIdentity::default(),
                RepositoryPolicy::Undeclared,
            )
        else {
            panic!("expected Indeterminate");
        };
        assert_eq!(err.rule_index, Some(2));
    }

    #[test]
    fn default_is_deny_and_must_be_known() {
        let config: PolicyConfig<AccessMode> = toml::from_str("").unwrap();
        assert_eq!(
            decide(
                &AccessPolicy::new(config),
                &Action::ApiVersion,
                &ClientIdentity::default()
            ),
            "deny"
        );
        assert!(toml::from_str::<PolicyConfig<AccessMode>>("default = \"maybe\"").is_err());
    }

    /// A mount authorizes as `mount-blob`, independent of the `start-upload`
    /// rules, and `request.from` exists only when `?from=` was given, so rules
    /// need `has(request.from)`.
    #[test]
    fn cross_repo_mount_is_gated_separately() {
        let target = Namespace::new("team/app").unwrap();
        let digest = Digest::try_from(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let mount = |from: Option<&str>| Action::MountBlob {
            namespace: target.clone(),
            digest: digest.clone(),
            from: from.map(|from| Namespace::new(from).unwrap()),
        };
        let upload = Action::StartUpload {
            namespace: target.clone(),
            digest: None,
            digest_algorithm: None,
        };
        let alice = ClientIdentity {
            username: Some("alice".to_string()),
            ..ClientIdentity::default()
        };
        let replicator = ClientIdentity {
            id: Some("replicator".to_string()),
            username: Some("svc".to_string()),
            ..ClientIdentity::default()
        };

        let replicator_only = policy(
            AccessMode::Deny,
            &[
                "identity.username != null && request.action == 'start-upload'",
                "identity.id == 'replicator' && request.action == 'mount-blob'",
            ],
        );
        let deny_untrusted = policy(
            AccessMode::Allow,
            &[
                "request.action == 'mount-blob' && has(request.from) && request.from == 'other/evil'",
            ],
        );
        let only_trusted = policy(
            AccessMode::Deny,
            &["request.action == 'mount-blob' && has(request.from) && request.from == 'team/base'"],
        );

        let cases = [
            (&replicator_only, &upload, &alice, "allow"),
            (&replicator_only, &mount(Some("team/base")), &alice, "deny"),
            (
                &replicator_only,
                &mount(Some("team/base")),
                &replicator,
                "allow",
            ),
            (&deny_untrusted, &upload, &alice, "allow"),
            (&deny_untrusted, &mount(Some("team/base")), &alice, "allow"),
            (&deny_untrusted, &mount(Some("other/evil")), &alice, "deny"),
            (&deny_untrusted, &mount(None), &alice, "allow"),
            (&only_trusted, &mount(Some("team/base")), &alice, "allow"),
            (&only_trusted, &mount(Some("other/evil")), &alice, "deny"),
            (&only_trusted, &mount(None), &alice, "deny"),
        ];
        for (index, (policy, action, identity, expected)) in cases.into_iter().enumerate() {
            assert_eq!(decide(policy, action, identity), expected, "case {index}");
        }
    }

    /// `request.reference` is documented as a string; a tagged-object
    /// serialization would make every such rule silently never fire.
    #[test]
    fn reference_is_a_string_in_the_policy_context() {
        let action = Action::GetManifest {
            namespace: Namespace::new("team/app").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
        };
        let policy = policy(AccessMode::Deny, &["request.reference == 'latest'"]);
        assert_eq!(
            decide(&policy, &action, &ClientIdentity::default()),
            "allow"
        );
    }

    #[test]
    fn has_repository_policy_reflects_the_covering_repository() {
        let policy = policy(AccessMode::Deny, &["has_repository_policy()"]);
        let identity = ClientIdentity::default();
        assert!(matches!(
            policy.evaluate(&Action::ApiVersion, &identity, RepositoryPolicy::Declared),
            PolicyDecision::Allow
        ));
        assert!(matches!(
            policy.evaluate(&Action::ApiVersion, &identity, RepositoryPolicy::Undeclared),
            PolicyDecision::Deny
        ));
    }
}
