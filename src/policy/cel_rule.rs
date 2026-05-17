//! CEL rule newtype that compiles at deserialisation time.
//!
//! Invalid CEL syntax is detected when the TOML configuration is parsed,
//! so the registry fails fast with a clear error rather than at request time.

use std::{fmt, sync::Arc};

use cel_interpreter::{Context, ExecutionError, Program, Value};
use serde::{Deserialize, de};

/// A pre-compiled CEL expression.
///
/// Wraps [`Program`] in an [`Arc`] so that [`CelRule`] implements [`Clone`]
/// without requiring [`Program`] itself to be cloneable.
#[derive(Debug, Clone)]
pub struct CelRule(Arc<Program>);

impl CelRule {
    /// Compiles a CEL expression from source.
    ///
    /// Returns `Err` with a human-readable message when the source is not
    /// valid CEL.
    ///
    /// # Errors
    ///
    /// Returns an error string describing the parse failure.
    pub fn compile(source: &str) -> Result<Self, String> {
        if source.trim().is_empty() {
            return Err("CEL rule cannot be empty".to_string());
        }
        Program::compile(source)
            .map(|program| Self(Arc::new(program)))
            .map_err(|e| format!("Failed to compile CEL rule '{source}': {e}"))
    }

    /// Executes the compiled program against a CEL context.
    pub fn execute(&self, context: &Context) -> Result<Value, ExecutionError> {
        self.0.execute(context)
    }
}

impl fmt::Display for CelRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<cel_rule>")
    }
}

impl<'de> Deserialize<'de> for CelRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        Self::compile(&source).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use cel_interpreter::{Context, ExecutionError};
    use serde::de::DeserializeOwned;

    use crate::policy::{AccessPolicyConfig, CelRule, RetentionPolicyConfig};

    #[test]
    fn valid_cel_rule_compiles() {
        assert!(CelRule::compile("1 + 1 == 2").is_ok());
    }

    #[test]
    fn invalid_cel_rule_fails_compile() {
        assert!(CelRule::compile("this is (((( not valid").is_err());
    }

    #[test]
    fn invalid_access_policy_cel_rule_fails_at_deserialize() {
        let toml = r#"rules = ["this is (((( not valid"]"#;
        let result: Result<AccessPolicyConfig, _> = toml::from_str(toml);
        assert!(
            result.is_err(),
            "invalid CEL rule must fail at deserialization"
        );
    }

    #[test]
    fn valid_access_policy_cel_rule_deserializes() {
        let toml = r#"rules = ["identity.username == 'admin'"]"#;
        let result: Result<AccessPolicyConfig, _> = toml::from_str(toml);
        assert!(
            result.is_ok(),
            "valid CEL rule must deserialize successfully"
        );
    }

    #[test]
    fn invalid_retention_policy_cel_rule_fails_at_deserialize() {
        let toml = r#"rules = ["this is (((( not valid"]"#;
        let result: Result<RetentionPolicyConfig, _> = toml::from_str(toml);
        assert!(
            result.is_err(),
            "invalid CEL rule must fail at deserialization"
        );
    }

    #[test]
    fn valid_retention_policy_cel_rule_deserializes() {
        let toml = r#"rules = ["image.pushed_at > now() - days(30)"]"#;
        let result: Result<RetentionPolicyConfig, _> = toml::from_str(toml);
        assert!(
            result.is_ok(),
            "valid CEL rule must deserialize successfully"
        );
    }

    #[test]
    fn empty_cel_rule_fails_compile() {
        assert!(CelRule::compile("").is_err());
    }

    #[test]
    fn whitespace_only_cel_rule_fails_compile() {
        assert!(CelRule::compile("   \n\t  ").is_err());
    }

    fn assert_empty_rule_fails_at_deserialize<C: DeserializeOwned>() {
        let toml = r#"rules = [""]"#;
        let result: Result<C, _> = toml::from_str(toml);
        let msg = result
            .err()
            .expect("empty CEL rule must fail at deserialization")
            .to_string();
        assert!(
            msg.contains("empty"),
            "error should mention empty, got: {msg}"
        );
    }

    #[test]
    fn empty_access_policy_cel_rule_fails_at_deserialize() {
        assert_empty_rule_fails_at_deserialize::<AccessPolicyConfig>();
    }

    #[test]
    fn empty_retention_policy_cel_rule_fails_at_deserialize() {
        assert_empty_rule_fails_at_deserialize::<RetentionPolicyConfig>();
    }

    #[test]
    fn undefined_variable_compiles_lazily() {
        assert!(
            CelRule::compile("nonexistent_var").is_ok(),
            "cel_interpreter defers variable resolution to execute time"
        );
    }

    #[test]
    fn undefined_variable_fails_at_execute_time() {
        let rule =
            CelRule::compile("nonexistent_var").expect("expression with unknown var must compile");
        let ctx = Context::default();
        let result = rule.execute(&ctx);
        assert!(
            matches!(result, Err(ExecutionError::UndeclaredReference(_))),
            "expected UndeclaredReference error at execute time, got: {result:?}"
        );
    }
}
