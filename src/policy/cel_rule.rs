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
    use super::*;
    use crate::policy::{AccessPolicyConfig, RetentionPolicyConfig};

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
}
