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

    // cel_interpreter (via the underlying antlr4rust parser) panics on an
    // empty source string rather than returning a parse error.  This is a
    // known limitation of the upstream library: the ANTLR-generated parser
    // hits an `unreachable!()` branch when the token stream is empty.
    // The test documents this behaviour so a future library upgrade that
    // converts the panic into an Err is detected immediately.
    #[test]
    #[should_panic(expected = "internal error: entered unreachable code")]
    fn empty_expression_panics_in_upstream_parser() {
        let _ = CelRule::compile("");
    }

    // The cel_interpreter library is lazy about variable resolution: it does
    // not check whether identifiers exist at compile time.  An expression
    // referencing an undeclared variable compiles without error.
    #[test]
    fn undefined_variable_compiles_lazily() {
        assert!(
            CelRule::compile("nonexistent_var").is_ok(),
            "cel_interpreter defers variable resolution to execute time"
        );
    }

    // Variable resolution errors surface at execute time, not compile time.
    // This test pins the runtime error variant so a change in error-reporting
    // behaviour is caught by CI.
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
