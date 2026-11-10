//! CEL rules, compiled when the configuration is parsed so a bad rule fails
//! at startup rather than at request time.

use std::sync::Arc;

use cel_interpreter::{Context, ExecutionError, Program, Value};
use serde::{Deserialize, Deserializer, de};

/// A compiled CEL expression and its source.
#[derive(Debug, Clone)]
pub struct CelRule {
    program: Arc<Program>,
    pub source: Arc<str>,
}

impl CelRule {
    pub fn compile(source: &str) -> Result<Self, String> {
        if source.trim().is_empty() {
            return Err("CEL rule cannot be empty".to_string());
        }
        Program::compile(source)
            .map(|program| Self {
                program: Arc::new(program),
                source: source.into(),
            })
            .map_err(|e| format!("Failed to compile CEL rule '{source}': {e}"))
    }
}

/// Outcome of [`evaluate_rules`]; indices are 1-based. Each caller decides
/// whether `Indeterminate` fails open or closed.
pub enum RuleOutcome {
    Matched(usize),
    NoMatch,
    /// The rule returned a non-boolean or errored.
    Indeterminate {
        index: usize,
        message: String,
    },
}

/// Runs `rules` in order: the first `true` matches, and a non-boolean or an
/// error stops evaluation.
#[must_use]
pub fn evaluate_rules(rules: &[CelRule], context: &Context) -> RuleOutcome {
    for (index, rule) in rules.iter().enumerate() {
        let rule_index = index + 1;
        match rule.program.execute(context) {
            Ok(Value::Bool(true)) => return RuleOutcome::Matched(rule_index),
            Ok(Value::Bool(false)) => {}
            Ok(value) => {
                return RuleOutcome::Indeterminate {
                    index: rule_index,
                    message: format!("non-boolean result of type {}", value.type_of()),
                };
            }
            Err(e) => {
                return RuleOutcome::Indeterminate {
                    index: rule_index,
                    message: sanitize_execution_error(&e),
                };
            }
        }
    }
    RuleOutcome::NoMatch
}

/// A log-safe rendering of a CEL execution error: the interpreter's `Display`
/// prints evaluated operands, which may be attacker-controlled identity or
/// request data, so only variants without values keep its text.
fn sanitize_execution_error(error: &ExecutionError) -> String {
    match error {
        ExecutionError::InvalidArgumentCount { .. }
        | ExecutionError::UnexpectedType { .. }
        | ExecutionError::NoSuchKey(_)
        | ExecutionError::UndeclaredReference(_)
        | ExecutionError::MissingArgumentOrTarget => error.to_string(),
        ExecutionError::UnsupportedTargetType { .. } => "invalid argument type".to_string(),
        ExecutionError::NotSupportedAsMethod { method, .. } => {
            format!("method '{method}' not supported on the target type")
        }
        ExecutionError::UnsupportedKeyType(_) => "unsupported map key type".to_string(),
        ExecutionError::ValuesNotComparable(..) => "values are not comparable".to_string(),
        ExecutionError::UnsupportedUnaryOperator(op, _) => {
            format!("unsupported unary operator '{op}'")
        }
        ExecutionError::UnsupportedBinaryOperator(op, ..) => {
            format!("unsupported binary operator '{op}'")
        }
        ExecutionError::UnsupportedMapIndex(_) => "unsupported map index type".to_string(),
        ExecutionError::UnsupportedListIndex(_) => "unsupported list index type".to_string(),
        ExecutionError::UnsupportedIndex(..) => "unsupported index type".to_string(),
        ExecutionError::UnsupportedFunctionCallIdentifierType(_) => {
            "unsupported function call identifier".to_string()
        }
        ExecutionError::UnsupportedFieldsConstruction(_) => {
            "unsupported fields construction".to_string()
        }
        ExecutionError::FunctionError { function, .. } => {
            format!("error executing function '{function}'")
        }
        ExecutionError::DivisionByZero(_) => "division by zero".to_string(),
        ExecutionError::RemainderByZero(_) => "remainder by zero".to_string(),
        ExecutionError::Overflow(op, ..) => format!("overflow from binary operator '{op}'"),
        // `ExecutionError` is `#[non_exhaustive]`; a future variant might embed
        // a value, so it is not rendered.
        _ => "rule evaluation failed".to_string(),
    }
}

impl<'de> Deserialize<'de> for CelRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        Self::compile(&source).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use cel_interpreter::Context;

    use crate::policy::cel_rule::{CelRule, RuleOutcome, evaluate_rules};
    use crate::policy::{AccessMode, PolicyConfig};

    /// A value-carrying execution error must not copy the evaluated operand
    /// into the message, or a client controls what the registry logs.
    #[test]
    fn an_execution_error_never_logs_the_evaluated_value() {
        let secret = "SENSITIVE_CLAIM_VALUE";
        let mut context = Context::default();
        context.add_variable("secret", secret).unwrap();

        // A string compared to a list is not comparable at run time.
        let rules = [CelRule::compile("secret < ['x']").unwrap()];
        let RuleOutcome::Indeterminate { message, .. } = evaluate_rules(&rules, &context) else {
            panic!("an incomparable comparison must be indeterminate");
        };
        assert!(
            !message.contains(secret),
            "the evaluated value leaked into the message: {message}"
        );
    }

    #[test]
    fn invalid_rule_fails_at_deserialize() {
        for (rule, expected) in [
            ("this is (((( not valid", "this is (((( not valid"),
            ("", "cannot be empty"),
            ("   \n\t  ", "cannot be empty"),
        ] {
            let toml = format!("rules = [{rule:?}]");
            let Err(error) = toml::from_str::<PolicyConfig<AccessMode>>(&toml) else {
                panic!("{rule:?} must fail to deserialize");
            };
            assert!(error.to_string().contains(expected), "{rule:?}: {error}");
        }
    }
}
