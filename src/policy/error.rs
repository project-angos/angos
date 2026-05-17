use std::fmt;

use cel_interpreter::SerializationError;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("Policy evaluation error: {0}")]
    Evaluation(String),
}

impl From<SerializationError> for Error {
    fn from(e: SerializationError) -> Self {
        Self::Evaluation(e.to_string())
    }
}

/// The outcome of evaluating an access policy against a request.
#[derive(Debug)]
pub enum PolicyDecision {
    /// Access is granted.
    Allow,
    /// Access is denied by a matching rule.
    Deny,
    /// A rule could not be evaluated; the request is denied and the error is carried for logging.
    Indeterminate(PolicyError),
}

/// Describes why a policy evaluation produced an `Indeterminate` decision.
#[derive(Debug)]
pub struct PolicyError {
    /// 1-based index of the failing rule, or `None` when evaluation failed
    /// before any rule was reached (for example, context construction).
    pub rule_index: Option<usize>,
    /// Description of the underlying CEL execution error. Must not contain
    /// rule-evaluated values: an evaluated CEL value may carry data derived
    /// from identity or request fields.
    pub message: String,
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.rule_index {
            Some(idx) => write!(
                f,
                "access policy rule {idx} evaluation failed: {}",
                self.message
            ),
            None => write!(f, "access policy evaluation failed: {}", self.message),
        }
    }
}

impl std::error::Error for PolicyError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluation_display_matches_original() {
        let err = Error::Evaluation("something went wrong".to_string());
        assert_eq!(
            err.to_string(),
            "Policy evaluation error: something went wrong"
        );
    }

    #[test]
    fn policy_error_display_with_rule_index() {
        let err = PolicyError {
            rule_index: Some(3),
            message: "UndeclaredReference".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "access policy rule 3 evaluation failed: UndeclaredReference"
        );
    }

    #[test]
    fn policy_error_display_without_rule_index() {
        let err = PolicyError {
            rule_index: None,
            message: "context build failed".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "access policy evaluation failed: context build failed"
        );
    }
}
