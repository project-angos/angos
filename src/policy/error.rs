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
#[derive(Debug, thiserror::Error)]
#[error(
    "access policy{} evaluation failed: {message}",
    .rule_index.map_or(String::new(), |index| format!(" rule {index}"))
)]
pub struct PolicyError {
    /// 1-based index of the failing rule, or `None` when evaluation failed
    /// before any rule was reached (for example, context construction).
    pub rule_index: Option<usize>,
    /// Description of the underlying CEL execution error. Must not contain
    /// rule-evaluated values: an evaluated CEL value may carry data derived
    /// from identity or request fields.
    pub message: String,
}
