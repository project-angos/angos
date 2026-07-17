use thiserror::Error;

pub type GateResult<T> = Result<T, GateError>;

/// Everything that can stop a gate: a failed assertion about the store or
/// registry (`Failure`), a broken environment contract, or an infrastructure
/// error from one of the harness's own dependencies.
#[derive(Debug, Error)]
pub enum GateError {
    #[error("GATE FAILURE: {0}")]
    Failure(String),
    #[error("environment: {0}")]
    Environment(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("storage: {0}")]
    Storage(#[from] angos_storage::Error),
    #[error("s3 client: {0}")]
    S3(#[from] angos_s3_client::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Build a gate-assertion failure from a message.
pub fn fail(msg: impl Into<String>) -> GateError {
    let text = msg.into();
    GateError::Failure(text)
}

/// Assert a gate condition, producing the failure message lazily.
pub fn ensure(cond: bool, msg: impl FnOnce() -> String) -> GateResult<()> {
    if cond {
        Ok(())
    } else {
        Err(GateError::Failure(msg()))
    }
}
