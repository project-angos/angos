use std::io::Error as IoError;

use angos_s3_client::Error as S3Error;
use angos_storage::Error as StorageError;
use reqwest::Error as HttpError;
use serde_json::Error as JsonError;
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
    Io(#[from] IoError),
    #[error("http: {0}")]
    Http(#[from] HttpError),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("s3 client: {0}")]
    S3(#[from] S3Error),
    #[error("json: {0}")]
    Json(#[from] JsonError),
}

/// Assert a gate condition, producing the failure message lazily.
pub fn ensure(cond: bool, msg: impl FnOnce() -> String) -> GateResult<()> {
    if cond {
        Ok(())
    } else {
        Err(GateError::Failure(msg()))
    }
}
