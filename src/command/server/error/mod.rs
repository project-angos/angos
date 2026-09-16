use hyper::StatusCode;
use serde_json::json;

use angos_oci::response::{ErrorCode, ErrorInfo, ErrorResponse};
use angos_transport::RenderError;

mod conversions;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Initialization(String),
    #[error("{0}")]
    Execution(String),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Range Not Satisfiable: {0}")]
    RangeNotSatisfiable(String),
    #[error("Not Found: {0}")]
    NotFound(String),
    #[error("Provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("Internal Server Error: {0}")]
    Internal(String),
    #[error("failed to build HTTP response: {0}")]
    HttpBuild(#[from] hyper::http::Error),
    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] hyper::header::InvalidHeaderValue),
    #[error("failed to serialize response body: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Error {status_code}: {code}{}", msg.as_deref().map(|m| format!(" - {m}")).unwrap_or_default())]
    Custom {
        status_code: StatusCode,
        code: String,
        msg: Option<String>,
    },
}

impl From<RenderError> for Error {
    fn from(error: RenderError) -> Self {
        match error {
            RenderError::Header(e) => Error::HttpBuild(e),
            RenderError::Serialize(e) => Error::Serialization(e),
        }
    }
}

/// Codes the spec does not define, which angos serves on failures the OCI set
/// has no word for. A 5xx code is unconstrained, so these never collide.
pub const INTERNAL_ERROR_CODE: &str = "INTERNAL_ERROR";
pub const PROVIDER_UNAVAILABLE_CODE: &str = "PROVIDER_UNAVAILABLE";
pub const RECLAMATION_IN_PROGRESS_CODE: &str = "RECLAMATION_IN_PROGRESS";

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::RangeNotSatisfiable(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::ProviderUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::Initialization(_)
            | Error::Execution(_)
            | Error::Internal(_)
            | Error::HttpBuild(_)
            | Error::InvalidHeader(_)
            | Error::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Custom { status_code, .. } => *status_code,
        }
    }

    /// The error answer's body. A 4XX `code` must come from the spec's fixed
    /// set, so each case picks the closest of those; a 5XX code is unconstrained.
    pub fn error_body(&self, request_id: Option<&String>) -> ErrorResponse {
        // A 5xx body carries no message: an internal error string must never
        // leak to the client, which gets the code plus a request id instead.
        let (code, message) = match self {
            Error::Unauthorized(msg) => (ErrorCode::Unauthorized.as_str(), Some(msg.as_str())),
            Error::BadRequest(msg) => (ErrorCode::Unsupported.as_str(), Some(msg.as_str())),
            Error::RangeNotSatisfiable(msg) => {
                (ErrorCode::SizeInvalid.as_str(), Some(msg.as_str()))
            }
            Error::NotFound(msg) => (ErrorCode::NameUnknown.as_str(), Some(msg.as_str())),
            Error::ProviderUnavailable(_) => (PROVIDER_UNAVAILABLE_CODE, None),
            Error::Initialization(_)
            | Error::Execution(_)
            | Error::Internal(_)
            | Error::HttpBuild(_)
            | Error::InvalidHeader(_)
            | Error::Serialization(_) => (INTERNAL_ERROR_CODE, None),
            Error::Custom {
                status_code,
                code,
                msg,
            } => {
                let message = if status_code.is_server_error() {
                    None
                } else {
                    msg.as_deref()
                };
                (code.as_str(), message)
            }
        };

        ErrorResponse {
            errors: vec![ErrorInfo {
                code: code.to_string(),
                message: message.map(ToString::to_string),
                detail: request_id.map(|request_id| json!({ "request_id": request_id })),
            }],
        }
    }
}

#[cfg(test)]
mod tests;
