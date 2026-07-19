use hyper::StatusCode;
use serde_json::json;

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
    #[error("Conflict: {0}")]
    Conflict(String),
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
    #[error("failed to serialize response body: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Error {status_code}: {code}{}", msg.as_deref().map(|m| format!(" - {m}")).unwrap_or_default())]
    Custom {
        status_code: StatusCode,
        code: String,
        msg: Option<String>,
    },
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::RangeNotSatisfiable(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::ProviderUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Error::Initialization(_)
            | Error::Execution(_)
            | Error::Internal(_)
            | Error::HttpBuild(_)
            | Error::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Custom { status_code, .. } => *status_code,
        }
    }

    pub fn as_json(&self, request_id: Option<&String>) -> serde_json::Value {
        // A 5xx body carries no message: an internal error string must never
        // leak to the client. The full detail is logged server-side (see
        // `error_for_log`); the client gets the code plus a request id.
        let (code, message) = match self {
            Error::Unauthorized(msg) => ("UNAUTHORIZED", Some(msg.as_str())),
            Error::BadRequest(msg) => ("BAD_REQUEST", Some(msg.as_str())),
            Error::Conflict(msg) => ("CONFLICT", Some(msg.as_str())),
            Error::RangeNotSatisfiable(msg) => ("RANGE_NOT_SATISFIABLE", Some(msg.as_str())),
            Error::NotFound(msg) => ("NOT_FOUND", Some(msg.as_str())),
            Error::ProviderUnavailable(_) => ("PROVIDER_UNAVAILABLE", None),
            Error::Initialization(_)
            | Error::Execution(_)
            | Error::Internal(_)
            | Error::HttpBuild(_)
            | Error::Serialization(_) => ("INTERNAL_ERROR", None),
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

        if let Some(request_id) = request_id {
            json!({
                "errors": [{
                    "code": code,
                    "message": message,
                    "detail": { "request_id": request_id }
                }]
            })
        } else {
            json!({
                "errors": [{
                    "code": code,
                    "message": message,
                }]
            })
        }
    }
}

#[cfg(test)]
mod tests;
