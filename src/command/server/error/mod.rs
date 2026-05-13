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

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::Initialization(a), Error::Initialization(b))
            | (Error::Execution(a), Error::Execution(b))
            | (Error::Unauthorized(a), Error::Unauthorized(b))
            | (Error::BadRequest(a), Error::BadRequest(b))
            | (Error::Conflict(a), Error::Conflict(b))
            | (Error::RangeNotSatisfiable(a), Error::RangeNotSatisfiable(b))
            | (Error::NotFound(a), Error::NotFound(b))
            | (Error::Internal(a), Error::Internal(b)) => a == b,
            (Error::HttpBuild(a), Error::HttpBuild(b)) => std::ptr::eq(a, b),
            (Error::Serialization(a), Error::Serialization(b)) => std::ptr::eq(a, b),
            (
                Error::Custom {
                    status_code: sc_a,
                    code: c_a,
                    msg: m_a,
                },
                Error::Custom {
                    status_code: sc_b,
                    code: c_b,
                    msg: m_b,
                },
            ) => sc_a == sc_b && c_a == c_b && m_a == m_b,
            _ => false,
        }
    }
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::RangeNotSatisfiable(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Initialization(_)
            | Error::Execution(_)
            | Error::Internal(_)
            | Error::HttpBuild(_)
            | Error::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Custom { status_code, .. } => *status_code,
        }
    }

    pub fn as_json(&self, request_id: Option<&String>) -> serde_json::Value {
        let (code, message) = match self {
            Error::Unauthorized(msg) => ("UNAUTHORIZED", Some(msg.as_str())),
            Error::BadRequest(msg) => ("BAD_REQUEST", Some(msg.as_str())),
            Error::Conflict(msg) => ("CONFLICT", Some(msg.as_str())),
            Error::RangeNotSatisfiable(msg) => ("RANGE_NOT_SATISFIABLE", Some(msg.as_str())),
            Error::NotFound(msg) => ("NOT_FOUND", Some(msg.as_str())),
            Error::Initialization(msg) | Error::Execution(msg) | Error::Internal(msg) => {
                ("INTERNAL_ERROR", Some(msg.as_str()))
            }
            Error::HttpBuild(_) | Error::Serialization(_) => ("INTERNAL_ERROR", None),
            Error::Custom { code, msg, .. } => (code.as_str(), msg.as_deref()),
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
