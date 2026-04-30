use hyper::StatusCode;
use serde_json::json;

use crate::{configuration, registry};

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

impl From<registry::Error> for Error {
    fn from(error: registry::Error) -> Self {
        match error {
            registry::Error::Initialization(msg) => Error::Initialization(msg),
            registry::Error::BlobUnknown => Error::Custom {
                status_code: StatusCode::NOT_FOUND,
                code: "BLOB_UNKNOWN".to_string(),
                msg: None,
            },
            registry::Error::BlobUploadUnknown => Error::Custom {
                status_code: StatusCode::NOT_FOUND,
                code: "BLOB_UPLOAD_UNKNOWN".to_string(),
                msg: None,
            },
            registry::Error::DigestInvalid => Error::Custom {
                status_code: StatusCode::BAD_REQUEST,
                code: "DIGEST_INVALID".to_string(),
                msg: None,
            },
            registry::Error::ManifestBlobUnknown => Error::Custom {
                status_code: StatusCode::NOT_FOUND,
                code: "MANIFEST_BLOB_UNKNOWN".to_string(),
                msg: None,
            },
            registry::Error::ManifestInvalid(msg) => Error::Custom {
                status_code: StatusCode::BAD_REQUEST,
                code: "MANIFEST_INVALID".to_string(),
                msg: Some(msg),
            },
            registry::Error::ManifestUnknown => Error::Custom {
                status_code: StatusCode::NOT_FOUND,
                code: "MANIFEST_UNKNOWN".to_string(),
                msg: None,
            },
            registry::Error::NameInvalid => Error::Custom {
                status_code: StatusCode::BAD_REQUEST,
                code: "NAME_INVALID".to_string(),
                msg: None,
            },
            registry::Error::NameUnknown => Error::Custom {
                status_code: StatusCode::NOT_FOUND,
                code: "NAME_UNKNOWN".to_string(),
                msg: None,
            },
            registry::Error::Unauthorized(msg) => Error::Custom {
                status_code: StatusCode::UNAUTHORIZED,
                code: "UNAUTHORIZED".to_string(),
                msg: Some(msg),
            },
            registry::Error::Denied(msg) => Error::Custom {
                status_code: StatusCode::FORBIDDEN,
                code: "DENIED".to_string(),
                msg: Some(msg),
            },
            registry::Error::Unsupported => Error::Custom {
                status_code: StatusCode::BAD_REQUEST,
                code: "UNSUPPORTED".to_string(),
                msg: None,
            },
            registry::Error::RangeNotSatisfiable => Error::Custom {
                status_code: StatusCode::RANGE_NOT_SATISFIABLE,
                code: "SIZE_INVALID".to_string(),
                msg: None,
            },
            registry::Error::Internal(msg) => Error::Custom {
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                code: "INTERNAL_SERVER_ERROR".to_string(),
                msg: Some(msg),
            },
            _ => Error::Custom {
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                code: "INTERNAL_SERVER_ERROR".to_string(),
                msg: Some(error.to_string()),
            },
        }
    }
}

impl From<configuration::Error> for Error {
    fn from(error: configuration::Error) -> Self {
        match error {
            configuration::Error::Initialization(msg)
            | configuration::Error::InvalidFormat(msg)
            | configuration::Error::NotReadable(msg) => Error::Internal(msg),
        }
    }
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
            (Error::HttpBuild(a), Error::HttpBuild(b)) => a.to_string() == b.to_string(),
            (Error::Serialization(a), Error::Serialization(b)) => a.to_string() == b.to_string(),
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
                ("INTERNAL_SERVER_ERROR", Some(msg.as_str()))
            }
            Error::HttpBuild(_) | Error::Serialization(_) => ("INTERNAL_SERVER_ERROR", None),
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
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = Error::Initialization("Some init error".to_string());
        assert_eq!(format!("{error}"), "Some init error");

        let error = Error::Execution("Some init error".to_string());
        assert_eq!(format!("{error}"), "Some init error");

        let error = Error::Unauthorized("Invalid token".to_string());
        assert_eq!(format!("{error}"), "Unauthorized: Invalid token");

        let error = Error::BadRequest("Malformed request".to_string());
        assert_eq!(format!("{error}"), "Bad Request: Malformed request");

        let error = Error::Conflict("Resource already exists".to_string());
        assert_eq!(format!("{error}"), "Conflict: Resource already exists");

        let error = Error::RangeNotSatisfiable("Invalid range '-'".to_string());
        assert_eq!(
            format!("{error}"),
            "Range Not Satisfiable: Invalid range '-'"
        );

        let error = Error::NotFound("Item not found".to_string());
        assert_eq!(format!("{error}"), "Not Found: Item not found");

        let error = Error::Internal("Unexpected error".to_string());
        assert_eq!(
            format!("{error}"),
            "Internal Server Error: Unexpected error"
        );

        let error = Error::Custom {
            status_code: StatusCode::BAD_GATEWAY,
            code: "UPSTREAM_ERROR".to_string(),
            msg: Some("Failed to connect".to_string()),
        };
        assert_eq!(
            format!("{error}"),
            "Error 502 Bad Gateway: UPSTREAM_ERROR - Failed to connect"
        );

        let error = Error::Custom {
            status_code: StatusCode::SERVICE_UNAVAILABLE,
            code: "SERVICE_UNAVAILABLE".to_string(),
            msg: None,
        };
        assert_eq!(
            format!("{error}"),
            "Error 503 Service Unavailable: SERVICE_UNAVAILABLE"
        );
    }

    #[test]
    fn test_as_json_with_request_id() {
        let error = Error::BadRequest("Missing parameter".to_string());
        let request_id = Some("req-12345".to_string());
        let json = error.as_json(request_id.as_ref());

        assert_eq!(json["errors"][0]["code"], "BAD_REQUEST");
        assert_eq!(json["errors"][0]["message"], "Missing parameter");
        assert_eq!(json["errors"][0]["detail"]["request_id"], "req-12345");
    }

    #[test]
    fn test_as_json_all_error_types() {
        let errors = vec![
            (
                Error::Unauthorized("auth error".to_string()),
                "UNAUTHORIZED",
                "auth error",
            ),
            (
                Error::BadRequest("bad request".to_string()),
                "BAD_REQUEST",
                "bad request",
            ),
            (
                Error::Conflict("conflict".to_string()),
                "CONFLICT",
                "conflict",
            ),
            (
                Error::RangeNotSatisfiable("range".to_string()),
                "RANGE_NOT_SATISFIABLE",
                "range",
            ),
            (
                Error::NotFound("not found".to_string()),
                "NOT_FOUND",
                "not found",
            ),
            (
                Error::Initialization("init".to_string()),
                "INTERNAL_SERVER_ERROR",
                "init",
            ),
            (
                Error::Execution("exec".to_string()),
                "INTERNAL_SERVER_ERROR",
                "exec",
            ),
            (
                Error::Internal("internal".to_string()),
                "INTERNAL_SERVER_ERROR",
                "internal",
            ),
        ];

        for (error, expected_code, expected_message) in errors {
            let json = error.as_json(None);
            assert_eq!(json["errors"][0]["code"], expected_code);
            assert_eq!(json["errors"][0]["message"], expected_message);
        }
    }

    #[test]
    fn test_as_json_custom_error() {
        let error = Error::Custom {
            status_code: StatusCode::BAD_GATEWAY,
            code: "UPSTREAM_TIMEOUT".to_string(),
            msg: Some("Backend timed out".to_string()),
        };
        let json = error.as_json(None);

        assert_eq!(json["errors"][0]["code"], "UPSTREAM_TIMEOUT");
        assert_eq!(json["errors"][0]["message"], "Backend timed out");
    }

    #[test]
    fn test_as_json_custom_error_without_message() {
        let error = Error::Custom {
            status_code: StatusCode::NOT_IMPLEMENTED,
            code: "NOT_IMPLEMENTED".to_string(),
            msg: None,
        };
        let json = error.as_json(None);

        assert_eq!(json["errors"][0]["code"], "NOT_IMPLEMENTED");
        assert!(json["errors"][0]["message"].is_null());
    }

    #[test]
    fn test_registry_error_to_server_error_mapping() {
        // (registry_error, expected_status, expected_code, expected_message)
        let cases: Vec<(registry::Error, StatusCode, &str, Option<&str>)> = vec![
            (
                registry::Error::BlobUnknown,
                StatusCode::NOT_FOUND,
                "BLOB_UNKNOWN",
                None,
            ),
            (
                registry::Error::BlobUploadUnknown,
                StatusCode::NOT_FOUND,
                "BLOB_UPLOAD_UNKNOWN",
                None,
            ),
            (
                registry::Error::DigestInvalid,
                StatusCode::BAD_REQUEST,
                "DIGEST_INVALID",
                None,
            ),
            (
                registry::Error::ManifestBlobUnknown,
                StatusCode::NOT_FOUND,
                "MANIFEST_BLOB_UNKNOWN",
                None,
            ),
            (
                registry::Error::ManifestInvalid("Invalid JSON".to_string()),
                StatusCode::BAD_REQUEST,
                "MANIFEST_INVALID",
                Some("Invalid JSON"),
            ),
            (
                registry::Error::ManifestUnknown,
                StatusCode::NOT_FOUND,
                "MANIFEST_UNKNOWN",
                None,
            ),
            (
                registry::Error::NameInvalid,
                StatusCode::BAD_REQUEST,
                "NAME_INVALID",
                None,
            ),
            (
                registry::Error::NameUnknown,
                StatusCode::NOT_FOUND,
                "NAME_UNKNOWN",
                None,
            ),
            (
                registry::Error::Unauthorized("Invalid token".to_string()),
                StatusCode::UNAUTHORIZED,
                "UNAUTHORIZED",
                Some("Invalid token"),
            ),
            (
                registry::Error::Denied("Access forbidden".to_string()),
                StatusCode::FORBIDDEN,
                "DENIED",
                Some("Access forbidden"),
            ),
            (
                registry::Error::Unsupported,
                StatusCode::BAD_REQUEST,
                "UNSUPPORTED",
                None,
            ),
            (
                registry::Error::RangeNotSatisfiable,
                StatusCode::RANGE_NOT_SATISFIABLE,
                "SIZE_INVALID",
                None,
            ),
            (
                registry::Error::Internal("Database error".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_SERVER_ERROR",
                Some("Database error"),
            ),
            (
                registry::Error::Initialization("Config error".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_SERVER_ERROR",
                Some("Config error"),
            ),
        ];

        for (registry_error, expected_status, expected_code, expected_message) in cases {
            let error: Error = registry_error.into();
            assert_eq!(error.status_code(), expected_status);
            let json = error.as_json(None);
            assert_eq!(json["errors"][0]["code"], expected_code);
            if let Some(msg) = expected_message {
                assert_eq!(json["errors"][0]["message"], msg);
            }
        }
    }

    /// Variants outside the OCI-spec set route through the wildcard arm to a
    /// generic 500 `INTERNAL_SERVER_ERROR` carrying the rendered Display text.
    /// Pins this contract so a regression that broke `error.to_string()`
    /// formatting (or accidentally rerouted typed variants) would fail.
    #[test]
    fn test_typed_registry_variants_route_to_internal_server_error() {
        let cases: Vec<(registry::Error, &str)> = vec![
            (
                registry::Error::Io(std::io::Error::other("disk full")),
                "I/O error during operations",
            ),
            (
                registry::Error::MetadataStore(crate::registry::metadata_store::Error::Lock(
                    "redis unreachable".to_string(),
                )),
                "metadata store error during operations",
            ),
        ];

        for (registry_error, expected_display_prefix) in cases {
            let display = registry_error.to_string();
            let server_error: Error = registry_error.into();
            assert_eq!(
                server_error.status_code(),
                StatusCode::INTERNAL_SERVER_ERROR
            );
            let json = server_error.as_json(None);
            assert_eq!(json["errors"][0]["code"], "INTERNAL_SERVER_ERROR");
            let message = json["errors"][0]["message"]
                .as_str()
                .expect("message must be a string");
            assert!(
                message.starts_with(expected_display_prefix),
                "expected message to start with {expected_display_prefix:?}, got: {message:?}"
            );
            assert_eq!(message, display, "message must equal Display output");
        }
    }

    #[test]
    fn test_json_structure_completeness() {
        let error = Error::NotFound("Resource missing".to_string());
        let request_id = Some("abc-123".to_string());
        let json = error.as_json(request_id.as_ref());

        assert!(json.get("errors").is_some());
        assert!(json["errors"].is_array());
        assert_eq!(json["errors"].as_array().unwrap().len(), 1);

        let error_obj = &json["errors"][0];
        assert!(error_obj.get("code").is_some());
        assert!(error_obj.get("message").is_some());
        assert!(error_obj.get("detail").is_some());
        assert_eq!(error_obj["detail"]["request_id"], "abc-123");
    }

    #[test]
    fn test_status_code_coverage() {
        let test_cases = vec![
            (StatusCode::UNAUTHORIZED, Error::Unauthorized(String::new())),
            (StatusCode::BAD_REQUEST, Error::BadRequest(String::new())),
            (StatusCode::CONFLICT, Error::Conflict(String::new())),
            (
                StatusCode::RANGE_NOT_SATISFIABLE,
                Error::RangeNotSatisfiable(String::new()),
            ),
            (StatusCode::NOT_FOUND, Error::NotFound(String::new())),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Error::Initialization(String::new()),
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Error::Execution(String::new()),
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Error::Internal(String::new()),
            ),
        ];

        for (expected_status, error) in test_cases {
            assert_eq!(error.status_code(), expected_status);
        }
    }

    #[test]
    fn test_from_configuration_error_initialization() {
        use crate::configuration;

        let config_error = configuration::Error::Initialization("webhook failed".to_string());
        let error: Error = config_error.into();

        assert!(matches!(error, Error::Internal(_)));
        assert_eq!(error.to_string(), "Internal Server Error: webhook failed");
    }
}
