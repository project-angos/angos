use std::fmt;

use hyper::StatusCode;
use serde_json::json;

use crate::{configuration, registry};

#[derive(Debug, PartialEq)]
pub enum Error {
    Initialization(String),
    Execution(String),
    // mappable to classical HTTP responses
    Unauthorized(String),
    BadRequest(String),
    Conflict(String),
    RangeNotSatisfiable(String),
    NotFound(String),
    Internal(String),
    Custom {
        status_code: StatusCode,
        code: String,
        msg: Option<String>,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Initialization(err) | Error::Execution(err) => write!(f, "{err}"),
            Error::Unauthorized(err) => write!(f, "Unauthorized: {err}"),
            Error::BadRequest(err) => write!(f, "Bad Request: {err}"),
            Error::Conflict(err) => write!(f, "Conflict: {err}"),
            Error::RangeNotSatisfiable(err) => write!(f, "Range Not Satisfiable: {err}"),
            Error::NotFound(err) => write!(f, "Not Found: {err}"),
            Error::Internal(err) => write!(f, "Internal Server Error: {err}"),
            Error::Custom {
                status_code,
                code: message,
                msg: details,
            } => {
                if let Some(details) = details {
                    write!(f, "Error {status_code}: {message} - {details}")
                } else {
                    write!(f, "Error {status_code}: {message}")
                }
            }
        }
    }
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

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::RangeNotSatisfiable(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Initialization(_) | Error::Execution(_) | Error::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
    fn test_status_code_mapping() {
        assert_eq!(
            Error::Unauthorized("test".to_string()).status_code(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            Error::BadRequest("test".to_string()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            Error::Conflict("test".to_string()).status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            Error::RangeNotSatisfiable("test".to_string()).status_code(),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(
            Error::NotFound("test".to_string()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            Error::Initialization("test".to_string()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            Error::Execution("test".to_string()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            Error::Internal("test".to_string()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            Error::Custom {
                status_code: StatusCode::IM_A_TEAPOT,
                code: "TEAPOT".to_string(),
                msg: None,
            }
            .status_code(),
            StatusCode::IM_A_TEAPOT
        );
    }

    #[test]
    fn test_as_json_without_request_id() {
        let error = Error::Unauthorized("Invalid credentials".to_string());
        let json = error.as_json(None);

        assert_eq!(json["errors"][0]["code"], "UNAUTHORIZED");
        assert_eq!(json["errors"][0]["message"], "Invalid credentials");
        assert!(json["errors"][0].get("detail").is_none());
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
    fn test_from_registry_error_blob_unknown() {
        let registry_error = registry::Error::BlobUnknown;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "BLOB_UNKNOWN");
    }

    #[test]
    fn test_from_registry_error_blob_upload_unknown() {
        let registry_error = registry::Error::BlobUploadUnknown;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "BLOB_UPLOAD_UNKNOWN");
    }

    #[test]
    fn test_from_registry_error_digest_invalid() {
        let registry_error = registry::Error::DigestInvalid;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "DIGEST_INVALID");
    }

    #[test]
    fn test_from_registry_error_manifest_blob_unknown() {
        let registry_error = registry::Error::ManifestBlobUnknown;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "MANIFEST_BLOB_UNKNOWN");
    }

    #[test]
    fn test_from_registry_error_manifest_invalid() {
        let registry_error = registry::Error::ManifestInvalid("Invalid JSON".to_string());
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "MANIFEST_INVALID");
        assert_eq!(json["errors"][0]["message"], "Invalid JSON");
    }

    #[test]
    fn test_from_registry_error_manifest_unknown() {
        let registry_error = registry::Error::ManifestUnknown;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "MANIFEST_UNKNOWN");
    }

    #[test]
    fn test_from_registry_error_name_invalid() {
        let registry_error = registry::Error::NameInvalid;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "NAME_INVALID");
    }

    #[test]
    fn test_from_registry_error_name_unknown() {
        let registry_error = registry::Error::NameUnknown;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "NAME_UNKNOWN");
    }

    #[test]
    fn test_from_registry_error_unauthorized() {
        let registry_error = registry::Error::Unauthorized("Invalid token".to_string());
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::UNAUTHORIZED);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "UNAUTHORIZED");
        assert_eq!(json["errors"][0]["message"], "Invalid token");
    }

    #[test]
    fn test_from_registry_error_denied() {
        let registry_error = registry::Error::Denied("Access forbidden".to_string());
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "DENIED");
        assert_eq!(json["errors"][0]["message"], "Access forbidden");
    }

    #[test]
    fn test_from_registry_error_unsupported() {
        let registry_error = registry::Error::Unsupported;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "UNSUPPORTED");
    }

    #[test]
    fn test_from_registry_error_range_not_satisfiable() {
        let registry_error = registry::Error::RangeNotSatisfiable;
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::RANGE_NOT_SATISFIABLE);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "SIZE_INVALID");
    }

    #[test]
    fn test_from_registry_error_internal() {
        let registry_error = registry::Error::Internal("Database error".to_string());
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "INTERNAL_SERVER_ERROR");
        assert_eq!(json["errors"][0]["message"], "Database error");
    }

    #[test]
    fn test_from_registry_error_initialization() {
        let registry_error = registry::Error::Initialization("Config error".to_string());
        let error: Error = registry_error.into();

        assert_eq!(error.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "INTERNAL_SERVER_ERROR");
        assert_eq!(json["errors"][0]["message"], "Config error");
    }

    #[test]
    fn test_error_partial_eq() {
        let error1 = Error::NotFound("test".to_string());
        let error2 = Error::NotFound("test".to_string());
        let error3 = Error::NotFound("different".to_string());
        let error4 = Error::BadRequest("test".to_string());

        assert_eq!(error1, error2);
        assert_ne!(error1, error3);
        assert_ne!(error1, error4);
    }

    #[test]
    fn test_custom_error_partial_eq() {
        let error1 = Error::Custom {
            status_code: StatusCode::BAD_GATEWAY,
            code: "TEST".to_string(),
            msg: Some("message".to_string()),
        };
        let error2 = Error::Custom {
            status_code: StatusCode::BAD_GATEWAY,
            code: "TEST".to_string(),
            msg: Some("message".to_string()),
        };
        let error3 = Error::Custom {
            status_code: StatusCode::BAD_GATEWAY,
            code: "TEST".to_string(),
            msg: None,
        };

        assert_eq!(error1, error2);
        assert_ne!(error1, error3);
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

        assert_eq!(error, Error::Internal("webhook failed".to_string()));
    }

    #[test]
    fn test_oci_spec_error_codes() {
        let oci_errors = vec![
            (registry::Error::BlobUnknown, "BLOB_UNKNOWN"),
            (registry::Error::BlobUploadUnknown, "BLOB_UPLOAD_UNKNOWN"),
            (registry::Error::DigestInvalid, "DIGEST_INVALID"),
            (
                registry::Error::ManifestBlobUnknown,
                "MANIFEST_BLOB_UNKNOWN",
            ),
            (
                registry::Error::ManifestInvalid(String::new()),
                "MANIFEST_INVALID",
            ),
            (registry::Error::ManifestUnknown, "MANIFEST_UNKNOWN"),
            (registry::Error::NameInvalid, "NAME_INVALID"),
            (registry::Error::NameUnknown, "NAME_UNKNOWN"),
            (registry::Error::Unauthorized(String::new()), "UNAUTHORIZED"),
            (registry::Error::Denied(String::new()), "DENIED"),
            (registry::Error::Unsupported, "UNSUPPORTED"),
        ];

        for (registry_error, expected_code) in oci_errors {
            let error: Error = registry_error.into();
            let json = error.as_json(None);
            assert_eq!(json["errors"][0]["code"], expected_code);
        }
    }
}
