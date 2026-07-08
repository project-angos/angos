use hyper::StatusCode;

use crate::{command::server::Error, event_webhook, registry};

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

    let error = Error::ProviderUnavailable("OIDC provider unavailable".to_string());
    assert_eq!(
        format!("{error}"),
        "Provider unavailable: OIDC provider unavailable"
    );

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
            Error::ProviderUnavailable("provider unavailable".to_string()),
            "PROVIDER_UNAVAILABLE",
            "provider unavailable",
        ),
        (
            Error::Initialization("init".to_string()),
            "INTERNAL_ERROR",
            "init",
        ),
        (
            Error::Execution("exec".to_string()),
            "INTERNAL_ERROR",
            "exec",
        ),
        (
            Error::Internal("internal".to_string()),
            "INTERNAL_ERROR",
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
            "INTERNAL_ERROR",
            Some("Database error"),
        ),
        (
            registry::Error::Initialization("Config error".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
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

#[test]
fn test_blob_referenced_registry_error_mapping() {
    let error: Error = registry::Error::BlobReferenced.into();
    assert_eq!(error.status_code(), StatusCode::METHOD_NOT_ALLOWED);

    let json = error.as_json(None);
    assert_eq!(json["errors"][0]["code"], "DENIED");
    assert_eq!(json["errors"][0]["message"], "blob is still referenced");
}

/// Variants outside the OCI-spec set route through the wildcard arm to a
/// generic 500 `INTERNAL_ERROR` carrying the rendered Display text.
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
            registry::Error::Serde(serde_json::from_str::<serde_json::Value>("{bad}").unwrap_err()),
            "(de)serialization error during operations",
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
        assert_eq!(json["errors"][0]["code"], "INTERNAL_ERROR");
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
            StatusCode::SERVICE_UNAVAILABLE,
            Error::ProviderUnavailable(String::new()),
        ),
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

#[test]
fn test_from_event_webhook_error_mapping() {
    let init_error = event_webhook::Error::Initialization("bad webhook config".to_string());
    let error: Error = init_error.into();
    assert!(matches!(error, Error::Initialization(_)));
    assert_eq!(error.to_string(), "bad webhook config");

    let dispatch_error = event_webhook::Error::Dispatch("webhook failed".to_string());
    let error: Error = dispatch_error.into();
    assert!(matches!(error, Error::Execution(_)));
    assert_eq!(error.to_string(), "webhook failed");
}
