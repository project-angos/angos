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
        // 5xx bodies suppress the internal message; 4xx surface it.
        if error.status_code().is_server_error() {
            assert!(
                json["errors"][0]["message"].is_null(),
                "5xx must not leak {expected_message:?}"
            );
        } else {
            assert_eq!(json["errors"][0]["message"], expected_message);
        }
    }
}

/// Security regression guard for L6: a 5xx body must never carry the internal
/// error string. The detail is logged server-side (via the response extension),
/// not surfaced to the client, which keeps the request id to quote to an
/// operator.
#[test]
fn test_5xx_body_omits_internal_error_string() {
    let secret = "connection failed to postgres://user:pw@internal-host/db";
    let errors = [
        Error::Internal(secret.to_string()),
        Error::Execution(secret.to_string()),
        Error::Initialization(secret.to_string()),
        Error::ProviderUnavailable(secret.to_string()),
        Error::Custom {
            status_code: StatusCode::BAD_GATEWAY,
            code: "UPSTREAM".to_string(),
            msg: Some(secret.to_string()),
        },
    ];
    let request_id = "req-abc".to_string();
    for error in errors {
        assert!(error.status_code().is_server_error());
        let body = error.as_json(Some(&request_id)).to_string();
        assert!(
            !body.contains(secret),
            "5xx body leaked internal detail: {body}"
        );
        assert!(
            body.contains(&request_id),
            "request id must still be present"
        );
    }
}

#[test]
fn test_as_json_custom_error() {
    // A non-5xx custom error surfaces its message (5xx suppression is covered
    // by `test_5xx_body_omits_internal_error_string`).
    let error = Error::Custom {
        status_code: StatusCode::TOO_MANY_REQUESTS,
        code: "TOOMANYREQUESTS".to_string(),
        msg: Some("Rate limit exceeded".to_string()),
    };
    let json = error.as_json(None);

    assert_eq!(json["errors"][0]["code"], "TOOMANYREQUESTS");
    assert_eq!(json["errors"][0]["message"], "Rate limit exceeded");
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
        // 500s carry no client message: the internal detail is logged, not leaked.
        (
            registry::Error::Internal("Database error".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            None,
        ),
        (
            registry::Error::Initialization("Config error".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            None,
        ),
    ];

    for (registry_error, expected_status, expected_code, expected_message) in cases {
        let error: Error = registry_error.into();
        assert_eq!(error.status_code(), expected_status);
        let json = error.as_json(None);
        assert_eq!(json["errors"][0]["code"], expected_code);
        match expected_message {
            Some(msg) => assert_eq!(json["errors"][0]["message"], msg),
            None => assert!(json["errors"][0]["message"].is_null()),
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
/// generic 500 `INTERNAL_ERROR`. The rendered Display text stays server-side
/// (logs); the client body carries no message. Pins both the routing and the
/// no-leak contract.
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
        // Display text is retained for server-side logging.
        assert!(
            registry_error
                .to_string()
                .starts_with(expected_display_prefix),
            "expected Display to start with {expected_display_prefix:?}"
        );
        let server_error: Error = registry_error.into();
        assert_eq!(
            server_error.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        let json = server_error.as_json(None);
        assert_eq!(json["errors"][0]["code"], "INTERNAL_ERROR");
        assert!(
            json["errors"][0]["message"].is_null(),
            "a 5xx body must not leak the internal Display text"
        );
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
