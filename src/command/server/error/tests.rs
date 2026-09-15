use std::io;

use hyper::StatusCode;

use angos_oci::response::ErrorCode;
use angos_oci_client::REPLICATION_SUPERSEDED_CODE;

use crate::{
    command::server::{Error, error::RECLAMATION_IN_PROGRESS_CODE},
    configuration, event_webhook, registry,
};

/// Whether this error's `code` must come from the set the spec fixes for a 4XX.
/// Exhaustive on purpose: a variant added later does not compile until it is
/// answered for here, so a new mapping cannot quietly invent a code.
fn owes_a_spec_code(error: &registry::Error) -> bool {
    match error {
        registry::Error::BlobUnknown
        | registry::Error::BlobReferenced
        | registry::Error::BlobUploadUnknown
        | registry::Error::DigestInvalid
        | registry::Error::ManifestBlobUnknown
        | registry::Error::ManifestBodyTooLarge { .. }
        | registry::Error::BlobBodyTooLarge { .. }
        | registry::Error::ManifestInvalid(_)
        | registry::Error::ManifestUnknown
        | registry::Error::NameInvalid
        | registry::Error::NameUnknown
        | registry::Error::NotFound
        | registry::Error::Unauthorized(_)
        | registry::Error::Denied(_)
        | registry::Error::Unsupported
        | registry::Error::RangeNotSatisfiable
        | registry::Error::Conflict(_) => true,
        registry::Error::ReplicationSuperseded(_)
        | registry::Error::ReclamationInProgress(_)
        | registry::Error::Initialization(_)
        | registry::Error::EventDelivery(_)
        | registry::Error::Internal(_)
        | registry::Error::Corrupt(_)
        | registry::Error::Configuration(_)
        | registry::Error::Cache(_)
        | registry::Error::Io(_)
        | registry::Error::Http(_)
        | registry::Error::Serde(_)
        | registry::Error::InvalidHeader(_) => false,
    }
}

/// One answer the server sends: the error and the status, code and message it
/// must map to. A `None` message is a body carrying none, as every 5XX does.
struct Answer {
    error: Error,
    status: u16,
    code: &'static str,
    message: Option<&'static str>,
}

fn answer(
    error: impl Into<Error>,
    status: u16,
    code: &'static str,
    message: Option<&'static str>,
) -> Answer {
    Answer {
        error: error.into(),
        status,
        code,
        message,
    }
}

/// The answers the server raises itself.
fn server_answers() -> Vec<Answer> {
    vec![
        answer(
            Error::Unauthorized("auth".into()),
            401,
            "UNAUTHORIZED",
            Some("auth"),
        ),
        answer(
            Error::BadRequest("bad".into()),
            400,
            "UNSUPPORTED",
            Some("bad"),
        ),
        answer(
            Error::RangeNotSatisfiable("r".into()),
            416,
            "SIZE_INVALID",
            Some("r"),
        ),
        answer(
            Error::NotFound("missing".into()),
            404,
            "NAME_UNKNOWN",
            Some("missing"),
        ),
        answer(
            Error::ProviderUnavailable("down".into()),
            503,
            "PROVIDER_UNAVAILABLE",
            None,
        ),
        answer(
            Error::Initialization("init".into()),
            500,
            "INTERNAL_ERROR",
            None,
        ),
        answer(Error::Execution("exec".into()), 500, "INTERNAL_ERROR", None),
        answer(
            Error::Internal("internal".into()),
            500,
            "INTERNAL_ERROR",
            None,
        ),
        // A custom answer passes its own status and code through.
        answer(
            Error::Custom {
                status_code: StatusCode::TOO_MANY_REQUESTS,
                code: "TOOMANYREQUESTS".to_string(),
                msg: Some("slow down".to_string()),
            },
            429,
            "TOOMANYREQUESTS",
            Some("slow down"),
        ),
    ]
}

/// The answers converted from a `registry::Error`.
fn registry_answers() -> Vec<Answer> {
    vec![
        // Every `registry::Error` [`owes_a_spec_code`] calls client-facing,
        // plus one that it does not.
        answer(registry::Error::BlobUnknown, 404, "BLOB_UNKNOWN", None),
        // end-10 lists `404/400/405` as a blob delete's failures, so a
        // still-referenced blob answers within that set.
        answer(
            registry::Error::BlobReferenced,
            405,
            "DENIED",
            Some("blob is still referenced"),
        ),
        answer(
            registry::Error::BlobUploadUnknown,
            404,
            "BLOB_UPLOAD_UNKNOWN",
            None,
        ),
        answer(registry::Error::DigestInvalid, 400, "DIGEST_INVALID", None),
        answer(
            registry::Error::ManifestBlobUnknown,
            404,
            "MANIFEST_BLOB_UNKNOWN",
            None,
        ),
        // A body over either cap answers `413` under the code naming which
        // body was refused.
        answer(
            registry::Error::ManifestBodyTooLarge { limit: 42 },
            413,
            "MANIFEST_INVALID",
            Some("manifest body exceeds supported size limit of 42 bytes"),
        ),
        answer(
            registry::Error::BlobBodyTooLarge { limit: 42 },
            413,
            "BLOB_UPLOAD_INVALID",
            Some("blob body exceeds supported size limit of 42 bytes"),
        ),
        answer(
            registry::Error::ManifestInvalid("bad".into()),
            400,
            "MANIFEST_INVALID",
            Some("bad"),
        ),
        answer(
            registry::Error::ManifestUnknown,
            404,
            "MANIFEST_UNKNOWN",
            None,
        ),
        answer(registry::Error::NameInvalid, 400, "NAME_INVALID", None),
        answer(registry::Error::NameUnknown, 404, "NAME_UNKNOWN", None),
        answer(registry::Error::NotFound, 404, "NAME_UNKNOWN", None),
        answer(
            registry::Error::Unauthorized("tok".into()),
            401,
            "UNAUTHORIZED",
            Some("tok"),
        ),
        answer(
            registry::Error::Denied("no".into()),
            403,
            "DENIED",
            Some("no"),
        ),
        answer(registry::Error::Unsupported, 400, "UNSUPPORTED", None),
        answer(
            registry::Error::RangeNotSatisfiable,
            416,
            "SIZE_INVALID",
            None,
        ),
        answer(
            registry::Error::Conflict("locked".into()),
            409,
            "DENIED",
            Some("locked"),
        ),
        // Transient by construction, so it answers 503 under a code outside the
        // spec's set; the 5XX body carries no message.
        answer(
            registry::Error::ReclamationInProgress("reclaiming".into()),
            503,
            RECLAMATION_IN_PROGRESS_CODE,
            None,
        ),
        // A typed variant routes to a generic 500 with its detail left in the log.
        answer(
            registry::Error::Io(io::Error::other("disk full")),
            500,
            "INTERNAL_ERROR",
            None,
        ),
    ]
}

/// The status, code and message of every answer, and the rule over them: a 4XX
/// names a code the spec defines, so a new mapping cannot invent one.
#[test]
fn every_answer_maps_to_its_status_code_and_message() {
    for Answer {
        error,
        status,
        code,
        message,
    } in server_answers().into_iter().chain(registry_answers())
    {
        let answered = error.status_code();
        assert_eq!(answered.as_u16(), status, "{error:?} answers a status");
        let body = serde_json::to_value(error.error_body(None)).unwrap();

        assert_eq!(body["errors"][0]["code"], code, "{error:?} names a code");
        match message {
            Some(message) => assert_eq!(body["errors"][0]["message"], message),
            None => assert!(
                body["errors"][0]["message"].is_null(),
                "{error:?} must carry no message"
            ),
        }

        if answered.is_client_error() {
            assert!(
                ErrorCode::ALL
                    .iter()
                    .any(|known| body["errors"][0]["code"] == known.as_str()),
                "{error:?} answers with {code}, which the spec does not define"
            );
        }
    }
}

/// The one code outside the spec's set: it answers a replication write only,
/// and its sender reads it to tell convergence from a refused write.
#[test]
fn the_replication_code_is_the_only_exception() {
    let error = registry::Error::ReplicationSuperseded("superseded".to_string());
    assert!(!owes_a_spec_code(&error));

    let error = Error::from(error);
    assert_eq!(error.status_code(), StatusCode::CONFLICT);
    let body = serde_json::to_value(error.error_body(None)).unwrap();
    assert_eq!(body["errors"][0]["code"], REPLICATION_SUPERSEDED_CODE);
    assert!(
        !ErrorCode::ALL
            .iter()
            .any(|known| known.as_str() == REPLICATION_SUPERSEDED_CODE),
        "the replication code must stay outside the spec's set"
    );
}

/// Security regression guard: a 5xx body must never carry the internal error
/// string, only the request id the client quotes to an operator.
#[test]
fn a_5xx_body_omits_the_internal_error_string() {
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
        let body = serde_json::to_value(error.error_body(Some(&request_id)))
            .unwrap()
            .to_string();
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

/// The body is one `errors` entry carrying the request id a client quotes back.
#[test]
fn a_body_carries_one_error_and_the_request_id() {
    let request_id = "abc-123".to_string();
    let body = serde_json::to_value(
        Error::NotFound("Resource missing".to_string()).error_body(Some(&request_id)),
    )
    .unwrap();

    assert_eq!(body["errors"].as_array().map(Vec::len), Some(1));
    let entry = &body["errors"][0];
    assert_eq!(entry["code"], "NAME_UNKNOWN");
    assert_eq!(entry["message"], "Resource missing");
    assert_eq!(entry["detail"]["request_id"], request_id);
}

#[test]
fn errors_display_their_kind_and_detail() {
    for (error, rendered) in [
        (Error::Initialization("boom".to_string()), "boom"),
        (Error::Execution("boom".to_string()), "boom"),
        (
            Error::Unauthorized("Invalid token".to_string()),
            "Unauthorized: Invalid token",
        ),
        (
            Error::BadRequest("Malformed request".to_string()),
            "Bad Request: Malformed request",
        ),
        (
            Error::RangeNotSatisfiable("Invalid range '-'".to_string()),
            "Range Not Satisfiable: Invalid range '-'",
        ),
        (
            Error::NotFound("Item not found".to_string()),
            "Not Found: Item not found",
        ),
        (
            Error::ProviderUnavailable("OIDC provider unavailable".to_string()),
            "Provider unavailable: OIDC provider unavailable",
        ),
        (
            Error::Internal("Unexpected error".to_string()),
            "Internal Server Error: Unexpected error",
        ),
        (
            Error::Custom {
                status_code: StatusCode::BAD_GATEWAY,
                code: "UPSTREAM_ERROR".to_string(),
                msg: Some("Failed to connect".to_string()),
            },
            "Error 502 Bad Gateway: UPSTREAM_ERROR - Failed to connect",
        ),
        (
            Error::Custom {
                status_code: StatusCode::SERVICE_UNAVAILABLE,
                code: "SERVICE_UNAVAILABLE".to_string(),
                msg: None,
            },
            "Error 503 Service Unavailable: SERVICE_UNAVAILABLE",
        ),
    ] {
        assert_eq!(error.to_string(), rendered);
    }
}

#[test]
fn test_from_configuration_error_initialization() {
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
