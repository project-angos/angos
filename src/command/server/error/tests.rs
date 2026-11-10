use std::io;

use hyper::StatusCode;
use hyper::header::{CONTENT_TYPE, HeaderValue, RETRY_AFTER, WWW_AUTHENTICATE};

use angos_oci::response::{
    ErrorCode, RECLAMATION_IN_PROGRESS_CODE, REPLICATION_SUPERSEDED_CODE, UPSTREAM_UNAVAILABLE_CODE,
};

use crate::command::server::Error;
use crate::command::server::http_server::error_to_response;
use crate::registry;

/// The error codes the distribution spec defines.
const SPEC_CODES: [ErrorCode; 14] = [
    ErrorCode::BlobUnknown,
    ErrorCode::BlobUploadInvalid,
    ErrorCode::BlobUploadUnknown,
    ErrorCode::DigestInvalid,
    ErrorCode::ManifestBlobUnknown,
    ErrorCode::ManifestInvalid,
    ErrorCode::ManifestUnknown,
    ErrorCode::NameInvalid,
    ErrorCode::NameUnknown,
    ErrorCode::SizeInvalid,
    ErrorCode::Unauthorized,
    ErrorCode::Denied,
    ErrorCode::Unsupported,
    ErrorCode::TooManyRequests,
];

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
        | registry::Error::UpstreamUnavailable(_)
        | registry::Error::Initialization(_)
        | registry::Error::EventDelivery(_)
        | registry::Error::Internal(_)
        | registry::Error::Corrupt(_)
        | registry::Error::Io(_)
        | registry::Error::Serde(_) => false,
    }
}

/// An answer the server sends: the error and the status, code and message it
/// must map to. A `None` message is a body carrying none, as every 5XX does.
type Answer = (Error, u16, &'static str, Option<&'static str>);

/// The answers the server raises itself.
fn server_answers() -> Vec<Answer> {
    vec![
        (
            Error::Unauthorized("auth".into()),
            401,
            "UNAUTHORIZED",
            Some("auth"),
        ),
        (
            Error::BadRequest("bad".into()),
            400,
            "UNSUPPORTED",
            Some("bad"),
        ),
        (
            Error::RangeNotSatisfiable("r".into()),
            416,
            "SIZE_INVALID",
            Some("r"),
        ),
        (
            Error::NotFound("missing".into()),
            404,
            "NAME_UNKNOWN",
            Some("missing"),
        ),
        (
            Error::ProviderUnavailable("down".into()),
            503,
            "PROVIDER_UNAVAILABLE",
            None,
        ),
        (
            Error::Initialization("init".into()),
            500,
            "INTERNAL_ERROR",
            None,
        ),
        (Error::Execution("exec".into()), 500, "INTERNAL_ERROR", None),
        (
            Error::Internal("internal".into()),
            500,
            "INTERNAL_ERROR",
            None,
        ),
        // A custom answer passes its own status and code through.
        (
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
    let errors: Vec<(registry::Error, u16, &str, Option<&str>)> = vec![
        // Every `registry::Error` [`owes_a_spec_code`] calls client-facing,
        // plus some that it does not.
        (registry::Error::BlobUnknown, 404, "BLOB_UNKNOWN", None),
        // end-10 lists `404/400/405` as a blob delete's failures, so a
        // still-referenced blob answers within that set.
        (
            registry::Error::BlobReferenced,
            405,
            "DENIED",
            Some("blob is still referenced"),
        ),
        (
            registry::Error::BlobUploadUnknown,
            404,
            "BLOB_UPLOAD_UNKNOWN",
            None,
        ),
        (registry::Error::DigestInvalid, 400, "DIGEST_INVALID", None),
        (
            registry::Error::ManifestBlobUnknown,
            404,
            "MANIFEST_BLOB_UNKNOWN",
            None,
        ),
        // A body over either cap answers `413` under the code naming which
        // body was refused.
        (
            registry::Error::ManifestBodyTooLarge { limit: 42 },
            413,
            "MANIFEST_INVALID",
            Some("manifest body exceeds supported size limit of 42 bytes"),
        ),
        (
            registry::Error::BlobBodyTooLarge { limit: 42 },
            413,
            "BLOB_UPLOAD_INVALID",
            Some("blob body exceeds supported size limit of 42 bytes"),
        ),
        (
            registry::Error::ManifestInvalid("bad".into()),
            400,
            "MANIFEST_INVALID",
            Some("bad"),
        ),
        (
            registry::Error::ManifestUnknown,
            404,
            "MANIFEST_UNKNOWN",
            None,
        ),
        (registry::Error::NameInvalid, 400, "NAME_INVALID", None),
        (registry::Error::NameUnknown, 404, "NAME_UNKNOWN", None),
        (registry::Error::NotFound, 404, "NAME_UNKNOWN", None),
        (
            registry::Error::Unauthorized("tok".into()),
            401,
            "UNAUTHORIZED",
            Some("tok"),
        ),
        (
            registry::Error::Denied("no".into()),
            403,
            "DENIED",
            Some("no"),
        ),
        (registry::Error::Unsupported, 400, "UNSUPPORTED", None),
        (
            registry::Error::RangeNotSatisfiable,
            416,
            "SIZE_INVALID",
            None,
        ),
        (
            registry::Error::Conflict("locked".into()),
            409,
            "DENIED",
            Some("locked"),
        ),
        // Transient by construction, so it answers 503 under a code outside the
        // spec's set; the 5XX body carries no message.
        (
            registry::Error::ReclamationInProgress("reclaiming".into()),
            503,
            RECLAMATION_IN_PROGRESS_CODE,
            None,
        ),
        // An unreachable upstream is a retry, not a missing image.
        (
            registry::Error::UpstreamUnavailable("down".into()),
            503,
            UPSTREAM_UNAVAILABLE_CODE,
            None,
        ),
        // A typed variant routes to a generic 500 with its detail left in the log.
        (
            registry::Error::Io(io::Error::other("disk full")),
            500,
            "INTERNAL_ERROR",
            None,
        ),
    ];
    errors
        .into_iter()
        .map(|(error, status, code, message)| {
            assert_eq!(owes_a_spec_code(&error), status / 100 == 4, "{error:?}");
            (error.into(), status, code, message)
        })
        .collect()
}

/// The status, code, message and headers of every answer, and the rule over
/// them: a 4XX names a code the spec defines, so a new mapping cannot invent one.
/// Only our own denial is challenged, and only the reclamation refusal, which
/// clears in about a second, says when to retry.
#[test]
fn every_answer_maps_to_its_status_code_and_message() {
    for (error, status, code, message) in server_answers().into_iter().chain(registry_answers()) {
        let response = error_to_response(&error, None, None);
        assert_eq!(
            response.status().as_u16(),
            status,
            "{error:?} answers a status"
        );
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            response.headers().get(WWW_AUTHENTICATE).is_some(),
            matches!(error, Error::Unauthorized(_)),
            "{error:?} challenges"
        );
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .map(HeaderValue::as_bytes),
            (code == RECLAMATION_IN_PROGRESS_CODE).then_some(b"1".as_slice()),
            "{error:?} retries"
        );

        let body = serde_json::to_value(error.error_body(Some("req-1"))).unwrap();
        assert_eq!(body["errors"].as_array().map(Vec::len), Some(1));
        let entry = &body["errors"][0];
        assert_eq!(entry["code"], code, "{error:?} names a code");
        assert_eq!(entry["detail"]["request_id"], "req-1");
        match message {
            Some(message) => assert_eq!(entry["message"], message),
            None => assert!(
                entry["message"].is_null(),
                "{error:?} must carry no message"
            ),
        }

        if response.status().is_client_error() {
            assert!(
                SPEC_CODES
                    .iter()
                    .any(|known| entry["code"] == known.as_str()),
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
        !SPEC_CODES
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
    for error in errors {
        assert!(error.status_code().is_server_error());
        let body = serde_json::to_value(error.error_body(Some("req-abc")))
            .unwrap()
            .to_string();
        assert!(
            !body.contains(secret),
            "5xx body leaked internal detail: {body}"
        );
    }
}
