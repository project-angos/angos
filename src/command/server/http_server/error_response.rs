use http_body_util::Full;
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{ALLOW, CONTENT_TYPE, HeaderValue, RETRY_AFTER, WWW_AUTHENTICATE},
};

use angos_oci::response::ErrorCode;
use angos_transport::ResponseBody;

use crate::command::server::error::{Error, RECLAMATION_IN_PROGRESS_CODE};

const BASIC_AUTH_CHALLENGE: &str = r#"Basic realm="Angos", charset="UTF-8""#;

/// `challenge` is the token service's bearer challenge when one is configured.
/// Only our own denial is challenged: a 401 relayed from a pull-through upstream
/// is that registry's refusal, and our realm would buy the client nothing.
pub fn error_to_response(
    error: &Error,
    request_id: Option<&String>,
    challenge: Option<HeaderValue>,
) -> Response<ResponseBody> {
    let Ok(body) = serde_json::to_vec(&error.error_body(request_id)) else {
        return fallback_500();
    };
    let body = Bytes::from(body);

    let mut response = Response::builder()
        .status(error.status_code())
        .header(CONTENT_TYPE, "application/json")
        .body(ResponseBody::Fixed(Full::new(body)))
        .unwrap_or_else(|_| fallback_500());
    response.extensions_mut().insert(error.to_string());

    if matches!(error, Error::Custom { code, .. } if code == RECLAMATION_IN_PROGRESS_CODE) {
        // The collector's writer backoff clears in about a second, so say when
        // to come back rather than leaving the client to guess.
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("1"));
    }

    // A 405 must name what the path does accept, and the only one angos answers
    // this way is the token endpoint's `GET`.
    if error.status_code() == StatusCode::METHOD_NOT_ALLOWED
        && matches!(error, Error::Custom { code, .. } if code == ErrorCode::Unsupported.as_str())
    {
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET"));
    }

    if matches!(error, Error::Unauthorized(_)) {
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            challenge.unwrap_or(HeaderValue::from_static(BASIC_AUTH_CHALLENGE)),
        );
    }

    response
}

pub fn fallback_500() -> Response<ResponseBody> {
    let body = ResponseBody::Fixed(Full::new(Bytes::from("Internal Server Error")));
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}
