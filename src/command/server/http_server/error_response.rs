use http_body_util::Full;
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{CONTENT_TYPE, HeaderValue, WWW_AUTHENTICATE},
};

use crate::command::server::{error::Error, response_body::ResponseBody};

const BASIC_AUTH_CHALLENGE: &str = r#"Basic realm="Simple Registry", charset="UTF-8""#;

pub fn error_to_response(error: &Error, request_id: Option<&String>) -> Response<ResponseBody> {
    let body = Bytes::from(error.as_json(request_id).to_string());

    let mut response = Response::builder()
        .status(error.status_code())
        .header(CONTENT_TYPE, "application/json")
        .body(ResponseBody::Fixed(Full::new(body)))
        .unwrap_or_else(|_| fallback_500());

    if matches!(error, Error::Unauthorized(_)) {
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static(BASIC_AUTH_CHALLENGE),
        );
    }

    response
}

pub fn fallback_500() -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "text/plain")
        .body(ResponseBody::Fixed(Full::new(Bytes::from(
            "Internal Server Error",
        ))))
        .expect("static fallback response must be valid")
}
