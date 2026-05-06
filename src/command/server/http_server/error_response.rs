use http_body_util::Full;
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{CONTENT_TYPE, WWW_AUTHENTICATE},
};

use crate::command::server::{error::Error, response_body::ResponseBody};

pub fn error_to_response(error: &Error, request_id: Option<&String>) -> Response<ResponseBody> {
    let status = error.status_code();
    let body = Bytes::from(error.as_json(request_id).to_string());

    let result = match error {
        Error::Unauthorized(_) => Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .header(
                WWW_AUTHENTICATE,
                r#"Basic realm="Simple Registry", charset="UTF-8""#,
            )
            .body(ResponseBody::Fixed(Full::new(body))),
        _ => Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .body(ResponseBody::Fixed(Full::new(body))),
    };

    result.unwrap_or_else(|_| fallback_500())
}

pub fn fallback_500() -> Response<ResponseBody> {
    let mut response = Response::new(ResponseBody::Fixed(Full::new(Bytes::from_static(
        b"Internal Server Error",
    ))));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response.headers_mut().insert(
        CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/plain"),
    );
    response
}
