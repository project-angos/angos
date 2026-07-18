use std::collections::HashMap;

use hyper::{Response, StatusCode};
use serde::Serialize;

use crate::command::server::{
    error::Error,
    response::{APPLICATION_JSON, ResponseHeaders},
    response_body::ResponseBody,
};

pub mod blob;
pub mod content_discovery;
pub mod ext;
pub mod manifest;
pub mod upload;
pub mod version;

pub fn build_response(
    status: StatusCode,
    headers: HashMap<&'static str, String>,
    body: ResponseBody,
) -> Result<Response<ResponseBody>, Error> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    Ok(builder.body(body)?)
}

/// Serialize `body` into an `application/json` response with `status`.
pub fn json_response<T: Serialize>(
    status: StatusCode,
    body: &T,
) -> Result<Response<ResponseBody>, Error> {
    build_response(
        status,
        ResponseHeaders::new()
            .content_type(APPLICATION_JSON)
            .into_inner(),
        ResponseBody::fixed(serde_json::to_vec(body)?),
    )
}
