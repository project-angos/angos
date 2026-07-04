use std::collections::HashMap;

use hyper::{Response, StatusCode};

use crate::command::server::{error::Error, response_body::ResponseBody};

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
