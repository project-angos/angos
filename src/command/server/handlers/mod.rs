use std::collections::HashMap;

use hyper::{Response, StatusCode, body::Incoming, http::request::Parts};
use serde::Serialize;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        response::{APPLICATION_JSON, ResponseHeaders},
        response_body::ResponseBody,
    },
    identity::ClientIdentity,
};

pub mod blob;
pub mod content_discovery;
pub mod ext;
pub mod manifest;
pub mod upload;
pub mod version;

/// The shared environment for a body-carrying `PUT` handler: the server
/// context, the parsed request head, the streaming body, and the authenticated
/// identity. Built by the dispatcher and consumed once, since the body moves.
pub struct PutRequest<'a> {
    pub context: &'a ServerContext,
    pub parts: &'a Parts,
    pub incoming: Incoming,
    pub identity: &'a ClientIdentity,
}

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
