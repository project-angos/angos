use std::collections::HashMap;

use hyper::{Response, StatusCode};

use crate::{
    command::server::{ServerContext, error::Error, response_body::ResponseBody},
    event_webhook::event::Event,
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

pub type EventfulResponse = (Response<ResponseBody>, Vec<Event>);

pub fn build_event_response(
    status: StatusCode,
    headers: HashMap<&'static str, String>,
    events: Vec<Event>,
) -> Result<EventfulResponse, Error> {
    Ok((
        build_response(status, headers, ResponseBody::empty())?,
        events,
    ))
}

pub async fn dispatch_eventful(
    context: &ServerContext,
    response: EventfulResponse,
) -> Result<Response<ResponseBody>, Error> {
    let (response, events) = response;
    context.dispatch_events(&events).await?;
    Ok(response)
}
