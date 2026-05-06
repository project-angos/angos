use hyper::{Response, StatusCode, body::Incoming, header::CONTENT_TYPE, http::request::Parts};
use tokio::io::AsyncRead;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::build_response,
        http_server::event_emission::dispatch_events,
        request_ext::{HeaderExt, incoming_into_async_read},
        response_body::ResponseBody,
    },
    event_webhook::event::{Event, EventActor},
    identity::ClientIdentity,
    oci::{Namespace, Reference},
    registry::GetManifestResponse,
};

pub async fn handle_head_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_types: &[String],
    is_tag_immutable: bool,
) -> Result<Response<ResponseBody>, Error> {
    let repository = context.registry.get_repository_for_namespace(namespace)?;
    let response = context
        .registry
        .head_manifest(
            repository,
            mime_types,
            namespace,
            reference,
            is_tag_immutable,
        )
        .await?;

    build_response(StatusCode::OK, response.headers, ResponseBody::empty())
}

pub async fn handle_get_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_types: &[String],
    is_tag_immutable: bool,
) -> Result<Response<ResponseBody>, Error> {
    let response = context
        .registry
        .resolve_get_manifest(namespace, reference, mime_types, is_tag_immutable)
        .await?;

    match response {
        GetManifestResponse::Redirect { headers } => build_response(
            StatusCode::TEMPORARY_REDIRECT,
            headers,
            ResponseBody::empty(),
        ),
        GetManifestResponse::Body { headers, content } => {
            build_response(StatusCode::OK, headers, ResponseBody::fixed(content))
        }
    }
}

pub async fn handle_put_manifest<S>(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_type: String,
    body_stream: S,
    identity: &ClientIdentity,
) -> Result<(Response<ResponseBody>, Vec<Event>), Error>
where
    S: AsyncRead + Unpin + Send,
{
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .accept_put_manifest(actor, namespace, reference, mime_type, body_stream)
        .await?;

    let http_response =
        build_response(StatusCode::CREATED, response.headers, ResponseBody::empty())?;
    Ok((http_response, response.events))
}

pub async fn handle_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<(Response<ResponseBody>, Vec<Event>), Error> {
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .delete_manifest(actor, namespace, &reference)
        .await?;

    let http_response = Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(ResponseBody::empty())?;
    Ok((http_response, response.events))
}

pub async fn dispatch_get_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handle_get_manifest(context, namespace, reference, &mime_types, is_immutable).await
}

pub async fn dispatch_head_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handle_head_manifest(context, namespace, reference, &mime_types, is_immutable).await
}

pub async fn dispatch_put_manifest(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_type = parts.get_header(CONTENT_TYPE).ok_or(Error::BadRequest(
        "No Content-Type header provided".to_string(),
    ))?;

    let body_stream = incoming_into_async_read(incoming);

    let (response, events) = handle_put_manifest(
        context,
        namespace,
        reference,
        mime_type,
        body_stream,
        identity,
    )
    .await?;

    dispatch_events(context, events).await?;
    Ok(response)
}

pub async fn dispatch_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let (response, events) =
        handle_delete_manifest(context, namespace, reference, identity).await?;

    dispatch_events(context, events).await?;
    Ok(response)
}
