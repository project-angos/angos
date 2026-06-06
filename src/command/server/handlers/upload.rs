use hyper::{Response, StatusCode, body::Incoming, header::CONTENT_RANGE, http::request::Parts};
use tokio::io::AsyncRead;
use uuid::Uuid;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::{EventfulResponse, build_event_response, build_response, dispatch_eventful},
        request::{RequestHeaders, incoming_into_async_read},
        response_body::ResponseBody,
    },
    event_webhook::event::EventActor,
    identity::ClientIdentity,
    oci::{Digest, Namespace},
    registry::{BlobMount, StartUploadResponse},
};

pub async fn handle_start_upload(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Option<Digest>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.start_upload(namespace, digest).await?;
    upload_start_response(response)
}

pub async fn handle_mount_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
    from: Option<Namespace>,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mount = BlobMount { digest, from };
    // A mount grants the target a reference to an existing blob with no upload, so
    // it must not hand the caller bytes they could not otherwise read. Authorize
    // the caller against a namespace that holds the blob first; when none is
    // readable, degrade to an ordinary upload session (an unsatisfiable mount
    // falls back to 202) rather than leaking the blob.
    let response = if let Some(source) = context
        .authorize_mount_source(&mount, identity, parts)
        .await?
    {
        context
            .registry
            .mount_blob(namespace, mount, &source)
            .await?
    } else {
        context.registry.start_upload(namespace, None).await?
    };
    upload_start_response(response)
}

/// Maps a [`StartUploadResponse`] to its HTTP response: an existing/mounted blob
/// is `201 Created`, a fresh session is `202 Accepted`.
fn upload_start_response(response: StartUploadResponse) -> Result<Response<ResponseBody>, Error> {
    match response {
        StartUploadResponse::ExistingBlob { headers } => {
            build_response(StatusCode::CREATED, headers, ResponseBody::empty())
        }
        StartUploadResponse::Session { headers } => {
            build_response(StatusCode::ACCEPTED, headers, ResponseBody::empty())
        }
    }
}

pub async fn handle_get_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_upload_status(namespace, uuid).await?;

    build_response(
        StatusCode::NO_CONTENT,
        response.headers,
        ResponseBody::empty(),
    )
}

pub async fn handle_patch_upload<S>(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
    start_offset: Option<u64>,
    content_length: u64,
    body_stream: S,
) -> Result<Response<ResponseBody>, Error>
where
    S: AsyncRead + Unpin + Send + Sync + 'static,
{
    let response = context
        .registry
        .patch_upload(namespace, uuid, start_offset, content_length, body_stream)
        .await?;

    build_response(
        StatusCode::ACCEPTED,
        response.headers,
        ResponseBody::empty(),
    )
}

pub async fn handle_put_upload<S>(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
    digest: &Digest,
    content_length: u64,
    body_reader: S,
    identity: &ClientIdentity,
) -> Result<EventfulResponse, Error>
where
    S: AsyncRead + Unpin + Send + Sync + 'static,
{
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .complete_upload(actor, namespace, uuid, digest, content_length, body_reader)
        .await?;

    build_event_response(StatusCode::CREATED, response.headers, response.events)
}

pub async fn handle_delete_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: Uuid,
) -> Result<Response<ResponseBody>, Error> {
    context.registry.delete_upload(namespace, uuid).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(ResponseBody::empty())?)
}

pub async fn dispatch_patch_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let start_offset = headers.range(CONTENT_RANGE)?.map(|(start, _)| start);
    let content_length = headers.content_length()?.ok_or(Error::BadRequest(
        "No Content-Length header provided".to_string(),
    ))?;
    let body_stream = incoming_into_async_read(incoming);

    handle_patch_upload(
        context,
        namespace,
        uuid,
        start_offset,
        content_length,
        body_stream,
    )
    .await
}

pub async fn dispatch_put_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
    digest: Digest,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let content_length = headers.content_length()?.unwrap_or(0);
    let body_stream = incoming_into_async_read(incoming);

    dispatch_eventful(
        context,
        handle_put_upload(
            context,
            namespace,
            uuid,
            &digest,
            content_length,
            body_stream,
            identity,
        )
        .await?,
    )
    .await
}
