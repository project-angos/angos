use hyper::{Response, StatusCode};
use tokio::io::AsyncRead;
use uuid::Uuid;

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::build_response, response_body::ResponseBody,
    },
    oci::{Digest, Namespace},
    registry::StartUploadResponse,
};

pub async fn handle_start_upload(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Option<Digest>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.start_upload(namespace, digest).await?;

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
) -> Result<Response<ResponseBody>, Error>
where
    S: AsyncRead + Unpin + Send + Sync + 'static,
{
    let response = context
        .registry
        .complete_upload(namespace, uuid, digest, content_length, body_reader)
        .await?;

    build_response(StatusCode::CREATED, response.headers, ResponseBody::empty())
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
