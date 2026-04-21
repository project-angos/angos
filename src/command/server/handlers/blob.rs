use hyper::{Response, StatusCode};

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::build_response, response_body::ResponseBody,
    },
    oci::{Digest, Namespace},
    registry::GetBlobResponse,
};

pub async fn handle_head_blob(
    context: &ServerContext,
    namespace: &Namespace,
    digest: &Digest,
    mime_types: &[String],
) -> Result<Response<ResponseBody>, Error> {
    let repository = context.registry.get_repository_for_namespace(namespace)?;
    let response = context
        .registry
        .head_blob(repository, mime_types, namespace, digest)
        .await?;

    build_response(StatusCode::OK, response.headers, ResponseBody::empty())
}

pub async fn handle_delete_blob(
    context: &ServerContext,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<Response<ResponseBody>, Error> {
    context.registry.delete_blob(namespace, digest).await?;

    Ok(Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(ResponseBody::empty())?)
}

pub async fn handle_get_blob(
    context: &ServerContext,
    namespace: &Namespace,
    digest: &Digest,
    mime_types: &[String],
    range: Option<(u64, Option<u64>)>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context
        .registry
        .resolve_get_blob(namespace, digest, mime_types, range)
        .await?;

    match response {
        GetBlobResponse::Redirect { headers } => build_response(
            StatusCode::TEMPORARY_REDIRECT,
            headers,
            ResponseBody::empty(),
        ),
        GetBlobResponse::Reader { headers, body } => {
            build_response(StatusCode::OK, headers, ResponseBody::streaming(body))
        }
        GetBlobResponse::RangedReader { headers, body } => build_response(
            StatusCode::PARTIAL_CONTENT,
            headers,
            ResponseBody::streaming(body),
        ),
    }
}
