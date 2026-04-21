use hyper::{Response, StatusCode};

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::build_response, response_body::ResponseBody,
    },
    oci::Namespace,
};

pub async fn handle_list_repositories(
    context: &ServerContext,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_repositories_info().await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_list_namespaces(
    context: &ServerContext,
    repository: &str,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_namespaces_info(repository).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_list_revisions(
    context: &ServerContext,
    namespace: &Namespace,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_revisions_info(namespace).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_list_uploads(
    context: &ServerContext,
    namespace: &Namespace,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_uploads_info(namespace).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}
