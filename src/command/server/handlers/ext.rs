use hyper::{Response, StatusCode};

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::build_response, response_body::ResponseBody,
    },
    oci::Namespace,
    registry::job_store::JobState,
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

pub async fn handle_list_jobs(
    context: &ServerContext,
    n: Option<u16>,
    after: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_jobs_info(n, after).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_list_failed_jobs(
    context: &ServerContext,
    n: Option<u16>,
    after: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_failed_jobs_info(n, after).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_retry_job(
    context: &ServerContext,
    storage_key: &str,
) -> Result<Response<ResponseBody>, Error> {
    context.registry.retry_failed_job(storage_key).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(ResponseBody::empty())?)
}

pub async fn handle_delete_job(
    context: &ServerContext,
    state: JobState,
    storage_key: &str,
) -> Result<Response<ResponseBody>, Error> {
    context.registry.delete_job(state, storage_key).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(ResponseBody::empty())?)
}
