use hyper::{Response, StatusCode};
use serde::Serialize;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::build_response,
        response::{APPLICATION_JSON, HeaderMap, ResponseHeaders},
        response_body::ResponseBody,
    },
    jobs::{JobState, Queue},
    oci::Namespace,
};

fn json_headers() -> HeaderMap {
    ResponseHeaders::new()
        .content_type(APPLICATION_JSON)
        .into_inner()
}

/// Serialize a typed admin body into a `200 OK` `application/json` response.
fn json_response<T: Serialize>(body: &T) -> Result<Response<ResponseBody>, Error> {
    build_response(
        StatusCode::OK,
        json_headers(),
        ResponseBody::fixed(serde_json::to_vec(body)?),
    )
}

pub async fn handle_list_repositories(
    context: &ServerContext,
) -> Result<Response<ResponseBody>, Error> {
    let body = context.registry.get_repositories_info().await?;

    json_response(&body)
}

pub async fn handle_list_namespaces(
    context: &ServerContext,
    repository: &str,
) -> Result<Response<ResponseBody>, Error> {
    let body = context.registry.get_namespaces_info(repository).await?;

    json_response(&body)
}

pub async fn handle_list_revisions(
    context: &ServerContext,
    namespace: &Namespace,
) -> Result<Response<ResponseBody>, Error> {
    let body = context.registry.get_revisions_info(namespace).await?;

    json_response(&body)
}

pub async fn handle_list_uploads(
    context: &ServerContext,
    namespace: &Namespace,
) -> Result<Response<ResponseBody>, Error> {
    let body = context.registry.get_uploads_info(namespace).await?;

    json_response(&body)
}

pub async fn handle_list_jobs(
    context: &ServerContext,
    queue: Queue,
    n: Option<u16>,
    after: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let body = context.registry.get_jobs_info(queue, n, after).await?;

    json_response(&body)
}

pub async fn handle_list_failed_jobs(
    context: &ServerContext,
    queue: Queue,
    n: Option<u16>,
    after: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let body = context
        .registry
        .get_failed_jobs_info(queue, n, after)
        .await?;

    json_response(&body)
}

pub async fn handle_retry_job(
    context: &ServerContext,
    queue: Queue,
    storage_key: &str,
) -> Result<Response<ResponseBody>, Error> {
    context
        .registry
        .retry_failed_job(queue, storage_key)
        .await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(ResponseBody::empty())?)
}

pub async fn handle_delete_job(
    context: &ServerContext,
    queue: Queue,
    state: JobState,
    storage_key: &str,
) -> Result<Response<ResponseBody>, Error> {
    context
        .registry
        .delete_job(queue, state, storage_key)
        .await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(ResponseBody::empty())?)
}
