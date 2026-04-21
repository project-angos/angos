use hyper::{Response, StatusCode};

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::build_response, response_body::ResponseBody,
    },
    oci::{Digest, Namespace},
};

pub async fn handle_get_referrers(
    context: &ServerContext,
    namespace: &Namespace,
    digest: &Digest,
    artifact_type: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context
        .registry
        .get_referrers(namespace, digest, artifact_type)
        .await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_list_catalog(
    context: &ServerContext,
    n: Option<u16>,
    last: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.list_catalog(n, last).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}

pub async fn handle_list_tags(
    context: &ServerContext,
    namespace: &Namespace,
    n: Option<u16>,
    last: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.list_tags(namespace, n, last).await?;

    build_response(
        StatusCode::OK,
        response.headers,
        ResponseBody::fixed(response.body),
    )
}
