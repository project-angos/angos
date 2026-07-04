use hyper::{Response, StatusCode, http::request::Parts};

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::build_response, request::RequestHeaders,
        response_body::ResponseBody,
    },
    event_webhook::event::EventActor,
    identity::ClientIdentity,
    oci::{Digest, Namespace},
    registry::{BlobRange, GetBlobResponse},
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
    range: Option<BlobRange>,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .resolve_get_blob(actor, namespace, digest, mime_types, range)
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

pub async fn dispatch_get_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_types = headers.accepted_content_types();
    let range = headers.blob_range()?;

    handle_get_blob(context, namespace, &digest, &mime_types, range, identity).await
}

pub async fn dispatch_head_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_types = headers.accepted_content_types();

    handle_head_blob(context, namespace, &digest, &mime_types).await
}

#[cfg(test)]
mod tests {
    use hyper::{Request, StatusCode};
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use crate::{
        command::server::server_context::tests::create_test_repo_context, identity::ClientIdentity,
        oci::Namespace, registry::test_utils::upload_blob,
    };

    use super::dispatch_get_blob;

    /// A blob GET emits one `blob.pull` event carrying the blob digest.
    #[tokio::test]
    async fn get_blob_emits_pull_event() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let context = create_test_repo_context(Some(&server.uri())).await;
        let namespace = Namespace::new("test/repo").unwrap();
        let identity = ClientIdentity::new(None);
        let digest = upload_blob(&context.registry, &namespace, b"pull event blob").await;

        let (parts, ()) = Request::builder().body(()).unwrap().into_parts();
        let response = dispatch_get_blob(&context, &parts, &namespace, digest.clone(), &identity)
            .await
            .expect("the pull must succeed");
        assert_eq!(response.status(), StatusCode::OK);

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "exactly one pull event must be posted");
        let event: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(event["kind"], "blob.pull");
        assert_eq!(event["repository"], "test");
        assert_eq!(event["digest"], digest.to_string());
    }
}
