use hyper::{
    Response, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_RANGE},
    http::request::Parts,
};

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::build_response,
        request::RequestHeaders,
        response::{HeaderMap, ResponseHeaders},
        response_body::ResponseBody,
    },
    event_webhook::event::EventActor,
    identity::ClientIdentity,
    oci::{Digest, Namespace},
    registry::{GetBlobResponse, blob::ResolvedRange},
};

fn head_blob_headers(digest: &Digest, size: u64) -> HeaderMap {
    ResponseHeaders::new()
        .docker_content_digest(digest)
        .content_length(size)
        .into_inner()
}

fn get_blob_headers(digest: &Digest, total_length: u64) -> HeaderMap {
    ResponseHeaders::new()
        .docker_content_digest(digest)
        .with(ACCEPT_RANGES.as_str(), "bytes")
        .content_length(total_length)
        .into_inner()
}

fn get_blob_range_headers(digest: &Digest, range: ResolvedRange) -> HeaderMap {
    ResponseHeaders::new()
        .docker_content_digest(digest)
        .with(ACCEPT_RANGES.as_str(), "bytes")
        .content_length(range.length)
        .with(
            CONTENT_RANGE.as_str(),
            format!("bytes {}-{}/{}", range.start, range.end, range.total_length),
        )
        .into_inner()
}

fn get_blob_redirect_headers(url: String, digest: &Digest) -> HeaderMap {
    ResponseHeaders::new()
        .location(url)
        .docker_content_digest(digest)
        .into_inner()
}

pub async fn handle_head_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: &Digest,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = RequestHeaders::new(&parts.headers).accepted_content_types();
    let repository = context.registry.get_repository_for_namespace(namespace)?;
    let response = context
        .registry
        .head_blob(repository, &mime_types, namespace, digest)
        .await?;

    build_response(
        StatusCode::OK,
        head_blob_headers(&response.digest, response.size),
        ResponseBody::empty(),
    )
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
    parts: &Parts,
    namespace: &Namespace,
    digest: &Digest,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_types = headers.accepted_content_types();
    let range = headers.blob_range()?;
    let allow_redirect = !headers.redirect_suppressed();

    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .resolve_get_blob(actor, namespace, digest, &mime_types, range, allow_redirect)
        .await?;

    match response {
        GetBlobResponse::Redirect {
            redirect_url,
            digest,
        } => build_response(
            StatusCode::TEMPORARY_REDIRECT,
            get_blob_redirect_headers(redirect_url, &digest),
            ResponseBody::empty(),
        ),
        GetBlobResponse::Reader {
            digest,
            total_length,
            body,
        } => build_response(
            StatusCode::OK,
            get_blob_headers(&digest, total_length),
            ResponseBody::streaming(body),
        ),
        GetBlobResponse::RangedReader {
            digest,
            range,
            body,
        } => build_response(
            StatusCode::PARTIAL_CONTENT,
            get_blob_range_headers(&digest, range),
            ResponseBody::streaming(body),
        ),
    }
}

#[cfg(test)]
mod tests {
    use hyper::{
        Request, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, LOCATION},
    };
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use crate::{
        command::server::server_context::tests::create_test_repo_context,
        identity::ClientIdentity,
        oci::{Digest, Namespace},
        registry::DOCKER_CONTENT_DIGEST,
        registry::test_utils::upload_blob,
    };

    use super::{
        ResolvedRange, get_blob_headers, get_blob_range_headers, get_blob_redirect_headers,
        handle_get_blob, head_blob_headers,
    };

    fn sample_digest() -> Digest {
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .parse()
            .unwrap()
    }

    #[test]
    fn head_blob_headers_contains_required_fields() {
        let headers = head_blob_headers(&sample_digest(), 42);
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], sample_digest().to_string());
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "42");
    }

    #[test]
    fn get_blob_headers_includes_accept_ranges() {
        let headers = get_blob_headers(&sample_digest(), 1024);
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], sample_digest().to_string());
        assert_eq!(headers[ACCEPT_RANGES.as_str()], "bytes");
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "1024");
    }

    #[test]
    fn get_blob_range_headers_computes_content_length() {
        let range = ResolvedRange {
            start: 5,
            end: 10,
            length: 6,
            total_length: 100,
        };
        let headers = get_blob_range_headers(&sample_digest(), range);
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "6");
        assert_eq!(headers[CONTENT_RANGE.as_str()], "bytes 5-10/100");
    }

    #[test]
    fn get_blob_redirect_headers_carries_location_and_digest() {
        let headers = get_blob_redirect_headers("https://cdn/blob".to_string(), &sample_digest());
        assert_eq!(headers[LOCATION.as_str()], "https://cdn/blob");
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], sample_digest().to_string());
    }

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
        let response = handle_get_blob(&context, &parts, &namespace, &digest, &identity)
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
