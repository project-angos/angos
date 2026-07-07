use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hyper::{Response, StatusCode, body::Incoming, http::request::Parts};
use tokio::io::AsyncRead;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::build_response,
        request::{RequestHeaders, incoming_into_async_read},
        response_body::ResponseBody,
    },
    event_webhook::event::EventActor,
    identity::ClientIdentity,
    oci::{MediaType, Namespace, Reference, Tag},
    registry::GetManifestResponse,
};

pub async fn handle_head_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = RequestHeaders::new(&parts.headers).accepted_content_types();
    let is_tag_immutable = context.is_reference_immutable(namespace, &reference);
    let repository = context.registry.get_repository_for_namespace(namespace)?;
    let response = context
        .registry
        .head_manifest(
            repository,
            &mime_types,
            namespace,
            reference,
            is_tag_immutable,
        )
        .await?;

    build_response(StatusCode::OK, response.headers, ResponseBody::empty())
}

pub async fn handle_get_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = RequestHeaders::new(&parts.headers).accepted_content_types();
    let is_tag_immutable = context.is_reference_immutable(namespace, &reference);
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .resolve_get_manifest(actor, namespace, reference, &mime_types, is_tag_immutable)
        .await?;

    match response {
        GetManifestResponse::Redirect { headers, .. } => build_response(
            StatusCode::TEMPORARY_REDIRECT,
            headers,
            ResponseBody::empty(),
        ),
        GetManifestResponse::Body {
            headers, content, ..
        } => build_response(StatusCode::OK, headers, ResponseBody::fixed(content)),
    }
}

/// The stream-generic core of [`handle_put_manifest`], separate so tests can
/// drive it with an in-memory body instead of a hyper [`Incoming`].
#[allow(clippy::too_many_arguments)]
async fn put_manifest<S>(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_type: MediaType,
    body_stream: S,
    tags: Vec<Tag>,
    identity: &ClientIdentity,
    source_ts: Option<DateTime<Utc>>,
) -> Result<Response<ResponseBody>, Error>
where
    S: AsyncRead + Unpin + Send,
{
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .accept_put_manifest(
            actor,
            source_ts,
            namespace,
            reference,
            mime_type,
            body_stream,
            tags,
        )
        .await?;

    build_response(StatusCode::CREATED, response.headers, ResponseBody::empty())
}

pub async fn handle_delete_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let source_ts = RequestHeaders::new(&parts.headers).source_timestamp();
    let actor = Some(EventActor::from(identity.clone()));
    context
        .registry
        .delete_manifest(actor, source_ts, namespace, &reference)
        .await?;

    build_response(StatusCode::ACCEPTED, HashMap::new(), ResponseBody::empty())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_put_manifest(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    reference: Reference,
    tags: Vec<Tag>,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_type = headers.content_type()?.ok_or(Error::BadRequest(
        "No Content-Type header provided".to_string(),
    ))?;
    let source_ts = headers.source_timestamp();
    let body_stream = incoming_into_async_read(incoming);

    put_manifest(
        context,
        namespace,
        reference,
        mime_type,
        body_stream,
        tags,
        identity,
        source_ts,
    )
    .await
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use hyper::{Request, StatusCode};
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use crate::{
        command::server::{
            ServerContext, error::Error, request::RequestHeaders,
            server_context::tests::create_test_repo_context,
        },
        identity::ClientIdentity,
        oci::{MediaType, Namespace, Reference, Tag},
        registry_client::{REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP},
    };

    use super::{handle_get_manifest, put_manifest};

    const MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

    // A context whose resolver matches "test/*", required for the read-back path
    // (the default test config declares no [repository.*] so it would resolve none).
    async fn context_with_test_repo() -> ServerContext {
        create_test_repo_context(None).await
    }

    // Distinct, reference-free manifests yield distinct digests with no blob uploads.
    fn manifest_a() -> Vec<u8> {
        br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"seam":"A"}}"#.to_vec()
    }

    fn manifest_b() -> Vec<u8> {
        br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","annotations":{"seam":"B"}}"#.to_vec()
    }

    // Build real Parts carrying the header, then parse via RequestHeaders so the
    // header-reading code path (not a hand-built Option) is genuinely exercised.
    fn source_ts_from_header(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        let request = Request::builder()
            .header(X_ANGOS_SOURCE_TIMESTAMP, value)
            .body(())
            .unwrap();
        let (parts, ()) = request.into_parts();
        RequestHeaders::new(&parts.headers).source_timestamp()
    }

    /// A tag GET emits one `manifest.pull` event carrying the resolved digest,
    /// the requested reference, and the tag.
    #[tokio::test]
    async fn get_manifest_emits_pull_event() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let context = create_test_repo_context(Some(&server.uri())).await;
        let namespace = Namespace::new("test/repo").unwrap();
        let identity = ClientIdentity::new(None);

        let put_resp = put_manifest(
            &context,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            MediaType::new(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(manifest_a()),
            Vec::new(),
            &identity,
            None,
        )
        .await
        .expect("seeding the manifest must succeed");
        assert_eq!(put_resp.status(), StatusCode::CREATED);

        let (parts, ()) = Request::builder().body(()).unwrap().into_parts();
        let response = handle_get_manifest(
            &context,
            &parts,
            &namespace,
            Reference::Tag(Tag::new("latest").unwrap()),
            &identity,
        )
        .await
        .expect("the pull must succeed");
        assert_eq!(response.status(), StatusCode::OK);

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "exactly one pull event must be posted");
        let event: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(event["kind"], "manifest.pull");
        assert_eq!(event["repository"], "test");
        assert_eq!(event["reference"], "latest");
        assert_eq!(event["tag"], "latest");
        assert!(
            event["digest"]
                .as_str()
                .is_some_and(|d| d.starts_with("sha256:")),
            "the event must carry the resolved digest, got: {event}"
        );
    }

    #[tokio::test]
    async fn backdated_source_ts_loses_to_newer_local_tag() {
        let context = context_with_test_repo().await;
        let namespace = Namespace::new("test/repo").unwrap();
        let identity = ClientIdentity::new(None);
        let tag = || Reference::Tag(Tag::new("latest").unwrap());

        // Seed the newer local tag with manifest B at a known recent source_ts so
        // created_at is stamped deterministically rather than from the wall clock.
        let newer_ts = Utc::now() - Duration::seconds(10);
        let seed_resp = put_manifest(
            &context,
            &namespace,
            tag(),
            MediaType::new(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(manifest_b()),
            Vec::new(),
            &identity,
            Some(newer_ts),
        )
        .await
        .expect("seeding the newer local tag must succeed");
        assert_eq!(seed_resp.status(), StatusCode::CREATED);

        // Record the digest the tag should keep (manifest B's digest).
        let repo = context
            .registry
            .get_repository_for_namespace(&namespace)
            .unwrap();
        let kept_digest = context
            .registry
            .get_manifest(
                repo,
                std::slice::from_ref(&MEDIA_TYPE.to_string()),
                &namespace,
                tag(),
                false,
            )
            .await
            .expect("seeded tag must be readable")
            .digest;

        // Replicate a different manifest with a backdated header (older than the
        // local tag), with source_ts derived from a real RequestHeaders parse.
        let backdated = (newer_ts - Duration::seconds(60)).to_rfc3339();
        let source_ts = source_ts_from_header(&backdated);
        assert!(source_ts.is_some(), "header must parse to a source_ts");

        let result = put_manifest(
            &context,
            &namespace,
            tag(),
            MediaType::new(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(manifest_a()),
            Vec::new(),
            &identity,
            source_ts,
        )
        .await;

        // The backdated write must be superseded (409 REPLICATION_SUPERSEDED).
        match result {
            Err(Error::Custom {
                status_code, code, ..
            }) => {
                assert_eq!(status_code, StatusCode::CONFLICT);
                assert_eq!(code, REPLICATION_SUPERSEDED_CODE);
            }
            Err(other) => panic!("expected ReplicationSuperseded conflict, got error: {other:?}"),
            Ok(_) => panic!("expected ReplicationSuperseded conflict, got Ok"),
        }

        // Kill criterion: the tag must still point at manifest B. If the handler
        // passed None (threading removed), the backdated put would have overwritten
        // the tag to manifest A's digest and this assertion would fail.
        let repo = context
            .registry
            .get_repository_for_namespace(&namespace)
            .unwrap();
        let after = context
            .registry
            .get_manifest(
                repo,
                std::slice::from_ref(&MEDIA_TYPE.to_string()),
                &namespace,
                tag(),
                false,
            )
            .await
            .expect("tag must still resolve")
            .digest;
        assert_eq!(
            after, kept_digest,
            "backdated push must not overwrite the newer local tag"
        );
    }
}
