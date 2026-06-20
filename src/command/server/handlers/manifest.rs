use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hyper::{Response, StatusCode, body::Incoming, http::request::Parts};
use tokio::io::AsyncRead;

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
    oci::{Namespace, Reference},
    registry::GetManifestResponse,
};

pub async fn handle_head_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_types: &[String],
    is_tag_immutable: bool,
) -> Result<Response<ResponseBody>, Error> {
    let repository = context.registry.get_repository_for_namespace(namespace)?;
    let response = context
        .registry
        .head_manifest(
            repository,
            mime_types,
            namespace,
            reference,
            is_tag_immutable,
        )
        .await?;

    build_response(StatusCode::OK, response.headers, ResponseBody::empty())
}

pub async fn handle_get_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_types: &[String],
    is_tag_immutable: bool,
) -> Result<Response<ResponseBody>, Error> {
    let response = context
        .registry
        .resolve_get_manifest(namespace, reference, mime_types, is_tag_immutable)
        .await?;

    match response {
        GetManifestResponse::Redirect { headers } => build_response(
            StatusCode::TEMPORARY_REDIRECT,
            headers,
            ResponseBody::empty(),
        ),
        GetManifestResponse::Body { headers, content } => {
            build_response(StatusCode::OK, headers, ResponseBody::fixed(content))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_put_manifest<S>(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    mime_type: String,
    body_stream: S,
    tags: Vec<String>,
    identity: &ClientIdentity,
    source_ts: Option<DateTime<Utc>>,
) -> Result<EventfulResponse, Error>
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

    build_event_response(StatusCode::CREATED, response.headers, response.events)
}

pub async fn handle_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
    source_ts: Option<DateTime<Utc>>,
) -> Result<EventfulResponse, Error> {
    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .delete_manifest(actor, source_ts, namespace, &reference)
        .await?;

    build_event_response(StatusCode::ACCEPTED, HashMap::new(), response.events)
}

pub async fn dispatch_get_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_types = headers.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handle_get_manifest(context, namespace, reference, &mime_types, is_immutable).await
}

pub async fn dispatch_head_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_types = headers.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handle_head_manifest(context, namespace, reference, &mime_types, is_immutable).await
}

#[allow(clippy::too_many_arguments)]
pub async fn dispatch_put_manifest(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    reference: Reference,
    tags: Vec<String>,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let mime_type = headers.content_type()?.ok_or(Error::BadRequest(
        "No Content-Type header provided".to_string(),
    ))?;
    let source_ts = headers.source_timestamp();

    let body_stream = incoming_into_async_read(incoming);

    dispatch_eventful(
        context,
        handle_put_manifest(
            context,
            namespace,
            reference,
            mime_type,
            body_stream,
            tags,
            identity,
            source_ts,
        )
        .await?,
    )
    .await
}

pub async fn dispatch_delete_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let source_ts = headers.source_timestamp();

    dispatch_eventful(
        context,
        handle_delete_manifest(context, namespace, reference, identity, source_ts).await?,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::{Duration, Utc};
    use hyper::{Request, StatusCode};

    use crate::{
        command::server::{
            ServerContext, error::Error, request::RequestHeaders,
            server_context::tests::create_test_server_context_from_config,
        },
        configuration::Configuration,
        identity::ClientIdentity,
        oci::{Namespace, Reference},
        replication::{REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP},
    };

    use super::handle_put_manifest;

    const MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

    // A context whose resolver matches "test/*", required for the read-back path
    // (the default test config declares no [repository.*] so it would resolve none).
    // Both stores share one unique root so a manifest written through the metadata
    // store is readable back through the blob store, as FSRegistryTestCase does.
    async fn context_with_test_repo() -> ServerContext {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let toml = format!(
            r#"
            [blob_store.fs]
            root_dir = "/tmp/angos-seam-{nonce}"

            [metadata_store.fs]
            root_dir = "/tmp/angos-seam-{nonce}"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false

            [global.access_policy]
            default = "allow"
            rules = []

            [repository.test]
            namespace_pattern = "^test/.*"

            [repository.test.access_policy]
            default = "allow"
            rules = []
        "#
        );
        let config: Configuration = toml::from_str(&toml).unwrap();
        create_test_server_context_from_config(&config).await
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

    #[tokio::test]
    async fn backdated_source_ts_loses_to_newer_local_tag() {
        let context = context_with_test_repo().await;
        let namespace = Namespace::new("test/repo").unwrap();
        let identity = ClientIdentity::new(None);
        let tag = || Reference::Tag("latest".to_string());

        // Seed the newer local tag with manifest B at a known recent source_ts so
        // created_at is stamped deterministically rather than from the wall clock.
        let newer_ts = Utc::now() - Duration::seconds(10);
        let (seed_resp, _events) = handle_put_manifest(
            &context,
            &namespace,
            tag(),
            MEDIA_TYPE.to_string(),
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

        let result = handle_put_manifest(
            &context,
            &namespace,
            tag(),
            MEDIA_TYPE.to_string(),
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
