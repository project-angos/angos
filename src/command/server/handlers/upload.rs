use hyper::{Response, StatusCode, body::Incoming, header::CONTENT_RANGE, http::request::Parts};

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::build_response,
        request::{RequestHeaders, incoming_into_async_read},
        response::{HeaderMap, ResponseHeaders},
        response_body::ResponseBody,
    },
    event_webhook::event::EventActor,
    identity::ClientIdentity,
    oci::{Digest, Namespace, UploadSessionId},
    registry::{BlobMount, DOCKER_UPLOAD_UUID, StartUploadResponse},
};

/// Headers for a completed-blob response (used by `StartUpload` when the digest
/// already exists, and by `CompleteUpload` when the upload finishes).
fn blob_location_headers(namespace: &Namespace, digest: &Digest) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/{digest}"))
        .docker_content_digest(digest)
        .into_inner()
}

fn upload_session_headers(namespace: &Namespace, session_uuid: &str) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/uploads/{session_uuid}"))
        .range("0-0")
        .with(DOCKER_UPLOAD_UUID, session_uuid)
        .into_inner()
}

fn upload_status_headers(namespace: &Namespace, session_id: &str, range_max: u64) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/uploads/{session_id}"))
        .range(format!("0-{range_max}"))
        .with(DOCKER_UPLOAD_UUID, session_id)
        .into_inner()
}

fn patch_upload_headers(namespace: &Namespace, session_id: &str, range_max: u64) -> HeaderMap {
    ResponseHeaders::new()
        .location(format!("/v2/{namespace}/blobs/uploads/{session_id}"))
        .range(format!("0-{range_max}"))
        .with(DOCKER_UPLOAD_UUID, session_id)
        .content_length(0)
        .into_inner()
}

pub async fn handle_start_upload(
    context: &ServerContext,
    namespace: &Namespace,
    digest: Option<Digest>,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.start_upload(namespace, digest).await?;
    upload_start_response(response)
}

pub async fn handle_mount_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
    from: Option<Namespace>,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mount = BlobMount { digest, from };
    // A mount must not hand the caller bytes they could not otherwise read, so
    // authorize against a namespace holding the blob first. An unsatisfiable
    // mount degrades to an ordinary upload session rather than leaking the blob.
    let Some(source) = context
        .authorize_mount_source(&mount, identity, parts)
        .await?
    else {
        let response = context.registry.start_upload(namespace, None).await?;
        return upload_start_response(response);
    };

    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .mount_blob(actor, namespace, &mount, &source)
        .await?;
    upload_start_response(response)
}

/// Maps a [`StartUploadResponse`] to its HTTP response: an existing/mounted blob
/// is `201 Created`, a fresh session is `202 Accepted`.
fn upload_start_response(response: StartUploadResponse) -> Result<Response<ResponseBody>, Error> {
    match response {
        StartUploadResponse::ExistingBlob { namespace, digest } => build_response(
            StatusCode::CREATED,
            blob_location_headers(&namespace, &digest),
            ResponseBody::empty(),
        ),
        StartUploadResponse::Session {
            namespace,
            session_uuid,
        } => build_response(
            StatusCode::ACCEPTED,
            upload_session_headers(&namespace, &session_uuid),
            ResponseBody::empty(),
        ),
    }
}

pub async fn handle_get_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: UploadSessionId,
) -> Result<Response<ResponseBody>, Error> {
    let response = context.registry.get_upload_status(namespace, &uuid).await?;

    let range_max = response.size.saturating_sub(1);
    build_response(
        StatusCode::NO_CONTENT,
        upload_status_headers(&response.namespace, &response.session_id, range_max),
        ResponseBody::empty(),
    )
}

pub async fn handle_patch_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: UploadSessionId,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let start_offset = headers.range(CONTENT_RANGE)?.map(|(start, _)| start);
    // A missing Content-Length is a chunked (Transfer-Encoding: chunked) upload,
    // which docker push sends; the body is then streamed to EOF.
    let content_length = headers.content_length()?;
    let body_stream = incoming_into_async_read(incoming);

    let response = context
        .registry
        .patch_upload(namespace, &uuid, start_offset, content_length, body_stream)
        .await?;

    let range_max = response.size.saturating_sub(1);
    build_response(
        StatusCode::ACCEPTED,
        patch_upload_headers(&response.namespace, &response.session_id, range_max),
        ResponseBody::empty(),
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_put_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: UploadSessionId,
    digest: &Digest,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let headers = RequestHeaders::new(&parts.headers);
    let start_offset = headers.range(CONTENT_RANGE)?.map(|(start, _)| start);
    let content_length = headers.content_length()?;
    let body_reader = incoming_into_async_read(incoming);

    let actor = Some(EventActor::from(identity.clone()));
    let response = context
        .registry
        .complete_upload(
            actor,
            namespace,
            &uuid,
            digest,
            start_offset,
            content_length,
            body_reader,
        )
        .await?;

    build_response(
        StatusCode::CREATED,
        blob_location_headers(&response.namespace, &response.digest),
        ResponseBody::empty(),
    )
}

pub async fn handle_delete_upload(
    context: &ServerContext,
    namespace: &Namespace,
    uuid: UploadSessionId,
) -> Result<Response<ResponseBody>, Error> {
    context.registry.delete_upload(namespace, &uuid).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(ResponseBody::empty())?)
}

#[cfg(test)]
mod tests {
    use hyper::header::{CONTENT_LENGTH, LOCATION, RANGE};

    use crate::{
        oci::{Digest, Namespace},
        registry::{DOCKER_CONTENT_DIGEST, DOCKER_UPLOAD_UUID},
    };

    use super::{
        blob_location_headers, patch_upload_headers, upload_session_headers, upload_status_headers,
    };

    fn sample_namespace() -> Namespace {
        Namespace::new("test-repo").unwrap()
    }

    fn sample_digest() -> Digest {
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .parse()
            .unwrap()
    }

    #[test]
    fn blob_location_headers_carries_location_and_digest() {
        let namespace = sample_namespace();
        let digest = sample_digest();
        let headers = blob_location_headers(&namespace, &digest);
        assert_eq!(
            headers[LOCATION.as_str()],
            format!("/v2/{namespace}/blobs/{digest}")
        );
        assert_eq!(headers[DOCKER_CONTENT_DIGEST], digest.to_string());
    }

    #[test]
    fn upload_session_headers_start_at_zero_range() {
        let namespace = sample_namespace();
        let headers = upload_session_headers(&namespace, "session-uuid");
        assert_eq!(
            headers[LOCATION.as_str()],
            format!("/v2/{namespace}/blobs/uploads/session-uuid")
        );
        assert_eq!(headers[RANGE.as_str()], "0-0");
        assert_eq!(headers[DOCKER_UPLOAD_UUID], "session-uuid");
    }

    #[test]
    fn upload_status_headers_reports_current_range() {
        let namespace = sample_namespace();
        let headers = upload_status_headers(&namespace, "session-uuid", 41);
        assert_eq!(
            headers[LOCATION.as_str()],
            format!("/v2/{namespace}/blobs/uploads/session-uuid")
        );
        assert_eq!(headers[RANGE.as_str()], "0-41");
        assert_eq!(headers[DOCKER_UPLOAD_UUID], "session-uuid");
    }

    #[test]
    fn patch_upload_headers_include_zero_content_length() {
        let namespace = sample_namespace();
        let headers = patch_upload_headers(&namespace, "session-uuid", 41);
        assert_eq!(
            headers[LOCATION.as_str()],
            format!("/v2/{namespace}/blobs/uploads/session-uuid")
        );
        assert_eq!(headers[RANGE.as_str()], "0-41");
        assert_eq!(headers[DOCKER_UPLOAD_UUID], "session-uuid");
        assert_eq!(headers[CONTENT_LENGTH.as_str()], "0");
    }
}
