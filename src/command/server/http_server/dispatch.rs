use std::sync::Arc;

use hyper::{
    Method, Request, Response,
    body::Incoming,
    header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    http::request::Parts,
};
use tracing::instrument;
use uuid::Uuid;

use super::{
    event_emission::dispatch_events,
    observability::{handle_healthz, handle_metrics, handle_readyz, handle_ui_config},
};
use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers,
        request_ext::{HeaderExt, IntoAsyncRead},
        response_body::ResponseBody,
        ui,
    },
    identity::{Action, ClientIdentity},
    oci::{Digest, Namespace, Reference},
};

#[instrument(skip(context, req, action))]
pub async fn dispatch_request(
    context: Arc<ServerContext>,
    req: Request<Incoming>,
    action: Option<Action>,
) -> Result<Response<ResponseBody>, Error> {
    let (parts, incoming) = req.into_parts();
    let Some(action) = action else {
        return handle_unknown_route(&parts);
    };

    let identity = authenticate_and_authorize(&context, &action, &parts).await?;
    dispatch_route(&context, action, &parts, incoming, &identity).await
}

#[instrument(skip(context, parts))]
pub async fn authenticate_and_authorize(
    context: &ServerContext,
    route: &Action,
    parts: &Parts,
) -> Result<ClientIdentity, Error> {
    let remote_address = parts.extensions.get::<std::net::SocketAddr>().copied();
    let identity = context.authenticate_request(parts, remote_address).await?;
    context.authorize_request(route, &identity, parts).await?;
    Ok(identity)
}

#[instrument(skip(context, parts, incoming, identity))]
async fn dispatch_route<'a>(
    context: &'a ServerContext,
    route: Action,
    parts: &'a Parts,
    incoming: Incoming,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    match route {
        Action::UiAsset { path } if context.enable_ui => ui::serve_asset(&path),
        Action::UiConfig if context.enable_ui => handle_ui_config(context),
        Action::UiAsset { .. } | Action::UiConfig => handle_unknown_route(parts),
        Action::ApiVersion => Ok(handlers::version::handle_get_api_version()?),
        Action::StartUpload { namespace, digest } => {
            handlers::upload::handle_start_upload(context, &namespace, digest).await
        }
        Action::GetUpload { namespace, uuid } => {
            handlers::upload::handle_get_upload(context, &namespace, uuid).await
        }
        Action::PatchUpload { namespace, uuid } => {
            handle_patch_upload(context, parts, incoming, &namespace, uuid).await
        }
        Action::PutUpload {
            namespace,
            uuid,
            digest,
        } => handle_put_upload(context, parts, incoming, &namespace, uuid, digest, identity).await,
        Action::DeleteUpload { namespace, uuid } => {
            handlers::upload::handle_delete_upload(context, &namespace, uuid).await
        }
        Action::GetBlob { namespace, digest } => {
            handle_get_blob(context, parts, &namespace, digest).await
        }
        Action::HeadBlob { namespace, digest } => {
            handle_head_blob(context, parts, &namespace, digest).await
        }
        Action::DeleteBlob { namespace, digest } => {
            handlers::blob::handle_delete_blob(context, &namespace, &digest).await
        }
        Action::GetManifest {
            namespace,
            reference,
        } => handle_get_manifest(context, parts, &namespace, reference).await,
        Action::HeadManifest {
            namespace,
            reference,
        } => handle_head_manifest(context, parts, &namespace, reference).await,
        Action::PutManifest {
            namespace,
            reference,
        } => handle_put_manifest(context, parts, incoming, &namespace, reference, identity).await,
        Action::DeleteManifest {
            namespace,
            reference,
        } => handle_delete_manifest(context, &namespace, reference, identity).await,
        Action::GetReferrer {
            namespace,
            digest,
            artifact_type,
        } => {
            handlers::content_discovery::handle_get_referrers(
                context,
                &namespace,
                &digest,
                artifact_type,
            )
            .await
        }
        Action::ListCatalog { n, last } => {
            handlers::content_discovery::handle_list_catalog(context, n, last).await
        }
        Action::ListTags { namespace, n, last } => {
            handlers::content_discovery::handle_list_tags(context, &namespace, n, last).await
        }
        Action::ListRevisions { namespace } => {
            handlers::ext::handle_list_revisions(context, &namespace).await
        }
        Action::ListUploads { namespace } => {
            handlers::ext::handle_list_uploads(context, &namespace).await
        }
        Action::ListRepositories => handlers::ext::handle_list_repositories(context).await,
        Action::ListNamespaces { repository } => {
            handlers::ext::handle_list_namespaces(context, &repository).await
        }
        Action::Healthz => handle_healthz(),
        Action::Readyz => handle_readyz(context).await,
        Action::Metrics => handle_metrics(),
    }
}

pub fn handle_unknown_route(parts: &Parts) -> Result<Response<ResponseBody>, Error> {
    if [Method::GET, Method::HEAD].contains(&parts.method) {
        let msg = format!("unknown route: {} {}", parts.method, parts.uri);
        Err(Error::NotFound(msg))
    } else {
        let msg = format!("unsupported route: {} {}", parts.method, parts.uri);
        Err(Error::BadRequest(msg))
    }
}

async fn handle_patch_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
) -> Result<Response<ResponseBody>, Error> {
    let start_offset = parts.range(CONTENT_RANGE)?.map(|(start, _)| start);
    let content_length: u64 = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_stream = incoming.into_async_read();

    handlers::upload::handle_patch_upload(
        context,
        namespace,
        uuid,
        start_offset,
        content_length,
        body_stream,
    )
    .await
}

async fn handle_put_upload(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    uuid: Uuid,
    digest: Digest,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let content_length: u64 = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_stream = incoming.into_async_read();

    let (response, events) = handlers::upload::handle_put_upload(
        context,
        namespace,
        uuid,
        &digest,
        content_length,
        body_stream,
        identity,
    )
    .await?;

    dispatch_events(context, events).await?;
    Ok(response)
}

async fn handle_get_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let range = parts.range(RANGE)?;

    handlers::blob::handle_get_blob(context, namespace, &digest, &mime_types, range).await
}

async fn handle_head_blob(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    digest: Digest,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();

    handlers::blob::handle_head_blob(context, namespace, &digest, &mime_types).await
}

async fn handle_get_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handlers::manifest::handle_get_manifest(
        context,
        namespace,
        reference,
        &mime_types,
        is_immutable,
    )
    .await
}

async fn handle_head_manifest(
    context: &ServerContext,
    parts: &Parts,
    namespace: &Namespace,
    reference: Reference,
) -> Result<Response<ResponseBody>, Error> {
    let mime_types = parts.accepted_content_types();
    let is_immutable = context.is_reference_immutable(namespace, &reference);

    handlers::manifest::handle_head_manifest(
        context,
        namespace,
        reference,
        &mime_types,
        is_immutable,
    )
    .await
}

async fn handle_put_manifest(
    context: &ServerContext,
    parts: &Parts,
    incoming: Incoming,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let mime_type = parts.get_header(CONTENT_TYPE).ok_or(Error::BadRequest(
        "No Content-Type header provided".to_string(),
    ))?;

    let body_stream = incoming.into_async_read();

    let (response, events) = handlers::manifest::handle_put_manifest(
        context,
        namespace,
        reference,
        mime_type,
        body_stream,
        identity,
    )
    .await?;

    dispatch_events(context, events).await?;
    Ok(response)
}

async fn handle_delete_manifest(
    context: &ServerContext,
    namespace: &Namespace,
    reference: Reference,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    let (response, events) =
        handlers::manifest::handle_delete_manifest(context, namespace, reference, identity).await?;

    dispatch_events(context, events).await?;
    Ok(response)
}
