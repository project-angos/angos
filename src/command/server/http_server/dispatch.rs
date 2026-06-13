use std::{net::SocketAddr, sync::Arc};

use hyper::{Method, Request, Response, body::Incoming, http::request::Parts};
use tracing::instrument;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers,
        http_server::observability::{
            handle_healthz, handle_metrics, handle_readyz, handle_ui_config,
        },
        response_body::ResponseBody,
        ui,
    },
    identity::{Action, ClientIdentity},
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
    let remote_address = parts.extensions.get::<SocketAddr>().copied();
    let identity = context.authenticate_request(parts, remote_address).await?;
    context.authorize_request(route, &identity, parts).await?;
    Ok(identity)
}

#[allow(clippy::too_many_lines)]
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
        Action::MountBlob {
            namespace,
            digest,
            from,
        } => {
            let eventful = handlers::upload::handle_mount_blob(
                context, parts, &namespace, digest, from, identity,
            )
            .await?;
            handlers::dispatch_eventful(context, eventful).await
        }
        Action::GetUpload { namespace, uuid } => {
            handlers::upload::handle_get_upload(context, &namespace, uuid).await
        }
        Action::PatchUpload { namespace, uuid } => {
            handlers::upload::dispatch_patch_upload(context, parts, incoming, &namespace, uuid)
                .await
        }
        Action::PutUpload {
            namespace,
            uuid,
            digest,
        } => {
            handlers::upload::dispatch_put_upload(
                context, parts, incoming, &namespace, uuid, digest, identity,
            )
            .await
        }
        Action::DeleteUpload { namespace, uuid } => {
            handlers::upload::handle_delete_upload(context, &namespace, uuid).await
        }
        Action::GetBlob { namespace, digest } => {
            handlers::blob::dispatch_get_blob(context, parts, &namespace, digest).await
        }
        Action::HeadBlob { namespace, digest } => {
            handlers::blob::dispatch_head_blob(context, parts, &namespace, digest).await
        }
        Action::DeleteBlob { namespace, digest } => {
            handlers::blob::handle_delete_blob(context, &namespace, &digest).await
        }
        Action::GetManifest {
            namespace,
            reference,
        } => handlers::manifest::dispatch_get_manifest(context, parts, &namespace, reference).await,
        Action::HeadManifest {
            namespace,
            reference,
        } => {
            handlers::manifest::dispatch_head_manifest(context, parts, &namespace, reference).await
        }
        Action::PutManifest {
            namespace,
            reference,
        } => {
            handlers::manifest::dispatch_put_manifest(
                context, parts, incoming, &namespace, reference, identity,
            )
            .await
        }
        Action::DeleteManifest {
            namespace,
            reference,
        } => {
            handlers::manifest::dispatch_delete_manifest(
                context, parts, &namespace, reference, identity,
            )
            .await
        }
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
        Action::ListJobs { queue, n, after } => {
            handlers::ext::handle_list_jobs(context, &queue, n, after).await
        }
        Action::ListFailedJobs { queue, n, after } => {
            handlers::ext::handle_list_failed_jobs(context, &queue, n, after).await
        }
        Action::RetryJob { queue, storage_key } => {
            handlers::ext::handle_retry_job(context, &queue, &storage_key).await
        }
        Action::DeleteJob {
            queue,
            state,
            storage_key,
        } => handlers::ext::handle_delete_job(context, &queue, state, &storage_key).await,
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
