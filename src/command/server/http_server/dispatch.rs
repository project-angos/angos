use std::{net::SocketAddr, sync::Arc};

use hyper::{
    Method, Request, Response, StatusCode, body::Incoming, header::CONTENT_RANGE,
    http::request::Parts,
};
use tracing::instrument;

use crate::registry::content_discovery::ListCatalogRequest;
use angos_oci::request::{
    BlobMount, CompleteUploadRequest, DeleteBlobRequest, DeleteManifestRequest,
    DeleteUploadRequest, GetBlobRequest, GetManifestRequest, GetReferrersRequest, GetUploadRequest,
    HeadBlobRequest, HeadManifestRequest, ListTagsRequest, MountBlobRequest, PatchUploadRequest,
    PutManifestRequest, StartUploadRequest, StartUploadTarget,
};
use angos_oci::response::ErrorCode;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers,
        request::{RequestHeaders, incoming_into_async_read},
        router,
    },
    event_webhook::event::EventActor,
    http_response::ResponseBody,
    identity::{Action, ClientIdentity},
    registry::{
        self, DeleteJobRequest, ListJobsRequest, ListPullsRequest, RetryJobRequest,
        layers::{LayerEntriesRequest, LayerFileRequest},
    },
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
    let headers = RequestHeaders::new(&parts.headers);
    let registry = &context.registry;
    // One actor for the request: only the arm that runs consumes it.
    let actor = Some(EventActor::from(identity.clone()));

    match route {
        Action::UiAsset { path } if context.enable_ui => handlers::handle_ui_asset(&path),
        Action::UiConfig if context.enable_ui => handlers::handle_ui_config(&context.ui_name),
        Action::UiAsset { .. } | Action::UiConfig => handle_unknown_route(parts),
        Action::Token => {
            let Some(token_issuer) = context.token_issuer() else {
                return Err(Error::NotFound(
                    "No token service is configured".to_string(),
                ));
            };

            handlers::handle_get_token(token_issuer, identity)
        }
        Action::ApiVersion => Ok(registry::api_version()?),
        Action::StartUpload {
            namespace,
            digest,
            digest_algorithm,
        } => {
            // A body with no `?digest=` has nothing to verify it against, so
            // the request opens a session and the body is not read.
            let content_length = headers.content_length()?;
            Ok(registry
                .start_upload(
                    actor,
                    StartUploadRequest {
                        namespace,
                        digest_algorithm,
                        target: digest.map(|digest| StartUploadTarget {
                            digest,
                            content_length,
                        }),
                    },
                    incoming_into_async_read(incoming),
                )
                .await?)
        }
        Action::MountBlob {
            namespace,
            digest,
            from,
        } => {
            let mount = BlobMount { digest, from };
            // A mount must not hand the caller bytes they could not otherwise
            // read, so resolve a source namespace they may read from first.
            let source = context
                .authorize_mount_source(&mount, identity, parts)
                .await?;

            Ok(registry
                .mount_blob(actor, MountBlobRequest { namespace, mount }, source)
                .await?)
        }
        Action::GetUpload {
            namespace,
            session_id,
        } => Ok(registry
            .get_upload_status(GetUploadRequest {
                namespace,
                session_id,
            })
            .await?),
        Action::PatchUpload {
            namespace,
            session_id,
        } => Ok(registry
            .patch_upload(
                PatchUploadRequest {
                    namespace,
                    session_id,
                    content_range: headers.chunk_range(CONTENT_RANGE)?,
                    content_length: headers.content_length()?,
                },
                incoming_into_async_read(incoming),
            )
            .await?),
        Action::PutUpload {
            namespace,
            session_id,
            digest,
        } => Ok(registry
            .complete_upload(
                actor,
                CompleteUploadRequest {
                    namespace,
                    session_id,
                    digest,
                    content_range: headers.chunk_range(CONTENT_RANGE)?,
                    content_length: headers.content_length()?,
                },
                incoming_into_async_read(incoming),
            )
            .await?),
        Action::DeleteUpload {
            namespace,
            session_id,
        } => Ok(registry
            .delete_upload(DeleteUploadRequest {
                namespace,
                session_id,
            })
            .await?),
        Action::GetBlob { namespace, digest } => Ok(registry
            .resolve_get_blob(
                actor,
                GetBlobRequest {
                    namespace,
                    digest,
                    accepted_types: headers.accepted_content_types(),
                    range: headers.blob_range()?,
                },
                !headers.redirect_suppressed(),
            )
            .await?),
        Action::HeadBlob { namespace, digest } => Ok(registry
            .head_blob(HeadBlobRequest {
                namespace,
                digest,
                accepted_types: headers.accepted_content_types(),
            })
            .await?),
        Action::DeleteBlob { namespace, digest } => Ok(registry
            .delete_blob(DeleteBlobRequest { namespace, digest })
            .await?),
        Action::GetManifest {
            namespace,
            reference,
        } => Ok(registry
            .resolve_get_manifest(
                actor,
                GetManifestRequest {
                    namespace,
                    reference,
                    accepted_types: headers.accepted_content_types(),
                },
                !headers.redirect_suppressed(),
            )
            .await?),
        Action::HeadManifest {
            namespace,
            reference,
        } => Ok(registry
            .head_manifest(
                actor,
                HeadManifestRequest {
                    namespace,
                    reference,
                    accepted_types: headers.accepted_content_types(),
                },
            )
            .await?),
        Action::PutManifest { namespace, target } => {
            let (reference, tags) = target.into_parts();
            let content_type = headers.content_type()?.ok_or(Error::BadRequest(
                "No Content-Type header provided".to_string(),
            ))?;

            Ok(registry
                .accept_put_manifest(
                    actor,
                    PutManifestRequest {
                        namespace,
                        reference,
                        content_type: Some(content_type),
                        tags,
                        source_ts: headers.source_timestamp(),
                    },
                    incoming_into_async_read(incoming),
                )
                .await?)
        }
        Action::DeleteManifest {
            namespace,
            reference,
        } => Ok(registry
            .accept_delete_manifest(
                actor,
                DeleteManifestRequest {
                    source_ts: headers.source_timestamp(),
                    namespace,
                    reference,
                },
            )
            .await?),
        Action::GetReferrer {
            namespace,
            digest,
            artifact_type,
            last,
        } => Ok(registry
            .get_referrers(GetReferrersRequest {
                namespace,
                digest,
                artifact_type,
                last,
            })
            .await?),
        Action::ListCatalog { n, last } => Ok(registry
            .list_catalog_entries(ListCatalogRequest { n, last }, |namespace| {
                context.catalog_lists_namespace(namespace, identity)
            })
            .await?),
        Action::ListTags { namespace, n, last } => Ok(registry
            .list_tag_entries(ListTagsRequest { namespace, n, last })
            .await?),
        Action::ListRevisions { namespace } => Ok(registry.get_revisions_info(&namespace).await?),
        Action::ListLayerEntries { namespace, digest } => Ok(registry
            .get_layer_entries(LayerEntriesRequest { namespace, digest })
            .await?),
        Action::GetLayerFile {
            namespace,
            digest,
            path,
            download,
        } => Ok(registry
            .get_layer_file(LayerFileRequest {
                namespace,
                digest,
                path,
                download,
            })
            .await?),
        Action::ListUploads { namespace } => Ok(registry.get_uploads_info(&namespace).await?),
        Action::ListPulls {
            namespace,
            reference,
        } => Ok(registry
            .get_pull_history(ListPullsRequest {
                namespace,
                reference,
            })
            .await?),
        Action::ListRepositories => Ok(registry.get_repositories_info().await?),
        Action::ListNamespaces { repository } => {
            Ok(registry.get_namespaces_info(&repository).await?)
        }
        Action::ListJobs { queue, n, after } => Ok(registry
            .get_jobs_info(ListJobsRequest { queue, n, after })
            .await?),
        Action::ListFailedJobs { queue, n, after } => Ok(registry
            .get_failed_jobs_info(ListJobsRequest { queue, n, after })
            .await?),
        Action::RetryJob { queue, storage_key } => Ok(registry
            .retry_failed_job(RetryJobRequest { queue, storage_key })
            .await?),
        Action::DeleteJob {
            queue,
            state,
            storage_key,
        } => Ok(registry
            .delete_job(DeleteJobRequest {
                queue,
                state,
                storage_key,
            })
            .await?),
        Action::Healthz => handlers::handle_healthz(),
        Action::Readyz => handlers::handle_readyz(registry).await,
        Action::Metrics => handlers::handle_metrics(),
    }
}

/// A read is a miss, a write is a bad request. The router collapses a malformed
/// reference, digest or query value into the same `None` as an unserved path,
/// and conformance requires `400` from a manifest `PUT` whose reference parses
/// as neither a tag nor a digest.
pub fn handle_unknown_route(parts: &Parts) -> Result<Response<ResponseBody>, Error> {
    // The referrers endpoint is the exception: the spec requires `400` from a
    // read whose digest or filter is malformed, not the miss below.
    if router::is_invalid_referrers_request(&parts.method, &parts.uri) {
        return Err(registry::Error::DigestInvalid.into());
    }

    // A path angos serves, reached with a method it does not: 405 says so, and
    // the catch-all below would answer 400. containerd probes the OAuth2 form
    // of the token endpoint, `POST /token`, before the plain `GET /token` this
    // registry serves, and falls back only on 401, 404 or 405 -- a 400 ends the
    // exchange and strands every push behind it.
    if parts.uri.path() == router::TOKEN_PATH {
        return Err(Error::Custom {
            status_code: StatusCode::METHOD_NOT_ALLOWED,
            code: ErrorCode::Unsupported.as_str().to_string(),
            msg: Some(format!(
                "the token endpoint serves GET, not {}",
                parts.method
            )),
        });
    }

    if [Method::GET, Method::HEAD].contains(&parts.method) {
        let msg = format!("unknown route: {} {}", parts.method, parts.uri);
        Err(Error::NotFound(msg))
    } else {
        let msg = format!("unsupported route: {} {}", parts.method, parts.uri);
        Err(Error::BadRequest(msg))
    }
}
