use hyper::{Method, Uri};

use angos_docker_extension_service::{Endpoint as DockerEndpoint, parse as parse_docker};
use angos_extension_service::{Endpoint as AngosEndpoint, parse as parse_angos};
use angos_oci::Namespace;
use angos_oci::path::API_PREFIX;
use angos_oci_service::{
    Endpoint as OciEndpoint, ManifestEndpoint, UploadEndpoint, parse as parse_oci,
};

use crate::identity::Action;
use crate::jobs::{JobState, Queue};

/// Shared with the dispatcher, which answers an unserved method here with 405.
pub const TOKEN_PATH: &str = "/token";

/// Served by the binary, so matched before the extension parser sees it.
const UI_CONFIG_PATH: &str = "/v2/_angos/ui/config";

/// An operation of one of the service surfaces, whose crates parse their own
/// paths, or one of the routes the binary itself serves.
#[derive(Debug)]
pub enum Route {
    UiAsset {
        path: String,
    },
    UiConfig,
    Token,
    Healthz,
    Readyz,
    Metrics,
    Oci(OciEndpoint),
    /// A Docker V2 extension operation (the catalog).
    Docker(DockerEndpoint),
    Angos(AngosEndpoint),
}

/// Returns `None` for a route nothing serves, which `handle_unknown_route`
/// answers before authentication: mostly a 404 for a read and a 400 for a write.
pub fn parse(method: &Method, uri: &Uri) -> Option<Route> {
    let path = uri.path();
    let query = uri.query();

    match path {
        // Guarded by GET so a HEAD does not fall through to the UI-asset arm
        // and answer `index.html` with a 200 while `/readyz` answers 503.
        "/healthz" => return (method == Method::GET).then_some(Route::Healthz),
        "/readyz" => return (method == Method::GET).then_some(Route::Readyz),
        "/metrics" => return (method == Method::GET).then_some(Route::Metrics),
        TOKEN_PATH => return (method == Method::GET).then_some(Route::Token),
        UI_CONFIG_PATH if method == Method::GET => return Some(Route::UiConfig),
        _ => {}
    }

    if let Some(endpoint) = parse_oci(method, path, query) {
        return Some(Route::Oci(endpoint));
    }
    if let Some(endpoint) = parse_docker(method, path, query) {
        return Some(Route::Docker(endpoint));
    }
    if let Some(endpoint) = parse_angos(method, path, query) {
        return Some(Route::Angos(endpoint));
    }

    // Anything under the API prefix that no surface claimed is a miss, not a UI
    // asset: `/v2` without its slash, and any unmatched `/v2/...` path.
    if path == "/v2" || path.starts_with(API_PREFIX) {
        return None;
    }

    (method == Method::GET || method == Method::HEAD).then(|| Route::UiAsset {
        path: path.to_string(),
    })
}

impl Route {
    #[must_use]
    pub fn metric_label(&self) -> &'static str {
        match self {
            Route::UiAsset { .. } => "ui-asset",
            Route::UiConfig => "ui-config",
            Route::Token => "get-token",
            Route::Healthz => "healthz",
            Route::Readyz => "readyz",
            Route::Metrics => "metrics",
            Route::Oci(endpoint) => endpoint.endpoint_name(),
            Route::Docker(endpoint) => endpoint.endpoint_name(),
            Route::Angos(endpoint) => endpoint.endpoint_name(),
        }
    }

    /// The namespace a pull addresses, mutably, so the proxy `?ns=` parameter
    /// can resolve it to the mirroring repository before authorization reads
    /// it. `None` for everything else: a write naming `?ns=` is left addressing
    /// the namespace it spelled out.
    pub fn pull_namespace_mut(&mut self) -> Option<&mut Namespace> {
        match self {
            Route::Oci(
                OciEndpoint::GetBlob { namespace, .. }
                | OciEndpoint::HeadBlob { namespace, .. }
                | OciEndpoint::Manifest(
                    ManifestEndpoint::Get { namespace, .. }
                    | ManifestEndpoint::Head { namespace, .. },
                )
                | OciEndpoint::ListTags { namespace, .. }
                | OciEndpoint::GetReferrers { namespace, .. },
            ) => Some(namespace),
            _ => None,
        }
    }
}

impl From<&Route> for Action {
    fn from(route: &Route) -> Self {
        match route {
            Route::UiAsset { path } => Action::UiAsset { path: path.clone() },
            Route::UiConfig => Action::UiConfig,
            Route::Token => Action::Token,
            Route::Healthz => Action::Healthz,
            Route::Readyz => Action::Readyz,
            Route::Metrics => Action::Metrics,

            Route::Oci(endpoint) => Action::from(endpoint.clone()),
            Route::Docker(DockerEndpoint::ListCatalog(request)) => Action::ListCatalog {
                n: request.n,
                last: request.last.clone(),
            },
            Route::Angos(endpoint) => Action::from(endpoint),
        }
    }
}

impl From<OciEndpoint> for Action {
    fn from(endpoint: OciEndpoint) -> Self {
        match endpoint {
            OciEndpoint::CheckVersion => Action::ApiVersion,
            OciEndpoint::Manifest(ManifestEndpoint::Get {
                namespace,
                reference,
            }) => Action::GetManifest {
                namespace,
                reference,
            },
            OciEndpoint::Manifest(ManifestEndpoint::Head {
                namespace,
                reference,
            }) => Action::HeadManifest {
                namespace,
                reference,
            },
            OciEndpoint::Manifest(ManifestEndpoint::Put { namespace, target }) => {
                Action::PutManifest { namespace, target }
            }
            OciEndpoint::Manifest(ManifestEndpoint::Delete {
                namespace,
                reference,
            }) => Action::DeleteManifest {
                namespace,
                reference,
            },
            OciEndpoint::GetBlob { namespace, digest } => Action::GetBlob { namespace, digest },
            OciEndpoint::HeadBlob { namespace, digest } => Action::HeadBlob { namespace, digest },
            OciEndpoint::DeleteBlob { namespace, digest } => {
                Action::DeleteBlob { namespace, digest }
            }
            OciEndpoint::Upload(UploadEndpoint::Start {
                namespace,
                digest,
                digest_algorithm,
            }) => Action::StartUpload {
                namespace,
                digest,
                digest_algorithm,
            },
            OciEndpoint::Upload(UploadEndpoint::Mount {
                namespace,
                digest,
                from,
            }) => Action::MountBlob {
                namespace,
                digest,
                from,
            },
            OciEndpoint::Upload(UploadEndpoint::Get {
                namespace,
                session_id,
            }) => Action::GetUpload {
                namespace,
                session_id,
            },
            OciEndpoint::Upload(UploadEndpoint::Patch {
                namespace,
                session_id,
            }) => Action::PatchUpload {
                namespace,
                session_id,
            },
            OciEndpoint::Upload(UploadEndpoint::Put {
                namespace,
                session_id,
                digest,
            }) => Action::PutUpload {
                namespace,
                session_id,
                digest,
            },
            OciEndpoint::Upload(UploadEndpoint::Delete {
                namespace,
                session_id,
            }) => Action::DeleteUpload {
                namespace,
                session_id,
            },
            OciEndpoint::ListTags { namespace, n, last } => Action::ListTags { namespace, n, last },
            OciEndpoint::GetReferrers {
                namespace,
                digest,
                artifact_type,
                last,
            } => Action::GetReferrer {
                namespace,
                digest,
                artifact_type,
                last,
            },
        }
    }
}

impl From<&AngosEndpoint> for Action {
    fn from(endpoint: &AngosEndpoint) -> Self {
        match endpoint {
            AngosEndpoint::ListRepositories { .. } => Action::ListRepositories,
            AngosEndpoint::ListNamespaces(request) => Action::ListNamespaces {
                repository: request.repository.clone(),
            },
            AngosEndpoint::ListRevisions { namespace, .. } => Action::ListRevisions {
                namespace: namespace.clone(),
            },
            AngosEndpoint::ListUploads { namespace, .. } => Action::ListUploads {
                namespace: namespace.clone(),
            },
            AngosEndpoint::ListPulls(request) => Action::ListPulls {
                namespace: request.namespace.clone(),
                reference: request.reference.clone(),
            },
            AngosEndpoint::ListLayerEntries { namespace, digest } => Action::ListLayerEntries {
                namespace: namespace.clone(),
                digest: digest.clone(),
            },
            AngosEndpoint::GetLayerFile {
                namespace,
                digest,
                path,
                download,
            } => Action::GetLayerFile {
                namespace: namespace.clone(),
                digest: digest.clone(),
                path: path.clone(),
                download: *download,
            },
            AngosEndpoint::GetLayerFileDetails(request) => Action::GetLayerFileDetails {
                namespace: request.namespace.clone(),
                digest: request.digest.clone(),
                path: request.path.clone(),
            },
            AngosEndpoint::ListJobs(request) => Action::ListJobs {
                queue: Queue::from(request.queue),
                n: request.n,
                after: request.after.clone(),
            },
            AngosEndpoint::ListFailedJobs(request) => Action::ListFailedJobs {
                queue: Queue::from(request.queue),
                n: request.n,
                after: request.after.clone(),
            },
            AngosEndpoint::RetryJob(request) => Action::RetryJob {
                queue: Queue::from(request.queue),
                storage_key: request.storage_key.clone(),
            },
            AngosEndpoint::DeleteJob(request) => Action::DeleteJob {
                queue: Queue::from(request.queue),
                state: JobState::from(request.state),
                storage_key: request.storage_key.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests;
