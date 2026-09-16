use hyper::{Method, Uri};

use angos_docker_extension_service::{Endpoint as DockerEndpoint, parse as parse_docker};
use angos_extension_service::{Endpoint as AngosEndpoint, parse as parse_angos};
use angos_oci::Namespace;
use angos_oci::path::API_PREFIX;
use angos_oci_service::{Endpoint as OciEndpoint, parse as parse_oci};

use crate::{identity::Action, jobs};

/// The token service's endpoint. Shared with the dispatcher, which answers a
/// method this route does not serve rather than letting it fall through to the
/// catch-all.
pub const TOKEN_PATH: &str = "/token";

/// The `_angos/` path the host's UI reads its configuration from. Served by the
/// binary, not by any service crate, so it is matched here before the extension
/// parser is consulted.
const UI_CONFIG_PATH: &str = "/v2/_angos/ui/config";

/// A parsed request: an operation of one of the service surfaces
/// ([`Route::Oci`], [`Route::Docker`], [`Route::Angos`]), or one of the routes
/// the binary itself serves (health, metrics, token, UI). Each surface's crate
/// owns the parsing of its own paths; this only sequences them and adds the
/// binary's own routes.
#[derive(Clone, Debug)]
pub enum Route {
    /// A static UI asset by path.
    UiAsset {
        path: String,
    },
    /// The UI's runtime configuration document.
    UiConfig,
    /// The bearer-token service.
    Token,
    Healthz,
    Readyz,
    Metrics,
    /// An OCI Distribution operation.
    Oci(OciEndpoint),
    /// A Docker V2 extension operation (the catalog).
    Docker(DockerEndpoint),
    /// An `_angos/` extension operation.
    Angos(AngosEndpoint),
}

/// Parses the HTTP method and URI into a [`Route`].
///
/// Returns `None` for paths that match no known route; callers should return a
/// `404` for `None` without running authentication or authorization. Each
/// service surface is tried in turn; a path one surface declines is offered to
/// the next, and only a path under `/v2` that every surface declines (or a
/// non-GET UI path) is a miss rather than a UI asset.
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

    if method == Method::GET || method == Method::HEAD {
        return Some(Route::UiAsset {
            path: path.to_string(),
        });
    }

    None
}

impl Route {
    /// The metrics label for this route: the binary's own routes named here,
    /// each service surface named by the crate that owns it.
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
                | OciEndpoint::GetManifest { namespace, .. }
                | OciEndpoint::HeadManifest { namespace, .. }
                | OciEndpoint::ListTags { namespace, .. }
                | OciEndpoint::GetReferrers { namespace, .. },
            ) => Some(namespace),
            _ => None,
        }
    }
}

impl From<&Route> for Action {
    #[allow(clippy::too_many_lines)]
    fn from(route: &Route) -> Self {
        match route {
            Route::UiAsset { path } => Action::UiAsset { path: path.clone() },
            Route::UiConfig => Action::UiConfig,
            Route::Token => Action::Token,
            Route::Healthz => Action::Healthz,
            Route::Readyz => Action::Readyz,
            Route::Metrics => Action::Metrics,

            Route::Oci(endpoint) => match endpoint {
                OciEndpoint::CheckVersion => Action::ApiVersion,
                OciEndpoint::GetManifest {
                    namespace,
                    reference,
                } => Action::GetManifest {
                    namespace: namespace.clone(),
                    reference: reference.clone(),
                },
                OciEndpoint::HeadManifest {
                    namespace,
                    reference,
                } => Action::HeadManifest {
                    namespace: namespace.clone(),
                    reference: reference.clone(),
                },
                OciEndpoint::PutManifest { namespace, target } => Action::PutManifest {
                    namespace: namespace.clone(),
                    target: target.clone(),
                },
                OciEndpoint::DeleteManifest {
                    namespace,
                    reference,
                } => Action::DeleteManifest {
                    namespace: namespace.clone(),
                    reference: reference.clone(),
                },
                OciEndpoint::GetBlob { namespace, digest } => Action::GetBlob {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                },
                OciEndpoint::HeadBlob { namespace, digest } => Action::HeadBlob {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                },
                OciEndpoint::DeleteBlob { namespace, digest } => Action::DeleteBlob {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                },
                OciEndpoint::StartUpload {
                    namespace,
                    digest,
                    digest_algorithm,
                } => Action::StartUpload {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    digest_algorithm: *digest_algorithm,
                },
                OciEndpoint::MountBlob {
                    namespace,
                    digest,
                    from,
                } => Action::MountBlob {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    from: from.clone(),
                },
                OciEndpoint::GetUpload {
                    namespace,
                    session_id,
                } => Action::GetUpload {
                    namespace: namespace.clone(),
                    session_id: session_id.clone(),
                },
                OciEndpoint::PatchUpload {
                    namespace,
                    session_id,
                } => Action::PatchUpload {
                    namespace: namespace.clone(),
                    session_id: session_id.clone(),
                },
                OciEndpoint::PutUpload {
                    namespace,
                    session_id,
                    digest,
                } => Action::PutUpload {
                    namespace: namespace.clone(),
                    session_id: session_id.clone(),
                    digest: digest.clone(),
                },
                OciEndpoint::DeleteUpload {
                    namespace,
                    session_id,
                } => Action::DeleteUpload {
                    namespace: namespace.clone(),
                    session_id: session_id.clone(),
                },
                OciEndpoint::ListTags { namespace, n, last } => Action::ListTags {
                    namespace: namespace.clone(),
                    n: *n,
                    last: last.clone(),
                },
                OciEndpoint::GetReferrers {
                    namespace,
                    digest,
                    artifact_type,
                    last,
                } => Action::GetReferrer {
                    namespace: namespace.clone(),
                    digest: digest.clone(),
                    artifact_type: artifact_type.clone(),
                    last: last.clone(),
                },
            },

            Route::Docker(DockerEndpoint::ListCatalog { n, last }) => Action::ListCatalog {
                n: *n,
                last: last.clone(),
            },

            Route::Angos(endpoint) => match endpoint {
                AngosEndpoint::ListRepositories => Action::ListRepositories,
                AngosEndpoint::ListNamespaces { repository } => Action::ListNamespaces {
                    repository: repository.clone(),
                },
                AngosEndpoint::ListRevisions { namespace } => Action::ListRevisions {
                    namespace: namespace.clone(),
                },
                AngosEndpoint::ListUploads { namespace } => Action::ListUploads {
                    namespace: namespace.clone(),
                },
                AngosEndpoint::ListPulls {
                    namespace,
                    reference,
                } => Action::ListPulls {
                    namespace: namespace.clone(),
                    reference: reference.clone(),
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
                AngosEndpoint::ListJobs { queue, n, after } => Action::ListJobs {
                    queue: jobs::Queue::from(*queue),
                    n: *n,
                    after: after.clone(),
                },
                AngosEndpoint::ListFailedJobs { queue, n, after } => Action::ListFailedJobs {
                    queue: jobs::Queue::from(*queue),
                    n: *n,
                    after: after.clone(),
                },
                AngosEndpoint::RetryJob { queue, storage_key } => Action::RetryJob {
                    queue: jobs::Queue::from(*queue),
                    storage_key: storage_key.clone(),
                },
                AngosEndpoint::DeleteJob {
                    queue,
                    state,
                    storage_key,
                } => Action::DeleteJob {
                    queue: jobs::Queue::from(*queue),
                    state: jobs::JobState::from(*state),
                    storage_key: storage_key.clone(),
                },
            },
        }
    }
}

#[cfg(test)]
mod tests;
