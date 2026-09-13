use std::{collections::BTreeSet, str::FromStr};

use hyper::{Method, Uri};
use serde::{Deserialize, de::DeserializeOwned};

use angos_oci::path::{API_PREFIX, TAGS_LIST, UPLOADS};
use angos_oci::server;
use angos_oci::{Algorithm, Digest, MediaType, Namespace, Reference, Tag, UploadSessionId};

use crate::{
    identity::{Action, ManifestPutTarget},
    jobs::{JobState, Queue},
};

/// The token service's endpoint. Shared with the dispatcher, which answers a
/// method this route does not serve rather than letting it fall through to the
/// catch-all.
pub const TOKEN_PATH: &str = "/token";

/// Angos's extension name and the two forms it is reached under. The spec fixes
/// the `_<extension>` shape; the name itself is ours.
const EXTENSION: &str = "_angos/";
const REPOSITORY_EXTENSION: &str = "/_angos/";

/// Deserializes a query string, returning `None` when a value fails to
/// deserialize so the caller can reject the route. An absent query is an empty
/// one, so every field takes its default.
fn parse_query<T: DeserializeOwned>(params: Option<&str>) -> Option<T> {
    serde_html_form::from_str(params.unwrap_or_default()).ok()
}

/// Parses the HTTP method and URI into a registry `Action`.
///
/// Returns `None` for paths that do not match any known route. Callers should
/// return 404 for `None` without running authentication or authorization.
pub fn parse(method: &Method, uri: &Uri) -> Option<Action> {
    let path = uri.path();
    let params = uri.query();

    match path {
        // Matched for every method: guarded by `if method == GET` a HEAD would
        // fall through to the UI-asset arm below and answer `index.html` with a
        // 200, so a probe reads a replica as healthy while `/readyz` is
        // answering 503.
        "/healthz" => return (method == Method::GET).then_some(Action::Healthz),
        "/readyz" => return (method == Method::GET).then_some(Action::Readyz),
        "/metrics" => return (method == Method::GET).then_some(Action::Metrics),
        TOKEN_PATH => return (method == Method::GET).then_some(Action::Token),
        // HEAD as well as GET: the version check is the OCI conformance probe,
        // and without this it falls through to the UI-asset arm below and
        // answers with `index.html`.
        "/v2/" if method == Method::GET || method == Method::HEAD => {
            return Some(Action::ApiVersion);
        }
        // end-1 is `/v2/`; the same path without its slash is not the version
        // endpoint, and must not reach the UI-asset arm below either.
        "/v2" => return None,
        // A Docker Registry V2 endpoint the OCI spec does not define. It keeps
        // its long-standing path, which clients already call.
        "/v2/_catalog" if method == Method::GET => {
            let PaginationQuery { n, last } = parse_query(params)?;
            return Some(Action::ListCatalog { n, last });
        }
        _ => {}
    }

    if let Some(api_path) = path.strip_prefix(API_PREFIX) {
        return try_parse_extension(method, api_path, params)
            .or_else(|| try_parse_upload(method, api_path, params))
            .or_else(|| try_find_blobs(method, api_path))
            .or_else(|| try_find_manifests(method, api_path, params))
            .or_else(|| try_find_referrers(method, api_path, params))
            .or_else(|| try_find_tags(method, api_path, params));
    }

    if method == Method::GET || method == Method::HEAD {
        return Some(Action::UiAsset {
            path: path.to_string(),
        });
    }

    None
}

/// The proxy `ns` parameter: the registry namespace a mirroring client believes
/// it is addressing. Parsed here with every other query value; resolving it to a
/// repository needs the configuration and happens at the server context.
#[derive(Deserialize, Default)]
struct NamespaceQuery {
    ns: Option<String>,
}

/// The `?ns=` a request names, if any.
pub fn proxy_namespace(uri: &Uri) -> Option<String> {
    parse_query::<NamespaceQuery>(uri.query())?
        .ns
        .filter(|ns| !ns.is_empty())
}

#[derive(Deserialize, Default)]
struct DigestQuery {
    digest: Option<Digest>,
}

fn digest_from_params(params: Option<&str>) -> Option<Digest> {
    parse_query::<DigestQuery>(params)?.digest
}

/// Repeated `tag` query parameters for the distribution-spec tag-on-push
/// feature. Each value deserializes through `Tag`, so an invalid tag fails the
/// parse and rejects the route. The `BTreeSet` drops duplicate values so a
/// repeated tag is linked, echoed in `OCI-Tag`, and event-emitted only once.
#[derive(Deserialize, Default)]
struct TagQuery {
    #[serde(default)]
    tag: BTreeSet<Tag>,
}

#[derive(Deserialize, Default)]
struct MountQuery {
    mount: Option<Digest>,
    from: Option<Namespace>,
    digest: Option<Digest>,
    /// The algorithm the client will close the upload with, so a chunked
    /// session hashes under that one alone instead of every supported one.
    #[serde(rename = "digest-algorithm")]
    digest_algorithm: Option<Algorithm>,
}

/// The referrers `?artifactType=` filter. The value is a media type per the
/// image spec, so it deserializes through [`MediaType`] and a malformed one
/// rejects the route instead of silently filtering nothing.
#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct ArtifactTypeQuery {
    artifact_type: Option<MediaType>,
}

#[derive(Deserialize, Default)]
struct PaginationQuery {
    n: Option<u16>,
    last: Option<String>,
}

/// The cursor alone, for the referrers listing: the spec pages it through the
/// `Link` header and defines no page size, so `?n=` is not part of it.
#[derive(Deserialize)]
struct CursorQuery {
    last: Option<String>,
}

/// The angos repository whose namespaces are listed. A repository is angos's
/// own grouping, not an OCI name, so it cannot sit in the `<name>` slot of the
/// path without claiming a scoping the registry does not have.
#[derive(Deserialize)]
struct NamespacesQuery {
    repository: Namespace,
}

#[derive(Deserialize)]
struct JobsQuery {
    n: Option<u16>,
    after: Option<String>,
    #[serde(default = "default_jobs_queue")]
    queue: Queue,
    /// The job a retry or a delete addresses. An extension path ends at its
    /// module, so the storage key rides in the query rather than the path.
    key: Option<String>,
}

fn default_jobs_queue() -> Queue {
    Queue::Cache
}

/// Angos's own endpoints, under the extension namespace the distribution spec
/// reserves: `_<extension>/<component>/<module>` for the registry and
/// `<name>/_<extension>/<component>/<module>` for one repository. `api_path` is
/// the path after the API prefix.
/// REF: <https://github.com/opencontainers/distribution-spec/blob/main/extensions/README.md>
fn try_parse_extension(method: &Method, api_path: &str, params: Option<&str>) -> Option<Action> {
    if let Some(rest) = api_path.strip_prefix(EXTENSION) {
        return registry_extension(method, rest, params);
    }

    // The marker is a whole path segment, so a namespace may hold the name.
    let (namespace, rest) = api_path.split_once(REPOSITORY_EXTENSION)?;

    repository_extension(method, Namespace::new(namespace).ok()?, rest, params)
}

/// `_angos/<component>/<module>`: what the registry as a whole answers. The
/// path ends at the module, so a mutation names its job in `?key=`.
fn registry_extension(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    match *method {
        Method::GET => match path {
            "ui/config" => Some(Action::UiConfig),
            "repositories/list" => Some(Action::ListRepositories),
            "namespaces/list" => {
                let NamespacesQuery { repository } = parse_query(params)?;
                Some(Action::ListNamespaces { repository })
            }
            "jobs/list" => {
                let JobsQuery {
                    n, after, queue, ..
                } = parse_query(params)?;
                Some(Action::ListJobs { queue, n, after })
            }
            "jobs/failed" => {
                let JobsQuery {
                    n, after, queue, ..
                } = parse_query(params)?;
                Some(Action::ListFailedJobs { queue, n, after })
            }
            _ => None,
        },
        Method::POST if path == "jobs/failed" => {
            let JobsQuery { queue, key, .. } = parse_query(params)?;
            Some(Action::RetryJob {
                queue,
                storage_key: key.filter(|key| is_job_key(key))?,
            })
        }
        Method::DELETE => {
            let state = match path {
                "jobs/failed" => JobState::Failed,
                "jobs/pending" => JobState::Pending,
                _ => return None,
            };
            let JobsQuery { queue, key, .. } = parse_query(params)?;
            Some(Action::DeleteJob {
                queue,
                state,
                storage_key: key.filter(|key| is_job_key(key))?,
            })
        }
        _ => None,
    }
}

/// `<name>/_angos/<component>/<module>`: what one namespace answers, `<name>`
/// being the OCI repository name.
fn repository_extension(
    method: &Method,
    namespace: Namespace,
    path: &str,
    params: Option<&str>,
) -> Option<Action> {
    if *method != Method::GET {
        return None;
    }

    // `layers/<digest>/entries` and `layers/<digest>/file?path=`.
    if let Some(rest) = path.strip_prefix("layers/") {
        let (digest, module) = rest.split_once('/')?;
        let digest: Digest = digest.parse().ok()?;
        return match module {
            "entries" => Some(Action::ListLayerEntries { namespace, digest }),
            "file" => {
                let LayerFileQuery { path, download } = parse_query(params)?;
                Some(Action::GetLayerFile {
                    namespace,
                    digest,
                    path: path?,
                    download: download.is_some(),
                })
            }
            _ => None,
        };
    }

    match path {
        "revisions/list" => Some(Action::ListRevisions { namespace }),
        "uploads/list" => Some(Action::ListUploads { namespace }),
        "pulls/list" => Some(Action::ListPulls {
            namespace,
            reference: parse_pulls_reference(params)?,
        }),
        _ => None,
    }
}

/// A file in a layer: `?path=` names it, a `download` parameter, whatever its
/// value, asks for it as an attachment.
#[derive(Deserialize)]
struct LayerFileQuery {
    path: Option<String>,
    download: Option<String>,
}

/// The pull-history target, named by exactly one of `?tag=` or `?digest=`.
#[derive(Deserialize)]
struct PullsQuery {
    tag: Option<Tag>,
    digest: Option<Digest>,
}

/// Parses `?tag=`/`?digest=` strictly: an unparseable or ambiguous target is
/// refused rather than silently narrowed to one of the two.
fn parse_pulls_reference(params: Option<&str>) -> Option<Reference> {
    let PullsQuery { tag, digest } = parse_query(params)?;
    match (tag, digest) {
        (Some(tag), None) => Some(Reference::Tag(tag)),
        (None, Some(digest)) => Some(Reference::Digest(digest)),
        _ => None,
    }
}

/// The job a mutation addresses. A storage key is one path-free token
/// (`<hex-millis>-<uuid>`), so one holding a `/` is refused.
fn is_job_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('/')
}

fn try_parse_upload(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    if let Some(namespace_str) = server::split_uploads_start_path(path) {
        let namespace = Namespace::new(namespace_str).ok()?;

        if *method != Method::POST {
            return None;
        }
        // The OCI fall-back-to-session rule covers unsatisfiable mounts, not
        // syntactically invalid ones, so a malformed query is a 400.
        let query: MountQuery = parse_query(params)?;

        if let Some(digest) = query.mount {
            return Some(Action::MountBlob {
                namespace,
                digest,
                from: query.from,
            });
        }

        return Some(Action::StartUpload {
            namespace,
            digest: query.digest,
            digest_algorithm: query.digest_algorithm,
        });
    }

    let (namespace_str, session_id) = path.rsplit_once(UPLOADS)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let session_id = UploadSessionId::from_str(session_id).ok()?;

    match *method {
        Method::GET => Some(Action::GetUpload {
            namespace,
            session_id,
        }),
        Method::PATCH => Some(Action::PatchUpload {
            namespace,
            session_id,
        }),
        Method::PUT => {
            let digest = digest_from_params(params)?;
            Some(Action::PutUpload {
                namespace,
                session_id,
                digest,
            })
        }
        Method::DELETE => Some(Action::DeleteUpload {
            namespace,
            session_id,
        }),
        _ => None,
    }
}

fn try_find_blobs(method: &Method, path: &str) -> Option<Action> {
    let (namespace_str, digest) = server::split_blob_path(path)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let digest = Digest::from_str(digest).ok()?;

    match *method {
        Method::GET => Some(Action::GetBlob { namespace, digest }),
        Method::HEAD => Some(Action::HeadBlob { namespace, digest }),
        Method::DELETE => Some(Action::DeleteBlob { namespace, digest }),
        _ => None,
    }
}

fn try_find_manifests(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    let (namespace_str, reference) = server::split_manifest_path(path)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let reference = Reference::from_str(reference).ok()?;

    match *method {
        Method::GET => Some(Action::GetManifest {
            namespace,
            reference,
        }),
        Method::HEAD => Some(Action::HeadManifest {
            namespace,
            reference,
        }),
        Method::PUT => {
            // `?tag=` applies only to a by-digest push; a by-tag push ignores it.
            // Strict parse: a single invalid tag rejects the PUT (generic 400)
            // rather than silently dropping every requested tag.
            let target = match reference {
                Reference::Tag(tag) => ManifestPutTarget::Tag(tag),
                Reference::Digest(digest) => ManifestPutTarget::Digest {
                    digest,
                    tags: parse_query::<TagQuery>(params)?.tag.into_iter().collect(),
                },
            };
            Some(Action::PutManifest { namespace, target })
        }
        Method::DELETE => Some(Action::DeleteManifest {
            namespace,
            reference,
        }),
        _ => None,
    }
}

/// Whether a request [`parse`] refused was a referrers read owing a `400`: a
/// registry serving the API must answer an invalid one that way and never with
/// a `404`. Reaching here means [`try_find_referrers`] declined a `GET` on this
/// path, and on a parsable namespace it declines only over the digest or the
/// `?artifactType=` filter, so the namespace is the whole test: one that does
/// not parse addresses no repository and keeps the `404` every route gives.
/// REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md#listing-referrers>
pub fn is_invalid_referrers_request(method: &Method, uri: &Uri) -> bool {
    *method == Method::GET
        && uri
            .path()
            .strip_prefix(API_PREFIX)
            .and_then(server::split_referrers_path)
            .is_some_and(|(namespace, _)| Namespace::new(namespace).is_ok())
}

fn try_find_referrers(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    let (namespace_str, digest) = server::split_referrers_path(path)?;
    if *method != Method::GET {
        return None;
    }
    let namespace = Namespace::new(namespace_str).ok()?;
    let digest = Digest::from_str(digest).ok()?;

    // Strict parse: a malformed `?artifactType=` is a bad filter, not an
    // absent one, so it must not degrade into an unfiltered listing. The spec
    // defines no page size here, so only the cursor the registry minted in its
    // own `Link` is read back.
    Some(Action::GetReferrer {
        namespace,
        digest,
        artifact_type: parse_query::<ArtifactTypeQuery>(params)?.artifact_type,
        last: parse_query::<CursorQuery>(params)?.last,
    })
}

fn try_find_tags(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    let namespace_str = path.strip_suffix(TAGS_LIST)?;
    if *method != Method::GET {
        return None;
    }
    let namespace = Namespace::new(namespace_str).ok()?;
    let PaginationQuery { n, last } = parse_query(params)?;
    Some(Action::ListTags { namespace, n, last })
}

#[cfg(test)]
mod tests;
