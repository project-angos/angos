use std::str::FromStr;

use hyper::{Method, Uri};
use serde::{Deserialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    identity::Action,
    oci::{Digest, Namespace, Reference},
    registry::{cache_job_handler::CACHE_QUEUE, job_store::JobState},
    replication::REPLICATION_QUEUE,
};

fn parse_query<T: DeserializeOwned + Default>(params: &str) -> T {
    serde_urlencoded::from_str(params).unwrap_or_default()
}

/// Like [`parse_query`] but returns `None` when a value fails to deserialize
/// (e.g. a malformed `?digest=`), so the caller can reject the route — the
/// non-GET/HEAD 400 path in `handle_unknown_route` — instead of silently
/// dropping the value. Other routes keep the lenient [`parse_query`] (e.g. a
/// bad `?n=` is ignored).
fn parse_query_strict<T: DeserializeOwned>(params: &str) -> Option<T> {
    serde_urlencoded::from_str(params).ok()
}

/// Parses the HTTP method and URI into a registry `Action`.
///
/// Returns `None` for paths that do not match any known route. Callers should
/// return 404 for `None` without running authentication or authorization.
pub fn parse(method: &Method, uri: &Uri) -> Option<Action> {
    let path = uri.path();
    let params = uri.query();

    match path {
        "/healthz" if method == Method::GET => return Some(Action::Healthz),
        "/readyz" if method == Method::GET => return Some(Action::Readyz),
        "/metrics" if method == Method::GET => return Some(Action::Metrics),
        "/_ui/config" if method == Method::GET => return Some(Action::UiConfig),
        "/v2" | "/v2/" if method == Method::GET => return Some(Action::ApiVersion),
        "/v2/_catalog" if method == Method::GET => {
            let (n, last) = parse_pagination(params);
            return Some(Action::ListCatalog { n, last });
        }
        _ => {}
    }

    // Angos-specific extension API lives at the top level so `/v2` stays purely
    // OCI. Unknown `_ext` paths return `None` (404) rather than falling back to
    // the UI-asset handler below.
    if let Some(ext_path) = path.strip_prefix("/_ext/") {
        return try_parse_extension(method, ext_path, params);
    }

    if let Some(api_path) = path.strip_prefix("/v2/") {
        return try_parse_upload(method, api_path, params)
            .or_else(|| try_find_blobs(method, api_path))
            .or_else(|| try_find_manifests(method, api_path))
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

#[derive(Deserialize, Default)]
struct DigestQuery {
    digest: Option<Digest>,
}

fn digest_from_params(params: Option<&str>) -> Option<Digest> {
    params
        .map(parse_query::<DigestQuery>)
        .and_then(|q| q.digest)
}

#[derive(Deserialize, Default)]
struct MountQuery {
    mount: Option<String>,
    from: Option<String>,
    digest: Option<Digest>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct ArtifactTypeQuery {
    artifact_type: Option<String>,
}

#[derive(Deserialize, Default)]
struct PaginationQuery {
    n: Option<u16>,
    last: Option<String>,
}

fn parse_pagination(params: Option<&str>) -> (Option<u16>, Option<String>) {
    let query: PaginationQuery = params.map(parse_query).unwrap_or_default();
    (query.n, query.last)
}

#[derive(Deserialize, Default)]
struct JobsQuery {
    n: Option<u16>,
    after: Option<String>,
    queue: Option<String>,
}

/// Parse the `?n=&after=&queue=` of a `_jobs` admin route. Returns `None` when
/// `?queue=` names no known queue, so the route is rejected (400) rather than
/// silently administering the wrong queue; an absent selector defaults to the
/// cache queue, preserving the single-queue behavior from before the
/// replication queue existed.
fn parse_jobs_query(params: Option<&str>) -> Option<(Option<u16>, Option<String>, String)> {
    let query: JobsQuery = params.map(parse_query).unwrap_or_default();
    let queue = match query.queue.as_deref() {
        None | Some(CACHE_QUEUE) => CACHE_QUEUE.to_string(),
        Some(REPLICATION_QUEUE) => REPLICATION_QUEUE.to_string(),
        Some(_) => return None,
    };
    Some((query.n, query.after, queue))
}

/// Parse the angos-specific extension API routes. `path` is relative to the
/// top-level `/_ext/` prefix (already stripped by the caller).
///
/// Routes are grouped by method, then by endpoint. Within `GET`, the bare
/// listings (`_repositories`, `_jobs`, `_jobs/failed`) are matched before the
/// namespace-suffixed routes. Job storage keys are a single path segment
/// (`<hex-millis>-<uuid>`, no `/`), so a segment containing `/` is rejected by
/// [`is_job_key`].
fn try_parse_extension(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    match *method {
        Method::GET => match path {
            "_repositories" => Some(Action::ListRepositories),
            "_jobs" => {
                let (n, after, queue) = parse_jobs_query(params)?;
                Some(Action::ListJobs { queue, n, after })
            }
            "_jobs/failed" => {
                let (n, after, queue) = parse_jobs_query(params)?;
                Some(Action::ListFailedJobs { queue, n, after })
            }
            _ => {
                if let Some(repository_str) = path.strip_suffix("/_namespaces") {
                    let repository = Namespace::new(repository_str).ok()?;
                    return Some(Action::ListNamespaces { repository });
                }
                if let Some(namespace_str) = path.strip_suffix("/_revisions") {
                    let namespace = Namespace::new(namespace_str).ok()?;
                    return Some(Action::ListRevisions { namespace });
                }
                if let Some(namespace_str) = path.strip_suffix("/_uploads") {
                    let namespace = Namespace::new(namespace_str).ok()?;
                    return Some(Action::ListUploads { namespace });
                }
                None
            }
        },
        Method::POST => {
            let key = path
                .strip_prefix("_jobs/failed/")
                .and_then(|rest| rest.strip_suffix("/retry"))
                .filter(|key| is_job_key(key))?;
            let (_, _, queue) = parse_jobs_query(params)?;
            Some(Action::RetryJob {
                queue,
                storage_key: key.to_string(),
            })
        }
        Method::DELETE => {
            if let Some(key) = path.strip_prefix("_jobs/failed/").filter(|k| is_job_key(k)) {
                let (_, _, queue) = parse_jobs_query(params)?;
                return Some(Action::DeleteJob {
                    queue,
                    state: JobState::Failed,
                    storage_key: key.to_string(),
                });
            }
            let key = path
                .strip_prefix("_jobs/pending/")
                .filter(|k| is_job_key(k))?;
            let (_, _, queue) = parse_jobs_query(params)?;
            Some(Action::DeleteJob {
                queue,
                state: JobState::Pending,
                storage_key: key.to_string(),
            })
        }
        _ => None,
    }
}

/// A job storage key is a single non-empty path segment.
fn is_job_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('/')
}

fn try_parse_upload(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    if let Some(namespace_str) = path
        .strip_suffix("/blobs/uploads")
        .or_else(|| path.strip_suffix("/blobs/uploads/"))
    {
        let namespace = Namespace::new(namespace_str).ok()?;

        if *method != Method::POST {
            return None;
        }
        // A present query is parsed strictly: a malformed `?digest=` fails
        // deserialization -> `None` -> the POST becomes a 400 via
        // `handle_unknown_route`, rather than silently starting a 202 session.
        // A missing query is a plain upload-start session.
        let query: MountQuery = match params {
            Some(p) => parse_query_strict(p)?,
            None => MountQuery::default(),
        };

        // A valid `?mount=<digest>` requests a cross-repo mount; anything else
        // (including a malformed mount) is an ordinary upload start.
        if let Some(value) = &query.mount
            && let Ok(digest) = value.parse::<Digest>()
        {
            // A present-but-invalid `?from=` is rejected (the POST becomes a 400)
            // rather than silently treated as from-less auto-discovery, which would
            // widen the authorized source set.
            let from = match &query.from {
                Some(repo) => Some(Namespace::new(repo).ok()?),
                None => None,
            };
            return Some(Action::MountBlob {
                namespace,
                digest,
                from,
            });
        }

        // Ordinary upload start, optionally monolithic via `?digest=`.
        let digest = query.digest;
        return Some(Action::StartUpload { namespace, digest });
    }

    let (namespace_str, uuid) = path.rsplit_once("/blobs/uploads/")?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let uuid = Uuid::from_str(uuid).ok()?;

    match *method {
        Method::GET => Some(Action::GetUpload { namespace, uuid }),
        Method::PATCH => Some(Action::PatchUpload { namespace, uuid }),
        Method::PUT => {
            let digest = digest_from_params(params)?;
            Some(Action::PutUpload {
                namespace,
                uuid,
                digest,
            })
        }
        Method::DELETE => Some(Action::DeleteUpload { namespace, uuid }),
        _ => None,
    }
}

fn try_find_blobs(method: &Method, path: &str) -> Option<Action> {
    if let Some((namespace_str, digest)) = path.rsplit_once("/blobs/") {
        let namespace = Namespace::new(namespace_str).ok()?;
        let digest = Digest::from_str(digest).ok()?;

        match *method {
            Method::GET => return Some(Action::GetBlob { namespace, digest }),
            Method::HEAD => return Some(Action::HeadBlob { namespace, digest }),
            Method::DELETE => return Some(Action::DeleteBlob { namespace, digest }),
            _ => {}
        }
    }

    None
}

fn try_find_manifests(method: &Method, path: &str) -> Option<Action> {
    if let Some((namespace_str, reference)) = path.rsplit_once("/manifests/") {
        let namespace = Namespace::new(namespace_str).ok()?;
        let reference = Reference::from_str(reference).ok()?;

        match *method {
            Method::GET => {
                return Some(Action::GetManifest {
                    namespace,
                    reference,
                });
            }
            Method::HEAD => {
                return Some(Action::HeadManifest {
                    namespace,
                    reference,
                });
            }
            Method::PUT => {
                return Some(Action::PutManifest {
                    namespace,
                    reference,
                });
            }
            Method::DELETE => {
                return Some(Action::DeleteManifest {
                    namespace,
                    reference,
                });
            }
            _ => {}
        }
    }

    None
}

fn try_find_referrers(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    if let Some((namespace_str, digest)) = path.rsplit_once("/referrers/") {
        let namespace = Namespace::new(namespace_str).ok()?;
        let digest = Digest::from_str(digest).ok()?;

        let artifact_type = params
            .map(parse_query::<ArtifactTypeQuery>)
            .and_then(|f| f.artifact_type);

        if *method == Method::GET {
            return Some(Action::GetReferrer {
                namespace,
                digest,
                artifact_type,
            });
        }
    }

    None
}

fn try_find_tags(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    if let Some(namespace_str) = path.strip_suffix("/tags/list")
        && *method == Method::GET
    {
        let namespace = Namespace::new(namespace_str).ok()?;
        let (n, last) = parse_pagination(params);
        return Some(Action::ListTags { namespace, n, last });
    }

    None
}

#[cfg(test)]
mod tests;
