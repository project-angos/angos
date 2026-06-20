use std::str::FromStr;

use hyper::{Method, Uri};
use serde::{Deserialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    identity::{Action, ManifestPutTarget},
    oci::{Digest, Namespace, Reference, Tag},
    registry::job_store::{JobState, Queue},
};

/// Deserializes a query string, returning `None` when a value fails to
/// deserialize so the caller can reject the route or fall back with
/// `unwrap_or_default`.
fn parse_query<T: DeserializeOwned>(params: &str) -> Option<T> {
    serde_html_form::from_str(params).ok()
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

#[derive(Deserialize, Default)]
struct DigestQuery {
    digest: Option<Digest>,
}

fn digest_from_params(params: Option<&str>) -> Option<Digest> {
    params
        .and_then(parse_query::<DigestQuery>)
        .and_then(|q| q.digest)
}

/// Repeated `tag` query parameters for the distribution-spec tag-on-push
/// feature. Each value deserializes through `Tag`, so an invalid tag fails the
/// parse and rejects the route.
#[derive(Deserialize, Default)]
struct TagQuery {
    #[serde(default)]
    tag: Vec<Tag>,
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
    let query: PaginationQuery = params.and_then(parse_query).unwrap_or_default();
    (query.n, query.last)
}

#[derive(Deserialize, Default)]
struct JobsQuery {
    n: Option<u16>,
    after: Option<String>,
    queue: Option<String>,
}

/// Parses the `?n=&after=&queue=` of a `_jobs` admin route strictly: a lenient
/// parse would reset the whole struct on one bad value and silently administer
/// the default `cache` queue. Returns `None` on a malformed value or unknown
/// queue; an absent selector defaults to `cache`.
fn parse_jobs_query(params: Option<&str>) -> Option<(Option<u16>, Option<String>, Queue)> {
    let query: JobsQuery = match params {
        Some(params) => parse_query(params)?,
        None => JobsQuery::default(),
    };
    let queue = match query.queue.as_deref() {
        None => Queue::Cache,
        Some(name) => name.parse().ok()?,
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
        // Strict parse: a malformed query rejects the POST as a 400 instead of
        // silently starting a session.
        let query: MountQuery = match params {
            Some(p) => parse_query(p)?,
            None => MountQuery::default(),
        };

        // A malformed `?mount=` rejects the POST: the OCI fall-back-to-session
        // rule covers unsatisfiable mounts, not syntactically invalid ones.
        if let Some(value) = &query.mount {
            let digest = value.parse::<Digest>().ok()?;
            // A malformed `?from=` is rejected rather than treated as from-less
            // auto-discovery, which would widen the authorized source set.
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

fn try_find_manifests(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
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
                // `?tag=` applies only to a by-digest push; a by-tag push ignores it.
                // Strict parse: a single invalid tag rejects the PUT (generic 400)
                // rather than silently dropping every requested tag.
                let target = match reference {
                    Reference::Tag(tag) => ManifestPutTarget::Tag(tag),
                    Reference::Digest(digest) => {
                        let tags = match params {
                            Some(p) => parse_query::<TagQuery>(p)?.tag,
                            None => Vec::new(),
                        };
                        ManifestPutTarget::Digest { digest, tags }
                    }
                };
                return Some(Action::PutManifest { namespace, target });
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
            .and_then(parse_query::<ArtifactTypeQuery>)
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
