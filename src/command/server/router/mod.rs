use std::str::FromStr;

use hyper::{Method, Uri};
use serde::{Deserialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    identity::Action,
    oci::{Digest, Namespace, Reference},
};

fn parse_query<T: DeserializeOwned + Default>(params: &str) -> T {
    serde_urlencoded::from_str(params).unwrap_or_default()
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

    if let Some(api_path) = path.strip_prefix("/v2/") {
        return try_parse_extension(method, api_path)
            .or_else(|| try_parse_upload(method, api_path, params))
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
    digest: Option<String>,
}

impl DigestQuery {
    fn to_digest(&self) -> Option<Digest> {
        self.digest.as_ref().and_then(|d| d.parse().ok())
    }
}

fn digest_from_params(params: Option<&str>) -> Option<Digest> {
    params
        .map(parse_query::<DigestQuery>)
        .and_then(|r| r.to_digest())
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

fn try_parse_extension(method: &Method, path: &str) -> Option<Action> {
    if *method != Method::GET {
        return None;
    }

    let path = path.strip_prefix("_ext/")?;

    if path == "_repositories" {
        return Some(Action::ListRepositories);
    }

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

fn try_parse_upload(method: &Method, path: &str, params: Option<&str>) -> Option<Action> {
    if let Some(namespace_str) = path
        .strip_suffix("/blobs/uploads")
        .or_else(|| path.strip_suffix("/blobs/uploads/"))
    {
        let namespace = Namespace::new(namespace_str).ok()?;
        let digest = digest_from_params(params);

        return match *method {
            Method::POST => Some(Action::StartUpload { namespace, digest }),
            _ => None,
        };
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
