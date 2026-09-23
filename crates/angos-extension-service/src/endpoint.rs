//! Parse an HTTP method and URL path into an [`Endpoint`] of the `_angos/`
//! surface.
//!
//! These endpoints live under the extension namespace the distribution spec
//! reserves: `_angos/<component>/<module>` for the registry as a whole, and
//! `<name>/_angos/<component>/<module>` for one repository. A path this surface
//! does not serve returns `None`, so a caller can try the next surface. The
//! `_angos/ui/config` path is intentionally *not* claimed: it is the host's UI
//! concern, not part of this service.

use std::num::NonZeroU16;

use http::Method;
use serde::{Deserialize, de::DeserializeOwned};

use angos_oci::path::API_PREFIX;
use angos_oci::{Digest, Namespace, Reference, Tag};

use crate::{JobState, Queue};

/// The two forms the extension is reached under. The spec fixes the
/// `_<extension>` shape; the name itself is angos's.
const EXTENSION: &str = "_angos/";
const REPOSITORY_EXTENSION: &str = "/_angos/";

/// An `_angos/` operation and the values its path and query carry.
#[derive(Clone, Debug)]
pub enum Endpoint {
    /// `GET /v2/_angos/repositories/list`.
    ListRepositories,
    /// `GET /v2/_angos/namespaces/list?repository=`.
    ListNamespaces { repository: Namespace },
    /// `GET /v2/<name>/_angos/revisions/list`.
    ListRevisions { namespace: Namespace },
    /// `GET /v2/<name>/_angos/uploads/list`.
    ListUploads { namespace: Namespace },
    /// `GET /v2/<name>/_angos/pulls/list?tag=|digest=&offset=&n=`.
    ListPulls {
        namespace: Namespace,
        reference: Reference,
        offset: u32,
        n: Option<NonZeroU16>,
    },
    /// `GET /v2/<name>/_angos/layers/<digest>/entries`.
    ListLayerEntries {
        namespace: Namespace,
        digest: Digest,
    },
    /// `GET /v2/<name>/_angos/layers/<digest>/file?path=&download`.
    GetLayerFile {
        namespace: Namespace,
        digest: Digest,
        path: String,
        download: bool,
    },
    /// `GET /v2/<name>/_angos/layers/<digest>/details?path=`.
    GetLayerFileDetails {
        namespace: Namespace,
        digest: Digest,
        path: String,
    },
    /// `GET /v2/_angos/jobs/list`.
    ListJobs {
        queue: Queue,
        n: Option<u16>,
        after: Option<String>,
    },
    /// `GET /v2/_angos/jobs/failed`.
    ListFailedJobs {
        queue: Queue,
        n: Option<u16>,
        after: Option<String>,
    },
    /// `POST /v2/_angos/jobs/failed?key=`.
    RetryJob { queue: Queue, storage_key: String },
    /// `DELETE /v2/_angos/jobs/{failed,pending}?key=`.
    DeleteJob {
        queue: Queue,
        state: JobState,
        storage_key: String,
    },
}

impl Endpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            Endpoint::ListRepositories => "list-repositories",
            Endpoint::ListNamespaces { .. } => "list-namespaces",
            Endpoint::ListRevisions { .. } => "list-revisions",
            Endpoint::ListUploads { .. } => "list-uploads",
            Endpoint::ListPulls { .. } => "list-pulls",
            Endpoint::ListLayerEntries { .. } => "list-layer-entries",
            Endpoint::GetLayerFile { .. } => "get-layer-file",
            Endpoint::GetLayerFileDetails { .. } => "get-layer-file-details",
            Endpoint::ListJobs { .. } => "list-jobs",
            Endpoint::ListFailedJobs { .. } => "list-failed-jobs",
            Endpoint::RetryJob { .. } => "retry-job",
            Endpoint::DeleteJob { .. } => "delete-job",
        }
    }
}

/// Parse `(method, path, query)` into an `_angos/` [`Endpoint`], or `None` when
/// the path is not part of this surface. `query` is the raw query without `?`.
#[must_use]
pub fn parse(method: &Method, path: &str, query: Option<&str>) -> Option<Endpoint> {
    let api_path = path.strip_prefix(API_PREFIX)?;

    if let Some(rest) = api_path.strip_prefix(EXTENSION) {
        return registry_extension(method, rest, query);
    }
    // The marker is a whole path segment, so a namespace may hold the name.
    let (namespace, rest) = api_path.split_once(REPOSITORY_EXTENSION)?;
    repository_extension(method, Namespace::new(namespace).ok()?, rest, query)
}

fn parse_query<T: DeserializeOwned>(params: Option<&str>) -> Option<T> {
    serde_html_form::from_str(params.unwrap_or_default()).ok()
}

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
    /// The job a retry or delete addresses. An extension path ends at its
    /// module, so the storage key rides in the query rather than the path.
    key: Option<String>,
}

fn default_jobs_queue() -> Queue {
    Queue::Cache
}

/// `_angos/<component>/<module>`: what the registry as a whole answers. The
/// `ui/config` path is left to the host's UI, not claimed here.
fn registry_extension(method: &Method, path: &str, params: Option<&str>) -> Option<Endpoint> {
    match *method {
        Method::GET => match path {
            "repositories/list" => Some(Endpoint::ListRepositories),
            "namespaces/list" => {
                let NamespacesQuery { repository } = parse_query(params)?;
                Some(Endpoint::ListNamespaces { repository })
            }
            "jobs/list" => {
                let JobsQuery {
                    n, after, queue, ..
                } = parse_query(params)?;
                Some(Endpoint::ListJobs { queue, n, after })
            }
            "jobs/failed" => {
                let JobsQuery {
                    n, after, queue, ..
                } = parse_query(params)?;
                Some(Endpoint::ListFailedJobs { queue, n, after })
            }
            _ => None,
        },
        Method::POST if path == "jobs/failed" => {
            let JobsQuery { queue, key, .. } = parse_query(params)?;
            Some(Endpoint::RetryJob {
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
            Some(Endpoint::DeleteJob {
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
) -> Option<Endpoint> {
    if *method != Method::GET {
        return None;
    }

    // `layers/<digest>/entries`, `layers/<digest>/file?path=` and
    // `layers/<digest>/details?path=`.
    if let Some(rest) = path.strip_prefix("layers/") {
        let (digest, module) = rest.split_once('/')?;
        let digest: Digest = digest.parse().ok()?;
        return match module {
            "entries" => Some(Endpoint::ListLayerEntries { namespace, digest }),
            "file" => {
                let LayerFileQuery { path, download } = parse_query(params)?;
                Some(Endpoint::GetLayerFile {
                    namespace,
                    digest,
                    path: path?,
                    download: download.is_some(),
                })
            }
            "details" => {
                let LayerFileQuery { path, .. } = parse_query(params)?;
                Some(Endpoint::GetLayerFileDetails {
                    namespace,
                    digest,
                    path: path?,
                })
            }
            _ => None,
        };
    }

    match path {
        "revisions/list" => Some(Endpoint::ListRevisions { namespace }),
        "uploads/list" => Some(Endpoint::ListUploads { namespace }),
        "pulls/list" => {
            let PullsQuery {
                tag,
                digest,
                offset,
                n,
            } = parse_query(params)?;
            Some(Endpoint::ListPulls {
                namespace,
                reference: pulls_reference(tag, digest)?,
                offset: offset.unwrap_or(0),
                n,
            })
        }
        _ => None,
    }
}

#[derive(Deserialize)]
struct LayerFileQuery {
    path: Option<String>,
    download: Option<String>,
}

#[derive(Deserialize)]
struct PullsQuery {
    tag: Option<Tag>,
    digest: Option<Digest>,
    offset: Option<u32>,
    n: Option<NonZeroU16>,
}

/// Takes `?tag=`/`?digest=` strictly: an unparseable or ambiguous target is
/// refused rather than silently narrowed to one of the two.
fn pulls_reference(tag: Option<Tag>, digest: Option<Digest>) -> Option<Reference> {
    match (tag, digest) {
        (Some(tag), None) => Some(Reference::Tag(tag)),
        (None, Some(digest)) => Some(Reference::Digest(digest)),
        _ => None,
    }
}

/// The job a mutation addresses. A storage key is one path-free token, so one
/// holding a `/` is refused.
fn is_job_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_listings() {
        assert!(matches!(
            parse(&Method::GET, "/v2/_angos/repositories/list", None),
            Some(Endpoint::ListRepositories)
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/_angos/namespaces/list",
                Some("repository=team")
            ),
            Some(Endpoint::ListNamespaces { .. })
        ));
    }

    #[test]
    fn jobs_default_queue_and_mutations() {
        assert!(matches!(
            parse(&Method::GET, "/v2/_angos/jobs/list", None),
            Some(Endpoint::ListJobs {
                queue: Queue::Cache,
                ..
            })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/_angos/jobs/list",
                Some("queue=replication")
            ),
            Some(Endpoint::ListJobs {
                queue: Queue::Replication,
                ..
            })
        ));
        assert!(matches!(
            parse(&Method::POST, "/v2/_angos/jobs/failed", Some("key=abc-123")),
            Some(Endpoint::RetryJob { .. })
        ));
        assert!(matches!(
            parse(
                &Method::DELETE,
                "/v2/_angos/jobs/pending",
                Some("key=abc-123")
            ),
            Some(Endpoint::DeleteJob {
                state: JobState::Pending,
                ..
            })
        ));
        // A key with a slash is refused.
        assert!(parse(&Method::POST, "/v2/_angos/jobs/failed", Some("key=a/b")).is_none());
    }

    #[test]
    fn repository_listings_and_layers() {
        assert!(matches!(
            parse(&Method::GET, "/v2/team/app/_angos/revisions/list", None),
            Some(Endpoint::ListRevisions { .. })
        ));
        let d = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(matches!(
            parse(
                &Method::GET,
                &format!("/v2/app/_angos/layers/{d}/entries"),
                None
            ),
            Some(Endpoint::ListLayerEntries { .. })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                &format!("/v2/app/_angos/layers/{d}/file"),
                Some("path=etc/hosts&download")
            ),
            Some(Endpoint::GetLayerFile { download: true, .. })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                &format!("/v2/app/_angos/layers/{d}/details"),
                Some("path=usr/bin/env")
            ),
            Some(Endpoint::GetLayerFileDetails { .. })
        ));
    }

    #[test]
    fn ui_config_not_claimed() {
        assert!(parse(&Method::GET, "/v2/_angos/ui/config", None).is_none());
    }

    #[test]
    fn declines_foreign_surfaces() {
        assert!(parse(&Method::GET, "/v2/_catalog", None).is_none());
        assert!(parse(&Method::GET, "/v2/app/tags/list", None).is_none());
    }

    #[test]
    fn pulls_listing() {
        match parse(
            &Method::GET,
            "/v2/myrepo/app/_angos/pulls/list",
            Some("tag=v1"),
        ) {
            Some(Endpoint::ListPulls {
                namespace,
                reference,
                offset,
                n,
            }) => {
                assert_eq!(namespace, "myrepo/app");
                assert_eq!(reference.to_string(), "v1");
                assert_eq!((offset, n), (0, None));
            }
            other => panic!("expected ListPulls, got {other:?}"),
        }

        let digest = format!("sha256:{}", "ab".repeat(32));
        match parse(
            &Method::GET,
            "/v2/myrepo/_angos/pulls/list",
            Some(&format!("digest={digest}")),
        ) {
            Some(Endpoint::ListPulls { reference, .. }) => {
                assert_eq!(reference.to_string(), digest);
            }
            other => panic!("expected ListPulls, got {other:?}"),
        }
        match parse(
            &Method::GET,
            "/v2/myrepo/_angos/pulls/list",
            Some("tag=v1&offset=200&n=50"),
        ) {
            Some(Endpoint::ListPulls { offset, n, .. }) => {
                assert_eq!((offset, n), (200, NonZeroU16::new(50)));
            }
            other => panic!("expected ListPulls, got {other:?}"),
        }
    }

    /// The target must be named exactly once, and must parse; anything else is a
    /// 404 rather than a lenient guess.
    #[test]
    fn pulls_reject_a_missing_ambiguous_or_invalid_target() {
        let digest = format!("sha256:{}", "ab".repeat(32));
        let ambiguous = format!("tag=v1&digest={digest}");
        for query in [
            None,
            Some(""),
            Some(ambiguous.as_str()),
            Some("tag=-bad"),
            Some("digest=sha256:nothex"),
            Some("tag="),
            // An empty page would name itself as the next one.
            Some("tag=v1&n=0"),
        ] {
            assert!(
                parse(&Method::GET, "/v2/myrepo/_angos/pulls/list", query).is_none(),
                "query {query:?} must not route"
            );
        }
    }

    #[test]
    fn failed_jobs_listing_pagination_and_deletion() {
        match parse(&Method::GET, "/v2/_angos/jobs/failed", None) {
            Some(Endpoint::ListFailedJobs { queue, n, after }) => {
                assert_eq!(queue, Queue::Cache);
                assert_eq!(n, None);
                assert_eq!(after, None);
            }
            other => panic!("expected ListFailedJobs, got {other:?}"),
        }

        match parse(&Method::GET, "/v2/_angos/jobs/list", Some("n=10&after=abc")) {
            Some(Endpoint::ListJobs { queue, n, after }) => {
                assert_eq!(queue, Queue::Cache);
                assert_eq!(n, Some(10));
                assert_eq!(after.as_deref(), Some("abc"));
            }
            other => panic!("expected ListJobs, got {other:?}"),
        }

        match parse(
            &Method::DELETE,
            "/v2/_angos/jobs/failed",
            Some("key=0000018b-abc"),
        ) {
            Some(Endpoint::DeleteJob {
                queue,
                state,
                storage_key,
            }) => {
                assert_eq!(queue, Queue::Cache);
                assert_eq!(state, JobState::Failed);
                assert_eq!(storage_key, "0000018b-abc");
            }
            other => panic!("expected DeleteJob(Failed), got {other:?}"),
        }
    }

    #[test]
    fn jobs_reject_unknown_queue_and_malformed_query() {
        assert!(parse(&Method::GET, "/v2/_angos/jobs/list", Some("queue=bogus")).is_none());
        assert!(
            parse(
                &Method::DELETE,
                "/v2/_angos/jobs/failed",
                Some("queue=bogus&key=0000018b-abc")
            )
            .is_none()
        );

        // A lenient parse would reset the whole query and administer the default
        // cache queue instead of the requested one.
        assert!(
            parse(
                &Method::GET,
                "/v2/_angos/jobs/list",
                Some("queue=replication&n=abc")
            )
            .is_none()
        );
        assert!(
            parse(
                &Method::GET,
                "/v2/_angos/jobs/list",
                Some("queue=replication&n=99999999")
            )
            .is_none()
        );
        assert!(
            parse(
                &Method::DELETE,
                "/v2/_angos/jobs/failed",
                Some("queue=replication&key=0000018b-abc&n=abc")
            )
            .is_none()
        );
    }

    /// A retry names its job in `?key=`, since an extension path ends at its
    /// module. Without one there is nothing to retry.
    #[test]
    fn retry_without_a_key_is_not_a_route() {
        assert!(parse(&Method::POST, "/v2/_angos/jobs/failed", None).is_none());
    }

    #[test]
    fn list_namespaces_rejects_invalid_repository() {
        assert!(
            parse(
                &Method::GET,
                "/v2/_angos/namespaces/list",
                Some("repository=INVALID")
            )
            .is_none()
        );
    }

    #[test]
    fn listings_reject_non_get_methods() {
        assert!(parse(&Method::POST, "/v2/myrepo/_angos/revisions/list", None).is_none());
        assert!(
            parse(
                &Method::POST,
                "/v2/myrepo/_angos/pulls/list",
                Some("tag=v1")
            )
            .is_none()
        );
        assert!(
            parse(
                &Method::POST,
                "/v2/_angos/namespaces/list",
                Some("repository=myrepo")
            )
            .is_none()
        );
        assert!(parse(&Method::POST, "/v2/_angos/jobs/list", None).is_none());
    }
}
