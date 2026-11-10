//! Routing for the `_angos/` surface, under the extension namespace the
//! distribution spec reserves: `_angos/<component>/<module>` for the registry,
//! `<name>/_angos/<component>/<module>` for one repository. `_angos/ui/config`
//! is the host's concern and deliberately not claimed.

use std::num::NonZeroU16;

use http::Method;
use serde::Deserialize;

use angos_oci::path::API_PREFIX;
use angos_oci::{Digest, Namespace, Reference, Tag};

use crate::{
    DeleteJobRequest, JobKey, JobState, LayerFileDetailsRequest, ListJobsRequest,
    ListNamespacesRequest, ListPullsRequest, PageRequest, Queue, RetryJobRequest,
    RevisionSelection, RevisionSort, SortOrder,
};

const EXTENSION: &str = "_angos/";
const REPOSITORY_EXTENSION: &str = "/_angos/";

#[derive(Clone, Debug)]
pub enum Endpoint {
    /// `GET /v2/_angos/repositories/list?order=&offset=&n=`.
    ListRepositories { order: SortOrder, page: PageRequest },
    /// `GET /v2/_angos/namespaces/list?repository=&under=&order=&offset=&n=`.
    ListNamespaces(ListNamespacesRequest),
    /// `GET /v2/<name>/_angos/revisions/list?sort=&order=&offset=&n=|digest=`.
    ListRevisions {
        namespace: Namespace,
        selection: RevisionSelection,
    },
    /// `GET /v2/<name>/_angos/uploads/list?offset=&n=`.
    ListUploads {
        namespace: Namespace,
        page: PageRequest,
    },
    /// `GET /v2/<name>/_angos/pulls/list?tag=|digest=&offset=&n=`.
    ListPulls(ListPullsRequest),
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
    GetLayerFileDetails(LayerFileDetailsRequest),
    /// `GET /v2/_angos/jobs/list`.
    ListJobs(ListJobsRequest),
    /// `GET /v2/_angos/jobs/failed`.
    ListFailedJobs(ListJobsRequest),
    /// `POST /v2/_angos/jobs/failed?key=`.
    RetryJob(RetryJobRequest),
    /// `DELETE /v2/_angos/jobs/{failed,pending}?key=`.
    DeleteJob(DeleteJobRequest),
}

impl Endpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            Endpoint::ListRepositories { .. } => "list-repositories",
            Endpoint::ListNamespaces(_) => "list-namespaces",
            Endpoint::ListRevisions { .. } => "list-revisions",
            Endpoint::ListUploads { .. } => "list-uploads",
            Endpoint::ListPulls(_) => "list-pulls",
            Endpoint::ListLayerEntries { .. } => "list-layer-entries",
            Endpoint::GetLayerFile { .. } => "get-layer-file",
            Endpoint::GetLayerFileDetails(_) => "get-layer-file-details",
            Endpoint::ListJobs(_) => "list-jobs",
            Endpoint::ListFailedJobs(_) => "list-failed-jobs",
            Endpoint::RetryJob(_) => "retry-job",
            Endpoint::DeleteJob(_) => "delete-job",
        }
    }
}

/// `None` when the path is not part of this surface, so a caller can try the
/// next one. `query` is the raw query without `?`.
#[must_use]
pub fn parse(method: &Method, path: &str, query: Option<&str>) -> Option<Endpoint> {
    let api_path = path.strip_prefix(API_PREFIX)?;
    let query = query.unwrap_or_default();

    if let Some(rest) = api_path.strip_prefix(EXTENSION) {
        return registry_extension(method, rest, query);
    }
    // The marker is a whole path segment, so a namespace may hold the name.
    let (namespace, rest) = api_path.split_once(REPOSITORY_EXTENSION)?;
    repository_extension(method, Namespace::new(namespace).ok()?, rest, query)
}

#[derive(Deserialize)]
struct NamespacesQuery {
    repository: Namespace,
    under: Option<Namespace>,
    #[serde(default)]
    order: SortOrder,
}

#[derive(Deserialize)]
struct OrderQuery {
    #[serde(default)]
    order: SortOrder,
}

/// A `digest` selects that revision, so `sort`, `order` and the page go unread.
#[derive(Deserialize)]
struct RevisionsQuery {
    #[serde(default)]
    sort: RevisionSort,
    #[serde(default)]
    order: SortOrder,
    digest: Option<Digest>,
}

#[derive(Deserialize)]
struct JobsQuery {
    n: Option<NonZeroU16>,
    after: Option<String>,
    #[serde(default)]
    queue: Queue,
    /// The job a retry or delete addresses. An extension path ends at its
    /// module, so the storage key rides in the query rather than the path.
    key: Option<String>,
}

fn registry_extension(method: &Method, path: &str, params: &str) -> Option<Endpoint> {
    match *method {
        Method::GET => match path {
            "repositories/list" => {
                let OrderQuery { order } = serde_html_form::from_str(params).ok()?;
                Some(Endpoint::ListRepositories {
                    order,
                    page: serde_html_form::from_str(params).ok()?,
                })
            }
            "namespaces/list" => {
                let NamespacesQuery {
                    repository,
                    under,
                    order,
                } = serde_html_form::from_str(params).ok()?;
                Some(Endpoint::ListNamespaces(ListNamespacesRequest {
                    repository,
                    under,
                    order,
                    page: serde_html_form::from_str(params).ok()?,
                }))
            }
            "jobs/list" | "jobs/failed" => {
                let JobsQuery {
                    n, after, queue, ..
                } = serde_html_form::from_str(params).ok()?;
                let request = ListJobsRequest { queue, n, after };
                Some(if path == "jobs/list" {
                    Endpoint::ListJobs(request)
                } else {
                    Endpoint::ListFailedJobs(request)
                })
            }
            _ => None,
        },
        Method::POST if path == "jobs/failed" => {
            let JobsQuery { queue, key, .. } = serde_html_form::from_str(params).ok()?;
            Some(Endpoint::RetryJob(RetryJobRequest {
                queue,
                storage_key: JobKey::try_from(key?).ok()?,
            }))
        }
        Method::DELETE => {
            let state = match path {
                "jobs/failed" => JobState::Failed,
                "jobs/pending" => JobState::Pending,
                _ => return None,
            };
            let JobsQuery { queue, key, .. } = serde_html_form::from_str(params).ok()?;
            Some(Endpoint::DeleteJob(DeleteJobRequest {
                queue,
                state,
                storage_key: JobKey::try_from(key?).ok()?,
            }))
        }
        _ => None,
    }
}

fn repository_extension(
    method: &Method,
    namespace: Namespace,
    path: &str,
    params: &str,
) -> Option<Endpoint> {
    if *method != Method::GET {
        return None;
    }

    if let Some(rest) = path.strip_prefix("layers/") {
        let (digest, module) = rest.split_once('/')?;
        let digest: Digest = digest.parse().ok()?;
        return match module {
            "entries" => Some(Endpoint::ListLayerEntries { namespace, digest }),
            "file" => {
                let LayerFileQuery { path, download } = serde_html_form::from_str(params).ok()?;
                Some(Endpoint::GetLayerFile {
                    namespace,
                    digest,
                    path: path?,
                    download: download.is_some(),
                })
            }
            "details" => {
                let LayerFileQuery { path, .. } = serde_html_form::from_str(params).ok()?;
                Some(Endpoint::GetLayerFileDetails(LayerFileDetailsRequest {
                    namespace,
                    digest,
                    path: path?,
                }))
            }
            _ => None,
        };
    }

    match path {
        "revisions/list" => {
            let RevisionsQuery {
                sort,
                order,
                digest,
            } = serde_html_form::from_str(params).ok()?;
            let selection = match digest {
                Some(digest) => RevisionSelection::Digest(digest),
                None => RevisionSelection::Page {
                    sort,
                    order,
                    page: serde_html_form::from_str(params).ok()?,
                },
            };
            Some(Endpoint::ListRevisions {
                namespace,
                selection,
            })
        }
        "uploads/list" => Some(Endpoint::ListUploads {
            namespace,
            page: serde_html_form::from_str(params).ok()?,
        }),
        "pulls/list" => {
            // Strict: an unparseable or ambiguous target is refused rather than
            // narrowed to one of the two.
            let reference = match serde_html_form::from_str(params).ok()? {
                PullsQuery {
                    tag: Some(tag),
                    digest: None,
                } => Reference::Tag(tag),
                PullsQuery {
                    tag: None,
                    digest: Some(digest),
                } => Reference::Digest(digest),
                _ => return None,
            };
            Some(Endpoint::ListPulls(ListPullsRequest {
                namespace,
                reference,
                page: serde_html_form::from_str(params).ok()?,
            }))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const D: &str = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn registry_listings() {
        assert!(matches!(
            parse(&Method::GET, "/v2/_angos/repositories/list", None),
            Some(Endpoint::ListRepositories {
                order: SortOrder::Asc,
                page: PageRequest { offset: 0, n: None },
            })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/_angos/repositories/list",
                Some("order=desc&offset=100&n=50")
            ),
            Some(Endpoint::ListRepositories {
                order: SortOrder::Desc,
                page: PageRequest { offset: 100, n: Some(n) },
            }) if n.get() == 50
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/_angos/namespaces/list",
                Some("repository=team")
            ),
            Some(Endpoint::ListNamespaces(ListNamespacesRequest {
                under: None,
                order: SortOrder::Asc,
                ..
            }))
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/_angos/namespaces/list",
                Some("repository=team&under=team/app&order=desc&offset=2")
            ),
            Some(Endpoint::ListNamespaces(ListNamespacesRequest {
                under: Some(under),
                order: SortOrder::Desc,
                page: PageRequest { offset: 2, n: None },
                ..
            })) if under.as_ref() == "team/app"
        ));
    }

    #[test]
    fn revisions_select_a_page_or_one_digest() {
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/team/app/_angos/revisions/list",
                Some("sort=digest&order=desc&n=10")
            ),
            Some(Endpoint::ListRevisions {
                selection: RevisionSelection::Page {
                    sort: RevisionSort::Digest,
                    order: SortOrder::Desc,
                    page: PageRequest {
                        offset: 0,
                        n: Some(_)
                    },
                },
                ..
            })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                "/v2/team/app/_angos/revisions/list",
                Some(&format!("digest={D}&offset=5"))
            ),
            Some(Endpoint::ListRevisions {
                selection: RevisionSelection::Digest(digest),
                ..
            }) if digest.to_string() == D
        ));
    }

    #[test]
    fn listings_reject_malformed_queries() {
        for (path, query) in [
            ("/v2/_angos/repositories/list", "order=up"),
            ("/v2/_angos/repositories/list", "n=0"),
            ("/v2/_angos/namespaces/list", "repository=team&order=up"),
            ("/v2/_angos/namespaces/list", "repository=team&under=Team"),
            ("/v2/_angos/namespaces/list", "repository=INVALID"),
            ("/v2/myrepo/_angos/revisions/list", "sort=size"),
            ("/v2/myrepo/_angos/revisions/list", "digest=sha256:abc"),
            ("/v2/myrepo/_angos/uploads/list", "offset=-1"),
            ("/v2/_angos/jobs/list", "n=0"),
            ("/v2/_angos/jobs/list", "queue=bogus"),
            // A lenient parse would reset the whole query and administer the
            // default cache queue instead of the requested one.
            ("/v2/_angos/jobs/list", "queue=replication&n=abc"),
        ] {
            assert!(
                parse(&Method::GET, path, Some(query)).is_none(),
                "{path}?{query} must not route"
            );
        }
    }

    #[test]
    fn layers() {
        assert!(matches!(
            parse(
                &Method::GET,
                &format!("/v2/app/_angos/layers/{D}/entries"),
                None
            ),
            Some(Endpoint::ListLayerEntries { .. })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                &format!("/v2/app/_angos/layers/{D}/file"),
                Some("path=etc/hosts&download")
            ),
            Some(Endpoint::GetLayerFile { download: true, .. })
        ));
        assert!(matches!(
            parse(
                &Method::GET,
                &format!("/v2/app/_angos/layers/{D}/details"),
                Some("path=usr/bin/env")
            ),
            Some(Endpoint::GetLayerFileDetails(_))
        ));
    }

    #[test]
    fn declines_foreign_surfaces_and_non_get_listings() {
        assert!(parse(&Method::GET, "/v2/_angos/ui/config", None).is_none());
        assert!(parse(&Method::GET, "/v2/_catalog", None).is_none());
        assert!(parse(&Method::GET, "/v2/app/tags/list", None).is_none());
        assert!(parse(&Method::POST, "/v2/myrepo/_angos/revisions/list", None).is_none());
        assert!(
            parse(
                &Method::POST,
                "/v2/_angos/namespaces/list",
                Some("repository=myrepo")
            )
            .is_none()
        );
    }

    #[test]
    fn pulls_listing() {
        match parse(
            &Method::GET,
            "/v2/myrepo/app/_angos/pulls/list",
            Some("tag=v1&offset=200&n=50"),
        ) {
            Some(Endpoint::ListPulls(ListPullsRequest {
                namespace,
                reference,
                page,
            })) => {
                assert_eq!(namespace, "myrepo/app");
                assert_eq!(reference.to_string(), "v1");
                assert_eq!(
                    page,
                    PageRequest {
                        offset: 200,
                        n: NonZeroU16::new(50)
                    }
                );
            }
            other => panic!("expected ListPulls, got {other:?}"),
        }
        match parse(
            &Method::GET,
            "/v2/myrepo/_angos/pulls/list",
            Some(&format!("digest={D}")),
        ) {
            Some(Endpoint::ListPulls(ListPullsRequest { reference, .. })) => {
                assert_eq!(reference.to_string(), D);
            }
            other => panic!("expected ListPulls, got {other:?}"),
        }
    }

    /// The target must be named exactly once, and must parse; anything else is a
    /// 404 rather than a lenient guess.
    #[test]
    fn pulls_reject_a_missing_ambiguous_or_invalid_target() {
        let ambiguous = format!("tag=v1&digest={D}");
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
    fn job_listings() {
        match parse(&Method::GET, "/v2/_angos/jobs/failed", None) {
            Some(Endpoint::ListFailedJobs(ListJobsRequest { queue, n, after })) => {
                assert_eq!(queue, Queue::Cache);
                assert_eq!(n, None);
                assert_eq!(after, None);
            }
            other => panic!("expected ListFailedJobs, got {other:?}"),
        }
        match parse(
            &Method::GET,
            "/v2/_angos/jobs/list",
            Some("queue=replication&n=10&after=abc"),
        ) {
            Some(Endpoint::ListJobs(ListJobsRequest { queue, n, after })) => {
                assert_eq!(queue, Queue::Replication);
                assert_eq!(n, NonZeroU16::new(10));
                assert_eq!(after.as_deref(), Some("abc"));
            }
            other => panic!("expected ListJobs, got {other:?}"),
        }
    }

    /// An extension path ends at its module, so a mutation names its job in
    /// `?key=`.
    #[test]
    fn job_mutations() {
        assert!(matches!(
            parse(&Method::POST, "/v2/_angos/jobs/failed", Some("key=abc-123")),
            Some(Endpoint::RetryJob(_))
        ));
        assert!(matches!(
            parse(
                &Method::DELETE,
                "/v2/_angos/jobs/pending",
                Some("key=abc-123")
            ),
            Some(Endpoint::DeleteJob(DeleteJobRequest {
                state: JobState::Pending,
                ..
            }))
        ));
        match parse(
            &Method::DELETE,
            "/v2/_angos/jobs/failed",
            Some("key=0000018b-abc"),
        ) {
            Some(Endpoint::DeleteJob(DeleteJobRequest {
                queue,
                state,
                storage_key,
            })) => {
                assert_eq!(queue, Queue::Cache);
                assert_eq!(state, JobState::Failed);
                assert_eq!(storage_key.as_str(), "0000018b-abc");
            }
            other => panic!("expected DeleteJob(Failed), got {other:?}"),
        }

        for (method, query) in [
            (Method::POST, None),
            (Method::POST, Some("key=a/b")),
            (Method::POST, Some("key=")),
            (Method::DELETE, Some("queue=bogus&key=0000018b-abc")),
            (
                Method::DELETE,
                Some("queue=replication&key=0000018b-abc&n=abc"),
            ),
        ] {
            assert!(
                parse(&method, "/v2/_angos/jobs/failed", query).is_none(),
                "{method} ?{query:?} must not route"
            );
        }
    }
}
