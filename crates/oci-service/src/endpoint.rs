//! Routing for the OCI Distribution surface: `(method, path, query)` to the
//! operation and the validated values it addresses.

use std::collections::BTreeSet;
use std::str::FromStr;

use http::Method;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use angos_oci::path::{API_PREFIX, TAGS_LIST, UPLOADS};
use angos_oci::request::ManifestPutTarget;
use angos_oci::server;
use angos_oci::{Algorithm, Digest, MediaType, Namespace, Reference, Tag, UploadSessionId};

/// HEAD and GET are distinct variants so a caller can route HEAD to a
/// body-less handler.
#[derive(Clone, Debug)]
pub enum Endpoint {
    /// `GET`/`HEAD /v2/`, end-1.
    CheckVersion,
    Manifest(ManifestEndpoint),
    /// `GET /v2/<name>/blobs/<digest>`, end-2.
    GetBlob {
        namespace: Namespace,
        digest: Digest,
    },
    /// `HEAD /v2/<name>/blobs/<digest>`, end-2.
    HeadBlob {
        namespace: Namespace,
        digest: Digest,
    },
    /// `DELETE /v2/<name>/blobs/<digest>`, end-10.
    DeleteBlob {
        namespace: Namespace,
        digest: Digest,
    },
    Upload(UploadEndpoint),
    /// `GET /v2/<name>/tags/list`, end-8.
    ListTags {
        namespace: Namespace,
        n: Option<u16>,
        last: Option<String>,
    },
    /// `GET /v2/<name>/referrers/<digest>`, end-12.
    GetReferrers {
        namespace: Namespace,
        digest: Digest,
        artifact_type: Option<MediaType>,
        last: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub enum ManifestEndpoint {
    /// `GET /v2/<name>/manifests/<reference>`, end-3.
    Get {
        namespace: Namespace,
        reference: Reference,
    },
    /// `HEAD /v2/<name>/manifests/<reference>`, end-3.
    Head {
        namespace: Namespace,
        reference: Reference,
    },
    /// `PUT /v2/<name>/manifests/<reference>`, end-7.
    Put {
        namespace: Namespace,
        target: ManifestPutTarget,
    },
    /// `DELETE /v2/<name>/manifests/<reference>`, end-9.
    Delete {
        namespace: Namespace,
        reference: Reference,
    },
}

#[derive(Clone, Debug)]
pub enum UploadEndpoint {
    /// `POST /v2/<name>/blobs/uploads/`, end-4a/4b.
    Start {
        namespace: Namespace,
        digest: Option<Digest>,
        digest_algorithm: Option<Algorithm>,
    },
    /// `POST /v2/<name>/blobs/uploads/?mount=<digest>&from=<name>`, end-11.
    Mount {
        namespace: Namespace,
        digest: Digest,
        from: Option<Namespace>,
    },
    /// `GET /v2/<name>/blobs/uploads/<session>`, end-13.
    Get {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
    /// `PATCH /v2/<name>/blobs/uploads/<session>`, end-5.
    Patch {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
    /// `PUT /v2/<name>/blobs/uploads/<session>?digest=<digest>`, end-6.
    Put {
        namespace: Namespace,
        session_id: UploadSessionId,
        digest: Digest,
    },
    /// `DELETE /v2/<name>/blobs/uploads/<session>`.
    Delete {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
}

impl Endpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            Endpoint::CheckVersion => "check-version",
            Endpoint::Manifest(endpoint) => endpoint.endpoint_name(),
            Endpoint::GetBlob { .. } => "get-blob",
            Endpoint::HeadBlob { .. } => "head-blob",
            Endpoint::DeleteBlob { .. } => "delete-blob",
            Endpoint::Upload(endpoint) => endpoint.endpoint_name(),
            Endpoint::ListTags { .. } => "list-tags",
            Endpoint::GetReferrers { .. } => "get-referrers",
        }
    }
}

impl ManifestEndpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            ManifestEndpoint::Get { .. } => "get-manifest",
            ManifestEndpoint::Head { .. } => "head-manifest",
            ManifestEndpoint::Put { .. } => "put-manifest",
            ManifestEndpoint::Delete { .. } => "delete-manifest",
        }
    }
}

impl UploadEndpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            UploadEndpoint::Start { .. } => "start-upload",
            UploadEndpoint::Mount { .. } => "mount-blob",
            UploadEndpoint::Get { .. } => "get-upload",
            UploadEndpoint::Patch { .. } => "patch-upload",
            UploadEndpoint::Put { .. } => "put-upload",
            UploadEndpoint::Delete { .. } => "delete-upload",
        }
    }
}

/// `None` when the path is not part of this surface, so a caller can try the
/// next one. `query` is the raw query string without `?`.
#[must_use]
pub fn parse(method: &Method, path: &str, query: Option<&str>) -> Option<Endpoint> {
    // HEAD as well as GET: the version check is the OCI conformance probe.
    if path == "/v2/" && (method == Method::GET || method == Method::HEAD) {
        return Some(Endpoint::CheckVersion);
    }

    let api_path = path.strip_prefix(API_PREFIX)?;
    try_upload(method, api_path, query)
        .or_else(|| try_blobs(method, api_path))
        .or_else(|| try_manifests(method, api_path, query))
        .or_else(|| try_referrers(method, api_path, query))
        .or_else(|| try_tags(method, api_path, query))
}

/// Whether a request [`parse`] refused was a referrers read owing a `400`: a
/// registry must answer an invalid one that way, not with a `404`. A `GET`
/// whose path is a referrers path over a parsable namespace, that [`parse`]
/// still declined (so the digest or `?artifactType=` was malformed), is one.
/// REF: <https://github.com/opencontainers/distribution-spec/blob/v1.1.0/spec.md#listing-referrers>
#[must_use]
pub fn is_invalid_referrers_request(method: &Method, path: &str) -> bool {
    *method == Method::GET
        && path
            .strip_prefix(API_PREFIX)
            .and_then(server::split_referrers_path)
            .is_some_and(|(namespace, _)| Namespace::new(namespace).is_ok())
}

fn parse_query<T: DeserializeOwned>(params: Option<&str>) -> Option<T> {
    serde_html_form::from_str(params.unwrap_or_default()).ok()
}

#[derive(Deserialize)]
struct DigestQuery {
    digest: Option<Digest>,
}

#[derive(Deserialize)]
struct TagQuery {
    #[serde(default)]
    tag: BTreeSet<Tag>,
}

#[derive(Deserialize)]
struct MountQuery {
    mount: Option<Digest>,
    from: Option<Namespace>,
    digest: Option<Digest>,
    #[serde(rename = "digest-algorithm")]
    digest_algorithm: Option<Algorithm>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReferrersQuery {
    artifact_type: Option<MediaType>,
    last: Option<String>,
}

#[derive(Deserialize)]
struct PaginationQuery {
    n: Option<u16>,
    last: Option<String>,
}

fn try_upload(method: &Method, path: &str, params: Option<&str>) -> Option<Endpoint> {
    if let Some(namespace_str) = server::split_uploads_start_path(path) {
        let namespace = Namespace::new(namespace_str).ok()?;
        if *method != Method::POST {
            return None;
        }
        // The OCI fall-back-to-session rule covers unsatisfiable mounts, not
        // syntactically invalid ones, so a malformed query is a 400.
        let query: MountQuery = parse_query(params)?;
        if let Some(digest) = query.mount {
            return Some(Endpoint::Upload(UploadEndpoint::Mount {
                namespace,
                digest,
                from: query.from,
            }));
        }
        return Some(Endpoint::Upload(UploadEndpoint::Start {
            namespace,
            digest: query.digest,
            digest_algorithm: query.digest_algorithm,
        }));
    }

    let (namespace_str, session_id) = path.rsplit_once(UPLOADS)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let session_id = UploadSessionId::from_str(session_id).ok()?;

    match *method {
        Method::GET => Some(Endpoint::Upload(UploadEndpoint::Get {
            namespace,
            session_id,
        })),
        Method::PATCH => Some(Endpoint::Upload(UploadEndpoint::Patch {
            namespace,
            session_id,
        })),
        Method::PUT => Some(Endpoint::Upload(UploadEndpoint::Put {
            namespace,
            session_id,
            digest: parse_query::<DigestQuery>(params)?.digest?,
        })),
        Method::DELETE => Some(Endpoint::Upload(UploadEndpoint::Delete {
            namespace,
            session_id,
        })),
        _ => None,
    }
}

fn try_blobs(method: &Method, path: &str) -> Option<Endpoint> {
    let (namespace_str, digest) = server::split_blob_path(path)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let digest = Digest::from_str(digest).ok()?;
    match *method {
        Method::GET => Some(Endpoint::GetBlob { namespace, digest }),
        Method::HEAD => Some(Endpoint::HeadBlob { namespace, digest }),
        Method::DELETE => Some(Endpoint::DeleteBlob { namespace, digest }),
        _ => None,
    }
}

fn try_manifests(method: &Method, path: &str, params: Option<&str>) -> Option<Endpoint> {
    let (namespace_str, reference) = server::split_manifest_path(path)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let reference = Reference::from_str(reference).ok()?;
    match *method {
        Method::GET => Some(Endpoint::Manifest(ManifestEndpoint::Get {
            namespace,
            reference,
        })),
        Method::HEAD => Some(Endpoint::Manifest(ManifestEndpoint::Head {
            namespace,
            reference,
        })),
        Method::PUT => {
            // `?tag=` applies only to a by-digest push; a by-tag push ignores
            // it. Strict parse: a single invalid tag rejects the PUT.
            let target = match reference {
                Reference::Tag(tag) => ManifestPutTarget::Tag(tag),
                Reference::Digest(digest) => ManifestPutTarget::Digest {
                    digest,
                    tags: parse_query::<TagQuery>(params)?.tag.into_iter().collect(),
                },
            };
            Some(Endpoint::Manifest(ManifestEndpoint::Put {
                namespace,
                target,
            }))
        }
        Method::DELETE => Some(Endpoint::Manifest(ManifestEndpoint::Delete {
            namespace,
            reference,
        })),
        _ => None,
    }
}

fn try_referrers(method: &Method, path: &str, params: Option<&str>) -> Option<Endpoint> {
    let (namespace_str, digest) = server::split_referrers_path(path)?;
    if *method != Method::GET {
        return None;
    }
    let namespace = Namespace::new(namespace_str).ok()?;
    let digest = Digest::from_str(digest).ok()?;
    // Strict parse: a malformed `?artifactType=` is a bad filter, not an absent
    // one, so it must not degrade into an unfiltered listing.
    let ReferrersQuery {
        artifact_type,
        last,
    } = parse_query(params)?;
    Some(Endpoint::GetReferrers {
        namespace,
        digest,
        artifact_type,
        last,
    })
}

fn try_tags(method: &Method, path: &str, params: Option<&str>) -> Option<Endpoint> {
    let namespace_str = path.strip_suffix(TAGS_LIST)?;
    if *method != Method::GET {
        return None;
    }
    let namespace = Namespace::new(namespace_str).ok()?;
    let PaginationQuery { n, last } = parse_query(params)?;
    Some(Endpoint::ListTags { namespace, n, last })
}

#[cfg(test)]
mod tests {
    use super::*;

    use angos_oci::client::referrers_path;
    use angos_oci::request::GetReferrersRequest;

    const D: &str = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const UPLOADS_START: &str = "/v2/myrepo/app/blobs/uploads/";

    fn ns(s: &str) -> Namespace {
        Namespace::new(s).unwrap()
    }

    #[test]
    fn version_probe() {
        assert!(matches!(
            parse(&Method::GET, "/v2/", None),
            Some(Endpoint::CheckVersion)
        ));
        assert!(matches!(
            parse(&Method::HEAD, "/v2/", None),
            Some(Endpoint::CheckVersion)
        ));
        assert!(parse(&Method::GET, "/v2", None).is_none());
        assert!(parse(&Method::POST, "/v2/", None).is_none());
    }

    #[test]
    fn blob_get_head_delete() {
        let path = format!("/v2/library/nginx/blobs/{D}");
        assert!(matches!(
            parse(&Method::GET, &path, None),
            Some(Endpoint::GetBlob { namespace, digest }) if namespace == ns("library/nginx") && digest.to_string() == D
        ));
        assert!(matches!(
            parse(&Method::HEAD, &path, None),
            Some(Endpoint::HeadBlob { .. })
        ));
        assert!(matches!(
            parse(&Method::DELETE, &path, None),
            Some(Endpoint::DeleteBlob { .. })
        ));
        assert!(parse(&Method::OPTIONS, &path, None).is_none());
    }

    #[test]
    fn manifest_get_head_delete() {
        // A by-tag GET ignores `?tag=`, which only a by-digest PUT reads.
        assert!(matches!(
            parse(&Method::GET, "/v2/myrepo/app/manifests/latest", Some("tag=a")),
            Some(Endpoint::Manifest(ManifestEndpoint::Get { namespace, reference }))
                if namespace == ns("myrepo/app") && reference.to_string() == "latest"
        ));
        assert!(matches!(
            parse(&Method::HEAD, "/v2/myrepo/app/manifests/v1.0.0", None),
            Some(Endpoint::Manifest(ManifestEndpoint::Head { reference, .. }))
                if reference.to_string() == "v1.0.0"
        ));
        assert!(matches!(
            parse(&Method::DELETE, &format!("/v2/myrepo/app/manifests/{D}"), None),
            Some(Endpoint::Manifest(ManifestEndpoint::Delete { reference, .. }))
                if reference.to_string() == D
        ));
    }

    #[test]
    fn manifest_put_by_tag_takes_no_extra_tags() {
        let Some(Endpoint::Manifest(ManifestEndpoint::Put { target, .. })) =
            parse(&Method::PUT, "/v2/app/manifests/latest", Some("tag=v1"))
        else {
            panic!("expected a by-tag put");
        };
        assert!(matches!(target, ManifestPutTarget::Tag(_)));
        assert_eq!(target.created_tags().len(), 1);
    }

    #[test]
    fn put_manifest_by_digest_dedups_repeated_tag_params() {
        let Some(Endpoint::Manifest(ManifestEndpoint::Put {
            target: ManifestPutTarget::Digest { tags, .. },
            ..
        })) = parse(
            &Method::PUT,
            &format!("/v2/foo/manifests/{D}"),
            Some("tag=a&tag=a&tag=b"),
        )
        else {
            panic!("a by-digest PUT must produce a Digest target");
        };
        assert_eq!(tags, vec![Tag::new("a").unwrap(), Tag::new("b").unwrap()]);
    }

    /// A single invalid `?tag=` rejects the route (the generic 400) rather than
    /// dropping every tag.
    #[test]
    fn put_manifest_by_digest_invalid_tag_param_rejected() {
        assert!(
            parse(
                &Method::PUT,
                &format!("/v2/foo/manifests/{D}"),
                Some("tag=a&tag=bad!tag"),
            )
            .is_none()
        );
    }

    #[test]
    fn list_tags() {
        assert!(matches!(
            parse(&Method::GET, "/v2/app/tags/list", Some("n=2")),
            Some(Endpoint::ListTags { n: Some(2), .. })
        ));
    }

    #[test]
    fn upload_start() {
        assert!(matches!(
            parse(&Method::POST, UPLOADS_START, None),
            Some(Endpoint::Upload(UploadEndpoint::Start { namespace, digest: None, digest_algorithm: None }))
                if namespace == ns("myrepo/app")
        ));
        let Some(Endpoint::Upload(UploadEndpoint::Start { digest, .. })) =
            parse(&Method::POST, UPLOADS_START, Some(&format!("digest={D}")))
        else {
            panic!("expected a one-shot upload start");
        };
        assert_eq!(digest.map(|digest| digest.to_string()).as_deref(), Some(D));
        // The hint makes the session hash under one algorithm.
        assert!(matches!(
            parse(
                &Method::POST,
                UPLOADS_START,
                Some("digest-algorithm=sha512")
            ),
            Some(Endpoint::Upload(UploadEndpoint::Start {
                digest_algorithm: Some(Algorithm::Sha512),
                ..
            }))
        ));

        assert!(parse(&Method::GET, UPLOADS_START, None).is_none());
        assert!(parse(&Method::POST, "/v2/foo/blobs/uploads", None).is_none());
        assert!(parse(&Method::POST, "/v2/blobs/uploads/", None).is_none());
        assert!(parse(&Method::POST, "/v2/MyRepo/blobs/uploads/", None).is_none());
    }

    /// An unset `from` makes the server attempt automatic content discovery.
    #[test]
    fn mount_blob_with_and_without_from() {
        assert!(matches!(
            parse(&Method::POST, UPLOADS_START, Some(&format!("mount={D}&from=other"))),
            Some(Endpoint::Upload(UploadEndpoint::Mount { from: Some(from), .. })) if from == ns("other")
        ));
        assert!(matches!(
            parse(&Method::POST, UPLOADS_START, Some(&format!("mount={D}"))),
            Some(Endpoint::Upload(UploadEndpoint::Mount { digest, from: None, .. })) if digest.to_string() == D
        ));
    }

    /// The OCI fall-back-to-session rule covers unsatisfiable mounts, not
    /// syntactically invalid ones, so any malformed parameter is a 400, even
    /// one the chosen operation would not read.
    #[test]
    fn upload_start_rejects_malformed_queries() {
        for query in [
            format!("mount={D}&from=Invalid"),
            "from=Invalid".to_string(),
            "digest=not-a-digest".to_string(),
            "mount=not-a-digest".to_string(),
            format!("mount={D}&digest=garbage"),
            "digest-algorithm=md5".to_string(),
        ] {
            assert!(
                parse(&Method::POST, UPLOADS_START, Some(&query)).is_none(),
                "?{query} must not route"
            );
        }
    }

    #[test]
    fn upload_session_get_patch_delete() {
        let session_id = UploadSessionId::generate();
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        for (method, name) in [
            (Method::GET, "get-upload"),
            (Method::PATCH, "patch-upload"),
            (Method::DELETE, "delete-upload"),
        ] {
            let endpoint = parse(&method, &path, None).expect("a session route");
            assert_eq!(endpoint.endpoint_name(), name);
            let Endpoint::Upload(
                UploadEndpoint::Get {
                    namespace,
                    session_id: parsed,
                }
                | UploadEndpoint::Patch {
                    namespace,
                    session_id: parsed,
                }
                | UploadEndpoint::Delete {
                    namespace,
                    session_id: parsed,
                },
            ) = endpoint
            else {
                panic!("{name} must address the session");
            };
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(parsed, session_id);
        }
        assert!(
            parse(
                &Method::GET,
                "/v2/myrepo/app/blobs/uploads/not-a-uuid",
                None
            )
            .is_none()
        );
    }

    #[test]
    fn put_upload_requires_a_digest() {
        let session_id = UploadSessionId::generate();
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        assert!(matches!(
            parse(&Method::PUT, &path, Some(&format!("digest={D}"))),
            Some(Endpoint::Upload(UploadEndpoint::Put { session_id: parsed, digest, .. }))
                if parsed == session_id && digest.to_string() == D
        ));
        assert!(parse(&Method::PUT, &path, None).is_none());
    }

    #[test]
    fn malformed_addresses_and_methods_decline() {
        assert!(parse(&Method::GET, "/v2/myrepo/app/blobs/not-a-digest", None).is_none());
        // The 400 vs 404 call is the transport's, via `is_invalid_referrers_request`.
        assert!(parse(&Method::GET, "/v2/myrepo/app/referrers/not-a-digest", None).is_none());
        assert!(parse(&Method::POST, "/v2/myrepo/app/tags/list", None).is_none());
        // Must not route, so the dispatcher answers the PUT `400` as OCI
        // conformance requires.
        assert!(
            parse(
                &Method::PUT,
                "/v2/myrepo/app/manifests/sha256:not-a-digest",
                None
            )
            .is_none()
        );
    }

    #[test]
    fn declines_foreign_surfaces() {
        assert!(parse(&Method::GET, "/v2/_catalog", None).is_none());
        assert!(parse(&Method::GET, "/v2/app/_angos/revisions/list", None).is_none());
        assert!(parse(&Method::GET, "/healthz", None).is_none());
    }

    #[test]
    fn invalid_referrers_flagged() {
        assert!(is_invalid_referrers_request(
            &Method::GET,
            "/v2/app/referrers/not-a-digest"
        ));
        assert!(!is_invalid_referrers_request(
            &Method::GET,
            "/v2/app/tags/list"
        ));
    }

    /// The `+json` suffix must survive query decoding in either spelling a
    /// client percent-encodes it as.
    #[test]
    fn artifact_type_filter_keeps_an_encoded_media_type_suffix() {
        let expected = MediaType::new("application/vnd.in-toto+json").unwrap();
        for query in [
            "artifactType=application%2Fvnd.in-toto%2Bjson",
            "artifactType=application/vnd.in-toto%2Bjson",
        ] {
            match parse(
                &Method::GET,
                &format!("/v2/lib/nginx/referrers/{D}"),
                Some(query),
            ) {
                Some(Endpoint::GetReferrers { artifact_type, .. }) => assert_eq!(
                    artifact_type.as_ref(),
                    Some(&expected),
                    "filter must survive {query}"
                ),
                other => panic!("{query} must route to a referrers listing, got {other:?}"),
            }
        }
    }

    /// A malformed filter must never degrade into an unfiltered listing. Only a
    /// header may carry media type parameters, so a filter naming them is
    /// refused rather than reduced to the type ahead of them.
    #[test]
    fn artifact_type_filter_rejects_a_malformed_value() {
        for filter in ["not-a-media-type", "application/json;charset=utf-8"] {
            assert!(
                parse(
                    &Method::GET,
                    &format!("/v2/lib/nginx/referrers/{D}"),
                    Some(&format!("artifactType={filter}")),
                )
                .is_none(),
                "'{filter}' must not resolve to a listing"
            );
        }
    }

    /// The spec defines no page size here, so `?n=` is ignored like any other
    /// unknown parameter rather than rejected.
    #[test]
    fn a_referrers_page_size_is_not_a_parameter() {
        for query in ["n=abc", "n=65536", "n=10"] {
            assert!(
                matches!(
                    parse(
                        &Method::GET,
                        &format!("/v2/lib/nginx/referrers/{D}"),
                        Some(query)
                    ),
                    Some(Endpoint::GetReferrers { .. })
                ),
                "?{query} must be ignored, not refused"
            );
        }
    }

    /// The serving side composes the `Link` a filtered listing advertises; a
    /// raw `+` would come back as a space and fail the media-type grammar.
    #[test]
    fn a_rendered_referrers_link_parses_back_into_the_same_filter() {
        let digest: Digest = D.parse().unwrap();
        let filter = MediaType::new("application/vnd.example.sbom.v1+json").unwrap();
        let link = referrers_path(
            "",
            &GetReferrersRequest {
                namespace: Namespace::new("lib/nginx").unwrap(),
                digest: digest.clone(),
                artifact_type: Some(filter.clone()),
                last: Some(digest.to_string()),
            },
        );

        let (path, query) = link
            .split_once('?')
            .expect("a filtered link carries a query");
        match parse(&Method::GET, path, Some(query)) {
            Some(Endpoint::GetReferrers {
                artifact_type,
                last,
                ..
            }) => {
                assert_eq!(artifact_type.as_ref(), Some(&filter));
                assert_eq!(last.as_deref(), Some(D));
            }
            other => panic!("the advertised next page must route, got {other:?}"),
        }
    }
}
