//! Parse an HTTP method and URL path into an [`Endpoint`] of the OCI
//! Distribution surface.
//!
//! This is the routing half of the spec: given `(method, path, query)`, name
//! the operation and pull the validated path/query values it addresses. Headers
//! and body are layered on by the transport afterwards. A path this surface
//! does not serve returns `None`, so a caller can try the next surface.

use std::collections::BTreeSet;
use std::str::FromStr;

use http::Method;
use serde::{Deserialize, de::DeserializeOwned};

use angos_oci::path::{API_PREFIX, TAGS_LIST, UPLOADS};
use angos_oci::server;
use angos_oci::{Algorithm, Digest, MediaType, Namespace, Reference, Tag, UploadSessionId};

use angos_oci::request::ManifestPutTarget;

/// An operation of the OCI Distribution HTTP API, with the values its path and
/// query carry. HEAD and GET are distinct variants so a caller can route them
/// to a body-less handler.
#[derive(Clone, Debug)]
pub enum Endpoint {
    /// `GET`/`HEAD /v2/`, end-1.
    CheckVersion,
    /// `GET /v2/<name>/manifests/<reference>`, end-3.
    GetManifest {
        namespace: Namespace,
        reference: Reference,
    },
    /// `HEAD /v2/<name>/manifests/<reference>`, end-3.
    HeadManifest {
        namespace: Namespace,
        reference: Reference,
    },
    /// `PUT /v2/<name>/manifests/<reference>`, end-7.
    PutManifest {
        namespace: Namespace,
        target: ManifestPutTarget,
    },
    /// `DELETE /v2/<name>/manifests/<reference>`, end-9.
    DeleteManifest {
        namespace: Namespace,
        reference: Reference,
    },
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
    /// `POST /v2/<name>/blobs/uploads/`, end-4a/4b.
    StartUpload {
        namespace: Namespace,
        digest: Option<Digest>,
        digest_algorithm: Option<Algorithm>,
    },
    /// `POST /v2/<name>/blobs/uploads/?mount=<digest>&from=<name>`, end-11.
    MountBlob {
        namespace: Namespace,
        digest: Digest,
        from: Option<Namespace>,
    },
    /// `GET /v2/<name>/blobs/uploads/<session>`, end-13.
    GetUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
    /// `PATCH /v2/<name>/blobs/uploads/<session>`, end-5.
    PatchUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
    /// `PUT /v2/<name>/blobs/uploads/<session>?digest=<digest>`, end-6.
    PutUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
        digest: Digest,
    },
    /// `DELETE /v2/<name>/blobs/uploads/<session>`.
    DeleteUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
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

impl Endpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            Endpoint::CheckVersion => "check-version",
            Endpoint::GetManifest { .. } => "get-manifest",
            Endpoint::HeadManifest { .. } => "head-manifest",
            Endpoint::PutManifest { .. } => "put-manifest",
            Endpoint::DeleteManifest { .. } => "delete-manifest",
            Endpoint::GetBlob { .. } => "get-blob",
            Endpoint::HeadBlob { .. } => "head-blob",
            Endpoint::DeleteBlob { .. } => "delete-blob",
            Endpoint::StartUpload { .. } => "start-upload",
            Endpoint::MountBlob { .. } => "mount-blob",
            Endpoint::GetUpload { .. } => "get-upload",
            Endpoint::PatchUpload { .. } => "patch-upload",
            Endpoint::PutUpload { .. } => "put-upload",
            Endpoint::DeleteUpload { .. } => "delete-upload",
            Endpoint::ListTags { .. } => "list-tags",
            Endpoint::GetReferrers { .. } => "get-referrers",
        }
    }
}

/// Parse `(method, path, query)` into an OCI [`Endpoint`], or `None` when the
/// path is not part of the OCI surface (so a caller may try another surface).
/// `path` is the raw URL path; `query` is the raw query string without `?`.
#[must_use]
pub fn parse(method: &Method, path: &str, query: Option<&str>) -> Option<Endpoint> {
    // HEAD as well as GET: the version check is the OCI conformance probe.
    if path == "/v2/" && (method == Method::GET || method == Method::HEAD) {
        return Some(Endpoint::CheckVersion);
    }
    // end-1 is `/v2/`; the same path without its slash is not the version
    // endpoint and addresses nothing.
    if path == "/v2" {
        return None;
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

#[derive(Deserialize, Default)]
struct DigestQuery {
    digest: Option<Digest>,
}

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
    #[serde(rename = "digest-algorithm")]
    digest_algorithm: Option<Algorithm>,
}

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

#[derive(Deserialize)]
struct CursorQuery {
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
            return Some(Endpoint::MountBlob {
                namespace,
                digest,
                from: query.from,
            });
        }
        return Some(Endpoint::StartUpload {
            namespace,
            digest: query.digest,
            digest_algorithm: query.digest_algorithm,
        });
    }

    let (namespace_str, session_id) = path.rsplit_once(UPLOADS)?;
    let namespace = Namespace::new(namespace_str).ok()?;
    let session_id = UploadSessionId::from_str(session_id).ok()?;

    match *method {
        Method::GET => Some(Endpoint::GetUpload {
            namespace,
            session_id,
        }),
        Method::PATCH => Some(Endpoint::PatchUpload {
            namespace,
            session_id,
        }),
        Method::PUT => Some(Endpoint::PutUpload {
            namespace,
            session_id,
            digest: parse_query::<DigestQuery>(params)?.digest?,
        }),
        Method::DELETE => Some(Endpoint::DeleteUpload {
            namespace,
            session_id,
        }),
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
        Method::GET => Some(Endpoint::GetManifest {
            namespace,
            reference,
        }),
        Method::HEAD => Some(Endpoint::HeadManifest {
            namespace,
            reference,
        }),
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
            Some(Endpoint::PutManifest { namespace, target })
        }
        Method::DELETE => Some(Endpoint::DeleteManifest {
            namespace,
            reference,
        }),
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
    Some(Endpoint::GetReferrers {
        namespace,
        digest,
        artifact_type: parse_query::<ArtifactTypeQuery>(params)?.artifact_type,
        last: parse_query::<CursorQuery>(params)?.last,
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
        let d = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let path = format!("/v2/library/nginx/blobs/{d}");
        assert!(matches!(
            parse(&Method::GET, &path, None),
            Some(Endpoint::GetBlob { namespace, digest }) if namespace == ns("library/nginx") && digest.to_string() == d
        ));
        assert!(matches!(
            parse(&Method::HEAD, &path, None),
            Some(Endpoint::HeadBlob { .. })
        ));
        assert!(matches!(
            parse(&Method::DELETE, &path, None),
            Some(Endpoint::DeleteBlob { .. })
        ));
    }

    #[test]
    fn manifest_put_by_digest_collects_tags() {
        let d = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let path = format!("/v2/app/manifests/{d}");
        let Some(Endpoint::PutManifest { target, .. }) =
            parse(&Method::PUT, &path, Some("tag=v1&tag=v2"))
        else {
            panic!("expected a by-digest put");
        };
        let (reference, tags) = target.into_parts();
        assert!(matches!(reference, Reference::Digest(_)));
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn manifest_put_by_tag_takes_no_extra_tags() {
        let Some(Endpoint::PutManifest { target, .. }) =
            parse(&Method::PUT, "/v2/app/manifests/latest", Some("tag=v1"))
        else {
            panic!("expected a by-tag put");
        };
        assert!(matches!(target, ManifestPutTarget::Tag(_)));
        assert_eq!(target.created_tags().len(), 1);
    }

    #[test]
    fn start_upload_and_mount() {
        assert!(matches!(
            parse(&Method::POST, "/v2/app/blobs/uploads/", None),
            Some(Endpoint::StartUpload { .. })
        ));
        let d = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(matches!(
            parse(
                &Method::POST,
                "/v2/app/blobs/uploads/",
                Some(&format!("mount={d}&from=other"))
            ),
            Some(Endpoint::MountBlob { from: Some(_), .. })
        ));
    }

    #[test]
    fn tags_and_referrers() {
        assert!(matches!(
            parse(&Method::GET, "/v2/app/tags/list", Some("n=2")),
            Some(Endpoint::ListTags { n: Some(2), .. })
        ));
        let d = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(matches!(
            parse(&Method::GET, &format!("/v2/app/referrers/{d}"), None),
            Some(Endpoint::GetReferrers { .. })
        ));
    }

    #[test]
    fn upload_start_method_slash_and_namespace() {
        // POST with the spec's trailing slash opens a session under the namespace.
        assert!(matches!(
            parse(&Method::POST, "/v2/myrepo/app/blobs/uploads/", None),
            Some(Endpoint::StartUpload { namespace, digest: None, .. }) if namespace == ns("myrepo/app")
        ));
        // Nested namespace parses correctly.
        assert!(matches!(
            parse(&Method::POST, "/v2/org/team/blobs/uploads/", None),
            Some(Endpoint::StartUpload { namespace, .. }) if namespace == ns("org/team")
        ));
        // A GET on the start path is not the endpoint.
        assert!(parse(&Method::GET, "/v2/myrepo/app/blobs/uploads/", None).is_none());
        // Missing trailing slash is not the uploads endpoint.
        assert!(parse(&Method::POST, "/v2/foo/blobs/uploads", None).is_none());
        // Empty or invalid namespaces do not route.
        assert!(parse(&Method::POST, "/v2/blobs/uploads/", None).is_none());
        assert!(parse(&Method::POST, "/v2/MyRepo/blobs/uploads/", None).is_none());
        assert!(parse(&Method::POST, "/v2/bad ns/blobs/uploads/", None).is_none());
        // A session id that is not a valid uuid does not route.
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
    fn malformed_addresses_and_methods_decline() {
        // A blob digest that does not parse is a miss.
        assert!(parse(&Method::GET, "/v2/myrepo/app/blobs/not-a-digest", None).is_none());
        // A referrers digest that does not parse is a miss (the 400 vs 404 call
        // is the transport's, via `is_invalid_referrers_request`).
        assert!(parse(&Method::GET, "/v2/myrepo/app/referrers/not-a-digest", None).is_none());
        // Tags list is read-only.
        assert!(parse(&Method::POST, "/v2/myrepo/app/tags/list", None).is_none());
        // A by-tag manifest read parses the tag reference.
        assert!(matches!(
            parse(&Method::GET, "/v2/myrepo/app/manifests/latest", None),
            Some(Endpoint::GetManifest { namespace, reference })
                if namespace == ns("myrepo/app") && reference.to_string() == "latest"
        ));
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

    // Cases migrated from the binary router's parser tests.

    /// A reference that parses as neither a tag nor a digest must not route, so
    /// the dispatcher answers a `PUT` carrying one with `400`, as OCI
    /// conformance requires.
    #[test]
    fn put_manifest_with_unparseable_reference_is_rejected() {
        assert!(
            parse(
                &Method::PUT,
                "/v2/myrepo/app/manifests/sha256:not-a-digest",
                None,
            )
            .is_none(),
        );
    }

    #[test]
    fn start_upload_with_digest() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let Some(Endpoint::StartUpload {
            namespace, digest, ..
        }) = parse(
            &Method::POST,
            "/v2/myrepo/app/blobs/uploads/",
            Some(&format!("digest={d}")),
        )
        else {
            panic!("Expected StartUpload route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert!(digest.is_some());
        assert_eq!(digest.unwrap().to_string(), d);
    }

    /// An unset `from` makes the server attempt automatic content discovery.
    #[test]
    fn mount_blob_without_from() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let Some(Endpoint::MountBlob { digest, from, .. }) = parse(
            &Method::POST,
            "/v2/myrepo/target/blobs/uploads/",
            Some(&format!("mount={d}")),
        ) else {
            panic!("Expected MountBlob route");
        };
        assert_eq!(digest.to_string(), d);
        assert!(from.is_none());
    }

    #[test]
    fn mount_blob_with_malformed_from_is_rejected() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        assert!(
            parse(
                &Method::POST,
                "/v2/myrepo/target/blobs/uploads/",
                Some(&format!("mount={d}&from=Invalid")),
            )
            .is_none(),
            "a malformed ?from= must not route (POST -> 400)"
        );
    }

    #[test]
    fn malformed_from_without_mount_is_rejected() {
        assert!(
            parse(
                &Method::POST,
                "/v2/myrepo/target/blobs/uploads/",
                Some("from=Invalid"),
            )
            .is_none(),
            "a malformed ?from= must not route even unused (POST -> 400)"
        );
    }

    #[test]
    fn start_upload_with_malformed_digest_is_rejected() {
        for query in ["digest=not-a-digest", "digest=garbage"] {
            assert!(
                parse(&Method::POST, "/v2/myrepo/app/blobs/uploads/", Some(query)).is_none(),
                "a malformed ?digest= must not start a session (POST -> 400)"
            );
        }
    }

    /// The OCI fall-back-to-session rule covers unsatisfiable mounts, not
    /// syntactically invalid ones.
    #[test]
    fn malformed_mount_is_rejected() {
        assert!(
            parse(
                &Method::POST,
                "/v2/myrepo/target/blobs/uploads/",
                Some("mount=not-a-digest"),
            )
            .is_none(),
            "a malformed ?mount= must reject the route (POST -> 400)"
        );
    }

    /// Real clients never combine `?mount=` with a monolithic `?digest=`;
    /// rejecting the combination is by design.
    #[test]
    fn mount_with_malformed_digest_is_rejected() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        assert!(
            parse(
                &Method::POST,
                "/v2/myrepo/target/blobs/uploads/",
                Some(&format!("mount={d}&digest=garbage")),
            )
            .is_none(),
            "a malformed ?digest= must poison the mount path too (POST -> 400)"
        );
    }

    #[test]
    fn get_upload() {
        let session_id = UploadSessionId::generate();
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        let Some(Endpoint::GetUpload {
            namespace,
            session_id: parsed,
        }) = parse(&Method::GET, &path, None)
        else {
            panic!("Expected GetUpload route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(parsed, session_id);
    }

    #[test]
    fn patch_upload() {
        let session_id = UploadSessionId::generate();
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        let Some(Endpoint::PatchUpload {
            namespace,
            session_id: parsed,
        }) = parse(&Method::PATCH, &path, None)
        else {
            panic!("Expected PatchUpload route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(parsed, session_id);
    }

    #[test]
    fn put_upload() {
        let session_id = UploadSessionId::generate();
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        let Some(Endpoint::PutUpload {
            namespace,
            session_id: parsed,
            digest,
        }) = parse(&Method::PUT, &path, Some(&format!("digest={d}")))
        else {
            panic!("Expected PutUpload route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(parsed, session_id);
        assert_eq!(digest.to_string(), d);
    }

    #[test]
    fn put_upload_without_digest() {
        let session_id = UploadSessionId::generate();
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        assert!(parse(&Method::PUT, &path, None).is_none());
    }

    #[test]
    fn delete_upload() {
        let session_id = UploadSessionId::generate();
        let path = format!("/v2/myrepo/app/blobs/uploads/{session_id}");
        let Some(Endpoint::DeleteUpload {
            namespace,
            session_id: parsed,
        }) = parse(&Method::DELETE, &path, None)
        else {
            panic!("Expected DeleteUpload route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(parsed, session_id);
    }

    #[test]
    fn head_manifest() {
        let Some(Endpoint::HeadManifest {
            namespace,
            reference,
        }) = parse(&Method::HEAD, "/v2/myrepo/app/manifests/v1.0.0", None)
        else {
            panic!("Expected HeadManifest route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(reference.to_string(), "v1.0.0");
    }

    #[test]
    fn put_manifest_by_digest_dedups_repeated_tag_params() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let Some(Endpoint::PutManifest {
            target: ManifestPutTarget::Digest { tags, .. },
            ..
        }) = parse(
            &Method::PUT,
            &format!("/v2/foo/manifests/{d}"),
            Some("tag=a&tag=a&tag=b"),
        )
        else {
            panic!("a by-digest PUT must produce a Digest target");
        };
        assert_eq!(
            tags,
            vec![Tag::new("a").unwrap(), Tag::new("b").unwrap()],
            "a repeated `?tag=` value is de-duplicated"
        );
    }

    /// A single invalid `?tag=` value fails deserialization, so the route is
    /// rejected (the generic 400) rather than dropping every tag.
    #[test]
    fn put_manifest_by_digest_invalid_tag_param_rejected() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        assert!(
            parse(
                &Method::PUT,
                &format!("/v2/foo/manifests/{d}"),
                Some("tag=a&tag=bad!tag"),
            )
            .is_none(),
            "an invalid `?tag=` value must reject the by-digest PUT route"
        );
    }

    #[test]
    fn get_manifest_by_tag_ignores_tag_params() {
        assert!(
            matches!(
                parse(&Method::GET, "/v2/foo/manifests/latest", Some("tag=a")),
                Some(Endpoint::GetManifest { .. })
            ),
            "GET by tag must not carry tag params"
        );
    }

    #[test]
    fn delete_manifest() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let Some(Endpoint::DeleteManifest {
            namespace,
            reference,
        }) = parse(
            &Method::DELETE,
            &format!("/v2/myrepo/app/manifests/{d}"),
            None,
        )
        else {
            panic!("Expected DeleteManifest route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(reference.to_string(), d);
    }

    #[test]
    fn get_referrer_with_artifact_type() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let Some(Endpoint::GetReferrers {
            namespace,
            digest,
            artifact_type,
            ..
        }) = parse(
            &Method::GET,
            &format!("/v2/myrepo/app/referrers/{d}"),
            Some("artifactType=application/vnd.oci.image.manifest.v1%2Bjson"),
        )
        else {
            panic!("Expected GetReferrer route");
        };
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(digest.to_string(), d);
        assert_eq!(
            artifact_type,
            Some(MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap())
        );
    }

    #[test]
    fn unknown_method_declines() {
        let d = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        assert!(parse(&Method::OPTIONS, &format!("/v2/myrepo/app/blobs/{d}"), None).is_none());
    }

    /// The `?digest-algorithm=` hint reaches the session so it hashes under one
    /// algorithm; an unsupported value is a malformed request, not an absent
    /// hint.
    #[test]
    fn upload_digest_algorithm_hint_is_parsed() {
        match parse(
            &Method::POST,
            "/v2/myrepo/app/blobs/uploads/",
            Some("digest-algorithm=sha512"),
        ) {
            Some(Endpoint::StartUpload {
                digest_algorithm, ..
            }) => assert_eq!(digest_algorithm, Some(Algorithm::Sha512)),
            other => panic!("a hinted upload must start a session, got {other:?}"),
        }

        match parse(&Method::POST, "/v2/myrepo/app/blobs/uploads/", None) {
            Some(Endpoint::StartUpload {
                digest_algorithm, ..
            }) => assert!(digest_algorithm.is_none()),
            other => panic!("an unhinted upload must start a session, got {other:?}"),
        }

        assert!(
            parse(
                &Method::POST,
                "/v2/myrepo/app/blobs/uploads/",
                Some("digest-algorithm=md5"),
            )
            .is_none(),
            "an unsupported algorithm must not start a session (POST -> 400)"
        );
    }

    /// The filter is a media type, so its `+json` style suffix must survive the
    /// query decoding, in either spelling a client percent-encodes it as.
    #[test]
    fn artifact_type_filter_keeps_an_encoded_media_type_suffix() {
        let d = format!("sha256:{}", "a".repeat(64));
        let expected = MediaType::new("application/vnd.in-toto+json").unwrap();

        for query in [
            "artifactType=application%2Fvnd.in-toto%2Bjson",
            "artifactType=application/vnd.in-toto%2Bjson",
        ] {
            match parse(
                &Method::GET,
                &format!("/v2/lib/nginx/referrers/{d}"),
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

    /// A value that is not a media type is a bad filter, not an absent one: it
    /// must never degrade into an unfiltered listing of every referrer. A
    /// parameter section is not a media type either: only a header may carry
    /// one, so a filter naming parameters is refused rather than quietly
    /// reduced to the type ahead of them.
    #[test]
    fn artifact_type_filter_rejects_a_malformed_value() {
        let d = format!("sha256:{}", "a".repeat(64));
        for filter in ["not-a-media-type", "application/json;charset=utf-8"] {
            assert!(
                parse(
                    &Method::GET,
                    &format!("/v2/lib/nginx/referrers/{d}"),
                    Some(&format!("artifactType={filter}")),
                )
                .is_none(),
                "'{filter}' must not resolve to a listing"
            );
        }
    }

    /// The spec defines no page size for this endpoint, so `?n=` is not part of
    /// it and is ignored like any other unknown parameter rather than rejected.
    #[test]
    fn a_referrers_page_size_is_not_a_parameter() {
        let d = format!("sha256:{}", "a".repeat(64));
        for query in ["n=abc", "n=65536", "n=10"] {
            assert!(
                matches!(
                    parse(
                        &Method::GET,
                        &format!("/v2/lib/nginx/referrers/{d}"),
                        Some(query)
                    ),
                    Some(Endpoint::GetReferrers { .. })
                ),
                "?{query} must be ignored, not refused"
            );
        }
    }

    /// The `Link` a filtered listing advertises is composed by the serving side
    /// and parsed back by this one, so the filter has to survive the round
    /// trip. A raw `+` would come back as a space and fail the media-type
    /// grammar.
    #[test]
    fn a_rendered_referrers_link_parses_back_into_the_same_filter() {
        let digest: Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
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
                assert_eq!(last.as_deref(), Some(digest.to_string().as_str()));
            }
            other => panic!("the advertised next page must route, got {other:?}"),
        }
    }
}
