use std::str::FromStr;

use hyper::{Method, Uri};
use serde::{Deserialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{
    identity::Route,
    oci::{Digest, Namespace, Reference},
};

fn parse_query<T: DeserializeOwned + Default>(params: &str) -> T {
    serde_urlencoded::from_str(params).unwrap_or_default()
}

pub fn parse<'a>(method: &Method, uri: &'a Uri) -> Route<'a> {
    let path = uri.path();
    let params = uri.query();

    match path {
        "/healthz" if method == Method::GET => return Route::Healthz,
        "/metrics" if method == Method::GET => return Route::Metrics,
        "/_ui/config" if method == Method::GET => return Route::UiConfig,
        "/v2" | "/v2/" if method == Method::GET => return Route::ApiVersion,
        "/v2/_catalog" if method == Method::GET => {
            let (n, last) = if let Some(p) = params.map(parse_query::<PaginationQuery>) {
                (p.n, p.last)
            } else {
                (None, None)
            };

            return Route::ListCatalog { n, last };
        }
        _ => {}
    }

    if let Some(api_path) = path.strip_prefix("/v2/") {
        if let Some(route) = try_parse_extension(method, api_path) {
            return route;
        }

        if let Some(route) = try_parse_uploads(method, api_path, params) {
            return route;
        }

        if let Some(route) = try_parse_upload(method, api_path, params) {
            return route;
        }

        if let Some(route) = try_find_blobs(method, api_path) {
            return route;
        }

        if let Some(route) = try_find_manifests(method, api_path) {
            return route;
        }

        if let Some(route) = try_find_referrers(method, api_path, params) {
            return route;
        }

        if let Some(route) = try_find_tags(method, api_path, params) {
            return route;
        }

        return Route::Unknown;
    }

    if method == Method::GET || method == Method::HEAD {
        return Route::UiAsset { path };
    }

    Route::Unknown
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

fn try_parse_extension<'a>(method: &Method, path: &'a str) -> Option<Route<'a>> {
    if *method != Method::GET {
        return None;
    }

    let path = path.strip_prefix("_ext/")?;

    if path == "_repositories" {
        return Some(Route::ListRepositories);
    }

    if let Some(repository) = path.strip_suffix("/_namespaces") {
        return Some(Route::ListNamespaces { repository });
    }

    if let Some(namespace_str) = path.strip_suffix("/_revisions") {
        let namespace = Namespace::new(namespace_str).ok()?;
        return Some(Route::ListRevisions { namespace });
    }

    if let Some(namespace_str) = path.strip_suffix("/_uploads") {
        let namespace = Namespace::new(namespace_str).ok()?;
        return Some(Route::ListUploads { namespace });
    }

    None
}

fn try_parse_uploads<'a>(
    method: &Method,
    path: &'a str,
    params: Option<&'a str>,
) -> Option<Route<'a>> {
    let suffixes = ["/blobs/uploads", "/blobs/uploads/"];

    for suffix in suffixes {
        if let Some(namespace_str) = path.strip_suffix(suffix)
            && method == Method::POST
        {
            let namespace = Namespace::new(namespace_str).ok()?;
            let digest = params
                .map(parse_query::<DigestQuery>)
                .and_then(|r| r.to_digest());

            return Some(Route::StartUpload { namespace, digest });
        }
    }

    None
}

fn try_parse_upload<'a>(
    method: &Method,
    path: &'a str,
    params: Option<&'a str>,
) -> Option<Route<'a>> {
    if let Some(upload_position) = path.rfind("/blobs/uploads/") {
        let namespace_str = &path[..upload_position];
        let namespace = Namespace::new(namespace_str).ok()?;

        let uuid = &path[upload_position + "/blobs/uploads/".len()..];
        let uuid = Uuid::from_str(uuid).ok()?;

        match *method {
            Method::GET => return Some(Route::GetUpload { namespace, uuid }),
            Method::PATCH => return Some(Route::PatchUpload { namespace, uuid }),
            Method::PUT => {
                if let Some(digest) = params
                    .map(parse_query::<DigestQuery>)
                    .and_then(|r| r.to_digest())
                {
                    return Some(Route::PutUpload {
                        namespace,
                        uuid,
                        digest,
                    });
                }
            }
            Method::DELETE => return Some(Route::DeleteUpload { namespace, uuid }),
            _ => {}
        }
    }

    None
}

fn try_find_blobs<'a>(method: &Method, path: &'a str) -> Option<Route<'a>> {
    if let Some(blob_position) = path.rfind("/blobs/") {
        let namespace_str = &path[..blob_position];
        let namespace = Namespace::new(namespace_str).ok()?;

        let digest = &path[blob_position + "/blobs/".len()..];
        let digest = Digest::from_str(digest).ok()?;

        match *method {
            Method::GET => return Some(Route::GetBlob { namespace, digest }),
            Method::HEAD => return Some(Route::HeadBlob { namespace, digest }),
            Method::DELETE => return Some(Route::DeleteBlob { namespace, digest }),
            _ => {}
        }
    }

    None
}

fn try_find_manifests<'a>(method: &Method, path: &'a str) -> Option<Route<'a>> {
    if let Some(manifest_position) = path.rfind("/manifests/") {
        let namespace_str = &path[..manifest_position];
        let namespace = Namespace::new(namespace_str).ok()?;

        let reference = &path[manifest_position + "/manifests/".len()..];
        let reference = Reference::from_str(reference).ok()?;

        match *method {
            Method::GET => {
                return Some(Route::GetManifest {
                    namespace,
                    reference,
                });
            }
            Method::HEAD => {
                return Some(Route::HeadManifest {
                    namespace,
                    reference,
                });
            }
            Method::PUT => {
                return Some(Route::PutManifest {
                    namespace,
                    reference,
                });
            }
            Method::DELETE => {
                return Some(Route::DeleteManifest {
                    namespace,
                    reference,
                });
            }
            _ => {}
        }
    }

    None
}

fn try_find_referrers<'a>(
    method: &Method,
    path: &'a str,
    params: Option<&'a str>,
) -> Option<Route<'a>> {
    if let Some(referrers_position) = path.rfind("/referrers/") {
        let namespace_str = &path[..referrers_position];
        let namespace = Namespace::new(namespace_str).ok()?;

        let digest = &path[referrers_position + "/referrers/".len()..];
        let digest = Digest::from_str(digest).ok()?;

        let artifact_type = params
            .map(parse_query::<ArtifactTypeQuery>)
            .and_then(|f| f.artifact_type);

        if *method == Method::GET {
            return Some(Route::GetReferrer {
                namespace,
                digest,
                artifact_type,
            });
        }
    }

    None
}

fn try_find_tags<'a>(method: &Method, path: &'a str, params: Option<&'a str>) -> Option<Route<'a>> {
    if let Some(namespace_str) = path.strip_suffix("/tags/list")
        && *method == Method::GET
    {
        let namespace = Namespace::new(namespace_str).ok()?;
        let (n, last) = if let Some(p) = params.map(parse_query::<PaginationQuery>) {
            (p.n, p.last)
        } else {
            (None, None)
        };

        return Some(Route::ListTags { namespace, n, last });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_healthz() {
        let method = Method::GET;
        let uri: Uri = "/healthz".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Healthz));
    }

    #[test]
    fn test_parse_metrics() {
        let method = Method::GET;
        let uri: Uri = "/metrics".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Metrics));
    }

    #[test]
    fn test_parse_api_version() {
        let method = Method::GET;
        let uri: Uri = "/v2".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::ApiVersion));
    }

    #[test]
    fn test_parse_api_version_with_trailing_slash() {
        let method = Method::GET;
        let uri: Uri = "/v2/".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::ApiVersion));
    }

    #[test]
    fn test_parse_list_catalog_no_params() {
        let method = Method::GET;
        let uri: Uri = "/v2/_catalog".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(
            route,
            Route::ListCatalog {
                n: None,
                last: None
            }
        ));
    }

    #[test]
    fn test_parse_list_catalog_with_pagination() {
        let method = Method::GET;
        let uri: Uri = "/v2/_catalog?n=10&last=myrepo".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::ListCatalog { n, last } = route {
            assert_eq!(n, Some(10));
            assert_eq!(last, Some("myrepo".to_string()));
        } else {
            panic!("Expected ListCatalog route");
        }
    }

    #[test]
    fn test_parse_start_upload() {
        let method = Method::POST;
        let uri: Uri = "/v2/myrepo/app/blobs/uploads".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::StartUpload { namespace, digest } = route {
            assert_eq!(namespace, "myrepo/app");
            assert!(digest.is_none());
        } else {
            panic!("Expected StartUpload route");
        }
    }

    #[test]
    fn test_parse_start_upload_with_trailing_slash() {
        let method = Method::POST;
        let uri: Uri = "/v2/myrepo/app/blobs/uploads/".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::StartUpload { namespace, digest } = route {
            assert_eq!(namespace, "myrepo/app");
            assert!(digest.is_none());
        } else {
            panic!("Expected StartUpload route");
        }
    }

    #[test]
    fn test_parse_start_upload_with_digest() {
        let method = Method::POST;
        let uri: Uri = "/v2/myrepo/app/blobs/uploads?digest=sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::StartUpload { namespace, digest } = route {
            assert_eq!(namespace, "myrepo/app");
            assert!(digest.is_some());
            assert_eq!(
                digest.unwrap().to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected StartUpload route");
        }
    }

    #[test]
    fn test_parse_get_upload() {
        let method = Method::GET;
        let uuid = Uuid::new_v4();
        let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}")
            .parse()
            .unwrap();
        let route = parse(&method, &uri);
        if let Route::GetUpload {
            namespace,
            uuid: parsed_uuid,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(parsed_uuid, uuid);
        } else {
            panic!("Expected GetUpload route");
        }
    }

    #[test]
    fn test_parse_patch_upload() {
        let method = Method::PATCH;
        let uuid = Uuid::new_v4();
        let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}")
            .parse()
            .unwrap();
        let route = parse(&method, &uri);
        if let Route::PatchUpload {
            namespace,
            uuid: parsed_uuid,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(parsed_uuid, uuid);
        } else {
            panic!("Expected PatchUpload route");
        }
    }

    #[test]
    fn test_parse_put_upload() {
        let method = Method::PUT;
        let uuid = Uuid::new_v4();
        let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}?digest=sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::PutUpload {
            namespace,
            uuid: parsed_uuid,
            digest,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(parsed_uuid, uuid);
            assert_eq!(
                digest.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected PutUpload route");
        }
    }

    #[test]
    fn test_parse_put_upload_without_digest() {
        let method = Method::PUT;
        let uuid = Uuid::new_v4();
        let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}")
            .parse()
            .unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_delete_upload() {
        let method = Method::DELETE;
        let uuid = Uuid::new_v4();
        let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}")
            .parse()
            .unwrap();
        let route = parse(&method, &uri);
        if let Route::DeleteUpload {
            namespace,
            uuid: parsed_uuid,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(parsed_uuid, uuid);
        } else {
            panic!("Expected DeleteUpload route");
        }
    }

    #[test]
    fn test_parse_get_blob() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetBlob { namespace, digest } = route {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                digest.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected GetBlob route");
        }
    }

    #[test]
    fn test_parse_head_blob() {
        let method = Method::HEAD;
        let uri: Uri = "/v2/myrepo/app/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::HeadBlob { namespace, digest } = route {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                digest.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected HeadBlob route");
        }
    }

    #[test]
    fn test_parse_delete_blob() {
        let method = Method::DELETE;
        let uri: Uri = "/v2/myrepo/app/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::DeleteBlob { namespace, digest } = route {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                digest.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected DeleteBlob route");
        }
    }

    #[test]
    fn test_parse_get_manifest_by_tag() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/manifests/v1.0.0".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(reference.to_string(), "v1.0.0");
        } else {
            panic!("Expected GetManifest route");
        }
    }

    #[test]
    fn test_parse_get_manifest_by_digest() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/manifests/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                reference.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected GetManifest route");
        }
    }

    #[test]
    fn test_parse_head_manifest() {
        let method = Method::HEAD;
        let uri: Uri = "/v2/myrepo/app/manifests/v1.0.0".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::HeadManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(reference.to_string(), "v1.0.0");
        } else {
            panic!("Expected HeadManifest route");
        }
    }

    #[test]
    fn test_parse_put_manifest() {
        let method = Method::PUT;
        let uri: Uri = "/v2/myrepo/app/manifests/v1.0.0".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::PutManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(reference.to_string(), "v1.0.0");
        } else {
            panic!("Expected PutManifest route");
        }
    }

    #[test]
    fn test_parse_delete_manifest() {
        let method = Method::DELETE;
        let uri: Uri = "/v2/myrepo/app/manifests/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::DeleteManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                reference.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
        } else {
            panic!("Expected DeleteManifest route");
        }
    }

    #[test]
    fn test_parse_get_referrer() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/referrers/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetReferrer {
            namespace,
            digest,
            artifact_type,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                digest.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
            assert!(artifact_type.is_none());
        } else {
            panic!("Expected GetReferrer route");
        }
    }

    #[test]
    fn test_parse_get_referrer_with_artifact_type() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/referrers/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef?artifactType=application/vnd.oci.image.manifest.v1%2Bjson".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetReferrer {
            namespace,
            digest,
            artifact_type,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(
                digest.to_string(),
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            );
            assert_eq!(
                artifact_type,
                Some("application/vnd.oci.image.manifest.v1+json".to_string())
            );
        } else {
            panic!("Expected GetReferrer route");
        }
    }

    #[test]
    fn test_parse_list_tags() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/tags/list".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::ListTags { namespace, n, last } = route {
            assert_eq!(namespace, "myrepo/app");
            assert!(n.is_none());
            assert!(last.is_none());
        } else {
            panic!("Expected ListTags route");
        }
    }

    #[test]
    fn test_parse_list_tags_with_pagination() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/tags/list?n=50&last=v1.0.0".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::ListTags { namespace, n, last } = route {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(n, Some(50));
            assert_eq!(last, Some("v1.0.0".to_string()));
        } else {
            panic!("Expected ListTags route");
        }
    }

    #[test]
    fn test_parse_unknown_route_becomes_ui_asset() {
        let method = Method::GET;
        let uri: Uri = "/unknown/path".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::UiAsset { path } = route {
            assert_eq!(path, "/unknown/path");
        } else {
            panic!("Expected UiAsset route for unknown GET path");
        }
    }

    #[test]
    fn test_parse_unknown_post_route() {
        let method = Method::POST;
        let uri: Uri = "/unknown/path".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_unknown_method() {
        let method = Method::OPTIONS;
        let uri: Uri = "/v2/myrepo/app/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_invalid_digest_in_blob_path() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/blobs/invalid-digest".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_invalid_uuid_in_upload_path() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/blobs/uploads/invalid-uuid".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_digest_query_from_params() {
        let params =
            "digest=sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let query: DigestQuery = parse_query(params);
        assert!(query.digest.is_some());
        assert_eq!(
            query.digest.unwrap(),
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
    }

    #[test]
    fn test_digest_query_from_empty_params() {
        let params = "";
        let query: DigestQuery = parse_query(params);
        assert!(query.digest.is_none());
    }

    #[test]
    fn test_digest_query_to_digest_valid() {
        let query = DigestQuery {
            digest: Some(
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                    .to_string(),
            ),
        };
        let digest = query.to_digest();
        assert!(digest.is_some());
        assert_eq!(
            digest.unwrap().to_string(),
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
    }

    #[test]
    fn test_digest_query_to_digest_invalid() {
        let query = DigestQuery {
            digest: Some("invalid-digest".to_string()),
        };
        let digest = query.to_digest();
        assert!(digest.is_none());
    }

    #[test]
    fn test_artifact_type_query_from_params() {
        let params = "artifactType=application/vnd.oci.image.manifest.v1%2Bjson";
        let query: ArtifactTypeQuery = parse_query(params);
        assert!(query.artifact_type.is_some());
        assert_eq!(
            query.artifact_type.unwrap(),
            "application/vnd.oci.image.manifest.v1+json"
        );
    }

    #[test]
    fn test_artifact_type_query_from_empty_params() {
        let params = "";
        let query: ArtifactTypeQuery = parse_query(params);
        assert!(query.artifact_type.is_none());
    }

    #[test]
    fn test_pagination_query_from_params() {
        let params = "n=100&last=previous-item";
        let query: PaginationQuery = parse_query(params);
        assert_eq!(query.n, Some(100));
        assert_eq!(query.last, Some("previous-item".to_string()));
    }

    #[test]
    fn test_pagination_query_from_empty_params() {
        let params = "";
        let query: PaginationQuery = parse_query(params);
        assert!(query.n.is_none());
        assert!(query.last.is_none());
    }

    #[test]
    fn test_pagination_query_partial_params() {
        let params = "n=25";
        let query: PaginationQuery = parse_query(params);
        assert_eq!(query.n, Some(25));
        assert!(query.last.is_none());
    }

    #[test]
    fn test_try_parse_uploads_post_method() {
        let method = Method::POST;
        let path = "myrepo/app/blobs/uploads";
        let route = try_parse_uploads(&method, path, None);
        assert!(route.is_some());
        if let Some(Route::StartUpload { namespace, digest }) = route {
            assert_eq!(namespace, "myrepo/app");
            assert!(digest.is_none());
        } else {
            panic!("Expected StartUpload route");
        }
    }

    #[test]
    fn test_try_parse_uploads_wrong_method() {
        let method = Method::GET;
        let path = "myrepo/app/blobs/uploads";
        let route = try_parse_uploads(&method, path, None);
        assert!(route.is_none());
    }

    #[test]
    fn test_try_parse_upload_invalid_uuid() {
        let method = Method::GET;
        let path = "myrepo/app/blobs/uploads/not-a-uuid";
        let route = try_parse_upload(&method, path, None);
        assert!(route.is_none());
    }

    #[test]
    fn test_try_find_blobs_invalid_digest() {
        let method = Method::GET;
        let path = "myrepo/app/blobs/not-a-digest";
        let route = try_find_blobs(&method, path);
        assert!(route.is_none());
    }

    #[test]
    fn test_try_find_manifests_valid_tag() {
        let method = Method::GET;
        let path = "myrepo/app/manifests/latest";
        let route = try_find_manifests(&method, path);
        assert!(route.is_some());
        if let Some(Route::GetManifest {
            namespace,
            reference,
        }) = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(reference.to_string(), "latest");
        } else {
            panic!("Expected GetManifest route");
        }
    }

    #[test]
    fn test_try_find_referrers_invalid_digest() {
        let method = Method::GET;
        let path = "myrepo/app/referrers/not-a-digest";
        let route = try_find_referrers(&method, path, None);
        assert!(route.is_none());
    }

    #[test]
    fn test_try_find_tags_wrong_method() {
        let method = Method::POST;
        let path = "myrepo/app/tags/list";
        let route = try_find_tags(&method, path, None);
        assert!(route.is_none());
    }

    #[test]
    fn test_parse_nested_namespace() {
        let method = Method::GET;
        let uri: Uri = "/v2/org/team/project/app/manifests/v1.0.0".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "org/team/project/app");
            assert_eq!(reference.to_string(), "v1.0.0");
        } else {
            panic!("Expected GetManifest route");
        }
    }

    #[test]
    fn test_parse_invalid_sha512_digest() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/blobs/sha512:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_tag_name_with_hyphen_and_dot() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/manifests/v1.0.0-alpha.1".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::GetManifest {
            namespace,
            reference,
        } = route
        {
            assert_eq!(namespace, "myrepo/app");
            assert_eq!(reference.to_string(), "v1.0.0-alpha.1");
        } else {
            panic!("Expected GetManifest route");
        }
    }

    #[test]
    fn test_parse_invalid_tag_with_plus_sign() {
        let method = Method::GET;
        let uri: Uri = "/v2/myrepo/app/manifests/v1.0.0+build.123".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_ui_asset_root() {
        let method = Method::GET;
        let uri: Uri = "/".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::UiAsset { path } = route {
            assert_eq!(path, "/");
        } else {
            panic!("Expected UiAsset route");
        }
    }

    #[test]
    fn test_parse_ui_asset_with_path() {
        let method = Method::GET;
        let uri: Uri = "/index.html".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::UiAsset { path } = route {
            assert_eq!(path, "/index.html");
        } else {
            panic!("Expected UiAsset route");
        }
    }

    #[test]
    fn test_parse_ui_asset_head_method() {
        let method = Method::HEAD;
        let uri: Uri = "/style.css".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::UiAsset { path } = route {
            assert_eq!(path, "/style.css");
        } else {
            panic!("Expected UiAsset route");
        }
    }

    #[test]
    fn test_parse_ui_asset_post_not_allowed() {
        let method = Method::POST;
        let uri: Uri = "/index.html".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }

    #[test]
    fn test_parse_list_revisions() {
        let method = Method::GET;
        let uri: Uri = "/v2/_ext/myrepo/app/_revisions".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::ListRevisions { namespace } = route {
            assert_eq!(namespace, "myrepo/app");
        } else {
            panic!("Expected ListRevisions route");
        }
    }

    #[test]
    fn test_parse_list_revisions_simple_namespace() {
        let method = Method::GET;
        let uri: Uri = "/v2/_ext/library/_revisions".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::ListRevisions { namespace } = route {
            assert_eq!(namespace, "library");
        } else {
            panic!("Expected ListRevisions route");
        }
    }

    #[test]
    fn test_parse_list_revisions_nested_namespace() {
        let method = Method::GET;
        let uri: Uri = "/v2/_ext/org/team/project/_revisions".parse().unwrap();
        let route = parse(&method, &uri);
        if let Route::ListRevisions { namespace } = route {
            assert_eq!(namespace, "org/team/project");
        } else {
            panic!("Expected ListRevisions route");
        }
    }

    #[test]
    fn test_parse_list_revisions_post_not_allowed() {
        let method = Method::POST;
        let uri: Uri = "/v2/_ext/myrepo/_revisions".parse().unwrap();
        let route = parse(&method, &uri);
        assert!(matches!(route, Route::Unknown));
    }
}
