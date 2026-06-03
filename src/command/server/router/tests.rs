use super::*;

#[test]
fn test_parse_healthz() {
    let method = Method::GET;
    let uri: Uri = "/healthz".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(matches!(route, Some(Action::Healthz)));
}

#[test]
fn test_parse_metrics() {
    let method = Method::GET;
    let uri: Uri = "/metrics".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(matches!(route, Some(Action::Metrics)));
}

#[test]
fn test_parse_api_version() {
    let method = Method::GET;
    let uri: Uri = "/v2".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(matches!(route, Some(Action::ApiVersion)));
}

#[test]
fn test_parse_api_version_with_trailing_slash() {
    let method = Method::GET;
    let uri: Uri = "/v2/".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(matches!(route, Some(Action::ApiVersion)));
}

#[test]
fn test_parse_list_catalog_no_params() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(matches!(
        route,
        Some(Action::ListCatalog {
            n: None,
            last: None
        })
    ));
}

#[test]
fn test_parse_list_catalog_with_pagination() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog?n=10&last=myrepo".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListCatalog { n, last }) = route {
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
    if let Some(Action::StartUpload { namespace, digest }) = route {
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
    if let Some(Action::StartUpload { namespace, digest }) = route {
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
    if let Some(Action::StartUpload { namespace, digest }) = route {
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
    if let Some(Action::GetUpload {
        namespace,
        uuid: parsed_uuid,
    }) = route
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
    if let Some(Action::PatchUpload {
        namespace,
        uuid: parsed_uuid,
    }) = route
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
    if let Some(Action::PutUpload {
        namespace,
        uuid: parsed_uuid,
        digest,
    }) = route
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
    assert!(route.is_none());
}

#[test]
fn test_parse_delete_upload() {
    let method = Method::DELETE;
    let uuid = Uuid::new_v4();
    let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}")
        .parse()
        .unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::DeleteUpload {
        namespace,
        uuid: parsed_uuid,
    }) = route
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
    if let Some(Action::GetBlob { namespace, digest }) = route {
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
    if let Some(Action::HeadBlob { namespace, digest }) = route {
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
    if let Some(Action::DeleteBlob { namespace, digest }) = route {
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
    if let Some(Action::GetManifest {
        namespace,
        reference,
    }) = route
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
    if let Some(Action::GetManifest {
        namespace,
        reference,
    }) = route
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
    if let Some(Action::HeadManifest {
        namespace,
        reference,
    }) = route
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
    if let Some(Action::PutManifest {
        namespace,
        reference,
    }) = route
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
    if let Some(Action::DeleteManifest {
        namespace,
        reference,
    }) = route
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
    if let Some(Action::GetReferrer {
        namespace,
        digest,
        artifact_type,
    }) = route
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
    if let Some(Action::GetReferrer {
        namespace,
        digest,
        artifact_type,
    }) = route
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
    if let Some(Action::ListTags { namespace, n, last }) = route {
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
    if let Some(Action::ListTags { namespace, n, last }) = route {
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
    if let Some(Action::UiAsset { path }) = route {
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
    assert!(route.is_none());
}

#[test]
fn test_parse_unknown_method() {
    let method = Method::OPTIONS;
    let uri: Uri = "/v2/myrepo/app/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

#[test]
fn test_parse_invalid_digest_in_blob_path() {
    let method = Method::GET;
    let uri: Uri = "/v2/myrepo/app/blobs/invalid-digest".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

#[test]
fn test_parse_invalid_uuid_in_upload_path() {
    let method = Method::GET;
    let uri: Uri = "/v2/myrepo/app/blobs/uploads/invalid-uuid".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

#[test]
fn test_digest_query_from_params() {
    let params = "digest=sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
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
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
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
fn test_parse_pagination_none_params() {
    assert_eq!(parse_pagination(None), (None, None));
}

#[test]
fn test_parse_pagination_empty_params() {
    assert_eq!(parse_pagination(Some("")), (None, None));
}

#[test]
fn test_parse_pagination_full_params() {
    assert_eq!(
        parse_pagination(Some("n=50&last=foo")),
        (Some(50), Some("foo".to_string()))
    );
}

#[test]
fn test_parse_pagination_partial_params() {
    assert_eq!(parse_pagination(Some("n=10")), (Some(10), None));
    assert_eq!(
        parse_pagination(Some("last=bar")),
        (None, Some("bar".to_string()))
    );
}

#[test]
fn test_parse_pagination_non_numeric_n() {
    // Non-numeric value for `n` fails deserialization; unwrap_or_default yields (None, None).
    assert_eq!(parse_pagination(Some("n=abc")), (None, None));
}

#[test]
fn test_parse_pagination_n_zero() {
    // Zero is a valid u16; it should be preserved.
    assert_eq!(parse_pagination(Some("n=0")), (Some(0), None));
}

#[test]
fn test_parse_pagination_n_exceeds_u16_max() {
    // 65536 overflows u16; deserialization fails and unwrap_or_default yields (None, None).
    assert_eq!(parse_pagination(Some("n=65536")), (None, None));
}

#[test]
fn test_parse_pagination_n_equals_only() {
    // `n=` with no value is an empty string, which fails u16 deserialization.
    assert_eq!(parse_pagination(Some("n=")), (None, None));
}

#[test]
fn test_parse_pagination_last_url_encoded_special_chars() {
    // `last` may contain URL-encoded characters; serde_urlencoded decodes them.
    let (n, last) = parse_pagination(Some("last=foo%2Fbar%3Abaz"));
    assert!(n.is_none());
    assert_eq!(last, Some("foo/bar:baz".to_string()));
}

#[test]
fn test_try_parse_upload_start_post_method() {
    let method = Method::POST;
    let path = "myrepo/app/blobs/uploads";
    let route = try_parse_upload(&method, path, None);
    assert!(route.is_some());
    if let Some(Action::StartUpload { namespace, digest }) = route {
        assert_eq!(namespace, "myrepo/app");
        assert!(digest.is_none());
    } else {
        panic!("Expected StartUpload route");
    }
}

#[test]
fn test_try_parse_upload_start_wrong_method() {
    let method = Method::GET;
    let path = "myrepo/app/blobs/uploads";
    let route = try_parse_upload(&method, path, None);
    assert!(route.is_none());
}

#[test]
fn test_try_parse_upload_start_no_slash() {
    let method = Method::POST;

    // No-slash variant: /v2/foo/blobs/uploads → StartUpload for namespace "foo".
    let route = try_parse_upload(&method, "foo/blobs/uploads", None);
    assert!(
        matches!(route, Some(Action::StartUpload { ref namespace, digest: None }) if namespace == "foo"),
        "no-slash variant must yield StartUpload with correct namespace"
    );

    // With-slash variant: /v2/foo/blobs/uploads/ → same result.
    let route = try_parse_upload(&method, "foo/blobs/uploads/", None);
    assert!(
        matches!(route, Some(Action::StartUpload { ref namespace, digest: None }) if namespace == "foo"),
        "with-slash variant must yield StartUpload with correct namespace"
    );

    // Nested namespace without slash.
    let route = try_parse_upload(&method, "org/team/blobs/uploads", None);
    assert!(
        matches!(route, Some(Action::StartUpload { ref namespace, .. }) if namespace == "org/team"),
        "nested namespace without slash must parse correctly"
    );

    // Invalid namespace: empty string before the suffix.
    let route = try_parse_upload(&method, "blobs/uploads", None);
    assert!(route.is_none(), "empty namespace must not yield a route");

    // Invalid namespace: contains uppercase letter.
    let route = try_parse_upload(&method, "MyRepo/blobs/uploads", None);
    assert!(
        route.is_none(),
        "uppercase namespace must not yield a route"
    );

    // Invalid namespace: contains a space (invalid character).
    let route = try_parse_upload(&method, "bad ns/blobs/uploads", None);
    assert!(
        route.is_none(),
        "namespace with space must not yield a route"
    );
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
    if let Some(Action::GetManifest {
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
    if let Some(Action::GetManifest {
        namespace,
        reference,
    }) = route
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
    assert!(route.is_none());
}

#[test]
fn test_parse_tag_name_with_hyphen_and_dot() {
    let method = Method::GET;
    let uri: Uri = "/v2/myrepo/app/manifests/v1.0.0-alpha.1".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::GetManifest {
        namespace,
        reference,
    }) = route
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
    assert!(route.is_none());
}

#[test]
fn test_parse_ui_asset_root() {
    let method = Method::GET;
    let uri: Uri = "/".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::UiAsset { path }) = route {
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
    if let Some(Action::UiAsset { path }) = route {
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
    if let Some(Action::UiAsset { path }) = route {
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
    assert!(route.is_none());
}

#[test]
fn test_parse_list_revisions() {
    let method = Method::GET;
    let uri: Uri = "/_ext/myrepo/app/_revisions".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListRevisions { namespace }) = route {
        assert_eq!(namespace, "myrepo/app");
    } else {
        panic!("Expected ListRevisions route");
    }
}

#[test]
fn test_parse_list_revisions_simple_namespace() {
    let method = Method::GET;
    let uri: Uri = "/_ext/library/_revisions".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListRevisions { namespace }) = route {
        assert_eq!(namespace, "library");
    } else {
        panic!("Expected ListRevisions route");
    }
}

#[test]
fn test_parse_list_revisions_nested_namespace() {
    let method = Method::GET;
    let uri: Uri = "/_ext/org/team/project/_revisions".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListRevisions { namespace }) = route {
        assert_eq!(namespace, "org/team/project");
    } else {
        panic!("Expected ListRevisions route");
    }
}

#[test]
fn test_parse_list_revisions_post_not_allowed() {
    let method = Method::POST;
    let uri: Uri = "/_ext/myrepo/_revisions".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

#[test]
fn test_parse_list_namespaces() {
    let method = Method::GET;
    let uri: Uri = "/_ext/myrepo/_namespaces".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListNamespaces { repository }) = route {
        assert_eq!(repository, "myrepo");
    } else {
        panic!("Expected ListNamespaces route");
    }
}

#[test]
fn test_parse_list_namespaces_nested() {
    let method = Method::GET;
    let uri: Uri = "/_ext/org/team/_namespaces".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListNamespaces { repository }) = route {
        assert_eq!(repository, "org/team");
    } else {
        panic!("Expected ListNamespaces route");
    }
}

#[test]
fn test_parse_list_namespaces_invalid_repository_returns_none() {
    let method = Method::GET;
    let uri: Uri = "/_ext/INVALID/_namespaces".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

#[test]
fn test_parse_list_namespaces_post_not_allowed() {
    let method = Method::POST;
    let uri: Uri = "/_ext/myrepo/_namespaces".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

// ── Durable job-queue administration routes ─────────────────────────────────

#[test]
fn test_parse_list_jobs() {
    let route = parse(&Method::GET, &"/_ext/_jobs".parse().unwrap());
    assert!(matches!(
        route,
        Some(Action::ListJobs {
            n: None,
            after: None
        })
    ));
}

#[test]
fn test_parse_list_jobs_with_pagination() {
    let route = parse(&Method::GET, &"/_ext/_jobs?n=10&after=abc".parse().unwrap());
    match route {
        Some(Action::ListJobs { n, after }) => {
            assert_eq!(n, Some(10));
            assert_eq!(after.as_deref(), Some("abc"));
        }
        other => panic!("expected ListJobs, got {other:?}"),
    }
}

#[test]
fn test_parse_list_failed_jobs() {
    let route = parse(&Method::GET, &"/_ext/_jobs/failed".parse().unwrap());
    assert!(matches!(
        route,
        Some(Action::ListFailedJobs {
            n: None,
            after: None
        })
    ));
}

#[test]
fn test_parse_retry_job() {
    let route = parse(
        &Method::POST,
        &"/_ext/_jobs/failed/0000018b-abc/retry".parse().unwrap(),
    );
    match route {
        Some(Action::RetryJob { storage_key }) => {
            assert_eq!(storage_key, "0000018b-abc");
        }
        other => panic!("expected RetryJob, got {other:?}"),
    }
}

#[test]
fn test_parse_delete_failed_job() {
    let route = parse(
        &Method::DELETE,
        &"/_ext/_jobs/failed/0000018b-abc".parse().unwrap(),
    );
    match route {
        Some(Action::DeleteJob { state, storage_key }) => {
            assert_eq!(state, JobState::Failed);
            assert_eq!(storage_key, "0000018b-abc");
        }
        other => panic!("expected DeleteJob(Failed), got {other:?}"),
    }
}

#[test]
fn test_parse_delete_pending_job() {
    let route = parse(
        &Method::DELETE,
        &"/_ext/_jobs/pending/0000018b-abc".parse().unwrap(),
    );
    match route {
        Some(Action::DeleteJob { state, storage_key }) => {
            assert_eq!(state, JobState::Pending);
            assert_eq!(storage_key, "0000018b-abc");
        }
        other => panic!("expected DeleteJob(Pending), got {other:?}"),
    }
}

#[test]
fn test_parse_jobs_rejects_post_on_listing() {
    let route = parse(&Method::POST, &"/_ext/_jobs".parse().unwrap());
    assert!(route.is_none());
}

#[test]
fn test_parse_jobs_rejects_key_with_slash() {
    // A storage key is a single path segment; a nested path must not match.
    let route = parse(&Method::DELETE, &"/_ext/_jobs/failed/a/b".parse().unwrap());
    assert!(route.is_none());
}

#[test]
fn test_parse_retry_requires_retry_suffix() {
    let route = parse(
        &Method::POST,
        &"/_ext/_jobs/failed/0000018b-abc".parse().unwrap(),
    );
    assert!(route.is_none());
}
