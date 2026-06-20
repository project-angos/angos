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
fn test_parse_list_catalog_full_pagination() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog?n=100&last=foo".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListCatalog { n, last }) = route {
        assert_eq!(n, Some(100));
        assert_eq!(last, Some("foo".to_string()));
    } else {
        panic!("Expected ListCatalog route");
    }
}

#[test]
fn test_parse_list_catalog_non_numeric_n() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog?n=abc".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListCatalog { n, last }) = route {
        assert_eq!(n, None);
        assert_eq!(last, None);
    } else {
        panic!("Expected ListCatalog route");
    }
}

#[test]
fn test_parse_list_catalog_overflowing_n() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog?n=65536".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListCatalog { n, last }) = route {
        assert_eq!(n, None);
        assert_eq!(last, None);
    } else {
        panic!("Expected ListCatalog route");
    }
}

#[test]
fn test_parse_list_catalog_empty_n() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog?n=".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListCatalog { n, last }) = route {
        assert_eq!(n, None);
        assert_eq!(last, None);
    } else {
        panic!("Expected ListCatalog route");
    }
}

#[test]
fn test_parse_list_catalog_url_encoded_last() {
    let method = Method::GET;
    let uri: Uri = "/v2/_catalog?last=foo%2Fbar".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::ListCatalog { n, last }) = route {
        assert_eq!(n, None);
        assert_eq!(last, Some("foo/bar".to_string()));
    } else {
        panic!("Expected ListCatalog route");
    }
}

#[test]
fn test_parse_start_upload() {
    let method = Method::POST;
    let uri: Uri = "/v2/myrepo/app/blobs/uploads".parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::StartUpload {
        namespace, digest, ..
    }) = route
    {
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
    if let Some(Action::StartUpload {
        namespace, digest, ..
    }) = route
    {
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
    if let Some(Action::StartUpload {
        namespace, digest, ..
    }) = route
    {
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
fn test_parse_mount_blob_with_from() {
    let method = Method::POST;
    let mount_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri =
        format!("/v2/myrepo/target/blobs/uploads/?mount={mount_digest}&from=myrepo/source")
            .parse()
            .unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::MountBlob {
        namespace,
        digest,
        from,
    }) = route
    {
        assert_eq!(namespace, "myrepo/target");
        assert_eq!(digest.to_string(), mount_digest);
        assert_eq!(from.unwrap(), "myrepo/source");
    } else {
        panic!("Expected MountBlob route");
    }
}

#[test]
fn test_parse_mount_blob_without_from() {
    // An unset `from` makes the server attempt automatic content discovery.
    let method = Method::POST;
    let mount_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri = format!("/v2/myrepo/target/blobs/uploads/?mount={mount_digest}")
        .parse()
        .unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::MountBlob { digest, from, .. }) = route {
        assert_eq!(digest.to_string(), mount_digest);
        assert!(from.is_none());
    } else {
        panic!("Expected MountBlob route");
    }
}

#[test]
fn test_parse_mount_blob_with_malformed_from_is_rejected() {
    let mount_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri = format!("/v2/myrepo/target/blobs/uploads/?mount={mount_digest}&from=Invalid")
        .parse()
        .unwrap();
    let route = parse(&Method::POST, &uri);
    assert!(
        route.is_none(),
        "a malformed ?from= must not route (POST -> 400), got: {route:?}"
    );
}

#[test]
fn test_parse_start_upload_with_malformed_digest_is_rejected() {
    let uri: Uri = "/v2/myrepo/app/blobs/uploads?digest=not-a-digest"
        .parse()
        .unwrap();
    let route = parse(&Method::POST, &uri);
    assert!(
        route.is_none(),
        "a malformed ?digest= must not start a session (POST -> 400), got: {route:?}"
    );

    let uri: Uri = "/v2/myrepo/app/blobs/uploads/?digest=garbage"
        .parse()
        .unwrap();
    let route = parse(&Method::POST, &uri);
    assert!(
        route.is_none(),
        "a malformed ?digest= must not start a session (POST -> 400), got: {route:?}"
    );
}

#[test]
fn test_parse_start_upload_no_digest_is_session() {
    let uri: Uri = "/v2/myrepo/app/blobs/uploads".parse().unwrap();
    let route = parse(&Method::POST, &uri);
    if let Some(Action::StartUpload {
        namespace, digest, ..
    }) = route
    {
        assert_eq!(namespace, "myrepo/app");
        assert!(digest.is_none());
    } else {
        panic!("Expected StartUpload route, got: {route:?}");
    }
}

#[test]
fn test_parse_malformed_mount_is_rejected() {
    // The OCI fall-back-to-session rule covers unsatisfiable mounts, not
    // syntactically invalid ones.
    let route = parse(
        &Method::POST,
        &"/v2/myrepo/target/blobs/uploads/?mount=not-a-digest"
            .parse()
            .unwrap(),
    );
    assert!(
        route.is_none(),
        "a malformed ?mount= must reject the route (POST -> 400), got: {route:?}"
    );
}

#[test]
fn test_parse_mount_with_malformed_digest_is_rejected() {
    // Real clients never combine `?mount=` with a monolithic `?digest=`;
    // rejecting the combination is by design.
    let mount_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri = format!("/v2/myrepo/target/blobs/uploads/?mount={mount_digest}&digest=garbage")
        .parse()
        .unwrap();
    let route = parse(&Method::POST, &uri);
    assert!(
        route.is_none(),
        "a malformed ?digest= must poison the mount path too (POST -> 400), got: {route:?}"
    );
}

#[test]
fn test_parse_get_upload() {
    let method = Method::GET;
    let uuid = UploadSessionId::generate();
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
    let uuid = UploadSessionId::generate();
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
    let uuid = UploadSessionId::generate();
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
    let uuid = UploadSessionId::generate();
    let uri: Uri = format!("/v2/myrepo/app/blobs/uploads/{uuid}")
        .parse()
        .unwrap();
    let route = parse(&method, &uri);
    assert!(route.is_none());
}

#[test]
fn test_parse_delete_upload() {
    let method = Method::DELETE;
    let uuid = UploadSessionId::generate();
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
    if let Some(Action::PutManifest { namespace, target }) = route {
        assert_eq!(namespace, "myrepo/app");
        assert!(
            matches!(target, ManifestPutTarget::Tag(tag) if tag == *"v1.0.0"),
            "a by-tag PUT must produce a Tag target carrying no query tags"
        );
    } else {
        panic!("Expected PutManifest route");
    }
}

#[test]
fn test_parse_put_manifest_by_digest_with_tag_params() {
    let method = Method::PUT;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri = format!("/v2/foo/manifests/{digest}?tag=a&tag=b")
        .parse()
        .unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::PutManifest { namespace, target }) = route {
        assert_eq!(namespace, "foo");
        let ManifestPutTarget::Digest { digest: d, tags } = target else {
            panic!("a by-digest PUT must produce a Digest target");
        };
        assert_eq!(d.to_string(), digest);
        assert_eq!(
            tags,
            vec![Tag::new("a").unwrap(), Tag::new("b").unwrap()],
            "valid `?tag=` values parse into Tags on the Digest target"
        );
    } else {
        panic!("Expected PutManifest route");
    }
}

#[test]
fn test_parse_put_manifest_by_digest_invalid_tag_param_rejected() {
    let method = Method::PUT;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri = format!("/v2/foo/manifests/{digest}?tag=a&tag=bad!tag")
        .parse()
        .unwrap();
    // A single invalid `?tag=` value fails deserialization, so the route is
    // rejected (the router's generic 400) rather than dropping every tag.
    assert!(
        parse(&method, &uri).is_none(),
        "an invalid `?tag=` value must reject the by-digest PUT route"
    );
}

#[test]
fn test_parse_get_manifest_by_tag_ignores_tag_params() {
    let method = Method::GET;
    let uri: Uri = "/v2/foo/manifests/latest?tag=a".parse().unwrap();
    let route = parse(&method, &uri);
    assert!(
        matches!(route, Some(Action::GetManifest { .. })),
        "GET by tag must not carry tag params"
    );
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
fn test_try_parse_upload_start_post_method() {
    let method = Method::POST;
    let path = "myrepo/app/blobs/uploads";
    let route = try_parse_upload(&method, path, None);
    assert!(route.is_some());
    if let Some(Action::StartUpload {
        namespace, digest, ..
    }) = route
    {
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
        matches!(route, Some(Action::StartUpload { ref namespace, digest: None, .. }) if namespace == "foo"),
        "no-slash variant must yield StartUpload with correct namespace"
    );

    // With-slash variant: /v2/foo/blobs/uploads/ → same result.
    let route = try_parse_upload(&method, "foo/blobs/uploads/", None);
    assert!(
        matches!(route, Some(Action::StartUpload { ref namespace, digest: None, .. }) if namespace == "foo"),
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
    let route = try_find_manifests(&method, path, None);
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
fn test_parse_get_blob_sha512() {
    let method = Method::GET;
    let digest = "sha512:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let uri: Uri = format!("/v2/myrepo/app/blobs/{digest}").parse().unwrap();
    let route = parse(&method, &uri);
    if let Some(Action::GetBlob {
        namespace,
        digest: parsed,
    }) = route
    {
        assert_eq!(namespace, "myrepo/app");
        assert_eq!(parsed.to_string(), digest);
    } else {
        panic!("Expected GetBlob route for a sha512 digest");
    }
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

// Durable job-queue administration routes

#[test]
fn test_parse_list_jobs() {
    let route = parse(&Method::GET, &"/_ext/_jobs".parse().unwrap());
    match route {
        Some(Action::ListJobs { queue, n, after }) => {
            assert_eq!(queue, Queue::Cache);
            assert_eq!(n, None);
            assert_eq!(after, None);
        }
        other => panic!("expected ListJobs, got {other:?}"),
    }
}

#[test]
fn test_parse_list_jobs_with_pagination() {
    let route = parse(&Method::GET, &"/_ext/_jobs?n=10&after=abc".parse().unwrap());
    match route {
        Some(Action::ListJobs { queue, n, after }) => {
            assert_eq!(queue, Queue::Cache);
            assert_eq!(n, Some(10));
            assert_eq!(after.as_deref(), Some("abc"));
        }
        other => panic!("expected ListJobs, got {other:?}"),
    }
}

#[test]
fn test_parse_list_failed_jobs() {
    let route = parse(&Method::GET, &"/_ext/_jobs/failed".parse().unwrap());
    match route {
        Some(Action::ListFailedJobs { queue, n, after }) => {
            assert_eq!(queue, Queue::Cache);
            assert_eq!(n, None);
            assert_eq!(after, None);
        }
        other => panic!("expected ListFailedJobs, got {other:?}"),
    }
}

#[test]
fn test_parse_retry_job() {
    let route = parse(
        &Method::POST,
        &"/_ext/_jobs/failed/0000018b-abc/retry".parse().unwrap(),
    );
    match route {
        Some(Action::RetryJob { queue, storage_key }) => {
            assert_eq!(queue, Queue::Cache);
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
        Some(Action::DeleteJob {
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
fn test_parse_delete_pending_job() {
    let route = parse(
        &Method::DELETE,
        &"/_ext/_jobs/pending/0000018b-abc".parse().unwrap(),
    );
    match route {
        Some(Action::DeleteJob {
            queue,
            state,
            storage_key,
        }) => {
            assert_eq!(queue, Queue::Cache);
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

#[test]
fn test_parse_list_jobs_selects_replication_queue() {
    let route = parse(
        &Method::GET,
        &"/_ext/_jobs?queue=replication".parse().unwrap(),
    );
    match route {
        Some(Action::ListJobs { queue, .. }) => assert_eq!(queue, Queue::Replication),
        other => panic!("expected ListJobs, got {other:?}"),
    }
}

#[test]
fn test_parse_delete_failed_job_selects_replication_queue() {
    let route = parse(
        &Method::DELETE,
        &"/_ext/_jobs/failed/0000018b-abc?queue=replication"
            .parse()
            .unwrap(),
    );
    match route {
        Some(Action::DeleteJob { queue, state, .. }) => {
            assert_eq!(queue, Queue::Replication);
            assert_eq!(state, JobState::Failed);
        }
        other => panic!("expected DeleteJob, got {other:?}"),
    }
}

#[test]
fn test_parse_jobs_rejects_unknown_queue() {
    assert!(parse(&Method::GET, &"/_ext/_jobs?queue=bogus".parse().unwrap()).is_none());
    assert!(
        parse(
            &Method::DELETE,
            &"/_ext/_jobs/failed/0000018b-abc?queue=bogus"
                .parse()
                .unwrap(),
        )
        .is_none()
    );
}

#[test]
fn test_parse_jobs_rejects_malformed_query_instead_of_defaulting_queue() {
    // A lenient parse would reset the whole query and administer the default
    // cache queue instead of the requested one.
    assert!(
        parse(
            &Method::GET,
            &"/_ext/_jobs?queue=replication&n=abc".parse().unwrap(),
        )
        .is_none()
    );
    assert!(
        parse(
            &Method::GET,
            &"/_ext/_jobs?queue=replication&n=99999999".parse().unwrap(),
        )
        .is_none()
    );
    assert!(
        parse(
            &Method::DELETE,
            &"/_ext/_jobs/failed/0000018b-abc?queue=replication&n=abc"
                .parse()
                .unwrap(),
        )
        .is_none()
    );
}
