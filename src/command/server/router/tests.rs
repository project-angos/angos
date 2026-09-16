use super::*;

// These tests cover the router's own responsibilities: recognising the binary
// paths it serves, delegating each `/v2` surface to the crate that parses it,
// and the miss-versus-UI-asset fallback. The per-endpoint parsing (field
// extraction and malformed-input rejection) belongs to and is tested in the
// service crates (`angos-oci-service`, `angos-docker-extension-service`,
// `angos-extension-service`); it is not re-tested here.

#[test]
fn test_parse_healthz() {
    let uri: Uri = "/healthz".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::Healthz)));
}

#[test]
fn test_parse_metrics() {
    let uri: Uri = "/metrics".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::Metrics)));
}

/// Without their own arm the probes reach the UI-asset arm and answer
/// `index.html` with a 200, so a `HEAD` probe reads a replica as healthy while
/// `/readyz` answers 503 on `GET`.
#[test]
fn test_parse_probes_reject_other_methods() {
    for path in ["/healthz", "/readyz", "/metrics"] {
        let uri: Uri = path.parse().unwrap();
        assert!(parse(&Method::HEAD, &uri).is_none(), "HEAD {path}");
        assert!(parse(&Method::POST, &uri).is_none(), "POST {path}");
    }
}

/// Without its own arm the token endpoint reaches the UI-asset arm and answers
/// `index.html` with a 200.
#[test]
fn test_parse_token() {
    let uri: Uri = "/token".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::Token)));
    assert!(parse(&Method::POST, &uri).is_none());
    assert!(parse(&Method::HEAD, &uri).is_none());
}

/// The router matches the UI configuration path itself, before the `_angos`
/// parser (which deliberately declines it).
#[test]
fn test_parse_ui_config() {
    let uri: Uri = "/v2/_angos/ui/config".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::UiConfig)));
    assert!(parse(&Method::POST, &uri).is_none());
    assert!(
        parse(&Method::GET, &"/_ui/config".parse().unwrap())
            .is_none_or(|route| !matches!(route, Route::UiConfig))
    );
}

/// A path the OCI surface claims is delegated to it, for `GET` and `HEAD`
/// alike (or `HEAD /v2/` would fall through to the UI-asset arm).
#[test]
fn oci_paths_delegate_to_the_oci_surface() {
    let uri: Uri = "/v2/".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::Oci(_))));
    assert!(matches!(parse(&Method::HEAD, &uri), Some(Route::Oci(_))));
}

/// The catalog path is delegated to the Docker surface; the OCI surface
/// declines it and the next surface in the chain claims it.
#[test]
fn catalog_delegates_to_the_docker_surface() {
    let uri: Uri = "/v2/_catalog".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::Docker(_))));
}

/// An `_angos` path is delegated to the Angos surface, which the OCI and
/// Docker surfaces decline.
#[test]
fn angos_paths_delegate_to_the_angos_surface() {
    let uri: Uri = "/v2/lib/nginx/_angos/revisions/list".parse().unwrap();
    assert!(matches!(parse(&Method::GET, &uri), Some(Route::Angos(_))));
}

/// end-1 spells the trailing slash; `/v2` without it is claimed by no surface
/// and must be a miss, not the UI-asset arm answering `index.html`.
#[test]
fn v2_without_trailing_slash_is_a_miss() {
    for method in [Method::GET, Method::HEAD] {
        let uri: Uri = "/v2".parse().unwrap();
        assert!(
            parse(&method, &uri).is_none(),
            "{method} /v2 must not route"
        );
    }
}

/// A `/v2/...` path no surface claims is a miss, never a UI asset.
#[test]
fn unclaimed_v2_path_is_a_miss() {
    let uri: Uri = "/v2/nginx".parse().unwrap();
    assert!(parse(&Method::GET, &uri).is_none());
}

#[test]
fn test_parse_unknown_route_becomes_ui_asset() {
    let uri: Uri = "/unknown/path".parse().unwrap();
    if let Some(Route::UiAsset { path }) = parse(&Method::GET, &uri) {
        assert_eq!(path, "/unknown/path");
    } else {
        panic!("Expected UiAsset route for unknown GET path");
    }
}

#[test]
fn test_parse_unknown_post_route() {
    let uri: Uri = "/unknown/path".parse().unwrap();
    assert!(parse(&Method::POST, &uri).is_none());
}

#[test]
fn test_parse_ui_asset_root() {
    let uri: Uri = "/".parse().unwrap();
    if let Some(Route::UiAsset { path }) = parse(&Method::GET, &uri) {
        assert_eq!(path, "/");
    } else {
        panic!("Expected UiAsset route");
    }
}

#[test]
fn test_parse_ui_asset_with_path() {
    let uri: Uri = "/index.html".parse().unwrap();
    if let Some(Route::UiAsset { path }) = parse(&Method::GET, &uri) {
        assert_eq!(path, "/index.html");
    } else {
        panic!("Expected UiAsset route");
    }
}

#[test]
fn test_parse_ui_asset_head_method() {
    let uri: Uri = "/style.css".parse().unwrap();
    if let Some(Route::UiAsset { path }) = parse(&Method::HEAD, &uri) {
        assert_eq!(path, "/style.css");
    } else {
        panic!("Expected UiAsset route");
    }
}

#[test]
fn test_parse_ui_asset_post_not_allowed() {
    let uri: Uri = "/index.html".parse().unwrap();
    assert!(parse(&Method::POST, &uri).is_none());
}

/// The proxy `ns` parameter selects no upstream, so it must not change or
/// reject the route it rides on: it is resolved from the namespace prefix.
#[test]
fn ns_parameter_is_accepted_and_ignored() {
    let digest = format!("sha256:{}", "a".repeat(64));
    for (method, path) in [
        (Method::GET, "/v2/lib/nginx/manifests/latest".to_string()),
        (Method::HEAD, "/v2/lib/nginx/manifests/latest".to_string()),
        (Method::GET, format!("/v2/lib/nginx/blobs/{digest}")),
        (Method::GET, "/v2/lib/nginx/tags/list".to_string()),
        (Method::GET, format!("/v2/lib/nginx/referrers/{digest}")),
        (Method::POST, "/v2/lib/nginx/blobs/uploads/".to_string()),
        (Method::GET, "/v2/_catalog".to_string()),
    ] {
        let plain = parse(&method, &path.parse::<Uri>().unwrap());
        let with_ns = parse(
            &method,
            &format!("{path}?ns=docker.io").parse::<Uri>().unwrap(),
        );
        assert!(plain.is_some(), "{method} {path} must route");
        assert_eq!(
            format!("{with_ns:?}"),
            format!("{plain:?}"),
            "?ns= must not change the {method} {path} route"
        );
    }
}
