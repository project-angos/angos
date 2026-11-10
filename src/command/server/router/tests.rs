use super::*;

// Per-endpoint parsing is tested in the service crates; these cover only the
// binary's own paths, the delegation order and the miss-versus-UI-asset fallback.

/// Each case names the route by its metric label, `None` being a miss. The
/// probes and the token endpoint answer `GET` only: otherwise a `HEAD` reaches
/// the UI-asset arm and answers `index.html` with a 200 while `/readyz` answers
/// 503. end-1 spells `/v2/` with its slash, so `/v2` must be a miss too.
#[test]
fn parse_routes_each_path_to_its_surface() {
    for (method, path, expected) in [
        (Method::GET, "/healthz", Some("healthz")),
        (Method::GET, "/readyz", Some("readyz")),
        (Method::GET, "/metrics", Some("metrics")),
        (Method::HEAD, "/healthz", None),
        (Method::POST, "/healthz", None),
        (Method::HEAD, "/readyz", None),
        (Method::POST, "/readyz", None),
        (Method::HEAD, "/metrics", None),
        (Method::POST, "/metrics", None),
        (Method::GET, "/token", Some("get-token")),
        (Method::POST, "/token", None),
        (Method::HEAD, "/token", None),
        // Matched before the `_angos` parser, which declines it.
        (Method::GET, "/v2/_angos/ui/config", Some("ui-config")),
        (Method::POST, "/v2/_angos/ui/config", None),
        (Method::GET, "/v2/", Some("check-version")),
        (Method::HEAD, "/v2/", Some("check-version")),
        (Method::GET, "/v2/_catalog", Some("list-catalog")),
        (
            Method::GET,
            "/v2/lib/nginx/_angos/revisions/list",
            Some("list-revisions"),
        ),
        (Method::GET, "/v2", None),
        (Method::HEAD, "/v2", None),
        (Method::GET, "/v2/nginx", None),
        (Method::GET, "/unknown/path", Some("ui-asset")),
        (Method::HEAD, "/style.css", Some("ui-asset")),
        (Method::POST, "/unknown/path", None),
    ] {
        let route = parse(&method, &path.parse().unwrap());
        assert_eq!(
            route.as_ref().map(Route::metric_label),
            expected,
            "{method} {path}"
        );
        if let Some(Route::UiAsset { path: asset }) = route {
            assert_eq!(asset, path);
        }
    }
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
