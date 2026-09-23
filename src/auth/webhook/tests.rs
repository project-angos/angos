use std::{fs, path::PathBuf, time::Duration};

use http::{Method, request::Builder};
use reqwest::{Client, redirect::Policy};
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method},
};

use angos_oci::{Namespace, Reference, Tag};

use crate::metrics_provider::init_for_tests;

use crate::{
    auth::Error,
    auth::webhook::{
        Config, WebhookAuthorizer,
        config::WebhookAuth,
        headers::{build_header_name, build_header_value, build_headers},
    },
    identity::{Action, ClientIdentity, RequestScheme},
    test_fixtures::{
        requests::parts_with_uri,
        webhook::{ca_bundle_pem, client_cert_pem, client_key_pem},
    },
};
use angos_mtls_client::MtlsClientBuilder;
use angos_secret::Secret;

#[test]
fn test_config_deserialize() {
    let valid_config = r#"
        url = "https://example.com"
        timeout_ms = 1000
        basic_auth = { username = "user", password = "pass" }
    "#;

    let config: Config = toml::from_str(valid_config).unwrap();

    assert_eq!(config.url.as_str(), "https://example.com/");
    assert_eq!(config.timeout_ms, 1000);
    assert!(
        matches!(config.auth, Some(WebhookAuth::BasicAuth { username, password }) if username == "user" && password.expose() == "pass")
    );
    assert!(config.client_certificate_bundle.is_none());
    assert!(config.client_private_key.is_none());
    assert!(config.server_ca_bundle.is_none());
    assert!(config.forward_headers.is_empty());

    let valid_config = r#"
        url = "https://example.com"
        timeout_ms = 1000
        bearer_token = "hello-token"
    "#;

    let config: Config = toml::from_str(valid_config).unwrap();

    assert_eq!(config.url.as_str(), "https://example.com/");
    assert_eq!(config.timeout_ms, 1000);
    assert!(
        matches!(config.auth, Some(WebhookAuth::BearerToken(token)) if token.expose() == "hello-token")
    );
    assert!(config.client_certificate_bundle.is_none());
    assert!(config.client_private_key.is_none());
    assert!(config.server_ca_bundle.is_none());
    assert!(config.forward_headers.is_empty());
}

#[test]
fn mtls_pair_must_be_complete_at_validation() {
    let config: Config = toml::from_str(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        client_certificate_bundle = "/valid/path/to/cert.pem"
    "#,
    )
    .unwrap();
    assert!(config.validate().is_err());

    let config: Config = toml::from_str(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        client_private_key = "/valid/path/to/key.pem"
    "#,
    )
    .unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn invalid_forward_header_fails_validation() {
    let config: Config = toml::from_str(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        forward_headers = ["X-Good-Header", "Invalid Header!"]
    "#,
    )
    .unwrap();

    let err = config.validate().unwrap_err();
    assert!(
        err.contains("Invalid Header!"),
        "error should identify the invalid forwarded header: {err}"
    );
}

#[test]
fn invalid_url_fails_at_deserialize() {
    let toml = r#"
        url = "ht!tp://::invalid"
        timeout_ms = 1000
    "#;

    let result: Result<Config, _> = toml::from_str(toml);
    assert!(result.is_err());
}

#[test]
fn test_build_header_name() {
    let header = "X-Custom-Header";
    let header = build_header_name(header);
    assert!(header.is_ok());

    let header = "Invalid Header!";
    let header = build_header_name(header);
    assert!(matches!(header, Err(Error::Execution(_))));
}
#[test]
fn test_build_header_value() {
    let value = "Some value";
    let value = build_header_value(value);
    assert!(value.is_ok());

    let value = "Invalid\r\nValue";
    let value = build_header_value(value);
    assert!(matches!(value, Err(Error::Execution(_))));
}

#[test]
fn test_build_headers() {
    let request = Builder::new()
        .method(Method::GET)
        .uri("https://example.com/v2/test-namespace/manifests/latest")
        .header("Host", "example.com")
        .header("X-Custom-Header", "custom-value")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();

    let action = Action::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };

    let mut identity = ClientIdentity::new(None);
    identity.username = Some("testuser".to_string());
    identity.client_ip = Some("192.168.1.1".to_string());

    let forward_headers = vec!["X-Custom-Header".to_string()];

    let headers = build_headers(&forward_headers, &action, &identity, &parts);

    assert!(headers.is_ok());
    let headers = headers.unwrap();

    assert_eq!(headers.get("X-Forwarded-Method").unwrap(), "GET");
    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");
    assert_eq!(headers.get("X-Forwarded-Host").unwrap(), "example.com");
    assert!(headers.get("X-Forwarded-Uri").is_some());
    assert_eq!(headers.get("X-Forwarded-For").unwrap(), "192.168.1.1");
    assert_eq!(headers.get("X-Registry-Action").unwrap(), "get-manifest");
    assert_eq!(
        headers.get("X-Registry-Namespace").unwrap(),
        "test-namespace"
    );
    assert_eq!(headers.get("X-Registry-Reference").unwrap(), "latest");
    assert_eq!(headers.get("X-Registry-Username").unwrap(), "testuser");
    assert_eq!(headers.get("X-Custom-Header").unwrap(), "custom-value");
}

/// The optional and multi-valued halves of `build_headers`: plain-http proto,
/// absent Host/client-ip/username produce no header, digest and identity id
/// are stamped, and every certificate CN/O value is appended.
#[test]
fn test_build_headers_optional_and_multi_valued_fields() {
    let request = Builder::new()
        .method(Method::HEAD)
        .uri("http://example.com/v2/ns/blobs/sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let action = Action::GetBlob {
        namespace: Namespace::new("ns").unwrap(),
        digest: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .parse()
            .unwrap(),
    };

    let mut identity = ClientIdentity::new(None);
    identity.id = Some("user-id-123".to_string());
    identity.certificate.common_names = vec!["cn1".to_string(), "cn2".to_string()];
    identity.certificate.organizations = vec!["org1".to_string(), "org2".to_string()];

    let headers = build_headers(&[], &action, &identity, &parts).unwrap();

    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "http");
    assert!(headers.get("X-Forwarded-Host").is_none());
    assert!(headers.get("X-Forwarded-For").is_none());
    assert!(headers.get("X-Registry-Username").is_none());
    assert!(headers.get("X-Registry-Reference").is_none());
    assert_eq!(
        headers.get("X-Registry-Digest").unwrap(),
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        headers.get("X-Registry-Identity-ID").unwrap(),
        "user-id-123"
    );
    let cns: Vec<_> = headers
        .get_all("X-Registry-Certificate-CN")
        .iter()
        .collect();
    assert_eq!(cns, ["cn1", "cn2"]);
    let orgs: Vec<_> = headers.get_all("X-Registry-Certificate-O").iter().collect();
    assert_eq!(orgs, ["org1", "org2"]);
}

fn build_test_config(
    url: Url,
    server_ca_bundle: Option<PathBuf>,
    client_certificate_bundle: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
) -> Config {
    Config {
        url,
        timeout_ms: 1000,
        auth: Some(WebhookAuth::BearerToken(Secret::new("token".to_string()))),
        client_certificate_bundle,
        client_private_key,
        server_ca_bundle,
        forward_headers: vec!["X-Custom-Header".to_string()],
    }
}

fn build_test_client(config: &Config) -> Result<Client, String> {
    MtlsClientBuilder::new()
        .with_redirect_policy(Policy::none())
        .with_timeout(Duration::from_millis(config.timeout_ms))
        .with_server_ca_bundle(config.server_ca_bundle.as_deref())
        .with_client_certificate(
            config
                .client_certificate_bundle
                .as_deref()
                .zip(config.client_private_key.as_deref()),
        )
        .build()
}

fn build_test_webhook(name: String, config: Config) -> Result<WebhookAuthorizer, Error> {
    init_for_tests();
    let client = build_test_client(&config).map_err(Error::Initialization)?;
    WebhookAuthorizer::new(name, config, client)
}

#[test]
fn test_new_invalid_mtls() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cert_file_path = tmp_dir.path().join("certificate.pem");
    fs::write(&cert_file_path, client_cert_pem()).unwrap();

    let key_file_path = tmp_dir.path().join("private-key.pem");
    fs::write(&key_file_path, ca_bundle_pem()).unwrap();

    let ca_file_path = tmp_dir.path().join("ca.pem");
    fs::write(&ca_file_path, ca_bundle_pem()).unwrap();

    let config = build_test_config(
        Url::parse("https://example.com").unwrap(),
        Some(ca_file_path),
        Some(cert_file_path),
        Some(key_file_path),
    );
    let webhook = build_test_webhook("test".to_string(), config);

    assert!(matches!(webhook, Err(Error::Initialization(_))));
}

#[test]
fn test_new_rejects_incomplete_mtls_config() {
    let config = build_test_config(
        Url::parse("https://example.com").unwrap(),
        None,
        Some(PathBuf::from("certificate.pem")),
        None,
    );
    let webhook = build_test_webhook("test".to_string(), config);

    assert!(
        matches!(webhook, Err(Error::Initialization(msg)) if msg.contains("client_private_key"))
    );
}

#[test]
fn test_new_mtls() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cert_file_path = tmp_dir.path().join("certificate.pem");
    fs::write(&cert_file_path, client_cert_pem()).unwrap();

    let key_file_path = tmp_dir.path().join("private-key.pem");
    fs::write(&key_file_path, client_key_pem()).unwrap();

    let ca_file_path = tmp_dir.path().join("ca.pem");
    fs::write(&ca_file_path, ca_bundle_pem()).unwrap();

    let config = build_test_config(
        Url::parse("https://example.com").unwrap(),
        Some(ca_file_path),
        Some(cert_file_path),
        Some(key_file_path),
    );
    let webhook = build_test_webhook("test".to_string(), config);

    assert!(webhook.is_ok());
}

#[test]
fn test_new_simple() {
    let config = build_test_config(Url::parse("https://example.com").unwrap(), None, None, None);
    let webhook = build_test_webhook("test".to_string(), config);

    assert!(webhook.is_ok());
}

#[tokio::test]
async fn test_authorize_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = None;

    let webhook = build_test_webhook("test".to_string(), config).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    assert!(webhook.authorize(&action, &identity, &parts).await.unwrap());
}

#[tokio::test]
async fn test_authorize_denied() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = None;

    let webhook = build_test_webhook("test".to_string(), config).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    assert!(!webhook.authorize(&action, &identity, &parts).await.unwrap());
}

#[tokio::test]
async fn test_authorize_with_bearer_token() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = Some(WebhookAuth::BearerToken(Secret::new(
        "test-token".to_string(),
    )));

    let webhook = build_test_webhook("test".to_string(), config).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    assert!(webhook.authorize(&action, &identity, &parts).await.unwrap());
}

#[tokio::test]
async fn test_authorize_with_basic_auth() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(header("Authorization", "Basic dGVzdHVzZXI6dGVzdHBhc3M="))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = Some(WebhookAuth::BasicAuth {
        username: "testuser".to_string(),
        password: Secret::new("testpass".to_string()),
    });

    let webhook = build_test_webhook("test".to_string(), config).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    assert!(webhook.authorize(&action, &identity, &parts).await.unwrap());
}

#[tokio::test]
async fn test_authorize_sends_correct_headers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(header("X-Forwarded-Method", "GET"))
        .and(header("X-Registry-Action", "get-api-version"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    let webhook = build_test_webhook("test".to_string(), config).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .method(Method::GET)
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert!(webhook.authorize(&action, &identity, &parts).await.unwrap());
}

#[tokio::test]
async fn test_authorize_returns_err_on_unreachable_url() {
    let mut config = build_test_config(Url::parse("http://127.0.0.1:1").unwrap(), None, None, None);
    config.auth = None;

    let webhook = build_test_webhook("test".to_string(), config).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    let result = webhook.authorize(&action, &identity, &parts).await;
    let err = result.expect_err("unreachable URL must produce Err, not Ok(false)");
    let msg = err.to_string();
    assert!(
        msg.contains("unreachable"),
        "transport-failure error must mention unreachability so it is distinguishable from explicit deny in logs: {msg}"
    );
}

fn build_webhook_against(mock_server: &MockServer) -> WebhookAuthorizer {
    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = None;
    build_test_webhook("test".to_string(), config).unwrap()
}

// 401/403 are decisions the webhook made: Ok(false), not Err.
async fn assert_explicit_deny(status: u16) {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status))
        .mount(&mock_server)
        .await;

    let webhook = build_webhook_against(&mock_server);
    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("https://example.com/v2/");

    assert!(
        !webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "status {status} must be an explicit deny"
    );
}

// Any other status is an unavailable webhook: Err, not Ok(false).
async fn assert_unavailable_fails_closed(status: u16) {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status))
        .mount(&mock_server)
        .await;

    let webhook = build_webhook_against(&mock_server);
    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("https://example.com/v2/");

    assert!(
        webhook.authorize(&action, &identity, &parts).await.is_err(),
        "status {status} must return Err, not Ok(false)"
    );
}

#[tokio::test]
async fn test_authorize_403_is_explicit_deny() {
    assert_explicit_deny(403).await;
}

#[tokio::test]
async fn test_authorize_401_is_explicit_deny() {
    assert_explicit_deny(401).await;
}

#[tokio::test]
async fn test_authorize_500_is_unavailable() {
    assert_unavailable_fails_closed(500).await;
}

#[tokio::test]
async fn test_authorize_503_is_unavailable() {
    assert_unavailable_fails_closed(503).await;
}

#[tokio::test]
async fn test_authorize_429_is_unavailable() {
    assert_unavailable_fails_closed(429).await;
}

#[tokio::test]
async fn test_authorize_404_is_unavailable() {
    assert_unavailable_fails_closed(404).await;
}

/// HTTP/1.1 sends origin-form targets, so the URI carries no scheme and only
/// the listener's own record can report TLS.
#[test]
fn build_headers_reports_the_listener_scheme_for_an_origin_form_target() {
    let request = Builder::new()
        .method(Method::GET)
        .uri("/v2/test-namespace/manifests/latest")
        .header("Host", "example.com")
        .body(())
        .unwrap();
    let (mut parts, ()) = request.into_parts();
    parts.extensions.insert(RequestScheme::Https);

    let action = Action::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };
    let headers = build_headers(&[], &action, &ClientIdentity::new(None), &parts).unwrap();

    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");
}

#[test]
fn build_headers_reports_http_without_a_listener_scheme() {
    let request = Builder::new()
        .method(Method::GET)
        .uri("/v2/test-namespace/manifests/latest")
        .header("Host", "example.com")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let action = Action::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };
    let headers = build_headers(&[], &action, &ClientIdentity::new(None), &parts).unwrap();

    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "http");
}

/// `cache_ttl` configured a decision cache that is gone; a configuration
/// carrying it must keep loading, like every other key of a removed subsystem.
#[test]
fn cache_ttl_parses_and_is_ignored() {
    let config: Config = toml::from_str(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        bearer_token = "hello-token"
        cache_ttl = 60
    "#,
    )
    .expect("a configuration carrying cache_ttl must still load");
    assert_eq!(config.timeout_ms, 1000);
}
