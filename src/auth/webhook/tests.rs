use std::{fs, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use hyper::{HeaderMap, Method, http::request::Builder};
use reqwest::Client;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method},
};

use crate::{
    auth::webhook::{
        Config, WebhookAuthorizer,
        config::WebhookAuth,
        headers::{
            build_header_name, build_header_value, build_headers, set_forwarded_for_header,
            set_forwarded_headers, set_forwarded_host_header, set_forwarded_method_header,
            set_forwarded_proto_header, set_forwarded_uri_header, set_registry_action_header,
            set_registry_certificate_cn_header, set_registry_certificate_o_header,
            set_registry_digest_header, set_registry_identity_id_header,
            set_registry_namespace_header, set_registry_reference_header,
            set_registry_username_header,
        },
    },
    cache::{self, Cache},
    command::server::Error,
    http_client::HttpClientBuilder,
    identity::{Action, ClientIdentity},
    oci::{Digest, Namespace, Reference, Tag},
    secret::Secret,
    test_fixtures::{
        requests::parts_with_uri,
        webhook::{ca_bundle_pem, client_cert_pem, client_key_pem},
    },
};

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
    assert_eq!(config.cache_ttl, 60);

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
    assert_eq!(config.cache_ttl, 60);
}

#[test]
fn mtls_pair_must_be_complete_at_deserialize_time() {
    let invalid_toml = r#"
        url = "https://example.com"
        timeout_ms = 1000
        client_certificate_bundle = "/valid/path/to/cert.pem"
    "#;
    assert!(toml::from_str::<Config>(invalid_toml).is_err());

    let invalid_toml = r#"
        url = "https://example.com"
        timeout_ms = 1000
        client_private_key = "/valid/path/to/key.pem"
    "#;
    assert!(toml::from_str::<Config>(invalid_toml).is_err());
}

#[test]
fn invalid_forward_header_fails_at_deserialize_time() {
    let toml = r#"
        url = "https://example.com"
        timeout_ms = 1000
        forward_headers = ["X-Good-Header", "Invalid Header!"]
    "#;

    let err = toml::from_str::<Config>(toml).unwrap_err();
    assert!(
        err.to_string().contains("Invalid Header!"),
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
fn test_set_forwarded_method_header() {
    let request = Builder::new()
        .method(Method::POST)
        .uri("https://example.com/path")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_method_header(&parts, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-Method").unwrap(), "POST");
}

#[test]
fn test_set_forwarded_proto_header() {
    let parts = parts_with_uri("https://example.com/path");
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_proto_header(&parts, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");

    let parts = parts_with_uri("http://example.com/path");
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_proto_header(&parts, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "http");
}

#[test]
fn test_set_forwarded_host_header() {
    let request = Builder::new()
        .uri("https://example.com/path")
        .header("Host", "example.com")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    set_forwarded_host_header(&parts, &mut headers);
    assert_eq!(headers.get("X-Forwarded-Host").unwrap(), "example.com");
}

#[test]
fn test_set_forwarded_uri_header() {
    let parts = parts_with_uri("https://example.com/v2/test/manifests/latest");
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_uri_header(&parts, &mut headers).is_ok());
    assert_eq!(
        headers.get("X-Forwarded-Uri").unwrap(),
        "https://example.com/v2/test/manifests/latest"
    );
}

#[test]
fn test_set_forwarded_for_header() {
    let mut identity = ClientIdentity::new(None);
    identity.client_ip = Some("192.168.1.1".to_string());

    let mut headers = HeaderMap::new();

    assert!(set_forwarded_for_header(&identity, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-For").unwrap(), "192.168.1.1");

    let identity_no_ip = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_for_header(&identity_no_ip, &mut headers).is_ok());
    assert!(headers.get("X-Forwarded-For").is_none());
}

#[test]
fn test_set_registry_action_header() {
    let action = Action::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_action_header(&action, &mut headers).is_ok());
    assert_eq!(headers.get("X-Registry-Action").unwrap(), "get-api-version");
}

#[test]
fn test_set_registry_namespace_header() {
    let action = Action::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };
    let mut headers = HeaderMap::new();

    assert!(set_registry_namespace_header(&action, &mut headers).is_ok());
    assert_eq!(
        headers.get("X-Registry-Namespace").unwrap(),
        "test-namespace"
    );

    let action = Action::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_namespace_header(&action, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Namespace").is_none());
}

#[test]
fn test_set_registry_reference_header() {
    let action = Action::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag(Tag::new("v1.0.0").unwrap()),
    };
    let mut headers = HeaderMap::new();

    assert!(set_registry_reference_header(&action, &mut headers).is_ok());
    assert_eq!(headers.get("X-Registry-Reference").unwrap(), "v1.0.0");

    let action = Action::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_reference_header(&action, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Reference").is_none());
}

#[test]
fn test_set_registry_digest_header() {
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let digest = Digest::from_str(digest).unwrap();
    let action = Action::DeleteBlob {
        namespace: Namespace::new("test-namespace").unwrap(),
        digest: digest.clone(),
    };
    let mut headers = HeaderMap::new();

    assert!(set_registry_digest_header(&action, &mut headers).is_ok());
    assert_eq!(
        headers.get("X-Registry-Digest").unwrap(),
        &digest.to_string()
    );

    let action = Action::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_digest_header(&action, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Digest").is_none());
}

#[test]
fn test_set_registry_username_header() {
    let mut identity = ClientIdentity::new(None);
    identity.username = Some("testuser".to_string());
    let mut headers = HeaderMap::new();

    assert!(set_registry_username_header(&identity, &mut headers).is_ok());

    assert_eq!(headers.get("X-Registry-Username").unwrap(), "testuser");

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_registry_username_header(&identity, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Username").is_none());
}

#[test]
fn test_set_registry_identity_id_header() {
    let user_id = "user-id-123".to_string();
    let mut identity = ClientIdentity::new(None);
    identity.id = Some(user_id.clone());

    let mut headers = HeaderMap::new();

    assert!(set_registry_identity_id_header(&identity, &mut headers).is_ok());
    assert_eq!(headers.get("X-Registry-Identity-ID").unwrap(), &user_id);
}

#[test]
fn test_set_registry_certificate_cn_header() {
    let mut identity = ClientIdentity::new(None);
    identity.certificate.common_names = vec!["cn1".to_string(), "cn2".to_string()];
    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_cn_header(&identity, &mut headers).is_ok());

    let values: Vec<_> = headers
        .get_all("X-Registry-Certificate-CN")
        .iter()
        .collect();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], "cn1");
    assert_eq!(values[1], "cn2");

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_cn_header(&identity, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Certificate-CN").is_none());
}

#[test]
fn test_set_registry_certificate_o_header() {
    let mut identity = ClientIdentity::new(None);
    identity.certificate.organizations = vec!["org1".to_string(), "org2".to_string()];

    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_o_header(&identity, &mut headers).is_ok());

    let values: Vec<_> = headers.get_all("X-Registry-Certificate-O").iter().collect();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], "org1");
    assert_eq!(values[1], "org2");

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_o_header(&identity, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Certificate-O").is_none());
}

#[test]
fn test_set_forwarded_headers() {
    let request = Builder::new()
        .uri("https://example.com/path")
        .header("X-Custom-Header", "custom-value")
        .header("X-Another-Header", "another-value")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    let forward_headers = vec![
        "X-Custom-Header".to_string(),
        "X-Another-Header".to_string(),
    ];
    assert!(set_forwarded_headers(&forward_headers, &parts, &mut headers).is_ok());

    assert_eq!(headers.get("X-Custom-Header").unwrap(), "custom-value");
    assert_eq!(headers.get("X-Another-Header").unwrap(), "another-value");
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
        cache_ttl: 60,
    }
}

fn build_test_client(config: &Config) -> Result<Client, String> {
    HttpClientBuilder::new()
        .rustls_tls()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_millis(config.timeout_ms))
        .tls_files(
            config.server_ca_bundle.as_deref(),
            config.client_certificate_bundle.as_deref(),
            config.client_private_key.as_deref(),
        )?
        .build()
}

fn build_test_webhook(
    name: String,
    config: Config,
    cache: Arc<Cache>,
) -> Result<WebhookAuthorizer, Error> {
    crate::metrics_provider::init_for_tests();
    let client = build_test_client(&config).map_err(Error::Initialization)?;
    WebhookAuthorizer::new(name, config, client, cache)
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
    let webhook = build_test_webhook(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

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
    let webhook = build_test_webhook(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

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
    let webhook = build_test_webhook(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

    assert!(webhook.is_ok());
}

#[test]
fn test_new_simple() {
    let config = build_test_config(Url::parse("https://example.com").unwrap(), None, None, None);
    let webhook = build_test_webhook(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

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

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

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

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

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

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

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

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

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
    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

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
async fn test_authorize_uses_cache() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = None;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    assert!(webhook.authorize(&action, &identity, &parts).await.unwrap());
    assert!(webhook.authorize(&action, &identity, &parts).await.unwrap());
}

#[tokio::test]
async fn test_authorize_returns_err_on_unreachable_url() {
    let mut config = build_test_config(Url::parse("http://127.0.0.1:1").unwrap(), None, None, None);
    config.auth = None;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook("test".to_string(), config, cache).unwrap();

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

#[tokio::test]
async fn test_authorize_does_not_cache_transport_errors() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);
    let request_parts = parts_with_uri("https://example.com/v2/");

    // First call: point at an unreachable port to produce TransportError.
    let mut unreachable_config =
        build_test_config(Url::parse("http://127.0.0.1:1").unwrap(), None, None, None);
    unreachable_config.auth = None;
    let cache = cache::Config::Memory.to_backend().unwrap();
    let unreachable_webhook =
        build_test_webhook("test".to_string(), unreachable_config, cache.clone()).unwrap();

    let first = unreachable_webhook
        .authorize(&action, &identity, &request_parts)
        .await;
    assert!(
        first.is_err(),
        "unreachable URL must produce Err, not Ok(false)"
    );

    // Second call: same cache, but now using the live mock server.
    // If transport failures had been cached as Deny the result would be false
    // without a network call, causing the mock's expect(1) assertion to fail.
    let mut live_config =
        build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    live_config.auth = None;
    let live_webhook = build_test_webhook("test".to_string(), live_config, cache).unwrap();

    let second = live_webhook
        .authorize(&action, &identity, &request_parts)
        .await;
    assert!(
        second.unwrap(),
        "second call must reach the live server, not return a cached denial"
    );
}

fn build_webhook_against(mock_server: &MockServer) -> WebhookAuthorizer {
    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = None;
    let cache = cache::Config::Memory.to_backend().unwrap();
    build_test_webhook("test".to_string(), config, cache).unwrap()
}

// Webhook returns `status` exactly once (the mock's `expect(1)` enforces it).
// First call must return Ok(false); second call must hit the cache and return
// Ok(false) without re-contacting the webhook.
async fn assert_cacheable_explicit_deny(status: u16) {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status))
        .expect(1)
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
    assert!(
        !webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "second call must be served from cache"
    );
}

// Webhook returns `status` once, then 200 on every subsequent call. First call
// must return Err (unavailable, not cached); second call must reach the webhook
// again and be allowed.
async fn assert_unavailable_not_cached(status: u16) {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(status))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
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
    assert!(
        webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "after status {status} the next call must reach the webhook again and be allowed"
    );
}

#[tokio::test]
async fn test_authorize_403_is_explicit_deny_and_cacheable() {
    assert_cacheable_explicit_deny(403).await;
}

#[tokio::test]
async fn test_authorize_401_is_explicit_deny_and_cacheable() {
    assert_cacheable_explicit_deny(401).await;
}

#[tokio::test]
async fn test_authorize_500_is_unavailable_and_not_cached() {
    assert_unavailable_not_cached(500).await;
}

#[tokio::test]
async fn test_authorize_503_is_unavailable_and_not_cached() {
    assert_unavailable_not_cached(503).await;
}

#[tokio::test]
async fn test_authorize_429_is_unavailable_and_not_cached() {
    assert_unavailable_not_cached(429).await;
}

#[tokio::test]
async fn test_authorize_404_is_unavailable_and_not_cached() {
    assert_unavailable_not_cached(404).await;
}

// Build a Config with non-default timeout_ms or cache_ttl.
fn build_test_config_with(url: Url, timeout_ms: u64, cache_ttl: u64) -> Config {
    Config {
        url,
        timeout_ms,
        auth: None,
        client_certificate_bundle: None,
        client_private_key: None,
        server_ca_bundle: None,
        forward_headers: vec![],
        cache_ttl,
    }
}

#[tokio::test]
// Verifies authorization succeeds even when the cache store returns an error.
async fn webhook_authorization_succeeds_despite_cache_store_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(Url::parse(&mock_server.uri()).unwrap(), None, None, None);
    config.auth = None;

    let failing_backend = cache::stub::Backend::new();
    failing_backend.set_store_error(Some("injected store failure".to_string()));
    let failing_cache = Arc::new(Cache::Stub(failing_backend.clone()));
    let webhook = build_test_webhook("test".to_string(), config, failing_cache.clone()).unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);

    let parts = parts_with_uri("https://example.com/v2/");

    assert!(
        webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "cache store error must not fail authorization"
    );
    assert_eq!(
        failing_backend.store_calls(),
        1,
        "store_value must be attempted exactly once"
    );
}

#[tokio::test]
// A timed-out request must not write to the cache. The next call must reach
// the real backend instead of returning a stale cached denial.
async fn test_authorize_timeout_does_not_cache_and_retries() {
    let slow_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
        .expect(1)
        .mount(&slow_server)
        .await;

    let shared_cache = cache::Config::Memory.to_backend().unwrap();

    // First call: very short timeout causes a transport error.
    let slow_webhook = build_test_webhook(
        "test".to_string(),
        build_test_config_with(Url::parse(&slow_server.uri()).unwrap(), 100, 60),
        shared_cache.clone(),
    )
    .unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("https://example.com/v2/");

    let first = slow_webhook.authorize(&action, &identity, &parts).await;
    assert!(first.is_err(), "timed-out request must return Err");

    // Second call: live server with the same cache. If the timeout had been
    // cached the live server would never be hit and the mock's expect(1) on
    // the live server would fail.
    let live_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&live_server)
        .await;

    let live_webhook = build_test_webhook(
        "test".to_string(),
        build_test_config_with(Url::parse(&live_server.uri()).unwrap(), 1000, 60),
        shared_cache,
    )
    .unwrap();

    let second = live_webhook.authorize(&action, &identity, &parts).await;
    assert!(
        second.unwrap(),
        "after a timeout the next call must reach the live server: no stale cache entry"
    );
}

#[tokio::test]
// After cache_ttl seconds the cached authorization decision must be discarded
// and the next call must hit the network again.
async fn test_authorize_cache_entry_expires_and_refetches() {
    let mock_server = MockServer::start().await;

    // First request returns 200 (allow); subsequent requests return 403 (deny).
    // Using up_to_n_times(1) means the 200 response is consumed on the first
    // network call; the fallback mock then serves 403 for any later calls.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = build_test_webhook(
        "test".to_string(),
        build_test_config_with(Url::parse(&mock_server.uri()).unwrap(), 1000, 1),
        cache,
    )
    .unwrap();

    let action = Action::ApiVersion;
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("https://example.com/v2/");

    // First call: goes to the network, gets 200, caches the allow decision.
    assert!(
        webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "first call must be allowed"
    );

    // Immediate second call: served from cache, still allow, no network hop.
    assert!(
        webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "second call must be served from cache"
    );

    // Wait for the 1-second TTL to expire.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Third call: cache expired, goes to network again, gets the fallback 403.
    assert!(
        !webhook.authorize(&action, &identity, &parts).await.unwrap(),
        "after TTL expiry authorization must fetch fresh from network and get deny"
    );
}
