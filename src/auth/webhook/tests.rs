use std::time::Duration;

use http::Method;
use http::request::Builder;
use reqwest::Client;
use reqwest::redirect::Policy;
use url::Url;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use angos_oci::{Namespace, Reference, Tag};

use crate::metrics_provider::initialize_metrics;

use crate::auth::Error;
use crate::auth::webhook::config::{ForwardHeader, WebhookAuth};
use crate::auth::webhook::headers::{build_header_value, build_headers};
use crate::auth::webhook::{Config, WebhookAuthorizer};
use crate::identity::{Action, ClientIdentity};
use crate::test_fixtures::requests::parts_with_uri;
use angos_mtls_client::{ClientTls, build_client};
use angos_secret::Secret;

/// `cache_ttl` configured a decision cache that is gone; a configuration
/// carrying it must keep loading, like every other key of a removed subsystem.
#[test]
fn config_parses_auth_and_ignores_cache_ttl() {
    let config: Config = toml::from_str(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        basic_auth = { username = "user", password = "pass" }
        cache_ttl = 60
    "#,
    )
    .unwrap();
    assert!(
        matches!(config.auth, Some(WebhookAuth::BasicAuth { username, password }) if username == "user" && password.expose() == "pass")
    );

    let config: Config = toml::from_str(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        bearer_token = "hello-token"
    "#,
    )
    .unwrap();
    assert!(
        matches!(config.auth, Some(WebhookAuth::BearerToken(token)) if token.expose() == "hello-token")
    );
}

#[test]
fn mtls_pair_must_be_complete_at_parse() {
    for half in [
        r#"client_certificate_bundle = "/valid/path/to/cert.pem""#,
        r#"client_private_key = "/valid/path/to/key.pem""#,
    ] {
        let toml = format!("url = \"https://example.com\"\ntimeout_ms = 1000\n{half}");
        assert!(
            toml::from_str::<Config>(&toml).is_err(),
            "{half} alone must be refused"
        );
    }
}

#[test]
fn invalid_forward_header_fails_at_parse() {
    let err = toml::from_str::<Config>(
        r#"
        url = "https://example.com"
        timeout_ms = 1000
        forward_headers = ["X-Good-Header", "Invalid Header!"]
    "#,
    )
    .unwrap_err()
    .to_string();

    assert!(
        err.contains("Invalid Header!"),
        "error should identify the invalid forwarded header: {err}"
    );
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

    let forward_headers = vec![ForwardHeader::try_from("X-Custom-Header".to_string()).unwrap()];

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

fn build_test_webhook(url: &str, auth: Option<WebhookAuth>) -> WebhookAuthorizer {
    initialize_metrics();
    let config = Config {
        url: Url::parse(url).unwrap(),
        timeout_ms: 1000,
        auth,
        tls: ClientTls::default(),
        forward_headers: Vec::new(),
    };
    let client = build_client(
        Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_millis(config.timeout_ms)),
        &config.tls,
    )
    .unwrap();
    WebhookAuthorizer::new("test".to_string(), config, client)
}

#[tokio::test]
async fn the_request_carries_the_configured_credential_and_context_headers() {
    for (auth, authorization) in [
        (
            WebhookAuth::BearerToken(Secret::new("test-token".to_string())),
            "Bearer test-token",
        ),
        (
            WebhookAuth::BasicAuth {
                username: "testuser".to_string(),
                password: Secret::new("testpass".to_string()),
            },
            "Basic dGVzdHVzZXI6dGVzdHBhc3M=",
        ),
    ] {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("Authorization", authorization))
            .and(header("X-Forwarded-Method", "GET"))
            .and(header("X-Registry-Action", "get-api-version"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let webhook = build_test_webhook(&mock_server.uri(), Some(auth));
        let parts = parts_with_uri("https://example.com/v2/");
        let allowed = webhook
            .authorize(&Action::ApiVersion, &ClientIdentity::new(None), &parts)
            .await;
        assert!(allowed.unwrap(), "{authorization}");
    }
}

/// 2xx allows, 401/403 are decisions the webhook made, and any other status is
/// an unavailable webhook, which fails closed with an error.
#[tokio::test]
async fn the_status_decides_the_outcome() {
    for (status, expected) in [
        (200, Some(true)),
        (401, Some(false)),
        (403, Some(false)),
        (404, None),
        (500, None),
    ] {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&mock_server)
            .await;

        let webhook = build_test_webhook(&mock_server.uri(), None);
        let parts = parts_with_uri("https://example.com/v2/");
        let result = webhook
            .authorize(&Action::ApiVersion, &ClientIdentity::new(None), &parts)
            .await;
        assert_eq!(result.ok(), expected, "status {status}");
    }
}

#[tokio::test]
async fn test_authorize_returns_err_on_unreachable_url() {
    let webhook = build_test_webhook("http://127.0.0.1:1", None);
    let parts = parts_with_uri("https://example.com/v2/");

    let err = webhook
        .authorize(&Action::ApiVersion, &ClientIdentity::new(None), &parts)
        .await
        .expect_err("unreachable URL must produce Err, not Ok(false)");
    assert!(
        err.to_string().contains("unreachable"),
        "a transport failure must read differently from an unavailable status: {err}"
    );
}
