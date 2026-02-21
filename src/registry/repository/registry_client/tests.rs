use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

use super::*;
use crate::{
    cache,
    oci::{Digest, Namespace, Reference},
    secret::Secret,
};

#[test]
fn test_get_upstream_namespace() {
    let local_name = "local";
    let upstream_name = "local/repo";

    let result = RegistryClient::get_upstream_namespace(local_name, upstream_name);
    assert_eq!(result, "repo");

    let repo_name = "local/nested";
    let namespace = &Namespace::new("completely/different/path").unwrap();
    let result = RegistryClient::get_upstream_namespace(repo_name, namespace);
    assert_eq!(result, "completely/different/path");
}

#[test]
fn test_get_upstream_namespace_no_prefix_match() {
    let result = RegistryClient::get_upstream_namespace("foo", "bar/baz");
    assert_eq!(result, "bar/baz");
}

#[tokio::test]
async fn test_get_manifest_path() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: Some("username".to_string()),
        password: Some(Secret::new("password".to_string())),
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let upstream = RegistryClient::new(&config, cache).unwrap();

    let repo_name = "local";
    let namespace = &Namespace::new("local/repo").unwrap();
    let reference = Reference::Tag("latest".to_string());

    let path = upstream.get_manifest_path(repo_name, namespace, &reference);
    assert_eq!(path, "https://example.com/v2/repo/manifests/latest");
}

#[tokio::test]
async fn test_get_blob_path() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let upstream = RegistryClient::new(&config, cache).unwrap();

    let repo_name = "local";
    let namespace = &Namespace::new("local/repo").unwrap();
    let digest =
        Digest::try_from("sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
            .unwrap();

    let path = upstream.get_blob_path(repo_name, namespace, &digest);
    assert_eq!(
        path,
        "https://example.com/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
}

#[tokio::test]
async fn test_new_with_username_only() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: Some("user".to_string()),
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();
    assert!(client.basic_auth.is_none());
}

#[tokio::test]
async fn test_new_with_password_only() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: Some(Secret::new("pass".to_string())),
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();
    assert!(client.basic_auth.is_none());
}

#[tokio::test]
async fn test_new_with_both_credentials() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: Some("user".to_string()),
        password: Some(Secret::new("pass".to_string())),
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();
    assert!(client.basic_auth.is_some());
}

#[tokio::test]
async fn test_head_blob_success() {
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/test/blobs/{test_digest}")))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, test_digest)
                .insert_header("Content-Length", "1234"),
        )
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .head_blob(
            &[],
            &format!("{}/v2/test/blobs/{test_digest}", mock_server.uri()),
        )
        .await;

    assert!(result.is_ok());
    let (digest, size) = result.unwrap();
    assert_eq!(digest, Digest::try_from(test_digest).unwrap());
    assert_eq!(size, 1234);
}

#[tokio::test]
async fn test_head_blob_not_found() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/v2/test/blobs/sha256:notfound"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .head_blob(
            &[],
            &format!("{}/v2/test/blobs/sha256:notfound", mock_server.uri()),
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::ManifestUnknown));
}

#[tokio::test]
async fn test_head_manifest_success() {
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

    Mock::given(method("HEAD"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, test_digest)
                .insert_header(
                    "Content-Type",
                    "application/vnd.docker.distribution.manifest.v2+json",
                )
                .insert_header("Content-Length", "5678"),
        )
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .head_manifest(
            &[],
            &format!("{}/v2/test/manifests/latest", mock_server.uri()),
        )
        .await;

    assert!(result.is_ok());
    let (media_type, digest, size) = result.unwrap();
    assert_eq!(
        media_type,
        Some("application/vnd.docker.distribution.manifest.v2+json".to_string())
    );
    assert_eq!(digest, Digest::try_from(test_digest).unwrap());
    assert_eq!(size, 5678);
}

#[tokio::test]
async fn test_get_manifest_success() {
    let mock_server = MockServer::start().await;
    let manifest_body = b"{\"schemaVersion\":2}";
    let test_digest = "sha256:fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(manifest_body)
                .insert_header(DOCKER_CONTENT_DIGEST, test_digest)
                .insert_header(
                    "Content-Type",
                    "application/vnd.docker.distribution.manifest.v2+json",
                ),
        )
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .get_manifest(
            &[],
            &format!("{}/v2/test/manifests/latest", mock_server.uri()),
        )
        .await;

    assert!(result.is_ok());
    let (media_type, digest, body) = result.unwrap();
    assert_eq!(
        media_type,
        Some("application/vnd.docker.distribution.manifest.v2+json".to_string())
    );
    assert_eq!(digest, Digest::try_from(test_digest).unwrap());
    assert_eq!(body, manifest_body);
}

#[tokio::test]
async fn test_bearer_authentication() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(
                r#"Bearer realm="{}",service="registry",scope="repository:test:pull""#,
                auth_server.uri()
            ),
        ))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(query_param("service", "registry"))
        .and(query_param("scope", "repository:test:pull"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"token": "test-bearer-token", "expires_in": 3600}),
            ),
        )
        .mount(&auth_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer test-bearer-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .get_manifest(
            &[],
            &format!("{}/v2/test/manifests/latest", mock_server.uri()),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_basic_authentication() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("WWW-Authenticate", "Basic realm=\"Registry\""),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Basic dXNlcjpwYXNz"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:2222222222222222222222222222222222222222222222222222222222222222",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: Some("user".to_string()),
        password: Some(Secret::new("pass".to_string())),
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .get_manifest(
            &[],
            &format!("{}/v2/test/manifests/latest", mock_server.uri()),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_forbidden_access() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .get_manifest(
            &[],
            &format!("{}/v2/test/manifests/latest", mock_server.uri()),
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Denied(_)));
}

#[tokio::test]
async fn test_get_blob_success() {
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let blob_data = b"test blob content here";

    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{test_digest}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(blob_data))
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .get_blob(
            &[],
            &format!("{}/v2/test/blobs/{test_digest}", mock_server.uri()),
        )
        .await;

    assert!(result.is_ok());
    let (size, mut reader) = result.unwrap();
    assert_eq!(size, blob_data.len() as u64);

    let mut buffer = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer)
        .await
        .unwrap();
    assert_eq!(buffer, blob_data);
}

#[tokio::test]
async fn test_get_blob_not_found() {
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:notfound1234567890abcdef1234567890abcdef1234567890abcdef12345678";

    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{test_digest}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: mock_server.uri(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    let result = client
        .get_blob(
            &[],
            &format!("{}/v2/test/blobs/{test_digest}", mock_server.uri()),
        )
        .await;

    assert!(result.is_err());
    match result {
        Err(Error::ManifestUnknown) => (),
        _ => panic!("Expected Error::ManifestUnknown"),
    }
}

#[test]
fn test_new_with_invalid_ca_bundle() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: Some("/nonexistent/ca.pem".to_string()),
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let result = RegistryClient::new(&config, cache);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Initialization(_)));
}

#[test]
fn test_new_with_certificate_only_ignored() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: Some("/path/to/cert.pem".to_string()),
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let result = RegistryClient::new(&config, cache);

    assert!(result.is_ok());
}

#[test]
fn test_new_with_private_key_only_ignored() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: Some("/path/to/key.pem".to_string()),
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let result = RegistryClient::new(&config, cache);

    assert!(result.is_ok());
}

#[test]
fn test_new_with_both_certificate_and_key_invalid_files() {
    let config = RegistryClientConfig {
        url: "https://example.com".to_string(),
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: Some("/nonexistent/cert.pem".to_string()),
        client_private_key: Some("/nonexistent/key.pem".to_string()),
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let result = RegistryClient::new(&config, cache);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Initialization(_)));
}
