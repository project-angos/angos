use std::time::Duration;

use futures_util::future::join_all;
use url::Url;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

use crate::{
    cache,
    oci::{Digest, Namespace, Reference},
    registry::{DOCKER_CONTENT_DIGEST, Error},
    registry_client::{
        RegistryClient, RegistryClientConfig,
        auth::{token_cache_key, token_index_cache_key},
        get_upstream_namespace,
    },
    secret::Secret,
};

#[test]
fn test_get_upstream_namespace() {
    let local_name = "local";
    let upstream_name = "local/repo";

    let result = get_upstream_namespace(local_name, upstream_name);
    assert_eq!(result, "repo");

    let repo_name = "local/nested";
    let namespace = &Namespace::new("completely/different/path").unwrap();
    let result = get_upstream_namespace(repo_name, namespace);
    assert_eq!(result, "completely/different/path");
}

#[test]
fn test_get_upstream_namespace_no_prefix_match() {
    let result = get_upstream_namespace("foo", "bar/baz");
    assert_eq!(result, "bar/baz");
}

#[test]
fn test_get_manifest_path() {
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

#[test]
fn test_get_blob_path() {
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

#[test]
fn test_new_with_username_only() {
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

#[test]
fn test_new_with_password_only() {
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

#[test]
fn test_new_with_both_credentials() {
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
    assert!(matches!(result.unwrap_err(), Error::BlobUnknown));
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
async fn test_get_manifest_rejects_oversized_body() {
    let mock_server = MockServer::start().await;
    let oversized_body = vec![b' '; 8];
    let test_digest = "sha256:fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(oversized_body)
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
    let client = RegistryClient::new_with_manifest_size_limit(&config, cache, 7).unwrap();

    let result = client
        .get_manifest(
            &[],
            &format!("{}/v2/test/manifests/latest", mock_server.uri()),
        )
        .await;

    assert!(matches!(
        result,
        Err(Error::ManifestBodyTooLarge { limit: 7 })
    ));
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
async fn test_cached_bearer_token_is_used() {
    let mock_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let location = format!("{registry_url}/v2/test/manifests/latest");
    let cache = cache::Config::Memory.to_backend().unwrap();
    let url = Url::parse(&location).unwrap();
    let cache_key = token_cache_key(
        &url,
        "https://auth.example.com/token",
        Some("registry"),
        Some("repository:test:pull"),
    )
    .unwrap();
    cache
        .store_value(&cache_key, "Bearer cached-token", 3600)
        .await
        .unwrap();
    cache
        .store_value(&token_index_cache_key(&url).unwrap(), &cache_key, 3600)
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer cached-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:3333333333333333333333333333333333333333333333333333333333333333",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: registry_url,
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let client = RegistryClient::new(&config, cache).unwrap();
    let result = client.get_manifest(&[], &location).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_bearer_tokens_are_cached_per_scope() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let alpha_location = format!("{registry_url}/v2/alpha/manifests/latest");
    let beta_location = format!("{registry_url}/v2/beta/manifests/latest");

    Mock::given(method("GET"))
        .and(path("/v2/alpha/manifests/latest"))
        .and(|request: &Request| !request.headers.contains_key("authorization"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(
                r#"Bearer realm="{}",service="registry",scope="repository:alpha:pull""#,
                auth_server.uri()
            ),
        ))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/beta/manifests/latest"))
        .and(|request: &Request| !request.headers.contains_key("authorization"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(
                r#"Bearer realm="{}",service="registry",scope="repository:beta:pull""#,
                auth_server.uri()
            ),
        ))
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(query_param("service", "registry"))
        .and(query_param("scope", "repository:alpha:pull"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"token": "token-alpha", "expires_in": 3600})),
        )
        .expect(1)
        .mount(&auth_server)
        .await;

    Mock::given(method("GET"))
        .and(query_param("service", "registry"))
        .and(query_param("scope", "repository:beta:pull"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"token": "token-beta", "expires_in": 3600})),
        )
        .expect(1)
        .mount(&auth_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/alpha/manifests/latest"))
        .and(header("Authorization", "Bearer token-alpha"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .expect(2)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/beta/manifests/latest"))
        .and(header("Authorization", "Bearer token-beta"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: registry_url,
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();

    assert!(client.get_manifest(&[], &alpha_location).await.is_ok());
    assert!(client.get_manifest(&[], &beta_location).await.is_ok());
    assert!(client.get_manifest(&[], &alpha_location).await.is_ok());
}

#[tokio::test]
async fn test_expired_bearer_token_is_refetched() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let location = format!("{registry_url}/v2/test/manifests/latest");
    let cache = cache::Config::Memory.to_backend().unwrap();
    let url = Url::parse(&location).unwrap();
    let cache_key = token_cache_key(
        &url,
        &format!("{}/token", auth_server.uri()),
        Some("registry"),
        Some("repository:test:pull"),
    )
    .unwrap();
    cache
        .store_value(&cache_key, "Bearer stale-token", 0)
        .await
        .unwrap();
    cache
        .store_value(&token_index_cache_key(&url).unwrap(), &cache_key, 3600)
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer stale-token"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&mock_server)
        .await;

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
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(query_param("service", "registry"))
        .and(query_param("scope", "repository:test:pull"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"token": "fresh-token", "expires_in": 3600})),
        )
        .expect(1)
        .mount(&auth_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer fresh-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:4444444444444444444444444444444444444444444444444444444444444444",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: registry_url,
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let client = RegistryClient::new(&config, cache).unwrap();
    let result = client.get_manifest(&[], &location).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_concurrent_bearer_refresh_uses_single_token_exchange() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let location = format!("{registry_url}/v2/test/manifests/latest");

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(|request: &Request| !request.headers.contains_key("authorization"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(
                r#"Bearer realm="{}",service="registry",scope="repository:test:pull""#,
                auth_server.uri()
            ),
        ))
        .expect(10)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(query_param("service", "registry"))
        .and(query_param("scope", "repository:test:pull"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(50))
                .set_body_json(serde_json::json!({"token": "shared-token", "expires_in": 3600})),
        )
        .expect(1)
        .mount(&auth_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer shared-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"{}")
                .insert_header(
                    DOCKER_CONTENT_DIGEST,
                    "sha256:5555555555555555555555555555555555555555555555555555555555555555",
                )
                .insert_header("Content-Type", "application/json"),
        )
        .expect(10)
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        url: registry_url,
        max_redirect: 5,
        server_ca_bundle: None,
        client_certificate: None,
        client_private_key: None,
        username: None,
        password: None,
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::new(&config, cache).unwrap();
    let results = join_all((0..10).map(|_| client.get_manifest(&[], &location))).await;

    assert!(results.iter().all(Result::is_ok));
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
        Err(Error::BlobUnknown) => (),
        _ => panic!("Expected Error::BlobUnknown"),
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
