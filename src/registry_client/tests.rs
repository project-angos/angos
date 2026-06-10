use std::{io::Cursor, time::Duration};

use futures_util::future::join_all;
use url::Url;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{body_bytes, header, method, path, query_param, query_param_is_missing},
};

use crate::{
    cache,
    oci::{Digest, Namespace, Reference},
    registry::{
        DOCKER_CONTENT_DIGEST, Error, OCI_SUBJECT, manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    },
    registry_client::{
        DeleteManifestOutcome, RegistryClient, RegistryClientConfig,
        auth::{token_cache_key, token_index_cache_key},
        get_upstream_namespace,
    },
    replication::{REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP},
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
    let upstream =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let upstream =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client = RegistryClient::from_config(&config, cache, 7).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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

    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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

    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

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
    let result = RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Initialization(_)));
}

#[test]
fn test_registry_client_config_cert_without_key_rejected_at_deserialize() {
    let toml = r#"
        url = "https://example.com"
        client_certificate = "/path/to/cert.pem"
    "#;
    let result: Result<RegistryClientConfig, _> = toml::from_str(toml);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("both client_certificate and client_private_key are required for mTLS"),
        "unexpected error: {msg}"
    );
}

#[test]
fn test_registry_client_config_key_without_cert_rejected_at_deserialize() {
    let toml = r#"
        url = "https://example.com"
        client_private_key = "/path/to/key.pem"
    "#;
    let result: Result<RegistryClientConfig, _> = toml::from_str(toml);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("both client_certificate and client_private_key are required for mTLS"),
        "unexpected error: {msg}"
    );
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
    let result = RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Initialization(_)));
}

/// Builds a no-auth client pointed at `mock_server` (the common setup for the
/// write/discovery wiremock tests below).
fn client_for(mock_server: &MockServer) -> RegistryClient {
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
    RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap()
}

#[tokio::test]
async fn test_blob_upload_sequence() {
    let mock_server = MockServer::start().await;
    let content = b"replicated blob content";
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let session_path = "/v2/test/blobs/uploads/session-1";

    // start_upload: POST returns the session Location.
    Mock::given(method("POST"))
        .and(path("/v2/test/blobs/uploads/"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=a")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // patch_upload: PATCH streams the chunk, returns the next Location.
    Mock::given(method("PATCH"))
        .and(path(session_path))
        .and(body_bytes(content.to_vec()))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=b")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // complete_upload: PUT with the digest query parameter.
    Mock::given(method("PUT"))
        .and(path(session_path))
        .and(query_param("digest", digest))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("", "test");
    assert_eq!(
        start_url,
        format!("{}/v2/test/blobs/uploads/", mock_server.uri())
    );

    let session_url = client.start_upload(&start_url).await.unwrap();
    assert_eq!(
        session_url,
        format!("{}{session_path}?state=a", mock_server.uri())
    );

    let next_url = client
        .patch_upload(
            &session_url,
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(
        next_url,
        format!("{}{session_path}?state=b", mock_server.uri())
    );

    client
        .complete_upload(&next_url, &Digest::try_from(digest).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_patch_upload_401_is_unauthorized_without_retry() {
    // A streamed (single-use) PATCH body cannot be replayed, so a 401 must
    // surface as Error::Unauthorized rather than triggering the byte-body
    // refresh-and-retry path. `.expect(1)` (verified when the MockServer drops)
    // proves exactly one PATCH was sent, i.e. no second attempt.
    let mock_server = MockServer::start().await;
    let content = b"replicated blob content";
    let session_path = "/v2/test/blobs/uploads/session-1";

    Mock::given(method("PATCH"))
        .and(path(session_path))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let session_url = format!("{}{session_path}", mock_server.uri());

    let result = client
        .patch_upload(
            &session_url,
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await;

    assert!(
        matches!(result, Err(Error::Unauthorized(_))),
        "a 401 on a streamed PATCH must be Error::Unauthorized (no replay), got {result:?}"
    );
    // Dropping the server here verifies `.expect(1)`: exactly one PATCH, no retry.
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_returns_none_on_201() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    // The POST must carry the mount query; the server answers 201 (mounted).
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header("Location", format!("/v2/target/blobs/{digest}")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("", "target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await
        .unwrap();

    assert_eq!(
        outcome, None,
        "a 201 to a mount request must report a mount (None — no transfer needed)"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_omits_from_when_none() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    // With from=None the POST carries a bare ?mount=<digest> and no &from=.
    // The server answers 201 (mounted), identical to the from=Some success path.
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param_is_missing("from"))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header("Location", format!("/v2/target/blobs/{digest}")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("", "target");
    let outcome = client
        .mount_blob(&start_url, &Digest::try_from(digest).unwrap(), None)
        .await
        .unwrap();

    assert_eq!(
        outcome, None,
        "a 201 to a from=None mount request must report a mount (None — no transfer needed)"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_falls_back_to_session_on_202() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let session_path = "/v2/target/blobs/uploads/session-9";

    // The server could not satisfy the mount and opened a session instead (202).
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(ResponseTemplate::new(202).insert_header("Location", session_path))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("", "target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Some(format!("{}{session_path}", mock_server.uri())),
        "a 202 to a mount request must fall back to a session with the continuation URL"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_rejects_mismatched_201_digest() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let other_digest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

    // The server answers 201 but advertises a *different* digest than requested.
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, other_digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("", "target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await;

    assert!(
        outcome.is_err(),
        "a 201 advertising a different digest must be rejected, got {outcome:?}"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_accepts_matching_201_digest() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    // The server answers 201 and advertises the *same* digest that was requested.
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("", "target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await
        .unwrap();

    assert_eq!(
        outcome, None,
        "a 201 advertising the requested digest must report a mount (None — no transfer needed)"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_put_manifest_with_oci_subject() {
    let mock_server = MockServer::start().await;
    let manifest = br#"{"schemaVersion":2}"#;
    let digest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let subject = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let media_type = "application/vnd.oci.image.manifest.v1+json";

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Content-Type", media_type))
        .and(body_bytes(manifest.to_vec()))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, digest)
                .insert_header(OCI_SUBJECT, subject),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("latest".to_string()));

    let result = client
        .put_manifest(&location, Some(media_type), manifest.to_vec(), None)
        .await
        .unwrap();

    assert_eq!(result.digest, Some(Digest::try_from(digest).unwrap()));
    assert_eq!(result.subject, Some(subject.to_string()));
    assert!(!result.superseded);
}

#[tokio::test]
async fn test_put_manifest_without_oci_subject() {
    let mock_server = MockServer::start().await;
    let manifest = br#"{"schemaVersion":2}"#;
    let digest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let media_type = "application/vnd.docker.distribution.manifest.v2+json";

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("v1".to_string()));

    let result = client
        .put_manifest(&location, Some(media_type), manifest.to_vec(), None)
        .await
        .unwrap();

    assert_eq!(result.digest, Some(Digest::try_from(digest).unwrap()));
    assert!(
        result.subject.is_none(),
        "OCI-1.0 downstream must not report an OCI-Subject header"
    );
    assert!(!result.superseded);
}

#[tokio::test]
async fn test_put_manifest_stamps_replication_headers() {
    let mock_server = MockServer::start().await;
    let manifest = br#"{"schemaVersion":2}"#;
    let digest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let media_type = "application/vnd.docker.distribution.manifest.v2+json";

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, "2026-06-03T00:00:00Z"))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("v1".to_string()));

    client
        .put_manifest(
            &location,
            Some(media_type),
            manifest.to_vec(),
            Some("2026-06-03T00:00:00Z"),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_put_manifest_superseded_409() {
    let mock_server = MockServer::start().await;
    let body = serde_json::json!({ "errors": [{ "code": REPLICATION_SUPERSEDED_CODE }] });

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .respond_with(ResponseTemplate::new(409).set_body_json(body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("v1".to_string()));

    let result = client
        .put_manifest(&location, Some("application/json"), b"{}".to_vec(), None)
        .await
        .unwrap();
    assert!(result.superseded, "an LWW-superseded 409 sets superseded");
    assert!(result.digest.is_none());
}

#[tokio::test]
async fn test_put_manifest_non_superseded_409_is_error() {
    let mock_server = MockServer::start().await;
    let body = serde_json::json!({ "errors": [{ "code": "CONFLICT" }] });

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .respond_with(ResponseTemplate::new(409).set_body_json(body))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("v1".to_string()));

    let result = client
        .put_manifest(&location, Some("application/json"), b"{}".to_vec(), None)
        .await;
    assert!(matches!(result, Err(Error::Internal(_))));
}

#[tokio::test]
async fn test_delete_manifest_superseded_409() {
    let mock_server = MockServer::start().await;
    let body = serde_json::json!({ "errors": [{ "code": REPLICATION_SUPERSEDED_CODE }] });

    Mock::given(method("DELETE"))
        .and(path("/v2/test/manifests/v1"))
        .respond_with(ResponseTemplate::new(409).set_body_json(body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("v1".to_string()));

    let outcome = client.delete_manifest(&location, None).await.unwrap();
    assert_eq!(outcome, DeleteManifestOutcome::Superseded);
}

#[tokio::test]
async fn test_delete_manifest() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";

    Mock::given(method("DELETE"))
        .and(path(format!("/v2/test/manifests/{digest}")))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path(
        "",
        "test",
        &Reference::Digest(Digest::try_from(digest).unwrap()),
    );

    let outcome = client.delete_manifest(&location, None).await.unwrap();
    assert_eq!(outcome, DeleteManifestOutcome::Deleted);
}

#[tokio::test]
async fn test_delete_manifest_surfaces_error_status() {
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("latest".to_string()));

    let result = client.delete_manifest(&location, None).await;
    assert!(matches!(result, Err(Error::Internal(_))));
}

#[tokio::test]
async fn test_delete_manifest_absent_404_is_already_absent() {
    // An already-absent target (404) is convergence, not failure: a retried or
    // already-converged replication delete must map to AlreadyAbsent, never
    // dead-letter, and distinct from Deleted so the caller records a converged
    // no-op rather than an applied delete.
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v2/test/manifests/gone"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("", "test", &Reference::Tag("gone".to_string()));

    let outcome = client.delete_manifest(&location, None).await.unwrap();
    assert_eq!(outcome, DeleteManifestOutcome::AlreadyAbsent);
}

#[test]
fn test_get_tags_list_path() {
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let path = client.get_tags_list_path("local", "local/repo");
    assert_eq!(path, "https://example.com/v2/repo/tags/list");
}

#[test]
fn test_get_uploads_start_path_strips_local_prefix() {
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
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    // The pull-mirror path strips the local repo-name prefix (unlike the
    // NO_LOCAL_PREFIX identity path the replication write methods exercise).
    let path = client.get_uploads_start_path("local", "local/repo");
    assert_eq!(path, "https://example.com/v2/repo/blobs/uploads/");
}

#[tokio::test]
async fn test_list_tags_single_page() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "test",
            "tags": ["v1", "v2", "v3"],
        })))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_tags_list_path("", "test");

    let tags = client.list_tags(&location).await.unwrap();
    assert_eq!(tags, vec!["v1", "v2", "v3"]);
}

#[tokio::test]
async fn test_list_tags_follows_pagination() {
    let mock_server = MockServer::start().await;

    // Page 1 (no `last`): returns the first tag and a Link rel="next".
    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .and(query_param("n", "1"))
        .and(query_param("last", "a"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "tags": ["b"] })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .and(query_param("n", "1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", "</v2/test/tags/list?n=1&last=a>; rel=\"next\"")
                .set_body_json(serde_json::json!({ "tags": ["a"] })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = format!("{}/v2/test/tags/list?n=1", mock_server.uri());

    let tags = client.list_tags(&location).await.unwrap();
    assert_eq!(
        tags,
        vec!["a", "b"],
        "tags from both pages must be returned"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_list_tags_absent_repo_returns_empty() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/missing/tags/list"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_tags_list_path("", "missing");

    let tags = client.list_tags(&location).await.unwrap();
    assert!(tags.is_empty(), "a 404 repo must yield an empty tag list");
}

#[tokio::test]
async fn list_tags_breaks_on_cyclic_next_link() {
    // A downstream advertising a cyclic `Link: rel="next"` must not drive an
    // unbounded request loop: the visited-page guard stops once a page URL
    // repeats, returning the tags gathered so far rather than hanging.
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "tags": ["a", "b"] }))
                .insert_header("Link", "</v2/test/tags/list?last=z>; rel=\"next\""),
        )
        .expect(2)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let tags = client
        .list_tags(&client.get_tags_list_path("", "test"))
        .await
        .expect("cyclic pagination must terminate, not error");

    // The base page and the single `?last=z` page are fetched, then the repeated
    // `?last=z` breaks the loop, never an unbounded fetch (asserted by expect(2)).
    assert_eq!(tags, vec!["a", "b", "a", "b"]);
}
