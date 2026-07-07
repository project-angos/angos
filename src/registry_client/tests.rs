use std::{io::Cursor, time::Duration};

use futures_util::future::join_all;
use url::Url;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{body_bytes, header, method, path, query_param, query_param_is_missing},
};

use crate::{
    cache,
    oci::{Digest, MediaType, Reference, Tag},
    registry::{DOCKER_CONTENT_DIGEST, OCI_SUBJECT, manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES},
    registry_client::{
        DeleteManifestOutcome, Error, REPLICATION_SUPERSEDED_CODE, RegistryClient,
        RegistryClientConfig, X_ANGOS_SOURCE_TIMESTAMP,
        auth::{token_cache_key, token_index_cache_key},
    },
    secret::Secret,
    test_fixtures::client::test_client_config,
};

/// Builds a no-auth client pointed at `mock_server`.
fn client_for(mock_server: &MockServer) -> RegistryClient {
    let config = test_client_config(mock_server.uri());
    let cache = cache::Config::Memory.to_backend().unwrap();
    RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap()
}

#[test]
fn test_get_manifest_path() {
    let config = RegistryClientConfig {
        username: Some("username".to_string()),
        password: Some(Secret::new("password".to_string())),
        ..test_client_config("https://example.com")
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let upstream =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let reference = Reference::Tag(Tag::new("latest").unwrap());

    let path = upstream.get_manifest_path("repo", &reference);
    assert_eq!(path, "https://example.com/v2/repo/manifests/latest");
}

#[test]
fn test_get_blob_path() {
    let config = test_client_config("https://example.com");

    let cache = cache::Config::Memory.to_backend().unwrap();
    let upstream =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let digest =
        Digest::try_from("sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
            .unwrap();

    let path = upstream.get_blob_path("repo", &digest);
    assert_eq!(
        path,
        "https://example.com/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
}

#[test]
fn test_new_with_username_only() {
    let config = RegistryClientConfig {
        username: Some("user".to_string()),
        ..test_client_config("https://example.com")
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
    assert!(client.basic_auth.is_none());
}

#[test]
fn test_new_with_password_only() {
    let config = RegistryClientConfig {
        password: Some(Secret::new("pass".to_string())),
        ..test_client_config("https://example.com")
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
    assert!(client.basic_auth.is_none());
}

#[test]
fn test_new_with_both_credentials() {
    let config = RegistryClientConfig {
        username: Some("user".to_string()),
        password: Some(Secret::new("pass".to_string())),
        ..test_client_config("https://example.com")
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

    let client = client_for(&mock_server);

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

    let client = client_for(&mock_server);

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
async fn test_blob_exists_true_when_digest_header_absent() {
    // The OCI spec makes Docker-Content-Digest a SHOULD on blob HEAD; a 2xx
    // without it must read as present, not as a probe failure that would
    // dead-letter a converged push.
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/test/blobs/{test_digest}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let present = client
        .blob_exists(&format!(
            "{}/v2/test/blobs/{test_digest}",
            mock_server.uri()
        ))
        .await
        .unwrap();
    assert!(
        present,
        "a 2xx HEAD without a digest header must read as present"
    );
}

#[tokio::test]
async fn test_blob_exists_false_on_404() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/v2/test/blobs/sha256:absent"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let present = client
        .blob_exists(&format!(
            "{}/v2/test/blobs/sha256:absent",
            mock_server.uri()
        ))
        .await
        .unwrap();
    assert!(!present, "a 404 HEAD must read as absent");
}

#[tokio::test]
async fn test_blob_exists_errors_on_server_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/v2/test/blobs/sha256:boom"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let result = client
        .blob_exists(&format!("{}/v2/test/blobs/sha256:boom", mock_server.uri()))
        .await;
    assert!(
        result.is_err(),
        "a transient 5xx must fail the probe so the job retries instead of re-uploading"
    );
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

    let client = client_for(&mock_server);

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
        Some(MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap())
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

    let client = client_for(&mock_server);

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
        Some(MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap())
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

    let config = test_client_config(mock_server.uri());

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

    let client = client_for(&mock_server);

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

    let config = test_client_config(registry_url);

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

    let client = client_for(&mock_server);

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

    let config = test_client_config(registry_url);

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

    let client = client_for(&mock_server);
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
        username: Some("user".to_string()),
        password: Some(Secret::new("pass".to_string())),
        ..test_client_config(mock_server.uri())
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

    let client = client_for(&mock_server);

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

    let client = client_for(&mock_server);

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

    let client = client_for(&mock_server);

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
        server_ca_bundle: Some("/nonexistent/ca.pem".to_string()),
        ..test_client_config("https://example.com")
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
        client_certificate: Some("/nonexistent/cert.pem".to_string()),
        client_private_key: Some("/nonexistent/key.pem".to_string()),
        ..test_client_config("https://example.com")
    };

    let cache = cache::Config::Memory.to_backend().unwrap();
    let result = RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Initialization(_)));
}

#[tokio::test]
async fn test_blob_upload_sequence() {
    let mock_server = MockServer::start().await;
    let content = b"replicated blob content";
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let session_path = "/v2/test/blobs/uploads/session-1";

    Mock::given(method("POST"))
        .and(path("/v2/test/blobs/uploads/"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=a")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("PATCH"))
        .and(path(session_path))
        .and(body_bytes(content.to_vec()))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=b")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path(session_path))
        .and(query_param("digest", digest))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("test");
    assert_eq!(
        start_url,
        format!("{}/v2/test/blobs/uploads/", mock_server.uri())
    );

    let session = client.start_upload(&start_url).await.unwrap();
    assert_eq!(
        session.url,
        format!("{}{session_path}?state=a", mock_server.uri())
    );

    let next_url = client
        .patch_upload(
            &session.url,
            session.auth.as_deref(),
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
    // A single-use streamed body cannot be replayed, so a 401 must not trigger
    // the refresh-and-retry path (`.expect(1)` proves a single PATCH).
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
            None,
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await;

    assert!(
        matches!(result, Err(Error::Unauthorized(_))),
        "a 401 on a streamed PATCH must be Error::Unauthorized (no replay), got {result:?}"
    );
    // Dropping the server verifies `.expect(1)`: exactly one PATCH, no retry.
    drop(mock_server);
}

#[tokio::test]
async fn test_blob_upload_reuses_session_open_bearer_token_on_patch() {
    // Regression: the streamed PATCH targets a server-assigned session URL that
    // never issues its own auth challenge, so it must reuse the bearer token the
    // start-upload POST resolved. The PATCH mock requires that token, so an
    // unauthenticated PATCH (the pre-fix behaviour) would not match and fail.
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let content = b"replicated blob content";
    let start_path = "/v2/test/blobs/uploads/";
    let session_path = "/v2/test/blobs/uploads/session-7";

    Mock::given(method("POST"))
        .and(path(start_path))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(
                r#"Bearer realm="{}",service="registry",scope="repository:test:pull,push""#,
                auth_server.uri()
            ),
        ))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(start_path))
        .and(header("Authorization", "Bearer push-token"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=a")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("service", "registry"))
        .and(query_param("scope", "repository:test:pull,push"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"token": "push-token", "expires_in": 3600})),
        )
        .mount(&auth_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(session_path))
        .and(header("Authorization", "Bearer push-token"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=b")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("test");
    let session = client.start_upload(&start_url).await.unwrap();
    assert_eq!(session.auth.as_deref(), Some("Bearer push-token"));
    let next_url = client
        .patch_upload(
            &session.url,
            session.auth.as_deref(),
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(
        next_url,
        format!("{}{session_path}?state=b", mock_server.uri())
    );
}

#[tokio::test]
async fn test_blob_upload_reuses_basic_credentials_on_patch() {
    // Basic credentials are never cached, so the streamed PATCH must carry the
    // header resolved when the session was opened.
    let mock_server = MockServer::start().await;
    let content = b"replicated blob content";
    let start_path = "/v2/test/blobs/uploads/";
    let session_path = "/v2/test/blobs/uploads/session-8";

    Mock::given(method("POST"))
        .and(path(start_path))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("WWW-Authenticate", "Basic realm=\"Registry\""),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path(start_path))
        .and(header("Authorization", "Basic dXNlcjpwYXNz"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=a")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(session_path))
        .and(header("Authorization", "Basic dXNlcjpwYXNz"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=b")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = RegistryClientConfig {
        username: Some("user".to_string()),
        password: Some(Secret::new("pass".to_string())),
        ..test_client_config(mock_server.uri())
    };
    let cache = cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let start_url = client.get_uploads_start_path("test");
    let session = client.start_upload(&start_url).await.unwrap();
    assert_eq!(session.auth.as_deref(), Some("Basic dXNlcjpwYXNz"));
    let next_url = client
        .patch_upload(
            &session.url,
            session.auth.as_deref(),
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(
        next_url,
        format!("{}{session_path}?state=b", mock_server.uri())
    );
}

#[tokio::test]
async fn test_mount_blob_returns_none_on_201() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

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
    let start_url = client.get_uploads_start_path("target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await
        .unwrap();

    assert!(
        outcome.is_none(),
        "a 201 to a mount request must report a mount (None: no transfer needed)"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_omits_from_when_none() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

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
    let start_url = client.get_uploads_start_path("target");
    let outcome = client
        .mount_blob(&start_url, &Digest::try_from(digest).unwrap(), None)
        .await
        .unwrap();

    assert!(
        outcome.is_none(),
        "a 201 to a from=None mount request must report a mount (None: no transfer needed)"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_falls_back_to_session_on_202() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let session_path = "/v2/target/blobs/uploads/session-9";

    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(ResponseTemplate::new(202).insert_header("Location", session_path))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await
        .unwrap();

    assert_eq!(
        outcome.map(|session| session.url),
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

    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, other_digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await;

    assert!(
        outcome.is_err(),
        "a 201 advertising a different digest must be rejected"
    );
    drop(mock_server);
}

#[tokio::test]
async fn test_mount_blob_accepts_matching_201_digest() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let start_url = client.get_uploads_start_path("target");
    let outcome = client
        .mount_blob(
            &start_url,
            &Digest::try_from(digest).unwrap(),
            Some("source"),
        )
        .await
        .unwrap();

    assert!(
        outcome.is_none(),
        "a 201 advertising the requested digest must report a mount (None: no transfer needed)"
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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("latest").unwrap()));

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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("v1").unwrap()));

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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("v1").unwrap()));

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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("v1").unwrap()));

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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("v1").unwrap()));

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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("v1").unwrap()));

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
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("latest").unwrap()));

    let result = client.delete_manifest(&location, None).await;
    assert!(matches!(result, Err(Error::Internal(_))));
}

#[tokio::test]
async fn test_delete_manifest_absent_404_is_already_absent() {
    // A retried or already-converged delete must converge as AlreadyAbsent,
    // never dead-letter.
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v2/test/manifests/gone"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("gone").unwrap()));

    let outcome = client.delete_manifest(&location, None).await.unwrap();
    assert_eq!(outcome, DeleteManifestOutcome::AlreadyAbsent);
}

#[tokio::test]
async fn test_delete_manifest_405_is_unsupported() {
    // Stock distribution rejects tag deletion with 405; it must read as
    // Unsupported (not a retryable error) so a delete never dead-letters one
    // job per deletion event against such a downstream.
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v2/test/manifests/v1"))
        .respond_with(ResponseTemplate::new(405))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let location = client.get_manifest_path("test", &Reference::Tag(Tag::new("v1").unwrap()));

    let outcome = client.delete_manifest(&location, None).await.unwrap();
    assert_eq!(outcome, DeleteManifestOutcome::Unsupported);
}

#[tokio::test]
async fn test_head_blob_5xx_is_transient_error_not_unknown() {
    // BlobUnknown means "absent (404)"; a 503 must surface as a transient
    // error so replication retries instead of starting a full upload.
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    Mock::given(method("HEAD"))
        .and(path(format!("/v2/test/blobs/{digest}")))
        .respond_with(ResponseTemplate::new(503))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let result = client
        .head_blob(
            &[],
            &format!("{}/v2/test/blobs/{digest}", mock_server.uri()),
        )
        .await;

    assert!(
        matches!(result, Err(Error::Internal(_))),
        "a 503 blob HEAD must be a transient error, not BlobUnknown, got {result:?}"
    );
}

#[tokio::test]
async fn test_delete_upload_204_and_404_are_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v2/test/blobs/uploads/s1"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v2/test/blobs/uploads/s2"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    client
        .delete_upload(&format!("{}/v2/test/blobs/uploads/s1", mock_server.uri()))
        .await
        .expect("a 204 session cancel must succeed");
    client
        .delete_upload(&format!("{}/v2/test/blobs/uploads/s2", mock_server.uri()))
        .await
        .expect("a 404 session cancel must succeed (already gone is the goal state)");
    drop(mock_server);
}

#[tokio::test]
async fn test_delete_upload_500_is_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v2/test/blobs/uploads/s1"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let result = client
        .delete_upload(&format!("{}/v2/test/blobs/uploads/s1", mock_server.uri()))
        .await;

    assert!(
        matches!(result, Err(Error::Internal(_))),
        "a 500 session cancel must surface as an error, got {result:?}"
    );
    drop(mock_server);
}

#[test]
fn test_get_tags_list_path() {
    let config = test_client_config("https://example.com");
    let cache = cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let path = client.get_tags_list_path("repo");
    assert_eq!(path, "https://example.com/v2/repo/tags/list");
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
    let location = client.get_tags_list_path("test");

    let tags = client.list_tags(&location).await.unwrap();
    assert_eq!(
        tags,
        vec![
            Tag::new("v1").unwrap(),
            Tag::new("v2").unwrap(),
            Tag::new("v3").unwrap(),
        ]
    );
}

#[tokio::test]
async fn test_list_tags_follows_pagination() {
    let mock_server = MockServer::start().await;

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
        vec![Tag::new("a").unwrap(), Tag::new("b").unwrap()],
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
    let location = client.get_tags_list_path("missing");

    let tags = client.list_tags(&location).await.unwrap();
    assert!(tags.is_empty(), "a 404 repo must yield an empty tag list");
}

#[tokio::test]
async fn list_tags_breaks_on_cyclic_next_link() {
    // The visited-page guard must return the tags gathered so far, not hang.
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
        .list_tags(&client.get_tags_list_path("test"))
        .await
        .expect("cyclic pagination must terminate, not error");

    // expect(2): the base page and one `?last=z` page are fetched before the
    // repeated `?last=z` breaks the loop.
    assert_eq!(
        tags,
        vec![
            Tag::new("a").unwrap(),
            Tag::new("b").unwrap(),
            Tag::new("a").unwrap(),
            Tag::new("b").unwrap(),
        ]
    );
}
