use std::{io::Cursor, path::PathBuf, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use serde_json::json;
use tracing::Level;
use url::Url;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{body_bytes, header, method, path, query_param, query_param_is_missing},
};

use angos_oci::header::{DOCKER_CONTENT_DIGEST, OCI_SUBJECT};
use angos_oci::request::{
    BlobMount, DeleteManifestRequest, GetBlobRequest, GetManifestRequest, HeadBlobRequest,
    HeadManifestRequest, ListTagsRequest, MountBlobRequest, PutManifestRequest, StartUploadRequest,
};
use angos_oci::response::{DeleteManifestOutcome, PutManifestOutcome};
use angos_oci::{Digest, MediaType, Namespace, Reference, Tag};

use crate::{
    Error, MtlsIdentity, REPLICATION_SUPERSEDED_CODE, RegistryClient, RegistryClientConfig,
    X_ANGOS_SOURCE_TIMESTAMP,
    auth::{token_cache_key, token_index_cache_key},
    without_query,
};
use angos_secret::Secret;

/// The registry's default; inlined so the client crate's tests do not reach
/// into the server-side manifest module.
const DEFAULT_MAX_MANIFEST_SIZE_BYTES: usize = 5 * 1024 * 1024;

fn test_client_config(url: impl Into<String>) -> RegistryClientConfig {
    RegistryClientConfig {
        url: url.into(),
        max_redirect: 5,
        connect_timeout_secs: 30,
        read_timeout_secs: 300,
        server_ca_bundle: None,
        mtls: None,
        username: None,
        password: None,
    }
}

/// Collects tracing output into a shared buffer; every clone appends to the
/// same buffer, so a clone passed to `with_writer` is read back via `contents`.
#[derive(Clone, Default)]
struct LogCapture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl LogCapture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// The blob the scope-cache test fetches; its URL is also what the cache keys
/// are derived from, so both sides read the digest from here.
const SCOPE_CACHE_DIGEST: &str =
    "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

/// The origin timestamp a replication write stamps. The header carries whatever
/// `to_rfc3339` renders it as, which is what the sender puts on the wire.
fn source_ts() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-06-03T00:00:00Z")
        .expect("fixture timestamp must be RFC 3339")
        .with_timezone(&Utc)
}

/// Builds a no-auth client pointed at `mock_server`.
fn client_for(mock_server: &MockServer) -> RegistryClient {
    let config = test_client_config(mock_server.uri());
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap()
}

#[test]
fn test_new_with_username_only() {
    let config = RegistryClientConfig {
        username: Some("user".to_string()),
        ..test_client_config("https://example.com")
    };

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
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

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();
    assert!(client.basic_auth.is_some());
}

// The client (and everything holding it, like a replication downstream) is
// Debug-formatted in logs; the configured password must never appear there.
#[test]
fn debug_output_redacts_basic_auth_password() {
    let config = RegistryClientConfig {
        username: Some("user".to_string()),
        password: Some(Secret::new("hunter2".to_string())),
        ..test_client_config("https://example.com")
    };

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let debug = format!("{client:?}");
    assert!(
        !debug.contains("hunter2"),
        "Debug must not leak the password, got: {debug}"
    );
    assert!(debug.contains("[REDACTED]"));
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
        .head_blob(HeadBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(test_digest).unwrap(),
            accepted_types: Vec::new(),
        })
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
        .and(path("/v2/test/blobs/sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let result = client
        .head_blob(HeadBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            )
            .unwrap(),
            accepted_types: Vec::new(),
        })
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
        .blob_exists(HeadBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(test_digest).unwrap(),
            accepted_types: Vec::new(),
        })
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
        .and(path("/v2/test/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let present = client
        .blob_exists(HeadBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap(),
            accepted_types: Vec::new(),
        })
        .await
        .unwrap();
    assert!(!present, "a 404 HEAD must read as absent");
}

#[tokio::test]
async fn test_blob_exists_errors_on_server_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/v2/test/blobs/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let result = client
        .blob_exists(HeadBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )
            .unwrap(),
            accepted_types: Vec::new(),
        })
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
        .head_manifest(HeadManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await;

    assert!(result.is_ok());
    let head = result.unwrap();
    assert_eq!(
        head.media_type,
        Some(MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap())
    );
    assert_eq!(head.digest, Some(Digest::try_from(test_digest).unwrap()));
    assert_eq!(head.size, 5678);
}

/// `Docker-Content-Digest` is a SHOULD, so a conformant upstream may omit it.
/// A HEAD carries no body to recompute it from, so the digest reads as unknown
/// instead of failing the whole probe.
#[tokio::test]
async fn head_manifest_without_content_digest_reports_an_unknown_digest() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    "Content-Type",
                    "application/vnd.docker.distribution.manifest.v2+json",
                )
                .insert_header("Content-Length", "5678"),
        )
        .mount(&mock_server)
        .await;

    let head = client_for(&mock_server)
        .head_manifest(HeadManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await
        .expect("a manifest HEAD without Docker-Content-Digest must still succeed");

    assert_eq!(head.digest, None);
    assert_eq!(
        head.media_type,
        Some(MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap())
    );
    assert_eq!(head.size, 5678);
}

/// The GET counterpart: the digest is unknown to the client, and the body it
/// returns is what the caller hashes to recover it.
#[tokio::test]
async fn get_manifest_without_content_digest_still_returns_the_body() {
    let mock_server = MockServer::start().await;
    let manifest_body = b"{\"schemaVersion\":2}";

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(manifest_body)
                .insert_header(
                    "Content-Type",
                    "application/vnd.docker.distribution.manifest.v2+json",
                ),
        )
        .mount(&mock_server)
        .await;

    let fetched = client_for(&mock_server)
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await
        .expect("a manifest GET without Docker-Content-Digest must still succeed");

    assert_eq!(fetched.digest, None);
    assert_eq!(fetched.body, manifest_body);
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
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await;

    assert!(result.is_ok());
    let fetched = result.unwrap();
    assert_eq!(
        fetched.media_type,
        Some(MediaType::new("application/vnd.docker.distribution.manifest.v2+json").unwrap())
    );
    assert_eq!(fetched.digest, Some(Digest::try_from(test_digest).unwrap()));
    assert_eq!(fetched.body, manifest_body);
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

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let client = RegistryClient::from_config(&config, cache, 7).unwrap();

    let result = client
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
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
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await;

    assert!(result.is_ok());
}

/// A scope-cache hit must index the URL it served. Only the token exchange
/// wrote that entry, so every other URL sharing the scope re-discovered the
/// token through a 401 on every single request.
#[tokio::test]
async fn a_scope_cache_hit_indexes_the_url_it_served() {
    let mock_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let location = format!("{registry_url}/v2/test/blobs/{SCOPE_CACHE_DIGEST}");
    let url = Url::parse(&location).unwrap();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    // The scope token is cached, but this URL has never been indexed: the
    // state left behind when some other URL triggered the exchange.
    let cache_key = token_cache_key(
        &url,
        &format!("{registry_url}/token"),
        Some("registry"),
        Some("repository:test:pull"),
        None,
    )
    .unwrap();
    cache
        .store_value(&cache_key, "Bearer scope-token", 3600)
        .await
        .unwrap();
    let index_key = token_index_cache_key(&url, None).unwrap();
    assert!(cache.retrieve_value(&index_key).await.unwrap().is_none());

    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{SCOPE_CACHE_DIGEST}")))
        .and(header("Authorization", "Bearer scope-token"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"blob"))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{SCOPE_CACHE_DIGEST}")))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(
                r#"Bearer realm="{registry_url}/token",service="registry",scope="repository:test:pull""#
            )
            .as_str(),
        ))
        .mount(&mock_server)
        .await;

    let config = test_client_config(registry_url);
    let client =
        RegistryClient::from_config(&config, cache.clone(), DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .unwrap();
    client
        .get_blob(GetBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(SCOPE_CACHE_DIGEST).unwrap(),
            accepted_types: Vec::new(),
            range: None,
        })
        .await
        .expect("blob fetch");

    assert_eq!(
        cache.retrieve_value(&index_key).await.unwrap().as_deref(),
        Some(cache_key.as_str()),
        "the served URL must be indexed so the next request skips the 401"
    );
}

#[tokio::test]
async fn test_cached_bearer_token_is_used() {
    let mock_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let location = format!("{registry_url}/v2/test/manifests/latest");
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let url = Url::parse(&location).unwrap();
    let cache_key = token_cache_key(
        &url,
        "https://auth.example.com/token",
        Some("registry"),
        Some("repository:test:pull"),
        None,
    )
    .unwrap();
    cache
        .store_value(&cache_key, "Bearer cached-token", 3600)
        .await
        .unwrap();
    cache
        .store_value(
            &token_index_cache_key(&url, None).unwrap(),
            &cache_key,
            3600,
        )
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
    let result = client
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_bearer_tokens_are_cached_per_scope() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let alpha_namespace = Namespace::new("alpha").unwrap();
    let beta_namespace = Namespace::new("beta").unwrap();
    let latest = Reference::Tag(Tag::new("latest").unwrap());

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

    let fetch = async |namespace: &Namespace| {
        client
            .get_manifest(GetManifestRequest {
                namespace: namespace.clone(),
                reference: latest.clone(),
                accepted_types: Vec::new(),
            })
            .await
    };
    // Alpha twice: the second read must be served by the cached scope token,
    // which the `expect(1)` on the 401 mock pins.
    assert!(fetch(&alpha_namespace).await.is_ok());
    assert!(fetch(&beta_namespace).await.is_ok());
    assert!(fetch(&alpha_namespace).await.is_ok());
}

#[tokio::test]
async fn test_expired_bearer_token_is_refetched() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let location = format!("{registry_url}/v2/test/manifests/latest");
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let url = Url::parse(&location).unwrap();
    let cache_key = token_cache_key(
        &url,
        &format!("{}/token", auth_server.uri()),
        Some("registry"),
        Some("repository:test:pull"),
        None,
    )
    .unwrap();
    cache
        .store_value(&cache_key, "Bearer stale-token", 0)
        .await
        .unwrap();
    cache
        .store_value(
            &token_index_cache_key(&url, None).unwrap(),
            &cache_key,
            3600,
        )
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
    let result = client
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_concurrent_bearer_refresh_uses_single_token_exchange() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;

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
    let namespace = Namespace::new("test").unwrap();
    let reference = Reference::Tag(Tag::new("latest").unwrap());
    let results = join_all((0..10).map(|_| {
        client.get_manifest(GetManifestRequest {
            namespace: namespace.clone(),
            reference: reference.clone(),
            accepted_types: Vec::new(),
        })
    }))
    .await;

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

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let result = client
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await;

    assert!(result.is_ok());
}

/// The normal scope-denied token flow: the first attempt 401s, the refreshed
/// retry 403s. Returning that raw made reads report `Internal` while writes
/// reported `Denied`, and replication retried to max attempts instead of
/// dead-lettering on the `Denied` fast path.
#[tokio::test]
async fn a_forbidden_response_after_a_token_refresh_is_denied() {
    let mock_server = MockServer::start().await;
    let registry_url = mock_server.uri();

    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "token": "scoped" })))
        .mount(&mock_server)
        .await;
    // Authenticated: the credential is valid but lacks the scope.
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer scoped"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(r#"Bearer realm="{registry_url}/token",service="registry""#).as_str(),
        ))
        .mount(&mock_server)
        .await;

    let error = client_for(&mock_server)
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await
        .expect_err("a 403 must not read as a successful fetch");

    assert!(
        matches!(error, Error::Denied(_)),
        "a scope denial must be terminal, got: {error:?}"
    );
}

/// A token obtained moments ago will not become valid on another attempt, so a
/// persistent 401 is terminal rather than a raw response for a read path to
/// mislabel.
#[tokio::test]
async fn a_persistent_unauthorized_after_a_refresh_is_terminal() {
    let mock_server = MockServer::start().await;
    let registry_url = mock_server.uri();

    Mock::given(method("GET"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "token": "scoped" })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "WWW-Authenticate",
            format!(r#"Bearer realm="{registry_url}/token",service="registry""#).as_str(),
        ))
        .mount(&mock_server)
        .await;

    let error = client_for(&mock_server)
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
        .await
        .expect_err("a persistent 401 must not read as a successful fetch");

    assert!(
        matches!(error, Error::Unauthorized(_)),
        "a rejected fresh token must be terminal, got: {error:?}"
    );
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
        .get_manifest(GetManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            accepted_types: Vec::new(),
        })
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
        .get_blob(GetBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(test_digest).unwrap(),
            accepted_types: Vec::new(),
            range: None,
        })
        .await;

    assert!(result.is_ok());
    let fetched = result.unwrap();
    assert_eq!(fetched.length, blob_data.len() as u64);
    let mut reader = fetched.reader;

    let mut buffer = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer)
        .await
        .unwrap();
    assert_eq!(buffer, blob_data);
}

#[tokio::test]
async fn get_blob_reports_an_upstream_outage_as_transient_not_missing() {
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{test_digest}")))
        .respond_with(ResponseTemplate::new(503))
        .mount(&mock_server)
        .await;

    let error = client_for(&mock_server)
        .get_blob(GetBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(test_digest).unwrap(),
            accepted_types: Vec::new(),
            range: None,
        })
        .await
        .err()
        .expect("a 503 must not read as a successful fetch");

    assert!(
        matches!(error, Error::Internal(_)),
        "an upstream outage must not be served as a missing blob, got: {error:?}"
    );
}

#[tokio::test]
async fn test_get_blob_not_found() {
    let mock_server = MockServer::start().await;
    let test_digest = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{test_digest}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let result = client
        .get_blob(GetBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(test_digest).unwrap(),
            accepted_types: Vec::new(),
            range: None,
        })
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
        server_ca_bundle: Some(PathBuf::from("/nonexistent/ca.pem")),
        ..test_client_config("https://example.com")
    };

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
        mtls: Some(MtlsIdentity {
            client_certificate: PathBuf::from("/nonexistent/cert.pem"),
            client_private_key: PathBuf::from("/nonexistent/key.pem"),
        }),
        ..test_client_config("https://example.com")
    };

    let cache = angos_cache::Config::Memory.to_backend().unwrap();
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
    let namespace = Namespace::new("test").unwrap();
    let session = client
        .start_upload(StartUploadRequest {
            namespace: namespace.clone(),
            digest_algorithm: None,
            target: None,
        })
        .await
        .unwrap();
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
async fn start_upload_refuses_a_cross_origin_location() {
    // A Location on another host would carry the upload's auth off-registry.
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/test/blobs/uploads/"))
        .respond_with(ResponseTemplate::new(202).insert_header(
            "Location",
            "https://evil.example/v2/test/blobs/uploads/session-1",
        ))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let outcome = client
        .start_upload(StartUploadRequest {
            namespace: Namespace::new("test").unwrap(),
            digest_algorithm: None,
            target: None,
        })
        .await
        .map(|_| ());
    assert!(
        matches!(outcome, Err(Error::Denied(_))),
        "a cross-origin upload Location must be refused: {outcome:?}"
    );
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
    let session = client
        .start_upload(StartUploadRequest {
            namespace: Namespace::new("test").unwrap(),
            digest_algorithm: None,
            target: None,
        })
        .await
        .unwrap();
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
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap();

    let session = client
        .start_upload(StartUploadRequest {
            namespace: Namespace::new("test").unwrap(),
            digest_algorithm: None,
            target: None,
        })
        .await
        .unwrap();
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
    let outcome = client
        .mount_blob(MountBlobRequest {
            namespace: Namespace::new("target").unwrap(),
            mount: BlobMount {
                digest: Digest::try_from(digest).unwrap(),
                from: Some(Namespace::new("source").unwrap()),
            },
        })
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
    let outcome = client
        .mount_blob(MountBlobRequest {
            namespace: Namespace::new("target").unwrap(),
            mount: BlobMount {
                digest: Digest::try_from(digest).unwrap(),
                from: None,
            },
        })
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
    let outcome = client
        .mount_blob(MountBlobRequest {
            namespace: Namespace::new("target").unwrap(),
            mount: BlobMount {
                digest: Digest::try_from(digest).unwrap(),
                from: Some(Namespace::new("source").unwrap()),
            },
        })
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
    let outcome = client
        .mount_blob(MountBlobRequest {
            namespace: Namespace::new("target").unwrap(),
            mount: BlobMount {
                digest: Digest::try_from(digest).unwrap(),
                from: Some(Namespace::new("source").unwrap()),
            },
        })
        .await;

    assert!(
        outcome.is_err(),
        "a 201 advertising a different digest must be rejected"
    );
    drop(mock_server);
}

/// A garbage digest header must not read as an absent one: doing so would skip
/// the mismatch rejection entirely, letting any unparseable value pass a blob
/// off as mounted.
#[tokio::test]
async fn mount_blob_rejects_an_unparseable_201_digest() {
    let mock_server = MockServer::start().await;
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", digest))
        .and(query_param("from", "source"))
        .respond_with(
            ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, "not-a-digest"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let outcome = client
        .mount_blob(MountBlobRequest {
            namespace: Namespace::new("target").unwrap(),
            mount: BlobMount {
                digest: Digest::try_from(digest).unwrap(),
                from: Some(Namespace::new("source").unwrap()),
            },
        })
        .await;

    assert!(
        outcome.is_err(),
        "a 201 advertising an unparseable digest must be rejected, not accepted as a mount"
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
    let outcome = client
        .mount_blob(MountBlobRequest {
            namespace: Namespace::new("target").unwrap(),
            mount: BlobMount {
                digest: Digest::try_from(digest).unwrap(),
                from: Some(Namespace::new("source").unwrap()),
            },
        })
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
    let result = client
        .put_manifest(
            PutManifestRequest {
                namespace: Namespace::new("test").unwrap(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(MediaType::new(media_type).unwrap()),
                tags: Vec::new(),
                source_ts: None,
            },
            manifest.to_vec(),
        )
        .await
        .unwrap();

    let PutManifestOutcome::Stored {
        digest: echoed,
        subject: echoed_subject,
    } = result
    else {
        panic!("a 2xx push must report the manifest as stored, got {result:?}");
    };
    assert_eq!(echoed, Some(Digest::try_from(digest).unwrap()));
    assert_eq!(echoed_subject, Some(subject.to_string()));
}

/// A push the downstream accepted must not fail over a bad echo, but the echo
/// is what the divergence check reads, so dropping it has to be visible rather
/// than silently reported as no digest at all.
#[tokio::test]
async fn put_manifest_warns_on_an_unparseable_advertised_digest() {
    let mock_server = MockServer::start().await;
    let manifest = br#"{"schemaVersion":2}"#;
    let media_type = "application/vnd.oci.image.manifest.v1+json";

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(
            ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, "not-a-digest"),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let log_capture = LogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::WARN)
        .with_writer(log_capture.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);

    let result = client
        .put_manifest(
            PutManifestRequest {
                namespace: Namespace::new("test").unwrap(),
                reference: Reference::Tag(Tag::new("latest").unwrap()),
                content_type: Some(MediaType::new(media_type).unwrap()),
                tags: Vec::new(),
                source_ts: None,
            },
            manifest.to_vec(),
        )
        .await
        .expect("an unparseable echo must not fail a push the downstream accepted");

    drop(guard);

    assert!(
        matches!(result, PutManifestOutcome::Stored { digest: None, .. }),
        "an unparseable echo must drop the digest, got {result:?}"
    );
    let logs = log_capture.contents();
    assert!(
        logs.contains("put_manifest") && logs.contains(DOCKER_CONTENT_DIGEST.as_str()),
        "the dropped digest echo must be logged, logs were: {logs}"
    );
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
    let result = client
        .put_manifest(
            PutManifestRequest {
                namespace: Namespace::new("test").unwrap(),
                reference: Reference::Tag(Tag::new("v1").unwrap()),
                content_type: Some(MediaType::new(media_type).unwrap()),
                tags: Vec::new(),
                source_ts: None,
            },
            manifest.to_vec(),
        )
        .await
        .unwrap();

    let PutManifestOutcome::Stored {
        digest: echoed,
        subject,
    } = result
    else {
        panic!("a 2xx push must report the manifest as stored, got {result:?}");
    };
    assert_eq!(echoed, Some(Digest::try_from(digest).unwrap()));
    assert!(
        subject.is_none(),
        "OCI-1.0 downstream must not report an OCI-Subject header"
    );
}

#[tokio::test]
async fn test_put_manifest_stamps_replication_headers() {
    let mock_server = MockServer::start().await;
    let manifest = br#"{"schemaVersion":2}"#;
    let digest = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let media_type = "application/vnd.docker.distribution.manifest.v2+json";

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, source_ts().to_rfc3339()))
        .respond_with(ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, digest))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    client
        .put_manifest(
            PutManifestRequest {
                namespace: Namespace::new("test").unwrap(),
                reference: Reference::Tag(Tag::new("v1").unwrap()),
                content_type: Some(MediaType::new(media_type).unwrap()),
                tags: Vec::new(),
                source_ts: Some(source_ts()),
            },
            manifest.to_vec(),
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
    let result = client
        .put_manifest(
            PutManifestRequest {
                namespace: Namespace::new("test").unwrap(),
                reference: Reference::Tag(Tag::new("v1").unwrap()),
                content_type: Some(MediaType::new("application/json").unwrap()),
                tags: Vec::new(),
                source_ts: None,
            },
            b"{}".to_vec(),
        )
        .await
        .unwrap();
    assert!(
        matches!(result, PutManifestOutcome::Superseded),
        "an LWW-superseded 409 must converge rather than report a store, got {result:?}"
    );
}

#[tokio::test]
async fn test_put_manifest_non_superseded_409_is_error() {
    let mock_server = MockServer::start().await;
    let body = serde_json::json!({ "errors": [{ "code": "DENIED" }] });

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .respond_with(ResponseTemplate::new(409).set_body_json(body))
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let result = client
        .put_manifest(
            PutManifestRequest {
                namespace: Namespace::new("test").unwrap(),
                reference: Reference::Tag(Tag::new("v1").unwrap()),
                content_type: Some(MediaType::new("application/json").unwrap()),
                tags: Vec::new(),
                source_ts: None,
            },
            b"{}".to_vec(),
        )
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
    let outcome = client
        .delete_manifest(DeleteManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("v1").unwrap()),
            source_ts: None,
        })
        .await
        .unwrap();
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
    let outcome = client
        .delete_manifest(DeleteManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Digest(Digest::try_from(digest).unwrap()),
            source_ts: None,
        })
        .await
        .unwrap();
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
    let result = client
        .delete_manifest(DeleteManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("latest").unwrap()),
            source_ts: None,
        })
        .await;
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
    let outcome = client
        .delete_manifest(DeleteManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("gone").unwrap()),
            source_ts: None,
        })
        .await
        .unwrap();
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
    let outcome = client
        .delete_manifest(DeleteManifestRequest {
            namespace: Namespace::new("test").unwrap(),
            reference: Reference::Tag(Tag::new("v1").unwrap()),
            source_ts: None,
        })
        .await
        .unwrap();
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
        .head_blob(HeadBlobRequest {
            namespace: Namespace::new("test").unwrap(),
            digest: Digest::try_from(digest).unwrap(),
            accepted_types: Vec::new(),
        })
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
    let tags = client
        .list_tags(ListTagsRequest {
            namespace: Namespace::new("test").unwrap(),
            n: None,
            last: None,
        })
        .await
        .unwrap();
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
        .and(query_param_is_missing("last"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", "</v2/test/tags/list?n=1&last=a>; rel=\"next\"")
                .set_body_json(serde_json::json!({ "tags": ["a"] })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);

    let tags = client
        .list_tags(ListTagsRequest {
            namespace: Namespace::new("test").unwrap(),
            n: None,
            last: None,
        })
        .await
        .unwrap();
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
    let tags = client
        .list_tags(ListTagsRequest {
            namespace: Namespace::new("missing").unwrap(),
            n: None,
            last: None,
        })
        .await
        .unwrap();
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
        .list_tags(ListTagsRequest {
            namespace: Namespace::new("test").unwrap(),
            n: None,
            last: None,
        })
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

#[tokio::test]
async fn list_tags_stops_at_a_cross_origin_next_link() {
    // A next link pointing off-host would send the paging request and its auth
    // elsewhere; pagination must stop with the page in hand.
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "tags": ["a", "b"] }))
                .insert_header(
                    "Link",
                    "<https://evil.example/v2/test/tags/list?last=z>; rel=\"next\"",
                ),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let tags = client
        .list_tags(ListTagsRequest {
            namespace: Namespace::new("test").unwrap(),
            n: None,
            last: None,
        })
        .await
        .expect("a cross-origin next link stops paging, it does not error");

    assert_eq!(tags, vec![Tag::new("a").unwrap(), Tag::new("b").unwrap()]);
}

/// A server-assigned upload-session URL carries signed state in its query, so
/// the logged form must drop it while leaving a plain location untouched.
#[test]
fn logged_locations_drop_the_query_string() {
    assert_eq!(
        without_query("https://up.example.com/v2/ns/blobs/uploads/s1?_state=SIGNED&sig=abc"),
        "https://up.example.com/v2/ns/blobs/uploads/s1"
    );
    assert_eq!(
        without_query("https://up.example.com/v2/ns/manifests/latest"),
        "https://up.example.com/v2/ns/manifests/latest"
    );
}
