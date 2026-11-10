use std::io::{self, Cursor, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;
use url::Url;
use wiremock::matchers::{body_bytes, header, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use angos_oci::header::{DOCKER_CONTENT_DIGEST, OCI_SUBJECT, X_ANGOS_SOURCE_TIMESTAMP};
use angos_oci::request::{
    BlobMount, DeleteManifestRequest, GetBlobRequest, GetManifestRequest, HeadBlobRequest,
    HeadManifestRequest, ListTagsRequest, MountBlobRequest, PutManifestRequest, StartUploadRequest,
};
use angos_oci::response::{DeleteManifestOutcome, PutManifestOutcome, REPLICATION_SUPERSEDED_CODE};
use angos_oci::{Algorithm, Digest, MediaType, Namespace, Reference, Tag};

use angos_cache::Config;
use angos_mtls_client::{BasicAuth, ClientTls};
use angos_secret::Secret;

use crate::auth::{token_cache_key, token_index_cache_key};
use crate::{Error, RegistryClient, RegistryClientConfig, without_query};

/// The registry's default; inlined so the client crate's tests do not reach
/// into the server-side manifest module.
const DEFAULT_MAX_MANIFEST_SIZE_BYTES: usize = 5 * 1024 * 1024;

const DIGEST: &str = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
const MANIFEST_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";

fn test_client_config(url: String) -> RegistryClientConfig {
    RegistryClientConfig {
        url,
        max_redirect: 5,
        connect_timeout_secs: 30,
        read_timeout_secs: 300,
        tls: ClientTls::default(),
        basic_auth: None,
    }
}

fn basic_auth_config(url: String, password: &str) -> RegistryClientConfig {
    RegistryClientConfig {
        basic_auth: Some(BasicAuth {
            username: "user".to_string(),
            password: Secret::new(password.to_string()),
        }),
        ..test_client_config(url)
    }
}

fn client_with(config: &RegistryClientConfig) -> RegistryClient {
    let cache = Config::Memory.to_backend().unwrap();
    RegistryClient::from_config(config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).unwrap()
}

fn client_for(mock_server: &MockServer) -> RegistryClient {
    client_with(&test_client_config(mock_server.uri()))
}

fn test_namespace() -> Namespace {
    Namespace::new("test").unwrap()
}

fn latest() -> Reference {
    Reference::Tag(Tag::new("latest").unwrap())
}

fn v1() -> Reference {
    Reference::Tag(Tag::new("v1").unwrap())
}

fn head_blob_request() -> HeadBlobRequest {
    HeadBlobRequest {
        namespace: test_namespace(),
        digest: Digest::try_from(DIGEST).unwrap(),
        accepted_types: Vec::new(),
    }
}

fn get_blob_request() -> GetBlobRequest {
    GetBlobRequest {
        namespace: test_namespace(),
        digest: Digest::try_from(DIGEST).unwrap(),
        accepted_types: Vec::new(),
        range: None,
    }
}

fn get_manifest_request() -> GetManifestRequest {
    GetManifestRequest {
        namespace: test_namespace(),
        reference: latest(),
        accepted_types: Vec::new(),
    }
}

fn put_manifest_request(source_ts: Option<DateTime<Utc>>) -> PutManifestRequest {
    PutManifestRequest {
        namespace: test_namespace(),
        reference: v1(),
        content_type: Some(MediaType::new(MANIFEST_TYPE).unwrap()),
        tags: Vec::new(),
        source_ts,
    }
}

fn list_tags_request(namespace: &str) -> ListTagsRequest {
    ListTagsRequest {
        namespace: Namespace::new(namespace).unwrap(),
        n: None,
        last: None,
    }
}

fn mount_request(from: Option<&str>) -> MountBlobRequest {
    MountBlobRequest {
        namespace: Namespace::new("target").unwrap(),
        mount: BlobMount {
            digest: Digest::try_from(DIGEST).unwrap(),
            from: from.map(|from| Namespace::new(from).unwrap()),
        },
    }
}

fn bearer_challenge(params: &str) -> ResponseTemplate {
    ResponseTemplate::new(401).insert_header("WWW-Authenticate", format!("Bearer {params}"))
}

fn token_response(token: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({"token": token, "expires_in": 3600}))
}

fn manifest_response() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_bytes(b"{}")
        .insert_header("Content-Type", "application/json")
}

/// Collects tracing output into a shared buffer; every clone appends to the
/// same buffer, so a clone passed to `with_writer` is read back from the original.
#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A lone username or password, a misspelled key away from the pair, would
/// otherwise run anonymous and surface later as 401s or rate limits.
#[test]
fn a_half_credential_pair_is_rejected_at_deserialize() {
    for half in [r#"username = "user""#, r#"password = "pass""#] {
        let toml = format!("url = \"https://example.com\"\n{half}");
        assert!(
            toml::from_str::<RegistryClientConfig>(&toml).is_err(),
            "{half} alone must be refused"
        );
    }
}

// The client (and everything holding it, like a replication downstream) is
// Debug-formatted in logs; the configured password must never appear there.
#[test]
fn debug_output_redacts_basic_auth_password() {
    let client = client_with(&basic_auth_config(
        "https://example.com".to_string(),
        "hunter2",
    ));

    let debug = format!("{client:?}");
    assert!(
        !debug.contains("hunter2"),
        "Debug must not leak the password, got: {debug}"
    );
    assert!(debug.contains("[REDACTED]"));
}

/// A registry that redirects blob HEADs to a CDN answers without
/// `Docker-Content-Digest`; the blob asked for by digest is that digest.
#[tokio::test]
async fn head_blob_reports_the_digest_and_size() {
    for with_digest in [true, false] {
        let mock_server = MockServer::start().await;
        let mut response = ResponseTemplate::new(200).insert_header("Content-Length", "1234");
        if with_digest {
            response = response.insert_header(DOCKER_CONTENT_DIGEST, DIGEST);
        }
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/test/blobs/{DIGEST}")))
            .respond_with(response)
            .mount(&mock_server)
            .await;

        let head = client_for(&mock_server)
            .head_blob(head_blob_request())
            .await
            .unwrap();

        assert_eq!(head, (Digest::try_from(DIGEST).unwrap(), 1234));
    }
}

/// `BlobUnknown` means absent: an outage read as one would make replication
/// re-upload and pull-through serve a miss.
#[tokio::test]
async fn only_a_404_reads_as_an_unknown_blob() {
    for status in [404, 503] {
        let mock_server = MockServer::start().await;
        Mock::given(path(format!("/v2/test/blobs/{DIGEST}")))
            .respond_with(ResponseTemplate::new(status))
            .mount(&mock_server)
            .await;
        let client = client_for(&mock_server);

        let head = client.head_blob(head_blob_request()).await.unwrap_err();
        let get = client.get_blob(get_blob_request()).await.err().unwrap();

        for error in [head, get] {
            if status == 404 {
                assert!(matches!(error, Error::BlobUnknown), "got {error:?}");
            } else {
                assert!(matches!(error, Error::Internal(_)), "got {error:?}");
            }
        }
    }
}

/// The OCI spec makes `Docker-Content-Digest` a SHOULD on blob HEAD, so a 2xx
/// without it is present; a 5xx fails the probe so the job retries instead of
/// re-uploading.
#[tokio::test]
async fn blob_exists_reads_2xx_as_present_404_as_absent_and_else_fails() {
    for (status, expected) in [(200, Some(true)), (404, Some(false)), (503, None)] {
        let mock_server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/test/blobs/{DIGEST}")))
            .respond_with(ResponseTemplate::new(status))
            .mount(&mock_server)
            .await;

        let present = client_for(&mock_server)
            .blob_exists(head_blob_request())
            .await
            .ok();

        assert_eq!(present, expected, "status {status}");
    }
}

/// `Docker-Content-Digest` is a SHOULD, so a missing one reads as an unknown
/// digest rather than failing the probe.
#[tokio::test]
async fn head_manifest_reports_media_type_digest_and_size() {
    for with_digest in [true, false] {
        let mock_server = MockServer::start().await;
        let mut response = ResponseTemplate::new(200)
            .insert_header("Content-Type", MANIFEST_TYPE)
            .insert_header("Content-Length", "5678");
        if with_digest {
            response = response.insert_header(DOCKER_CONTENT_DIGEST, DIGEST);
        }
        Mock::given(method("HEAD"))
            .and(path("/v2/test/manifests/latest"))
            .respond_with(response)
            .mount(&mock_server)
            .await;

        let head = client_for(&mock_server)
            .head_manifest(HeadManifestRequest {
                namespace: test_namespace(),
                reference: latest(),
                accepted_types: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(
            head.media_type,
            Some(MediaType::new(MANIFEST_TYPE).unwrap())
        );
        assert_eq!(
            head.digest,
            with_digest.then(|| Digest::try_from(DIGEST).unwrap())
        );
        assert_eq!(head.size, 5678);
    }
}

/// Without `Docker-Content-Digest` the body is what the caller hashes to
/// recover the digest.
#[tokio::test]
async fn get_manifest_returns_the_body_with_or_without_a_digest() {
    let manifest_body = b"{\"schemaVersion\":2}";
    for with_digest in [true, false] {
        let mock_server = MockServer::start().await;
        let mut response = ResponseTemplate::new(200)
            .set_body_bytes(manifest_body)
            .insert_header("Content-Type", MANIFEST_TYPE);
        if with_digest {
            response = response.insert_header(DOCKER_CONTENT_DIGEST, DIGEST);
        }
        Mock::given(method("GET"))
            .and(path("/v2/test/manifests/latest"))
            .respond_with(response)
            .mount(&mock_server)
            .await;

        let fetched = client_for(&mock_server)
            .get_manifest(get_manifest_request())
            .await
            .unwrap();

        assert_eq!(
            fetched.media_type,
            Some(MediaType::new(MANIFEST_TYPE).unwrap())
        );
        assert_eq!(
            fetched.digest,
            with_digest.then(|| Digest::try_from(DIGEST).unwrap())
        );
        assert_eq!(fetched.body, manifest_body);
    }
}

#[tokio::test]
async fn test_get_manifest_rejects_oversized_body() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b' '; 8]))
        .mount(&mock_server)
        .await;

    let cache = Config::Memory.to_backend().unwrap();
    let client =
        RegistryClient::from_config(&test_client_config(mock_server.uri()), cache, 7).unwrap();
    let result = client.get_manifest(get_manifest_request()).await;

    assert!(matches!(
        result,
        Err(Error::ManifestBodyTooLarge { limit: 7 })
    ));
}

/// A scope-cache hit must index the URL it served. Only the token exchange
/// wrote that entry, so every other URL sharing the scope re-discovered the
/// token through a 401 on every single request.
#[tokio::test]
async fn a_scope_cache_hit_indexes_the_url_it_served() {
    let mock_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let url = Url::parse(&format!("{registry_url}/v2/test/blobs/{DIGEST}")).unwrap();
    let cache = Config::Memory.to_backend().unwrap();

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
        .and(path(format!("/v2/test/blobs/{DIGEST}")))
        .and(header("Authorization", "Bearer scope-token"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"blob"))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{DIGEST}")))
        .respond_with(bearer_challenge(&format!(
            r#"realm="{registry_url}/token",service="registry",scope="repository:test:pull""#
        )))
        .mount(&mock_server)
        .await;

    let client = RegistryClient::from_config(
        &test_client_config(registry_url),
        cache.clone(),
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .unwrap();
    client
        .get_blob(get_blob_request())
        .await
        .expect("blob fetch");

    assert_eq!(
        cache.retrieve_value(&index_key).await.unwrap().as_deref(),
        Some(cache_key.as_str()),
        "the served URL must be indexed so the next request skips the 401"
    );
}

#[tokio::test]
async fn test_bearer_tokens_are_cached_per_scope() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;

    for name in ["alpha", "beta"] {
        Mock::given(method("GET"))
            .and(path(format!("/v2/{name}/manifests/latest")))
            .and(|request: &Request| !request.headers.contains_key("authorization"))
            .respond_with(bearer_challenge(&format!(
                r#"realm="{}",service="registry",scope="repository:{name}:pull""#,
                auth_server.uri()
            )))
            .expect(1)
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(query_param("service", "registry"))
            .and(query_param("scope", format!("repository:{name}:pull")))
            .respond_with(token_response(&format!("token-{name}")))
            .expect(1)
            .mount(&auth_server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/v2/alpha/manifests/latest"))
        .and(header("Authorization", "Bearer token-alpha"))
        .respond_with(manifest_response())
        .expect(2)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/beta/manifests/latest"))
        .and(header("Authorization", "Bearer token-beta"))
        .respond_with(manifest_response())
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let fetch = async |name: &str| {
        client
            .get_manifest(GetManifestRequest {
                namespace: Namespace::new(name).unwrap(),
                reference: latest(),
                accepted_types: Vec::new(),
            })
            .await
    };
    // Alpha twice: the second read must be served by the cached scope token,
    // which the `expect(1)` on the 401 mock pins.
    assert!(fetch("alpha").await.is_ok());
    assert!(fetch("beta").await.is_ok());
    assert!(fetch("alpha").await.is_ok());
}

#[tokio::test]
async fn test_expired_bearer_token_is_refetched() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let registry_url = mock_server.uri();
    let cache = Config::Memory.to_backend().unwrap();
    let url = Url::parse(&format!("{registry_url}/v2/test/manifests/latest")).unwrap();
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
        .and(header("Authorization", "Bearer stale-token"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer fresh-token"))
        .respond_with(manifest_response())
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(bearer_challenge(&format!(
            r#"realm="{}",service="registry",scope="repository:test:pull""#,
            auth_server.uri()
        )))
        .up_to_n_times(1)
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("scope", "repository:test:pull"))
        .respond_with(token_response("fresh-token"))
        .expect(1)
        .mount(&auth_server)
        .await;

    let client = RegistryClient::from_config(
        &test_client_config(registry_url),
        cache,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .unwrap();
    assert!(client.get_manifest(get_manifest_request()).await.is_ok());
}

#[tokio::test]
async fn test_concurrent_bearer_refresh_uses_single_token_exchange() {
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(|request: &Request| !request.headers.contains_key("authorization"))
        .respond_with(bearer_challenge(&format!(
            r#"realm="{}",service="registry",scope="repository:test:pull""#,
            auth_server.uri()
        )))
        .expect(10)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("scope", "repository:test:pull"))
        .respond_with(token_response("shared-token").set_delay(Duration::from_millis(50)))
        .expect(1)
        .mount(&auth_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .and(header("Authorization", "Bearer shared-token"))
        .respond_with(manifest_response())
        .expect(10)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let results = join_all((0..10).map(|_| client.get_manifest(get_manifest_request()))).await;

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
        .respond_with(manifest_response())
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_with(&basic_auth_config(mock_server.uri(), "pass"));

    assert!(client.get_manifest(get_manifest_request()).await.is_ok());
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
        .respond_with(token_response("scoped"))
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
        .respond_with(bearer_challenge(&format!(
            r#"realm="{registry_url}/token",service="registry""#
        )))
        .mount(&mock_server)
        .await;

    let error = client_for(&mock_server)
        .get_manifest(get_manifest_request())
        .await
        .expect_err("a 403 must not read as a successful fetch");

    assert!(
        matches!(error, Error::Denied(_)),
        "a scope denial must be terminal, got: {error:?}"
    );
}

/// A token obtained moments ago will not become valid on another attempt, so a
/// persistent 401 is terminal; a token service that is down or throttling
/// refused nothing, so it reads as an outage, not a refusal.
#[tokio::test]
async fn a_failed_token_refresh_is_classified_by_who_failed() {
    for (token_status, outage) in [(200, false), (401, false), (429, true), (503, true)] {
        let mock_server = MockServer::start().await;
        let registry_url = mock_server.uri();

        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(token_status).set_body_json(json!({ "token": "scoped" })),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v2/test/manifests/latest"))
            .respond_with(bearer_challenge(&format!(
                r#"realm="{registry_url}/token",service="registry""#
            )))
            .mount(&mock_server)
            .await;

        let error = client_for(&mock_server)
            .get_manifest(get_manifest_request())
            .await
            .expect_err("a failed token refresh must fail the fetch");

        if outage {
            assert!(
                matches!(error, Error::Internal(_)),
                "token service {token_status}: got {error:?}"
            );
        } else {
            assert!(
                matches!(error, Error::Unauthorized(_)),
                "token service {token_status}: got {error:?}"
            );
        }
    }
}

#[tokio::test]
async fn test_forbidden_access() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/test/manifests/latest"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let result = client_for(&mock_server)
        .get_manifest(get_manifest_request())
        .await;

    assert!(matches!(result, Err(Error::Denied(_))));
}

#[tokio::test]
async fn test_get_blob_success() {
    let mock_server = MockServer::start().await;
    let blob_data = b"test blob content here";
    Mock::given(method("GET"))
        .and(path(format!("/v2/test/blobs/{DIGEST}")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(blob_data))
        .mount(&mock_server)
        .await;

    let mut fetched = client_for(&mock_server)
        .get_blob(get_blob_request())
        .await
        .unwrap();

    assert_eq!(fetched.length, blob_data.len() as u64);
    let mut buffer = Vec::new();
    fetched.reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, blob_data);
}

#[test]
fn test_new_with_invalid_ca_bundle() {
    let config = RegistryClientConfig {
        tls: ClientTls {
            server_ca_bundle: Some(PathBuf::from("/nonexistent/ca.pem")),
            identity: None,
        },
        ..test_client_config("https://example.com".to_string())
    };

    let cache = Config::Memory.to_backend().unwrap();
    let result = RegistryClient::from_config(&config, cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES);
    assert!(matches!(result.unwrap_err(), Error::Initialization(_)));
}

/// `client_certificate` is the registry clients' spelling of the key, so a
/// lone one must still be read as half an identity.
#[test]
fn test_registry_client_config_cert_without_key_rejected_at_deserialize() {
    let toml = r#"
        url = "https://example.com"
        client_certificate = "/path/to/cert.pem"
    "#;
    assert!(toml::from_str::<RegistryClientConfig>(toml).is_err());
}

#[tokio::test]
async fn test_blob_upload_sequence() {
    let mock_server = MockServer::start().await;
    let content = b"replicated blob content";
    let session_path = "/v2/test/blobs/uploads/session-1";

    Mock::given(method("POST"))
        .and(path("/v2/test/blobs/uploads/"))
        .respond_with(
            ResponseTemplate::new(202).insert_header("Location", format!("{session_path}?state=a")),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(session_path))
        .and(query_param("state", "a"))
        .and(query_param("digest", DIGEST))
        .and(body_bytes(content.to_vec()))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let session = client
        .start_upload(StartUploadRequest {
            namespace: test_namespace(),
            digest_algorithm: None,
            target: None,
        })
        .await
        .unwrap();
    assert_eq!(
        session.url,
        format!("{}{session_path}?state=a", mock_server.uri())
    );

    client
        .put_upload(
            &session.url,
            session.auth.as_deref(),
            &Digest::try_from(DIGEST).unwrap(),
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
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

    let outcome = client_for(&mock_server)
        .start_upload(StartUploadRequest {
            namespace: test_namespace(),
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
async fn test_put_upload_401_is_unauthorized_without_retry() {
    // A single-use streamed body cannot be replayed, so a 401 must not trigger
    // the refresh-and-retry path (`.expect(1)` proves a single PUT).
    let mock_server = MockServer::start().await;
    let content = b"replicated blob content";
    let session_path = "/v2/test/blobs/uploads/session-1";

    Mock::given(method("PUT"))
        .and(path(session_path))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&mock_server)
        .await;

    let result = client_for(&mock_server)
        .put_upload(
            &format!("{}{session_path}", mock_server.uri()),
            None,
            &Digest::from_bytes(Algorithm::Sha256, content),
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await;

    assert!(
        matches!(result, Err(Error::Unauthorized(_))),
        "a 401 on a streamed PUT must be Error::Unauthorized (no replay), got {result:?}"
    );
}

#[tokio::test]
async fn test_blob_upload_reuses_session_open_bearer_token_on_put() {
    // The session URL never issues its own auth challenge, so the streamed PUT
    // must reuse the bearer token the start-upload POST resolved; the PUT mock
    // requires it.
    let mock_server = MockServer::start().await;
    let auth_server = MockServer::start().await;
    let content = b"replicated blob content";
    let start_path = "/v2/test/blobs/uploads/";
    let session_path = "/v2/test/blobs/uploads/session-7";

    Mock::given(method("POST"))
        .and(path(start_path))
        .respond_with(bearer_challenge(&format!(
            r#"realm="{}",service="registry",scope="repository:test:pull,push""#,
            auth_server.uri()
        )))
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
        .and(query_param("scope", "repository:test:pull,push"))
        .respond_with(token_response("push-token"))
        .mount(&auth_server)
        .await;
    Mock::given(method("PUT"))
        .and(path(session_path))
        .and(header("Authorization", "Bearer push-token"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = client_for(&mock_server);
    let session = client
        .start_upload(StartUploadRequest {
            namespace: test_namespace(),
            digest_algorithm: None,
            target: None,
        })
        .await
        .unwrap();
    assert_eq!(session.auth.as_deref(), Some("Bearer push-token"));
    client
        .put_upload(
            &session.url,
            session.auth.as_deref(),
            &Digest::from_bytes(Algorithm::Sha256, content),
            content.len() as u64,
            Cursor::new(content.to_vec()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_mount_blob_omits_from_when_none() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .and(query_param("mount", DIGEST))
        .and(query_param_is_missing("from"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&mock_server)
        .await;

    let outcome = client_for(&mock_server)
        .mount_blob(mount_request(None))
        .await
        .unwrap();

    assert!(outcome.is_none(), "a 201 must report a mount");
}

/// An advertised digest must match the request, or the blob would be falsely
/// marked converged; a garbage one must not read as absent and skip that check.
#[tokio::test]
async fn mount_blob_checks_the_digest_a_201_advertises() {
    let other = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    for (advertised, mounted) in [(DIGEST, true), (other, false), ("not-a-digest", false)] {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v2/target/blobs/uploads/"))
            .and(query_param("mount", DIGEST))
            .and(query_param("from", "source"))
            .respond_with(
                ResponseTemplate::new(201).insert_header(DOCKER_CONTENT_DIGEST, advertised),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let outcome = client_for(&mock_server)
            .mount_blob(mount_request(Some("source")))
            .await;

        assert_eq!(
            matches!(outcome, Ok(None)),
            mounted,
            "advertised {advertised}"
        );
    }
}

#[tokio::test]
async fn test_mount_blob_falls_back_to_session_on_202() {
    let mock_server = MockServer::start().await;
    let session_path = "/v2/target/blobs/uploads/session-9";
    Mock::given(method("POST"))
        .and(path("/v2/target/blobs/uploads/"))
        .respond_with(ResponseTemplate::new(202).insert_header("Location", session_path))
        .expect(1)
        .mount(&mock_server)
        .await;

    let outcome = client_for(&mock_server)
        .mount_blob(mount_request(Some("source")))
        .await
        .unwrap();

    assert_eq!(
        outcome.map(|session| session.url),
        Some(format!("{}{session_path}", mock_server.uri())),
        "a 202 to a mount request must fall back to a session with the continuation URL"
    );
}

#[tokio::test]
async fn test_put_manifest_stamps_headers_and_reports_echoes() {
    let mock_server = MockServer::start().await;
    let manifest = br#"{"schemaVersion":2}"#;
    let subject = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let source_ts = DateTime::parse_from_rfc3339("2026-06-03T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
        .and(header("Content-Type", MANIFEST_TYPE))
        .and(header(X_ANGOS_SOURCE_TIMESTAMP, source_ts.to_rfc3339()))
        .and(body_bytes(manifest.to_vec()))
        .respond_with(
            ResponseTemplate::new(201)
                .insert_header(DOCKER_CONTENT_DIGEST, DIGEST)
                .insert_header(OCI_SUBJECT, subject),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let result = client_for(&mock_server)
        .put_manifest(put_manifest_request(Some(source_ts)), manifest.to_vec())
        .await
        .unwrap();

    let PutManifestOutcome::Stored {
        digest: echoed,
        subject: echoed_subject,
    } = result
    else {
        panic!("a 2xx push must report the manifest as stored, got {result:?}");
    };
    assert_eq!(echoed, Some(Digest::try_from(DIGEST).unwrap()));
    assert_eq!(echoed_subject, Some(subject.to_string()));
}

/// A push the downstream accepted must not fail over a bad echo, but the echo
/// is what the divergence check reads, so dropping it has to be visible rather
/// than silently reported as no digest at all.
#[tokio::test]
async fn put_manifest_warns_on_an_unparseable_advertised_digest() {
    let mock_server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/v2/test/manifests/v1"))
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
        .put_manifest(put_manifest_request(None), b"{}".to_vec())
        .await
        .expect("an unparseable echo must not fail a push the downstream accepted");

    drop(guard);

    assert!(
        matches!(result, PutManifestOutcome::Stored { digest: None, .. }),
        "an unparseable echo must drop the digest, got {result:?}"
    );
    let logs = String::from_utf8(log_capture.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("put_manifest") && logs.contains(DOCKER_CONTENT_DIGEST.as_str()),
        "the dropped digest echo must be logged, logs were: {logs}"
    );
}

/// Only a last-writer-wins 409 converges; any other 409 fails so the job
/// retries or dead-letters.
#[tokio::test]
async fn test_put_manifest_409_converges_only_when_superseded() {
    for (code, superseded) in [(REPLICATION_SUPERSEDED_CODE, true), ("DENIED", false)] {
        let mock_server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v2/test/manifests/v1"))
            .respond_with(
                ResponseTemplate::new(409).set_body_json(json!({ "errors": [{ "code": code }] })),
            )
            .mount(&mock_server)
            .await;

        let result = client_for(&mock_server)
            .put_manifest(put_manifest_request(None), b"{}".to_vec())
            .await;

        if superseded {
            assert!(
                matches!(result, Ok(PutManifestOutcome::Superseded)),
                "got {result:?}"
            );
        } else {
            assert!(matches!(result, Err(Error::Internal(_))), "got {result:?}");
        }
    }
}

/// A retried delete converges on 404, and stock distribution's 405 on tag
/// deletion reads as unsupported so it never dead-letters a job per event.
#[tokio::test]
async fn test_delete_manifest_outcomes() {
    let superseded = json!({ "errors": [{ "code": REPLICATION_SUPERSEDED_CODE }] });
    for (response, expected) in [
        (
            ResponseTemplate::new(202),
            Some(DeleteManifestOutcome::Deleted),
        ),
        (
            ResponseTemplate::new(409).set_body_json(superseded),
            Some(DeleteManifestOutcome::Superseded),
        ),
        (
            ResponseTemplate::new(404),
            Some(DeleteManifestOutcome::AlreadyAbsent),
        ),
        (
            ResponseTemplate::new(405),
            Some(DeleteManifestOutcome::Unsupported),
        ),
        (ResponseTemplate::new(500), None),
    ] {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v2/test/manifests/v1"))
            .respond_with(response)
            .expect(1)
            .mount(&mock_server)
            .await;

        let outcome = client_for(&mock_server)
            .delete_manifest(DeleteManifestRequest {
                namespace: test_namespace(),
                reference: v1(),
                source_ts: None,
            })
            .await
            .ok();

        assert_eq!(outcome, expected);
    }
}

/// An already-gone session is the goal state of a cancel.
#[tokio::test]
async fn test_delete_upload_succeeds_on_2xx_and_404() {
    for (status, succeeds) in [(204, true), (404, true), (500, false)] {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v2/test/blobs/uploads/s1"))
            .respond_with(ResponseTemplate::new(status))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client_for(&mock_server)
            .delete_upload(&format!("{}/v2/test/blobs/uploads/s1", mock_server.uri()))
            .await;

        assert_eq!(result.is_ok(), succeeds, "status {status}");
    }
}

#[tokio::test]
async fn test_list_tags_follows_pagination() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .and(query_param("n", "1"))
        .and(query_param("last", "a"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tags": ["b"] })))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/test/tags/list"))
        .and(query_param_is_missing("last"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", "</v2/test/tags/list?n=1&last=a>; rel=\"next\"")
                .set_body_json(json!({ "tags": ["a"] })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let tags = client_for(&mock_server)
        .list_tags(list_tags_request("test"))
        .await
        .unwrap();

    assert_eq!(tags, vec![Tag::new("a").unwrap(), Tag::new("b").unwrap()]);
}

#[tokio::test]
async fn test_list_tags_absent_repo_returns_empty() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/missing/tags/list"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let tags = client_for(&mock_server)
        .list_tags(list_tags_request("missing"))
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
                .set_body_json(json!({ "tags": ["a", "b"] }))
                .insert_header("Link", "</v2/test/tags/list?last=z>; rel=\"next\""),
        )
        .expect(2)
        .mount(&mock_server)
        .await;

    let tags = client_for(&mock_server)
        .list_tags(list_tags_request("test"))
        .await
        .expect("cyclic pagination must terminate, not error");

    // The base page and one `?last=z` page are fetched before the repeated
    // `?last=z` breaks the loop.
    let page = [Tag::new("a").unwrap(), Tag::new("b").unwrap()];
    assert_eq!(tags, [page.clone(), page].concat());
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
                .set_body_json(json!({ "tags": ["a", "b"] }))
                .insert_header(
                    "Link",
                    "<https://evil.example/v2/test/tags/list?last=z>; rel=\"next\"",
                ),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let tags = client_for(&mock_server)
        .list_tags(list_tags_request("test"))
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
