use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, Version,
    password_hash::{SaltString, rand_core::OsRng},
};
use base64::Engine;
use hyper::{
    Request, StatusCode, Uri,
    header::{HOST, HeaderMap, HeaderValue},
};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use angos_cache::Cache;
use angos_oci::{Digest, Namespace, Reference, Tag};
use angos_oci_service::Endpoint as OciEndpoint;

use crate::{
    command::{
        bootstrap,
        server::{
            router::Route,
            server_context::{ServerContext, build_ui_config, resolve_forwarded_ip},
        },
    },
    configuration::{Configuration, TrustedProxy},
    event_webhook::{config::EventWebhookConfig, dispatcher::EventDispatcher, event::Event},
    identity::{Action, ClientIdentity, RequestScheme},
    metrics_provider,
    policy::{AccessMode, PolicyConfig},
    registry::{
        Error as RegistryError, Registry, RegistryConfig, Repository,
        metadata_store::{MetadataStore, Settings},
        repository_resolver::RepositoryResolver,
        test_utils::test_job_store,
    },
    test_fixtures::configuration::{load_config, minimal_config},
};
#[derive(Default)]
pub struct TestConfigOptions<'a> {
    pub access_policy: Option<PolicyConfig<AccessMode>>,
    pub webhooks: Vec<TestWebhook<'a>>,
}

pub struct TestWebhook<'a> {
    pub name: &'a str,
    pub url: &'a str,
}

pub fn create_test_config_with(options: TestConfigOptions<'_>) -> Configuration {
    metrics_provider::init_for_tests();
    let mut config = minimal_config();
    if let Some(access_policy) = options.access_policy {
        config.global.access_policy = access_policy;
    }
    for webhook in options.webhooks {
        let webhook_config = format!(
            r#"
            url = "{}"
            policy = "optional"
            events = ["manifest.push"]
        "#,
            webhook.url
        );
        config.global.event_webhooks.push(webhook.name.to_string());
        config.event_webhook.insert(
            webhook.name.to_string(),
            toml::from_str::<EventWebhookConfig>(&webhook_config).unwrap(),
        );
    }
    config
}

fn create_test_config() -> Configuration {
    create_test_config_with(TestConfigOptions::default())
}

pub async fn create_test_server_context() -> ServerContext {
    let config = create_test_config();
    create_test_server_context_from_config(&config).await
}

pub async fn create_test_server_context_with(options: TestConfigOptions<'_>) -> ServerContext {
    let config = create_test_config_with(options);
    create_test_server_context_from_config(&config).await
}

/// The in-memory cache a test context shares with its authenticator and
/// authorizer.
fn test_cache() -> Arc<Cache> {
    angos_cache::Config::Memory
        .to_backend()
        .expect("memory cache")
}

pub async fn create_test_server_context_from_config(config: &Configuration) -> ServerContext {
    let registry = create_test_registry(config).await;
    ServerContext::new(config, &test_cache(), registry).unwrap()
}

pub async fn create_test_registry(config: &Configuration) -> Arc<Registry> {
    let blob_backend = std::sync::Arc::new(config.blob_store.build_backend().unwrap());
    let auth_cache = config.cache.to_backend().unwrap();
    let storage_config = config.resolve_registry_storage();
    let store = bootstrap::build_object_store(&storage_config).unwrap();
    let metadata_store = Arc::new(MetadataStore::new(store, Settings::default()));

    let mut repositories_map = HashMap::new();
    for (name, repo_config) in &config.repository {
        let repo = Repository::new(
            name,
            repo_config,
            &auth_cache,
            config.global.max_manifest_size_bytes(),
        )
        .await
        .unwrap();
        repositories_map.insert(name.clone(), repo);
    }
    let resolver = Arc::new(
        RepositoryResolver::new(Arc::new(repositories_map))
            .expect("test repositories must not have overlapping prefixes"),
    );

    let registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        enable_blob_redirect: config.global.enable_blob_redirect,
        enable_manifest_redirect: config.global.enable_manifest_redirect,
        max_manifest_size_bytes: config.global.max_manifest_size_bytes(),
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        event_dispatcher: EventDispatcher::from_config(&config.event_webhook).unwrap(),
        ..RegistryConfig::new(test_job_store(&metadata_store))
    };

    Registry::new(blob_backend, metadata_store, resolver, registry_config)
}

pub fn create_test_event() -> Event {
    let digest: Digest = "sha256:abc1230000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("test digest should parse");
    Event::push_manifest(
        &Namespace::new("test/repo").expect("test namespace should parse"),
        "test-repo",
        &digest,
        &Reference::Digest(digest.clone()),
        None,
    )
}

#[tokio::test]
async fn test_authenticate_request_no_credentials() {
    let config = minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert!(identity.username.is_none());
}

#[tokio::test]
async fn test_authenticate_request_with_basic_auth() {
    let salt = SaltString::generate(OsRng);
    let argon_config = Params::default();
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, argon_config);
    let password_hash = argon.hash_password(b"testpass", &salt).unwrap().to_string();

    let config = load_config(&format!(
        r#"
        [auth.identity.testuser]
        username = "testuser"
        password = "{password_hash}"
    "#
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let auth_header = format!(
        "Basic {}",
        base64::prelude::BASE64_STANDARD.encode("testuser:testpass")
    );
    let request = Request::builder()
        .header("Authorization", auth_header)
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.username, Some("testuser".to_string()));
    assert_eq!(identity.id, Some("testuser".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_ignores_x_forwarded_for_from_untrusted_peer() {
    let config = minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let request = Request::builder()
        .header("X-Forwarded-For", "192.168.1.100, 10.0.0.1")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();
    let remote_addr: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();

    let result = context
        .authenticate_request(&parts, Some(remote_addr))
        .await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(
        identity.client_ip,
        Some("127.0.0.1".to_string()),
        "a peer outside trusted_proxies must not spoof its IP via headers"
    );
}

#[tokio::test]
async fn test_authenticate_request_x_forwarded_for_from_trusted_proxy_overrides_remote_address() {
    let config = load_config(r#"trusted_proxies = ["127.0.0.1"]"#);
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let request = Request::builder()
        .header("X-Forwarded-For", "192.168.1.100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();
    let remote_addr: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();

    let result = context
        .authenticate_request(&parts, Some(remote_addr))
        .await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_authorize_request_with_global_policy() {
    let config = load_config(
        r#"
        [global.access_policy]
        default = "allow"
        rules = []

        [repository.test]
        namespace_pattern = "^test/.*"

        [repository.test.access_policy]
        default = "allow"
        rules = []
    "#,
    );
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let route = Action::GetManifest {
        namespace: Namespace::new("test/repo").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };
    let identity = ClientIdentity::new(None);
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authorize_request(&route, &identity, &parts).await;

    assert!(result.is_ok());
}

/// A pull naming a mirrored registry in `?ns=` is served from the repository
/// that mirrors it, so a client addressing the upstream's own paths reaches the
/// cache without prefixing them.
#[tokio::test]
async fn proxy_namespace_resolves_to_the_mirroring_repository() {
    let config = load_config(
        r#"
        [repository."docker-hub"]
        namespace = "docker.io"

        [[repository."docker-hub".upstream]]
        url = "https://registry-1.docker.io"
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;

    let uri: Uri = "/v2/library/nginx/manifests/latest?ns=docker.io"
        .parse()
        .unwrap();
    let mut route = Route::Oci(OciEndpoint::GetManifest {
        namespace: Namespace::new("library/nginx").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    });

    let served = context
        .apply_proxy_namespace(Some(&mut route), &uri)
        .expect("a mapped namespace must apply");

    assert_eq!(served.as_deref(), Some("docker.io"));
    assert_eq!(
        route
            .pull_namespace_mut()
            .map(|namespace| namespace.to_string()),
        Some("docker-hub/library/nginx".to_string()),
        "the request must be served from the repository mirroring docker.io"
    );
}

/// A request already addressing the repository keeps the namespace it named:
/// prefixing it again would look for `docker-hub/docker-hub/...`.
#[tokio::test]
async fn proxy_namespace_leaves_an_already_prefixed_request() {
    let config = load_config(
        r#"
        [repository."docker-hub"]
        namespace = "docker.io"

        [[repository."docker-hub".upstream]]
        url = "https://registry-1.docker.io"
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;

    let uri: Uri = "/v2/docker-hub/library/nginx/tags/list?ns=docker.io"
        .parse()
        .unwrap();
    let mut route = Route::Oci(OciEndpoint::ListTags {
        namespace: Namespace::new("docker-hub/library/nginx").unwrap(),
        n: None,
        last: None,
    });

    let served = context
        .apply_proxy_namespace(Some(&mut route), &uri)
        .expect("a mapped namespace must apply");

    assert_eq!(served.as_deref(), Some("docker.io"));
    assert_eq!(
        route
            .pull_namespace_mut()
            .map(|namespace| namespace.to_string()),
        Some("docker-hub/library/nginx".to_string())
    );
}

/// An `ns` no repository claims selects nothing: the request is served as it
/// arrived, and no `OCI-Namespace` is echoed to claim otherwise.
#[tokio::test]
async fn an_unclaimed_proxy_namespace_is_ignored() {
    let config = load_config(
        r#"
        [repository."docker-hub"]
        namespace = "docker.io"

        [[repository."docker-hub".upstream]]
        url = "https://registry-1.docker.io"
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;

    let uri: Uri = "/v2/library/nginx/tags/list?ns=quay.io".parse().unwrap();
    let mut route = Route::Oci(OciEndpoint::ListTags {
        namespace: Namespace::new("library/nginx").unwrap(),
        n: None,
        last: None,
    });

    assert!(
        context
            .apply_proxy_namespace(Some(&mut route), &uri)
            .expect("an ignored ns is not a failure")
            .is_none()
    );
    assert_eq!(
        route
            .pull_namespace_mut()
            .map(|namespace| namespace.to_string()),
        Some("library/nginx".to_string())
    );
}

/// The spec defines `ns` on pull operations, so a write naming it keeps the
/// namespace it spelled out rather than landing in the mirror's cache.
#[tokio::test]
async fn proxy_namespace_leaves_a_write_alone() {
    let config = load_config(
        r#"
        [repository."docker-hub"]
        namespace = "docker.io"

        [[repository."docker-hub".upstream]]
        url = "https://registry-1.docker.io"
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;

    let uri: Uri = "/v2/library/nginx/manifests/latest?ns=docker.io"
        .parse()
        .unwrap();
    let mut route = Route::Oci(OciEndpoint::DeleteManifest {
        namespace: Namespace::new("library/nginx").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    });

    assert!(
        context
            .apply_proxy_namespace(Some(&mut route), &uri)
            .expect("an ignored ns is not a failure")
            .is_none(),
        "a write must not be scoped by ns, so nothing is echoed"
    );
    let Route::Oci(OciEndpoint::DeleteManifest { namespace, .. }) = &route else {
        panic!("the action must be untouched");
    };
    assert_eq!(namespace.as_ref(), "library/nginx");
}

/// Nesting under the mirror can breach the namespace length cap, and falling
/// through would serve different content than the client asked for, so the
/// request is refused instead.
#[tokio::test]
async fn proxy_namespace_refuses_a_name_it_cannot_map() {
    let config = load_config(
        r#"
        [repository."docker-hub"]
        namespace = "docker.io"

        [[repository."docker-hub".upstream]]
        url = "https://registry-1.docker.io"
    "#,
    );
    let context = create_test_server_context_from_config(&config).await;

    // One character short of the cap, so any prefix overflows it.
    let long = "a".repeat(255);
    let uri: Uri = format!("/v2/{long}/tags/list?ns=docker.io")
        .parse()
        .unwrap();
    let mut route = Route::Oci(OciEndpoint::ListTags {
        namespace: Namespace::new(&long).unwrap(),
        n: None,
        last: None,
    });

    let error = context
        .apply_proxy_namespace(Some(&mut route), &uri)
        .expect_err("a namespace that cannot be mapped must fail the request");

    assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
    let Route::Oci(OciEndpoint::ListTags { namespace, .. }) = &route else {
        panic!("the action must keep its shape");
    };
    assert_eq!(
        namespace.as_ref(),
        long,
        "a refused mapping must not leave a half-applied namespace"
    );
}

#[tokio::test]
async fn test_server_context_new_with_event_webhooks() {
    let config = load_config(
        r#"
        [event_webhook.test_hook]
        url = "https://example.com/webhook"
        policy = "optional"
        events = ["manifest.push"]
    "#,
    );
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    assert!(context.has_event_dispatcher());
}

#[tokio::test]
async fn test_server_context_new_without_event_webhooks() {
    let config = minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    assert!(!context.has_event_dispatcher());
}

#[tokio::test]
async fn test_dispatch_event_with_no_dispatcher() {
    let config = minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let event = create_test_event();

    let result = context.registry.dispatch_events(&[event]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_dispatch_event_delivers_to_webhook() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "optional"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let event = create_test_event();

    let result = context.registry.dispatch_events(&[event]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_dispatch_event_required_webhook_failure_returns_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "required"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let event = create_test_event();

    let result = context.registry.dispatch_events(&[event]).await;
    assert!(matches!(result, Err(RegistryError::EventDelivery(_))));
}

#[tokio::test]
async fn test_server_context_shutdown_drains_in_flight_async_delivery() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
        event_webhooks = ["slow_hook"]

        [event_webhook.slow_hook]
        url = "{}/webhook"
        policy = "async"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let event = create_test_event();

    context.registry.dispatch_events(&[event]).await.unwrap();

    context.shutdown().await;

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "ServerContext::shutdown() must drain in-flight async deliveries"
    );
}

#[tokio::test]
async fn test_server_context_shutdown_rejects_new_async_dispatches() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
        [event_webhook.async_hook]
        url = "{}"
        policy = "async"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    context.shutdown().await;

    let event = create_test_event();

    let _ = context.registry.dispatch_events(&[event]).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        0,
        "No async deliveries should occur after ServerContext::shutdown()"
    );
}

fn proxies(sources: &[&str]) -> Vec<TrustedProxy> {
    sources
        .iter()
        .map(|s| TrustedProxy::parse(*s).unwrap())
        .collect()
}

#[test]
fn test_resolve_forwarded_ip_takes_rightmost_untrusted_entry() {
    // 10.0.0.1 is an intermediate trusted proxy and the entry it appended is
    // the client; the leftmost entries are client-supplied and must not win.
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Forwarded-For",
        "1.2.3.4, 192.168.1.100, 10.0.0.1".parse().unwrap(),
    );
    assert_eq!(
        resolve_forwarded_ip(&headers, &proxies(&["10.0.0.0/8"])),
        Some("192.168.1.100".to_string())
    );
}

#[test]
fn test_resolve_forwarded_ip_whitespace() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "  192.168.1.100  ".parse().unwrap());
    assert_eq!(
        resolve_forwarded_ip(&headers, &[]),
        Some("192.168.1.100".to_string())
    );
}

#[test]
fn test_resolve_forwarded_ip_missing_header() {
    let headers = HeaderMap::new();
    assert_eq!(resolve_forwarded_ip(&headers, &[]), None);
}

#[test]
fn test_resolve_forwarded_ip_rejects_non_ip_entry() {
    // A forged non-IP entry must not become the client IP; it breaks the
    // chain rather than passing through verbatim.
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "not-an-ip, 10.0.0.1".parse().unwrap());
    assert_eq!(
        resolve_forwarded_ip(&headers, &proxies(&["10.0.0.0/8"])),
        None
    );
}

#[test]
fn test_resolve_forwarded_ip_canonicalises_mapped_ipv4() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "::ffff:192.168.1.100".parse().unwrap());
    assert_eq!(
        resolve_forwarded_ip(&headers, &[]),
        Some("192.168.1.100".to_string())
    );
}

#[test]
fn test_resolve_forwarded_ip_x_real_ip_fallback() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Real-IP", "192.168.1.200".parse().unwrap());
    assert_eq!(
        resolve_forwarded_ip(&headers, &[]),
        Some("192.168.1.200".to_string())
    );
}

#[test]
fn test_resolve_forwarded_ip_all_entries_trusted_falls_back_to_x_real_ip() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "10.0.0.2, 10.0.0.1".parse().unwrap());
    headers.insert("X-Real-IP", "192.168.1.200".parse().unwrap());
    assert_eq!(
        resolve_forwarded_ip(&headers, &proxies(&["10.0.0.0/8"])),
        Some("192.168.1.200".to_string())
    );
}

#[test]
fn test_resolve_forwarded_ip_x_forwarded_for_takes_precedence() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "192.168.1.100".parse().unwrap());
    headers.insert("X-Real-IP", "192.168.1.200".parse().unwrap());
    assert_eq!(
        resolve_forwarded_ip(&headers, &[]),
        Some("192.168.1.100".to_string())
    );
}

#[tokio::test]
async fn dispatch_events_first_failure_does_not_abort_batch() {
    // With max_retries = 0 each event fails in a single attempt, so the mock
    // records exactly one POST per event.
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "required"
        max_retries = 0
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let events = vec![
        create_test_event(),
        create_test_event(),
        create_test_event(),
    ];
    let result = context.registry.dispatch_events(&events).await;

    assert!(result.is_err(), "a delivery failure must surface overall");
    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        3,
        "all events must be attempted even when an earlier one fails"
    );
}

#[tokio::test]
async fn dispatch_events_all_success_returns_ok() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "required"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    ));
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, &test_cache(), registry).unwrap();

    let events = vec![create_test_event(), create_test_event()];
    let result = context.registry.dispatch_events(&events).await;

    assert!(result.is_ok());
    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
}

/// `global` is appended to the minimal config's `[global]` table, so it must come
/// before the token service's own table header.
async fn token_service_context(global: &str, token_service: &str) -> ServerContext {
    let config = load_config(&format!(
        r#"{global}
        [auth.token_service]
        secret_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
        {token_service}"#
    ));
    create_test_server_context_from_config(&config).await
}

fn challenge_request(scheme: RequestScheme, headers: &[(&str, &str)]) -> Request<()> {
    let mut builder = Request::builder()
        .uri("/v2/")
        .header(HOST, "registry.example.com");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }

    let mut request = builder.body(()).unwrap();
    request.extensions_mut().insert(scheme);
    request
        .extensions_mut()
        .insert("10.0.0.1:9999".parse::<SocketAddr>().unwrap());
    request
}

/// The steps the connection handler runs: resolve the scheme into the
/// extension, then derive the challenge from it on the denial path.
fn challenge_for(context: &ServerContext, mut request: Request<()>) -> Option<HeaderValue> {
    let scheme = context.resolve_scheme(&request);
    request.extensions_mut().insert(scheme);
    let (scheme, host) = context.challenge_origin(&request)?;
    context.bearer_challenge(scheme, &host)
}

#[tokio::test]
async fn no_bearer_challenge_without_a_token_service() {
    let context = create_test_server_context().await;

    assert!(challenge_for(&context, challenge_request(RequestScheme::Https, &[])).is_none());
}

#[tokio::test]
async fn the_bearer_challenge_falls_back_to_the_request_host() {
    let context = token_service_context("", "").await;

    assert_eq!(
        challenge_for(&context, challenge_request(RequestScheme::Https, &[])).unwrap(),
        r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#
    );
}

#[tokio::test]
async fn a_configured_realm_wins_over_the_request_host() {
    let context = token_service_context("", r#"realm = "https://public.example.com/token""#).await;

    assert_eq!(
        challenge_for(&context, challenge_request(RequestScheme::Https, &[])).unwrap(),
        r#"Bearer realm="https://public.example.com/token",service="public.example.com""#
    );
}

/// A TLS-terminating proxy serves angos over plaintext, so without honouring its
/// `X-Forwarded-Proto` the challenge would send credentials to an http realm.
#[tokio::test]
async fn a_trusted_proxy_decides_the_realm_scheme() {
    let context = token_service_context(r#"trusted_proxies = ["10.0.0.0/8"]"#, "").await;

    assert_eq!(
        challenge_for(
            &context,
            challenge_request(RequestScheme::Http, &[("X-Forwarded-Proto", "https")])
        )
        .unwrap(),
        r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#
    );
}

#[tokio::test]
async fn an_untrusted_peer_cannot_change_the_realm_scheme() {
    let context = token_service_context("", "").await;

    assert_eq!(
        challenge_for(
            &context,
            challenge_request(RequestScheme::Http, &[("X-Forwarded-Proto", "https")])
        )
        .unwrap(),
        r#"Bearer realm="http://registry.example.com/token",service="registry.example.com""#
    );
}

#[test]
fn the_ui_config_carries_the_sign_in_provider_issuer() {
    let config = load_config(
        r#"
    [ui]
    enabled = true
    name = "my-registry"

    [ui.oidc]
    provider = "dex"
    client_id = "angos-ui"
    scopes = "openid groups"

    [auth.oidc.dex]
    issuer = "https://dex.example.com"
    "#,
    );

    let body = build_ui_config(&config);
    let oidc = body.oidc.unwrap();

    assert_eq!(body.name, "my-registry");
    assert_eq!(oidc.issuer, "https://dex.example.com");
    assert_eq!(oidc.client_id, "angos-ui");
    assert_eq!(oidc.scopes, "openid groups");
}

#[test]
fn the_ui_config_omits_sign_in_when_none_is_configured() {
    assert!(build_ui_config(&minimal_config()).oidc.is_none());
}
