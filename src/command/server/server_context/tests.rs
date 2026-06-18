use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, Version,
    password_hash::{SaltString, rand_core::OsRng},
};
use base64::Engine;
use chrono::Utc;
use hyper::{Request, header::HeaderMap};
use uuid::Uuid;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use angos_s3_client::Backend as S3HttpBackend;
use angos_storage::{ConditionalStore, ObjectStore, s3::Backend as StorageS3Backend};
use angos_tx_engine::{executor::build_executor, lock::LockStrategy};

use crate::{
    command::server::{
        error::Error,
        server_context::{ServerContext, resolve_client_ip},
    },
    configuration::Configuration,
    event_webhook::{
        config::EventWebhookConfig,
        event::{Event, EventKind},
    },
    identity::{Action, ClientIdentity},
    metrics_provider,
    oci::{Digest, Namespace, Reference},
    policy::AccessPolicyConfig,
    registry::{
        Registry, RegistryConfig, Repository,
        blob_store::{BlobStoreConfig, FsBackendConfig as BlobFsConfig},
        metadata_store::{LinkKind, LinkOperation, MetadataStore},
        repository_resolver::RepositoryResolver,
        s3_connection::S3ConnectionConfig,
        test_utils::build_store,
    },
    secret::Secret,
};

#[derive(Default)]
pub struct TestConfigOptions<'a> {
    pub access_policy: Option<AccessPolicyConfig>,
    pub webhooks: Vec<TestWebhook<'a>>,
}

pub struct TestWebhook<'a> {
    pub name: &'a str,
    pub url: &'a str,
}

pub fn create_test_config_with(options: TestConfigOptions<'_>) -> Configuration {
    metrics_provider::init_for_tests();
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test-blobs"

        [metadata_store.fs]
        root_dir = "/tmp/test-metadata"

        [cache.memory]

        [server]
        bind_address = "127.0.0.1"
        port = 8080

        [global]
        update_pull_time = false
    "#;

    let mut config: Configuration = toml::from_str(toml).unwrap();
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

pub async fn create_test_server_context_from_config(config: &Configuration) -> ServerContext {
    let registry = create_test_registry(config).await;
    ServerContext::new(config, registry).unwrap()
}

fn create_minimal_config() -> Configuration {
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
    "#;

    toml::from_str(toml).unwrap()
}

pub async fn create_test_registry(config: &Configuration) -> Registry {
    let blob_backend = std::sync::Arc::new(config.blob_store.build_backend().unwrap());
    let auth_cache = config.cache.to_backend().unwrap();
    let storage_config = config.resolve_registry_storage();
    let store = storage_config.build_store().await.unwrap();
    let metadata_store = Arc::new(MetadataStore::builder(store).build());

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

    let registry_config = RegistryConfig::default()
        .update_pull_time(config.global.update_pull_time)
        .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
        .max_manifest_size_bytes(config.global.max_manifest_size_bytes())
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    Registry::new(blob_backend, metadata_store, resolver, registry_config).unwrap()
}

pub fn create_test_event() -> Event {
    Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    }
}

#[tokio::test]
async fn test_server_context_new_with_basic_auth() {
    let salt = SaltString::generate(OsRng);
    let argon_config = Params::default();
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, argon_config);
    let password_hash = argon.hash_password(b"testpass", &salt).unwrap().to_string();

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false

        [auth.identity.testuser]
        username = "testuser"
        password = "{password_hash}"
    "#
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;

    let context = ServerContext::new(&config, registry);

    assert!(context.is_ok());
}

#[tokio::test]
async fn test_authenticate_request_no_credentials() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

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

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false

        [auth.identity.testuser]
        username = "testuser"
        password = "{password_hash}"
    "#
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

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
async fn test_authenticate_request_with_x_forwarded_for() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder()
        .header("X-Forwarded-For", "192.168.1.100, 10.0.0.1")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_with_x_forwarded_for_single_ip() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder()
        .header("X-Forwarded-For", "192.168.1.100")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_with_x_forwarded_for_whitespace() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder()
        .header("X-Forwarded-For", "  192.168.1.100  , 10.0.0.1")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_with_x_real_ip() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder()
        .header("X-Real-IP", "192.168.1.200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("192.168.1.200".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_x_forwarded_for_takes_precedence() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder()
        .header("X-Forwarded-For", "192.168.1.100")
        .header("X-Real-IP", "192.168.1.200")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authenticate_request(&parts, None).await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_with_remote_address() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();
    let remote_addr: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();

    let result = context
        .authenticate_request(&parts, Some(remote_addr))
        .await;

    assert!(result.is_ok());
    let identity = result.unwrap();
    assert_eq!(identity.client_ip, Some("127.0.0.1".to_string()));
}

#[tokio::test]
async fn test_authenticate_request_x_forwarded_for_overrides_remote_address() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

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
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false

        [global.access_policy]
        default = "allow"
        rules = []

        [repository.test]
        namespace_pattern = "^test/.*"

        [repository.test.access_policy]
        default = "allow"
        rules = []
    "#;

    let config: Configuration = toml::from_str(toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let route = Action::GetManifest {
        namespace: Namespace::new("test/repo").unwrap(),
        reference: Reference::Tag("latest".to_string()),
    };
    let identity = ClientIdentity::new(None);
    let request = Request::builder().body(()).unwrap();
    let (parts, ()) = request.into_parts();

    let result = context.authorize_request(&route, &identity, &parts).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_server_context_new_with_event_webhooks() {
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false

        [event_webhook.test_hook]
        url = "https://example.com/webhook"
        policy = "optional"
        events = ["manifest.push"]
    "#;

    let config: Configuration = toml::from_str(toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    assert!(context.has_event_dispatcher());
}

#[tokio::test]
async fn test_server_context_new_without_event_webhooks() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    assert!(!context.has_event_dispatcher());
}

#[tokio::test]
async fn test_dispatch_event_with_no_dispatcher() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let event = Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    };

    let result = context.dispatch_event(&event).await;
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

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "optional"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let event = Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    };

    let result = context.dispatch_event(&event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_dispatch_event_required_webhook_failure_returns_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "required"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let event = Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    };

    let result = context.dispatch_event(&event).await;
    assert!(matches!(result, Err(Error::Execution(_))));
}

#[tokio::test]
async fn test_server_context_shutdown_with_no_dispatcher() {
    let config = create_minimal_config();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    assert!(!context.has_event_dispatcher());
    context.shutdown_with_timeout(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn test_server_context_shutdown_drains_in_flight_async_delivery() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .expect(1)
        .mount(&mock_server)
        .await;

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
        event_webhooks = ["slow_hook"]

        [event_webhook.slow_hook]
        url = "{}/webhook"
        policy = "async"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let event = Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    };

    context.dispatch_event(&event).await.unwrap();

    context.shutdown_with_timeout(Duration::from_secs(10)).await;

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

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false

        [event_webhook.async_hook]
        url = "{}"
        policy = "async"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    context.shutdown_with_timeout(Duration::from_secs(10)).await;

    let event = Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    };

    let _ = context.dispatch_event(&event).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        0,
        "No async deliveries should occur after ServerContext::shutdown()"
    );
}

struct ShutdownFlushHarness {
    registry: Registry,
    metadata_store: Arc<MetadataStore>,
    namespace: String,
}

fn build_shutdown_flush_harness(unique_prefix: &str) -> ShutdownFlushHarness {
    metrics_provider::init_for_tests();
    let conn = S3ConnectionConfig {
        access_key_id: Secret::new("root".to_string()),
        secret_key: Secret::new("roottoor".to_string()),
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "registry".to_string(),
        region: "region".to_string(),
        key_prefix: unique_prefix.to_string(),
    };
    let http = Arc::new(S3HttpBackend::new(&conn.to_client_config()).expect("s3 http client"));
    let raw_storage = Arc::new(StorageS3Backend::builder(http).build());
    let object_store: Arc<dyn ObjectStore> = raw_storage.clone();
    let cond_store: Arc<dyn ConditionalStore> = raw_storage;
    let executor = build_executor(
        object_store.clone(),
        Some(cond_store),
        LockStrategy::Memory,
        None,
        false,
        false,
    )
    .expect("build executor");
    let facade = build_store(object_store, executor);
    let metadata_store: Arc<MetadataStore> = Arc::new(
        MetadataStore::builder(facade)
            .access_time_debounce_secs(3600)
            .link_cache_ttl(0)
            .build(),
    );

    let blob_backend = Arc::new(
        BlobStoreConfig::FS(BlobFsConfig {
            root_dir: "/tmp/test-blobs-shutdown-flush".to_string(),
            ..Default::default()
        })
        .build_backend()
        .unwrap(),
    );

    let registry = Registry::new(
        blob_backend,
        metadata_store.clone(),
        Arc::new(RepositoryResolver::new(Arc::new(HashMap::new())).unwrap()),
        RegistryConfig::default()
            .update_pull_time(false)
            .enable_blob_redirect(false)
            .enable_manifest_redirect(false)
            .global_immutable_tags(false)
            .global_immutable_tags_exclusions(Vec::new()),
    )
    .unwrap();

    ShutdownFlushHarness {
        registry,
        metadata_store,
        namespace: format!("{unique_prefix}/myimage"),
    }
}

#[tokio::test]
async fn test_shutdown_flushes_pending_access_times() {
    // shutdown_with_timeout() must flush the S3 metadata backend's buffered
    // access-time writes before returning. With access_time_debounce_secs > 0
    // those writes sit in a background loop and would be lost on a naïve
    // shutdown.
    let unique_prefix = format!("test-shutdown-flush-{}", Uuid::new_v4());
    let ShutdownFlushHarness {
        registry,
        metadata_store,
        namespace,
    } = build_shutdown_flush_harness(&unique_prefix);

    let digest =
        Digest::from_str("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let tag = LinkKind::Tag("v1.0.0".to_string());
    let ops = vec![LinkOperation::Create {
        link: tag.clone(),
        target: digest.clone(),
        referrer: None,
        media_type: None,
        descriptor: None,
    }];
    metadata_store.update_links(&namespace, &ops).await.unwrap();
    metadata_store
        .read_link_recording_access(&namespace, &tag)
        .await
        .unwrap();

    let before = metadata_store.read_link(&namespace, &tag).await.unwrap();
    assert!(
        before.accessed_at.is_none(),
        "accessed_at should not be written yet (debounce is 3600s)"
    );

    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test-blobs-shutdown-flush"

        [metadata_store.fs]
        root_dir = "/tmp/test-metadata-shutdown-flush"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
    "#;
    let config: Configuration = toml::from_str(toml).unwrap();
    let context = ServerContext::new(&config, registry).unwrap();
    context.shutdown_with_timeout(Duration::from_secs(10)).await;

    let after = metadata_store.read_link(&namespace, &tag).await.unwrap();
    assert!(
        after.accessed_at.is_some(),
        "shutdown_with_timeout() must flush pending access times to S3"
    );
}

#[test]
fn test_resolve_client_ip_single_ip() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "192.168.1.100".parse().unwrap());
    assert_eq!(
        resolve_client_ip(&headers),
        Some("192.168.1.100".to_string())
    );
}

#[test]
fn test_resolve_client_ip_comma_separated() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Forwarded-For",
        "192.168.1.100, 10.0.0.1".parse().unwrap(),
    );
    assert_eq!(
        resolve_client_ip(&headers),
        Some("192.168.1.100".to_string())
    );
}

#[test]
fn test_resolve_client_ip_whitespace() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Forwarded-For",
        "  192.168.1.100  , 10.0.0.1".parse().unwrap(),
    );
    assert_eq!(
        resolve_client_ip(&headers),
        Some("192.168.1.100".to_string())
    );
}

#[test]
fn test_resolve_client_ip_missing_header() {
    let headers = HeaderMap::new();
    assert_eq!(resolve_client_ip(&headers), None);
}

#[test]
fn test_resolve_client_ip_x_real_ip_fallback() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Real-IP", "192.168.1.200".parse().unwrap());
    assert_eq!(
        resolve_client_ip(&headers),
        Some("192.168.1.200".to_string())
    );
}

#[test]
fn test_resolve_client_ip_x_forwarded_for_takes_precedence() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Forwarded-For", "192.168.1.100".parse().unwrap());
    headers.insert("X-Real-IP", "192.168.1.200".parse().unwrap());
    assert_eq!(
        resolve_client_ip(&headers),
        Some("192.168.1.100".to_string())
    );
}

fn make_event(id: Uuid) -> Event {
    Event {
        id,
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "test/repo".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: None,
        actor: None,
        repository: "test-repo".to_string(),
    }
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

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "required"
        max_retries = 0
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let events = vec![
        make_event(Uuid::new_v4()),
        make_event(Uuid::new_v4()),
        make_event(Uuid::new_v4()),
    ];
    let result = context.dispatch_events(&events).await;

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

    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [metadata_store.fs]
        root_dir = "/tmp/test"

        [cache.memory]

        [server]
        bind_address = "0.0.0.0"
        port = 8000

        [global]
        update_pull_time = false
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{}/webhook"
        policy = "required"
        events = ["manifest.push"]
    "#,
        mock_server.uri()
    );

    let config: Configuration = toml::from_str(&toml).unwrap();
    let registry = create_test_registry(&config).await;
    let context = ServerContext::new(&config, registry).unwrap();

    let events = vec![make_event(Uuid::new_v4()), make_event(Uuid::new_v4())];
    let result = context.dispatch_events(&events).await;

    assert!(result.is_ok());
    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
}
