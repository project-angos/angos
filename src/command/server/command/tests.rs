use std::sync::{Arc, Mutex};

use super::*;
use crate::{
    command::server::listeners::tls::tests::build_config,
    configuration,
    policy::{AccessMode, AccessPolicyConfig, CelRule},
    secret::Secret,
};

fn create_minimal_config() -> Configuration {
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
        max_concurrent_cache_jobs = 10
    "#;

    Configuration::load_from_str(toml).unwrap()
}

fn create_config_with_repository() -> Configuration {
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
        max_concurrent_cache_jobs = 10

        [repository.test-repo.access_policy]
        default = "allow"
        rules = []
    "#;

    Configuration::load_from_str(toml).unwrap()
}

#[test]
fn test_build_blob_store_filesystem_success() {
    let config = create_minimal_config();
    let auth_cache = build_auth_cache(&config.cache).unwrap();
    let result = build_blob_stores(&config.blob_store, &auth_cache);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_metadata_store_filesystem_success() {
    let config = create_minimal_config();
    let auth_cache = build_auth_cache(&config.cache).unwrap();
    let result = build_metadata_store(&config, &auth_cache, &Arc::new(Mutex::new(None))).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_metadata_store_with_explicit_config() {
    let toml = r#"
        [blob_store.fs]
        root_dir = "/tmp/test-blobs"

        [metadata_store.fs]
        root_dir = "/tmp/test-metadata-explicit"

        [cache.memory]

        [server]
        bind_address = "127.0.0.1"
        port = 8080

        [global]
        update_pull_time = false
        max_concurrent_cache_jobs = 10
    "#;

    let config = Configuration::load_from_str(toml).unwrap();
    let auth_cache = build_auth_cache(&config.cache).unwrap();
    let result = build_metadata_store(&config, &auth_cache, &Arc::new(Mutex::new(None))).await;

    assert!(result.is_ok());
}

#[test]
fn test_build_auth_cache_memory_success() {
    let config = cache::Config::Memory;
    let result = build_auth_cache(&config);

    assert!(result.is_ok());
}

#[test]
fn test_build_repository_success() {
    let repo_config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        ..repository::Config::default()
    };
    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();

    let result = build_repository("test-repo", &repo_config, &cache);

    assert!(result.is_ok());
    let repo = result.unwrap();
    assert_eq!(repo.name, "test-repo");
}

#[test]
fn test_build_repository_with_upstream() {
    let repo_config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        upstream: vec![repository::RegistryClientConfig {
            url: "https://registry-1.docker.io".to_string(),
            max_redirect: 5,
            server_ca_bundle: None,
            client_certificate: None,
            client_private_key: None,
            username: Some("testuser".to_string()),
            password: Some(Secret::new("testpass".to_string())),
        }],
        ..repository::Config::default()
    };
    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();

    let result = build_repository("cached-repo", &repo_config, &cache);

    assert!(result.is_ok());
}

#[test]
fn test_build_repository_with_immutable_tags() {
    let repo_config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        immutable_tags: true,
        immutable_tags_exclusions: vec![
            configuration::RegexPattern::compile("latest").unwrap(),
            configuration::RegexPattern::compile("dev-.*").unwrap(),
        ],
        ..repository::Config::default()
    };
    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();

    let result = build_repository("immutable-repo", &repo_config, &cache);

    assert!(result.is_ok());
}

#[test]
fn test_build_repositories_empty() {
    let configs = HashMap::new();
    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();

    let result = build_repositories(&configs, &cache);

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 0);
}

#[test]
fn test_build_repositories_single() {
    let repo_config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        ..repository::Config::default()
    };
    let mut configs = HashMap::new();
    configs.insert("repo1".to_string(), repo_config);

    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();

    let result = build_repositories(&configs, &cache);

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 1);
    assert!(repos.contains_key("repo1"));
}

#[test]
fn test_build_repositories_multiple() {
    let repo_config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        ..repository::Config::default()
    };
    let mut configs = HashMap::new();
    configs.insert("repo1".to_string(), repo_config.clone());
    configs.insert("repo2".to_string(), repo_config.clone());
    configs.insert("repo3".to_string(), repo_config);

    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();

    let result = build_repositories(&configs, &cache);

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 3);
    assert!(repos.contains_key("repo1"));
    assert!(repos.contains_key("repo2"));
    assert!(repos.contains_key("repo3"));
}

#[tokio::test]
async fn test_build_registry_minimal_config() {
    let config = create_minimal_config();
    let result = build_registry(&config, &Arc::new(Mutex::new(None))).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_registry_with_repositories() {
    let config = create_config_with_repository();
    let result = build_registry(&config, &Arc::new(Mutex::new(None))).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_registry_with_update_pull_time() {
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
        update_pull_time = true
        max_concurrent_cache_jobs = 20
    "#;

    let config = Configuration::load_from_str(toml).unwrap();
    let result = build_registry(&config, &Arc::new(Mutex::new(None))).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_command_new_insecure_listener() {
    let config = create_minimal_config();
    let result = Command::new(&config).await;

    assert!(result.is_ok());
    let command = result.unwrap();

    match command.listener {
        ServiceListener::Insecure(_) => {}
        ServiceListener::Secure(_) => panic!("Expected insecure listener"),
    }
}

#[tokio::test]
async fn test_command_new_with_repositories() {
    let config = create_config_with_repository();
    let result = Command::new(&config).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_command_notify_config_change_insecure() {
    let config = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    let new_config = create_minimal_config();
    let result = command.notify_config_change(&new_config).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_command_notify_tls_config_change_with_insecure_listener() {
    let config = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    let (tls_config, _temp_files) = build_config(false);
    let result = command.notify_tls_config_change(&tls_config);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_service_listener_enum_variants() {
    let config = create_minimal_config();
    let ServerConfig::Insecure(insecure_config) = &config.server else {
        panic!("Expected insecure config")
    };

    let registry = build_registry(&config, &Arc::new(Mutex::new(None)))
        .await
        .unwrap();
    let context = ServerContext::new(&config, registry).unwrap();

    let insecure_listener = InsecureListener::new(insecure_config, context);
    let _service_listener = ServiceListener::Insecure(insecure_listener);
}

#[test]
fn test_build_repositories_preserves_names() {
    let repo_config = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        ..repository::Config::default()
    };
    let mut configs = HashMap::new();
    configs.insert("alpha".to_string(), repo_config.clone());
    configs.insert("beta".to_string(), repo_config.clone());
    configs.insert("gamma".to_string(), repo_config);

    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();
    let repos = build_repositories(&configs, &cache).unwrap();

    assert!(repos.get("alpha").is_some());
    assert!(repos.get("beta").is_some());
    assert!(repos.get("gamma").is_some());
    assert!(repos.get("delta").is_none());
}

#[tokio::test]
async fn test_build_registry_components_integration() {
    let config = create_config_with_repository();

    let auth_cache = build_auth_cache(&config.cache).unwrap();
    let (blob_store, upload_store, presigned_blob_store) =
        build_blob_stores(&config.blob_store, &auth_cache).unwrap();
    let metadata_store = build_metadata_store(&config, &auth_cache, &Arc::new(Mutex::new(None)))
        .await
        .unwrap();
    let repositories = build_repositories(&config.repository, &auth_cache).unwrap();

    let registry_config = RegistryConfig::new()
        .update_pull_time(config.global.update_pull_time)
        .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
        .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    let registry = Registry::new(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        repositories,
        registry_config,
    );

    assert!(registry.is_ok());
}

#[tokio::test]
async fn test_command_new_validates_configuration() {
    let config = create_minimal_config();
    let result = Command::new(&config).await;

    assert!(result.is_ok());
}

#[test]
fn test_build_repositories_with_different_configs() {
    let repo_config1 = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        },
        ..repository::Config::default()
    };
    let repo_config2 = repository::Config {
        access_policy: AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![CelRule::compile("identity.username == 'admin'").unwrap()],
        },
        ..repository::Config::default()
    };

    let mut configs = HashMap::new();
    configs.insert("public".to_string(), repo_config1);
    configs.insert("private".to_string(), repo_config2);

    let cache_config = cache::Config::Memory;
    let cache = build_auth_cache(&cache_config).unwrap();
    let result = build_repositories(&configs, &cache);

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 2);
}

fn create_config_with_webhook(url: &str) -> Configuration {
    let toml = format!(
        r#"
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
        max_concurrent_cache_jobs = 10

        [event_webhook.test_hook]
        url = "{url}"
        policy = "optional"
        events = ["manifest.push"]
    "#
    );

    Configuration::load_from_str(&toml).unwrap()
}

fn create_config_with_two_webhooks(url_a: &str, url_b: &str) -> Configuration {
    let toml = format!(
        r#"
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
        max_concurrent_cache_jobs = 10

        [event_webhook.hook_a]
        url = "{url_a}"
        policy = "optional"
        events = ["manifest.push"]

        [event_webhook.hook_b]
        url = "{url_b}"
        policy = "optional"
        events = ["manifest.push"]
    "#
    );

    Configuration::load_from_str(&toml).unwrap()
}

fn create_test_event() -> crate::event_webhook::event::Event {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::event_webhook::event::{Event, EventKind};

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
async fn test_hot_reload_adds_webhook_via_command() {
    let config = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_none()
    );

    let new_config = create_config_with_webhook("https://example.com/webhook");
    command.notify_config_change(&new_config).await.unwrap();

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_some()
    );
}

#[tokio::test]
async fn test_hot_reload_removes_webhook_via_command() {
    let config = create_config_with_webhook("https://example.com/webhook");
    let command = Command::new(&config).await.unwrap();

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_some()
    );

    let new_config = create_minimal_config();
    command.notify_config_change(&new_config).await.unwrap();

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_none()
    );
}

#[tokio::test]
async fn test_hot_reload_changes_webhook_url_via_command() {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server_a)
        .await;

    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_b)
        .await;

    let config_a = create_config_with_webhook(&format!("{}/webhook", server_a.uri()));
    let command = Command::new(&config_a).await.unwrap();

    let config_b = create_config_with_webhook(&format!("{}/webhook", server_b.uri()));
    command.notify_config_change(&config_b).await.unwrap();

    let context = command.insecure_listener().current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
}

#[tokio::test]
async fn test_hot_reload_adds_second_webhook() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_a)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_b)
        .await;

    let config_one = create_config_with_webhook(&server_a.uri());
    let command = Command::new(&config_one).await.unwrap();

    let config_two = create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
    command.notify_config_change(&config_two).await.unwrap();

    let context = command.insecure_listener().current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
}

#[tokio::test]
async fn test_hot_reload_removes_one_of_two_webhooks() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_a)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server_b)
        .await;

    let config_two = create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
    let command = Command::new(&config_two).await.unwrap();

    let config_one = create_config_with_webhook(&server_a.uri());
    command.notify_config_change(&config_one).await.unwrap();

    let context = command.insecure_listener().current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
}

#[tokio::test]
async fn test_hot_reload_inflight_old_dispatcher_still_works() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    let server_old = MockServer::start().await;
    let server_new = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_old)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_new)
        .await;

    let config_old = create_config_with_webhook(&server_old.uri());
    let command = Command::new(&config_old).await.unwrap();

    let old_context = Arc::clone(&command.insecure_listener().current_context());

    let config_new = create_config_with_webhook(&server_new.uri());
    command.notify_config_change(&config_new).await.unwrap();

    old_context
        .dispatch_event(&create_test_event())
        .await
        .unwrap();

    let new_context = command.insecure_listener().current_context();
    new_context
        .dispatch_event(&create_test_event())
        .await
        .unwrap();
}

fn create_invalid_cache_config_with_webhook(url: &str) -> Configuration {
    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "/tmp/test-blobs"

        [metadata_store.fs]
        root_dir = "/tmp/test-metadata"

        [cache.redis]
        url = "redis://invalid:99999"
        key_prefix = "test:"

        [server]
        bind_address = "127.0.0.1"
        port = 8080

        [global]
        update_pull_time = false
        max_concurrent_cache_jobs = 10

        [event_webhook.test_hook]
        url = "{url}"
        policy = "optional"
        events = ["manifest.push"]
    "#
    );

    Configuration::load_from_str(&toml).unwrap()
}

#[tokio::test]
async fn test_failed_reload_preserves_old_context() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let valid_config = create_config_with_webhook(&server.uri());
    let command = Command::new(&valid_config).await.unwrap();

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_some()
    );

    let invalid_config = create_invalid_cache_config_with_webhook("https://other.example.com");
    let result = command.notify_config_change(&invalid_config).await;
    assert!(result.is_err());

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_some()
    );

    let context = command.insecure_listener().current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
}

#[tokio::test]
async fn test_valid_reload_works_after_failed_reload() {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server_a)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_b)
        .await;

    let config_a = create_config_with_webhook(&server_a.uri());
    let command = Command::new(&config_a).await.unwrap();

    let invalid_config = create_invalid_cache_config_with_webhook("https://bad.example.com");
    let result = command.notify_config_change(&invalid_config).await;
    assert!(result.is_err());

    let config_b = create_config_with_webhook(&server_b.uri());
    command.notify_config_change(&config_b).await.unwrap();

    let context = command.insecure_listener().current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
}

#[tokio::test]
async fn test_config_notifier_trait_handles_reload_error_gracefully() {
    let valid_config = create_minimal_config();
    let command = Command::new(&valid_config).await.unwrap();

    let invalid_config = create_invalid_cache_config_with_webhook("https://example.com");

    ConfigNotifier::notify_config_change(&command, &invalid_config).await;

    assert!(
        command
            .insecure_listener()
            .current_context()
            .event_dispatcher()
            .is_none()
    );
}

#[tokio::test]
async fn test_command_shutdown_with_no_dispatcher() {
    use std::time::Duration;

    let config = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    command.shutdown_with_timeout(Duration::from_secs(10)).await;
}

#[tokio::test]
async fn test_command_shutdown_drains_in_flight_async_delivery() {
    use std::time::Duration;

    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .expect(1)
        .mount(&server)
        .await;

    let config = create_config_with_webhook(&server.uri());
    let command = Command::new(&config).await.unwrap();

    let context = command.insecure_listener().current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
    drop(context);

    command.shutdown_with_timeout(Duration::from_secs(10)).await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "Command::shutdown() must drain in-flight async webhook deliveries"
    );
}
