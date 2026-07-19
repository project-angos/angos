use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Once},
};

use tempfile::TempDir;

use crate::{
    cache,
    command::{
        bootstrap,
        server::{
            Command,
            command::{ServiceListener, setup},
            listeners::tls::tests::build_config,
            server_context::tests::create_test_event,
        },
    },
    configuration::{self, Configuration},
    policy::{AccessMode, AccessPolicyConfig, CelRule},
    registry::{Registry, RegistryConfig, manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES, repository},
    secret::Secret,
    test_fixtures::client::test_client_config,
};

static CRYPTO_INIT: Once = Once::new();

fn init_crypto_provider() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();
    });
}

// The tests here build real filesystem stores, so unlike the shared fixture
// in `test_fixtures::configuration` each config needs fresh temporary
// directories; the returned `TempDir` values must be kept alive for the
// duration of the test that uses the config. `extra` is appended after the
// `[global]` section, so it may extend it with bare keys or add new tables.
fn tempdir_config(extra: &str) -> (Configuration, TempDir, TempDir) {
    crate::metrics_provider::init_for_tests();
    let blobs = TempDir::new().unwrap();
    let meta = TempDir::new().unwrap();
    let toml = format!(
        r#"
        [blob_store.fs]
        root_dir = "{blobs}"

        [metadata_store.fs]
        root_dir = "{meta}"

        [cache.memory]

        [server]
        bind_address = "127.0.0.1"
        port = 8080

        [global]
        update_pull_time = false

        {extra}
    "#,
        blobs = blobs.path().display(),
        meta = meta.path().display(),
    );
    (Configuration::load_from_str(&toml).unwrap(), blobs, meta)
}

fn create_minimal_config() -> (Configuration, TempDir, TempDir) {
    tempdir_config("")
}

fn create_config_with_repository() -> (Configuration, TempDir, TempDir) {
    tempdir_config(
        r#"
        [repository.test-repo.access_policy]
        default = "allow"
        rules = []
    "#,
    )
}

#[test]
fn test_build_blob_store_filesystem_success() {
    let (config, _blobs, _meta) = create_minimal_config();
    let result = config.blob_store.build_backend();

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_metadata_store_filesystem_success() {
    let (config, _blobs, _meta) = create_minimal_config();
    let auth_cache = bootstrap::auth_cache(&config.cache).unwrap();
    let result = bootstrap::metadata_store(
        &config.resolve_registry_storage(),
        &auth_cache,
        config.global.namespace_walk_concurrency,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_repository_with_upstream() {
    let repo_config = repository::Config {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        }),
        upstream: vec![repository::RegistryClientConfig {
            username: Some("testuser".to_string()),
            password: Some(Secret::new("testpass".to_string())),
            ..test_client_config("https://registry-1.docker.io")
        }],
        ..repository::Config::default()
    };
    let cache_config = cache::Config::Memory;
    let cache = bootstrap::auth_cache(&cache_config).unwrap();

    let result = bootstrap::repository(
        "cached-repo",
        &repo_config,
        &cache,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_repository_with_immutable_tags() {
    let repo_config = repository::Config {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        }),
        immutable_tags: true,
        immutable_tags_exclusions: vec![
            configuration::RegexPattern::compile("latest").unwrap(),
            configuration::RegexPattern::compile("dev-.*").unwrap(),
        ],
        ..repository::Config::default()
    };
    let cache_config = cache::Config::Memory;
    let cache = bootstrap::auth_cache(&cache_config).unwrap();

    let result = bootstrap::repository(
        "immutable-repo",
        &repo_config,
        &cache,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_repositories_single() {
    let repo_config = repository::Config {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        }),
        ..repository::Config::default()
    };
    let mut configs = HashMap::new();
    configs.insert("repo1".to_string(), repo_config);

    let cache_config = cache::Config::Memory;
    let cache = bootstrap::auth_cache(&cache_config).unwrap();

    let result = bootstrap::repositories(&configs, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await;

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 1);
    assert!(repos.contains_key("repo1"));
}

#[tokio::test]
async fn test_build_repositories_multiple() {
    let repo_config = repository::Config {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        }),
        ..repository::Config::default()
    };
    let mut configs = HashMap::new();
    configs.insert("repo1".to_string(), repo_config.clone());
    configs.insert("repo2".to_string(), repo_config.clone());
    configs.insert("repo3".to_string(), repo_config);

    let cache_config = cache::Config::Memory;
    let cache = bootstrap::auth_cache(&cache_config).unwrap();

    let result = bootstrap::repositories(&configs, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await;

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 3);
    assert!(repos.contains_key("repo1"));
    assert!(repos.contains_key("repo2"));
    assert!(repos.contains_key("repo3"));
}

#[tokio::test]
async fn test_build_registry_minimal_config() {
    let (config, _blobs, _meta) = create_minimal_config();
    let result = setup::build_registry(
        &config,
        &bootstrap::auth_cache(&config.cache).expect("auth cache"),
        &Arc::new(Mutex::new(None)),
        None,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_registry_with_repositories() {
    let (config, _blobs, _meta) = create_config_with_repository();
    let result = setup::build_registry(
        &config,
        &bootstrap::auth_cache(&config.cache).expect("auth cache"),
        &Arc::new(Mutex::new(None)),
        None,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_registry_with_update_pull_time() {
    // The [global] key cannot be overridden through the appended extra TOML,
    // so flip it on the parsed configuration instead.
    let (mut config, _blobs, _meta) = create_minimal_config();
    config.global.update_pull_time = true;

    let result = setup::build_registry(
        &config,
        &bootstrap::auth_cache(&config.cache).expect("auth cache"),
        &Arc::new(Mutex::new(None)),
        None,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_command_new_insecure_listener() {
    let (config, _blobs, _meta) = create_minimal_config();
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
    let (config, _blobs, _meta) = create_config_with_repository();
    let result = Command::new(&config).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_command_notify_config_change_insecure() {
    let (config, _blobs, _meta) = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    let (new_config, _new_blobs, _new_meta) = create_minimal_config();
    let result = command.notify_config_change(&new_config).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_command_notify_tls_config_change_with_insecure_listener() {
    let (config, _blobs, _meta) = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    let (tls_config, _temp_files) = build_config(false);
    let result = command.notify_tls_config_change(&tls_config);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_build_registry_components_integration() {
    let (config, _blobs, _meta) = create_config_with_repository();

    let auth_cache = bootstrap::auth_cache(&config.cache).unwrap();
    let blob_backend = std::sync::Arc::new(config.blob_store.build_backend().unwrap());
    let metadata_store = bootstrap::metadata_store(
        &config.resolve_registry_storage(),
        &auth_cache,
        config.global.namespace_walk_concurrency,
    )
    .await
    .unwrap();
    let repositories = bootstrap::repositories(
        &config.repository,
        &auth_cache,
        DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    )
    .await
    .unwrap();

    let registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        enable_blob_redirect: config.global.enable_blob_redirect,
        enable_manifest_redirect: config.global.enable_manifest_redirect,
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        ..RegistryConfig::default()
    };

    let registry = Registry::new(blob_backend, metadata_store, repositories, registry_config);

    let (namespaces, _) = registry.list_catalog_entries(None, None).await.unwrap();
    assert!(
        namespaces.is_empty(),
        "a freshly built registry must serve an empty catalog"
    );
}

#[tokio::test]
async fn test_build_repositories_with_different_configs() {
    let repo_config1 = repository::Config {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Allow,
            ..AccessPolicyConfig::default()
        }),
        ..repository::Config::default()
    };
    let repo_config2 = repository::Config {
        access_policy: Some(AccessPolicyConfig {
            default: AccessMode::Deny,
            rules: vec![CelRule::compile("identity.username == 'admin'").unwrap()],
        }),
        ..repository::Config::default()
    };

    let mut configs = HashMap::new();
    configs.insert("public".to_string(), repo_config1);
    configs.insert("private".to_string(), repo_config2);

    let cache_config = cache::Config::Memory;
    let cache = bootstrap::auth_cache(&cache_config).unwrap();
    let result = bootstrap::repositories(&configs, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await;

    assert!(result.is_ok());
    let repos = result.unwrap();
    assert_eq!(repos.len(), 2);
}

fn create_config_with_webhook(url: &str) -> (Configuration, TempDir, TempDir) {
    tempdir_config(&format!(
        r#"
        event_webhooks = ["test_hook"]

        [event_webhook.test_hook]
        url = "{url}"
        policy = "optional"
        events = ["manifest.push"]
    "#
    ))
}

fn create_config_with_two_webhooks(url_a: &str, url_b: &str) -> (Configuration, TempDir, TempDir) {
    tempdir_config(&format!(
        r#"
        event_webhooks = ["hook_a", "hook_b"]

        [event_webhook.hook_a]
        url = "{url_a}"
        policy = "optional"
        events = ["manifest.push"]

        [event_webhook.hook_b]
        url = "{url_b}"
        policy = "optional"
        events = ["manifest.push"]
    "#
    ))
}

#[tokio::test]
async fn test_hot_reload_adds_webhook_via_command() {
    let (config, _blobs, _meta) = create_minimal_config();
    let command = Command::new(&config).await.unwrap();

    assert!(
        !command
            .as_insecure()
            .unwrap()
            .current_context()
            .has_event_dispatcher()
    );

    let (new_config, _new_blobs, _new_meta) =
        create_config_with_webhook("https://example.com/webhook");
    command.notify_config_change(&new_config).await.unwrap();

    assert!(
        command
            .as_insecure()
            .unwrap()
            .current_context()
            .has_event_dispatcher()
    );
}

#[tokio::test]
async fn test_hot_reload_removes_webhook_via_command() {
    let (config, _blobs, _meta) = create_config_with_webhook("https://example.com/webhook");
    let command = Command::new(&config).await.unwrap();

    assert!(
        command
            .as_insecure()
            .unwrap()
            .current_context()
            .has_event_dispatcher()
    );

    let (new_config, _new_blobs, _new_meta) = create_minimal_config();
    command.notify_config_change(&new_config).await.unwrap();

    assert!(
        !command
            .as_insecure()
            .unwrap()
            .current_context()
            .has_event_dispatcher()
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

    let (config_a, _blobs_a, _meta_a) =
        create_config_with_webhook(&format!("{}/webhook", server_a.uri()));
    let command = Command::new(&config_a).await.unwrap();

    let (config_b, _blobs_b, _meta_b) =
        create_config_with_webhook(&format!("{}/webhook", server_b.uri()));
    command.notify_config_change(&config_b).await.unwrap();

    let context = command.as_insecure().unwrap().current_context();
    context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
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

    let (config_one, _blobs_one, _meta_one) = create_config_with_webhook(&server_a.uri());
    let command = Command::new(&config_one).await.unwrap();

    let (config_two, _blobs_two, _meta_two) =
        create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
    command.notify_config_change(&config_two).await.unwrap();

    let context = command.as_insecure().unwrap().current_context();
    context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
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

    let (config_two, _blobs_two, _meta_two) =
        create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
    let command = Command::new(&config_two).await.unwrap();

    let (config_one, _blobs_one, _meta_one) = create_config_with_webhook(&server_a.uri());
    command.notify_config_change(&config_one).await.unwrap();

    let context = command.as_insecure().unwrap().current_context();
    context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
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

    let (config_old, _blobs_old, _meta_old) = create_config_with_webhook(&server_old.uri());
    let command = Command::new(&config_old).await.unwrap();

    let old_context = Arc::clone(&command.as_insecure().unwrap().current_context());

    let (config_new, _blobs_new, _meta_new) = create_config_with_webhook(&server_new.uri());
    command.notify_config_change(&config_new).await.unwrap();

    old_context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();

    let new_context = command.as_insecure().unwrap().current_context();
    new_context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_command_shutdown_with_no_dispatcher() {
    use std::time::Duration;

    let (config, _blobs, _meta) = create_minimal_config();
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

    let (config, _blobs, _meta) = create_config_with_webhook(&server.uri());
    let command = Command::new(&config).await.unwrap();

    let context = command.as_insecure().unwrap().current_context();
    context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
    drop(context);

    command.shutdown_with_timeout(Duration::from_secs(10)).await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "Command::shutdown() must drain in-flight async webhook deliveries"
    );
}

fn create_tls_config() -> (
    Configuration,
    TempDir,
    TempDir,
    (
        tempfile::NamedTempFile,
        tempfile::NamedTempFile,
        tempfile::NamedTempFile,
    ),
) {
    let (tls_config, temp_files) = build_config(false);
    let (config, blobs, meta) = tempdir_config(&format!(
        r#"
        [server.tls]
        server_certificate_bundle = "{cert}"
        server_private_key = "{key}"
    "#,
        cert = tls_config.server_certificate_bundle.display(),
        key = tls_config.server_private_key.display(),
    ));

    (config, blobs, meta, temp_files)
}

#[tokio::test]
async fn test_notify_config_change_insecure_to_tls_does_not_fail() {
    init_crypto_provider();

    let (insecure_config, _blobs, _meta) = create_minimal_config();
    let command = Command::new(&insecure_config).await.unwrap();

    let (tls_config, _tls_blobs, _tls_meta, _temp_files) = create_tls_config();
    let result = command.notify_config_change(&tls_config).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_notify_config_change_tls_to_insecure_does_not_fail() {
    init_crypto_provider();

    let (tls_config, _tls_blobs, _tls_meta, _temp_files) = create_tls_config();
    let command = Command::new(&tls_config).await.unwrap();

    let (insecure_config, _blobs, _meta) = create_minimal_config();
    let result = command.notify_config_change(&insecure_config).await;

    assert!(result.is_ok());
}
