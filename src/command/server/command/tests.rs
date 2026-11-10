use std::sync::Arc;
use std::time::Duration;

use tempfile::{NamedTempFile, TempDir};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::command::server::command::ServiceListener;
use crate::command::server::listeners::tls::tests::{build_config, init_crypto_provider};
use crate::command::server::server_context::tests::create_test_event;
use crate::command::server::{Command, ServerContext};
use crate::configuration::Configuration;
use crate::metrics_provider::initialize_metrics;

// These tests build real filesystem stores, so the returned `TempDir` values
// must be kept alive for as long as the config is used. `extra` is appended
// after the `[global]` section, so it may extend it or add new tables.
fn tempdir_config(extra: &str) -> (Configuration, TempDir, TempDir) {
    initialize_metrics();
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

#[tokio::test]
async fn test_command_notify_tls_config_change_with_insecure_listener() {
    let (config, _blobs, _meta) = tempdir_config("");
    let command = Command::new(&config).await.unwrap();

    let (tls_config, _temp_files) = build_config(false);
    let result = command.notify_tls_config_change(&tls_config);

    assert!(result.is_ok());
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

/// The live context of a command built from an insecure-listener config.
fn context(command: &Command) -> Arc<ServerContext> {
    let ServiceListener::Insecure(listener) = &command.listener else {
        panic!("test commands listen insecurely");
    };
    listener.context.load_full()
}

#[tokio::test]
async fn test_hot_reload_adds_and_removes_webhook_via_command() {
    let has_dispatcher = |command: &Command| context(command).registry.event_dispatcher().is_some();
    let (config, _blobs, _meta) = tempdir_config("");
    let command = Command::new(&config).await.unwrap();
    assert!(!has_dispatcher(&command));

    let (with_hook, _hook_blobs, _hook_meta) =
        create_config_with_webhook("https://example.com/webhook");
    command.notify_config_change(&with_hook).await.unwrap();
    assert!(has_dispatcher(&command));

    command.notify_config_change(&config).await.unwrap();
    assert!(!has_dispatcher(&command));
}

/// Each generation delivers to its own webhook: a request the old context
/// started is not redirected by the reload, and the new one never reaches the
/// old URL (each server expects exactly one delivery).
#[tokio::test]
async fn test_hot_reload_inflight_old_dispatcher_still_works() {
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

    let old_context = context(&command);

    let (config_new, _blobs_new, _meta_new) = create_config_with_webhook(&server_new.uri());
    command.notify_config_change(&config_new).await.unwrap();

    old_context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();

    let new_context = context(&command);
    new_context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_command_shutdown_drains_in_flight_async_delivery() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
        .expect(1)
        .mount(&server)
        .await;

    let (config, _blobs, _meta) = create_config_with_webhook(&server.uri());
    let command = Command::new(&config).await.unwrap();

    let context = context(&command);
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
    (NamedTempFile, NamedTempFile, NamedTempFile),
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

    let (insecure_config, _blobs, _meta) = tempdir_config("");
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

    let (insecure_config, _blobs, _meta) = tempdir_config("");
    let result = command.notify_config_change(&insecure_config).await;

    assert!(result.is_ok());
}
