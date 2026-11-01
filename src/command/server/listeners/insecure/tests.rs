use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::command::server::ServerContext;
use crate::command::server::listeners::insecure::InsecureListener;
use crate::command::server::server_context::tests::{
    TestConfigOptions, TestWebhook, create_test_event, create_test_server_context,
    create_test_server_context_with,
};
use crate::configuration::listeners::ListenerBaseConfig;

#[test]
fn test_config_default_values() {
    let config = ListenerBaseConfig::default();

    assert_eq!(config.port, 8000);
    assert_eq!(config.query_timeout.get(), 3600);
    assert_eq!(config.query_timeout_grace_period.get(), 60);
    assert_eq!(config.bind_address, IpAddr::from(Ipv4Addr::from([0; 4])));
}

#[test]
fn test_config_rejects_zero_query_timeout() {
    let toml = r#"
        bind_address = "192.168.1.100"
        query_timeout = 0
    "#;

    let error = toml::from_str::<ListenerBaseConfig>(toml).unwrap_err();

    assert!(error.to_string().contains("query_timeout"), "got: {error}");
}

#[test]
fn test_config_rejects_zero_query_timeout_grace_period() {
    let toml = r#"
        bind_address = "192.168.1.100"
        query_timeout_grace_period = 0
    "#;

    let error = toml::from_str::<ListenerBaseConfig>(toml).unwrap_err();

    assert!(
        error.to_string().contains("query_timeout_grace_period"),
        "got: {error}"
    );
}

async fn create_context_with_webhook(webhook_url: &str) -> ServerContext {
    create_test_server_context_with(TestConfigOptions {
        webhooks: vec![TestWebhook {
            name: "test_hook",
            url: webhook_url,
        }],
        ..TestConfigOptions::default()
    })
    .await
}

#[tokio::test]
async fn test_hot_reload_updates_webhook_config() {
    let listener_config = ListenerBaseConfig::default();
    let context_without_webhooks = create_test_server_context().await;
    let listener = InsecureListener::new(&listener_config, context_without_webhooks);

    assert!(!listener.current_context().registry.has_event_dispatcher());

    let context_with_webhooks = create_context_with_webhook("https://example.com/webhook").await;
    listener.notify_config_change(&listener_config, context_with_webhooks);

    assert!(listener.current_context().registry.has_event_dispatcher());
}

#[tokio::test]
async fn test_hot_reload_removes_webhooks() {
    let listener_config = ListenerBaseConfig::default();
    let context_with_webhooks = create_context_with_webhook("https://example.com/webhook").await;
    let listener = InsecureListener::new(&listener_config, context_with_webhooks);

    assert!(listener.current_context().registry.has_event_dispatcher());

    let context_without_webhooks = create_test_server_context().await;
    listener.notify_config_change(&listener_config, context_without_webhooks);

    assert!(!listener.current_context().registry.has_event_dispatcher());
}

#[tokio::test]
async fn test_hot_reload_changes_webhook_url() {
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

    let listener_config = ListenerBaseConfig::default();
    let webhook_a = format!("{}/webhook", server_a.uri());
    let context_a = create_context_with_webhook(&webhook_a).await;
    let listener = InsecureListener::new(&listener_config, context_a);

    let webhook_b = format!("{}/webhook", server_b.uri());
    let context_b = create_context_with_webhook(&webhook_b).await;
    listener.notify_config_change(&listener_config, context_b);

    let current_context = listener.current_context();
    let event = create_test_event();
    current_context
        .registry
        .dispatch_events(&[event])
        .await
        .unwrap();
}

// A reload must apply changed timeouts to the running listener, not only the
// server context.
#[tokio::test]
async fn test_hot_reload_updates_timeouts() {
    let listener_config = ListenerBaseConfig::default();
    let context = create_test_server_context().await;
    let listener = InsecureListener::new(&listener_config, context);
    assert_eq!(listener.current_timeouts().query, Duration::from_hours(1));

    let reloaded = ListenerBaseConfig {
        query_timeout: NonZeroU64::new(10).unwrap(),
        query_timeout_grace_period: NonZeroU64::new(5).unwrap(),
        ..ListenerBaseConfig::default()
    };
    listener.notify_config_change(&reloaded, create_test_server_context().await);

    assert_eq!(listener.current_timeouts().query, Duration::from_secs(10));
    assert_eq!(listener.current_timeouts().grace, Duration::from_secs(5));
}

#[tokio::test]
async fn test_hot_reload_in_flight_delivery_not_disrupted() {
    let server_old = MockServer::start().await;
    let server_new = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server_old)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server_new)
        .await;

    let listener_config = ListenerBaseConfig::default();
    let context_old = create_context_with_webhook(&server_old.uri()).await;
    let listener = InsecureListener::new(&listener_config, context_old);

    let old_context = Arc::clone(&listener.current_context());

    let context_new = create_context_with_webhook(&server_new.uri()).await;
    listener.notify_config_change(&listener_config, context_new);

    old_context
        .registry
        .dispatch_events(&[create_test_event()])
        .await
        .unwrap();
}
