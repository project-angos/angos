use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::NonZeroU64,
    sync::Arc,
    time::Duration,
};

use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::{
    command::server::{
        ServerContext,
        listeners::insecure::{InsecureListener, InsecureListenerConfig},
        server_context::tests::{
            TestConfigOptions, TestWebhook, create_test_event, create_test_server_context,
            create_test_server_context_with,
        },
    },
    configuration::listeners::ListenerBaseConfig,
};

#[test]
fn test_config_default_values() {
    let config = InsecureListenerConfig::default();

    assert_eq!(config.base.port, 8000);
    assert_eq!(config.base.query_timeout.get(), 3600);
    assert_eq!(config.base.query_timeout_grace_period.get(), 60);
    assert_eq!(
        config.base.bind_address,
        IpAddr::from(Ipv4Addr::from([0; 4]))
    );
}

#[test]
fn test_config_custom_values() {
    let toml = r#"
        bind_address = "192.168.1.100"
        port = 9000
        query_timeout = 7200
        query_timeout_grace_period = 120
    "#;

    let config: InsecureListenerConfig = toml::from_str(toml).unwrap();

    assert_eq!(config.base.port, 9000);
    assert_eq!(config.base.query_timeout.get(), 7200);
    assert_eq!(config.base.query_timeout_grace_period.get(), 120);
    assert_eq!(
        config.base.bind_address,
        "192.168.1.100".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn test_config_rejects_zero_query_timeout() {
    let toml = r#"
        bind_address = "192.168.1.100"
        query_timeout = 0
    "#;

    let error = toml::from_str::<InsecureListenerConfig>(toml).unwrap_err();

    assert!(error.to_string().contains("query_timeout must be > 0"));
}

#[test]
fn test_config_rejects_zero_query_timeout_grace_period() {
    let toml = r#"
        bind_address = "192.168.1.100"
        query_timeout_grace_period = 0
    "#;

    let error = toml::from_str::<InsecureListenerConfig>(toml).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("query_timeout_grace_period must be > 0")
    );
}

#[test]
fn test_config_partial_defaults() {
    let toml = r#"
        bind_address = "10.0.0.1"
    "#;

    let config: InsecureListenerConfig = toml::from_str(toml).unwrap();

    assert_eq!(config.base.port, 8000);
    assert_eq!(config.base.query_timeout.get(), 3600);
    assert_eq!(config.base.query_timeout_grace_period.get(), 60);
}

#[test]
fn test_config_ipv6_address() {
    let toml = r#"
        bind_address = "::1"
        port = 8443
    "#;

    let config: InsecureListenerConfig = toml::from_str(toml).unwrap();

    assert_eq!(config.base.bind_address, IpAddr::from(Ipv6Addr::LOCALHOST));
    assert_eq!(config.base.port, 8443);
}

#[tokio::test]
async fn test_insecure_listener_new() {
    let config = InsecureListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8080,
            query_timeout: NonZeroU64::new(1800).unwrap(),
            query_timeout_grace_period: NonZeroU64::new(30).unwrap(),
        },
    };

    let context = create_test_server_context().await;
    let listener = InsecureListener::new(&config, context);

    assert_eq!(
        listener.binding_address,
        SocketAddr::from(([127, 0, 0, 1], 8080))
    );
}

#[tokio::test]
async fn test_insecure_listener_new_with_ipv6() {
    let config = InsecureListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "::1".parse().unwrap(),
            port: 9000,
            query_timeout: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period: NonZeroU64::new(60).unwrap(),
        },
    };

    let context = create_test_server_context().await;
    let listener = InsecureListener::new(&config, context);

    assert_eq!(
        listener.binding_address.ip(),
        "::1".parse::<IpAddr>().unwrap()
    );
    assert_eq!(listener.binding_address.port(), 9000);
}

#[tokio::test]
async fn test_insecure_listener_notify_config_change() {
    let config = InsecureListenerConfig::default();
    let context1 = create_test_server_context().await;
    let listener = InsecureListener::new(&config, context1);

    let context2 = create_test_server_context().await;
    listener.notify_config_change(context2);
}

#[tokio::test]
async fn test_insecure_listener_timeouts_initialization() {
    let config = InsecureListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8080,
            query_timeout: NonZeroU64::new(5000).unwrap(),
            query_timeout_grace_period: NonZeroU64::new(100).unwrap(),
        },
    };

    let context = create_test_server_context().await;
    let listener = InsecureListener::new(&config, context);

    let timeouts = listener.timeouts.load();
    assert_eq!(timeouts[0], Duration::from_secs(5000));
    assert_eq!(timeouts[1], Duration::from_secs(100));
}

#[tokio::test]
async fn test_insecure_listener_with_zero_port() {
    let config = InsecureListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 0,
            query_timeout: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period: NonZeroU64::new(60).unwrap(),
        },
    };

    let context = create_test_server_context().await;
    let listener = InsecureListener::new(&config, context);

    assert_eq!(listener.binding_address.port(), 0);
}

#[tokio::test]
async fn test_insecure_listener_multiple_config_changes() {
    let config = InsecureListenerConfig::default();
    let context1 = create_test_server_context().await;
    let listener = InsecureListener::new(&config, context1);

    for _ in 0..5 {
        let context = create_test_server_context().await;
        listener.notify_config_change(context);
    }
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
    let listener_config = InsecureListenerConfig::default();
    let context_without_webhooks = create_test_server_context().await;
    let listener = InsecureListener::new(&listener_config, context_without_webhooks);

    assert!(!listener.current_context().has_event_dispatcher());

    let context_with_webhooks = create_context_with_webhook("https://example.com/webhook").await;
    listener.notify_config_change(context_with_webhooks);

    assert!(listener.current_context().has_event_dispatcher());
}

#[tokio::test]
async fn test_hot_reload_removes_webhooks() {
    let listener_config = InsecureListenerConfig::default();
    let context_with_webhooks = create_context_with_webhook("https://example.com/webhook").await;
    let listener = InsecureListener::new(&listener_config, context_with_webhooks);

    assert!(listener.current_context().has_event_dispatcher());

    let context_without_webhooks = create_test_server_context().await;
    listener.notify_config_change(context_without_webhooks);

    assert!(!listener.current_context().has_event_dispatcher());
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

    let listener_config = InsecureListenerConfig::default();
    let webhook_a = format!("{}/webhook", server_a.uri());
    let context_a = create_context_with_webhook(&webhook_a).await;
    let listener = InsecureListener::new(&listener_config, context_a);

    let webhook_b = format!("{}/webhook", server_b.uri());
    let context_b = create_context_with_webhook(&webhook_b).await;
    listener.notify_config_change(context_b);

    let current_context = listener.current_context();
    let event = create_test_event();
    current_context.dispatch_event(&event).await.unwrap();
}

async fn create_context_with_two_webhooks(url_a: &str, url_b: &str) -> ServerContext {
    create_test_server_context_with(TestConfigOptions {
        webhooks: vec![
            TestWebhook {
                name: "hook_a",
                url: url_a,
            },
            TestWebhook {
                name: "hook_b",
                url: url_b,
            },
        ],
        ..TestConfigOptions::default()
    })
    .await
}

#[tokio::test]
async fn test_hot_reload_adds_second_webhook() {
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

    let listener_config = InsecureListenerConfig::default();
    let context_one = create_context_with_webhook(&server_a.uri()).await;
    let listener = InsecureListener::new(&listener_config, context_one);

    let context_two = create_context_with_two_webhooks(&server_a.uri(), &server_b.uri()).await;
    listener.notify_config_change(context_two);

    let context = listener.current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
}

#[tokio::test]
async fn test_hot_reload_removes_one_of_two_webhooks() {
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

    let listener_config = InsecureListenerConfig::default();
    let context_two = create_context_with_two_webhooks(&server_a.uri(), &server_b.uri()).await;
    let listener = InsecureListener::new(&listener_config, context_two);

    let context_one = create_context_with_webhook(&server_b.uri()).await;
    listener.notify_config_change(context_one);

    let context = listener.current_context();
    context.dispatch_event(&create_test_event()).await.unwrap();
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

    let listener_config = InsecureListenerConfig::default();
    let context_old = create_context_with_webhook(&server_old.uri()).await;
    let listener = InsecureListener::new(&listener_config, context_old);

    let old_context = Arc::clone(&listener.current_context());

    let context_new = create_context_with_webhook(&server_new.uri()).await;
    listener.notify_config_change(context_new);

    old_context
        .dispatch_event(&create_test_event())
        .await
        .unwrap();
}
