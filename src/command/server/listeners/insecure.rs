use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tracing::{debug, info};

use crate::command::server::ServerContext;
use crate::command::server::error::Error;
use crate::command::server::listeners::{accept, build_listener};
use crate::command::server::serve_request;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub bind_address: IpAddr,
    #[serde(default = "Config::default_port")]
    pub port: u16,
    #[serde(default = "Config::default_query_timeout")]
    pub query_timeout: u64,
    #[serde(default = "Config::default_query_timeout_grace_period")]
    pub query_timeout_grace_period: u64,
}

impl Config {
    fn default_port() -> u16 {
        8000
    }

    fn default_query_timeout() -> u64 {
        3600
    }

    fn default_query_timeout_grace_period() -> u64 {
        60
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: IpAddr::from(Ipv4Addr::from([0; 4])),
            port: Self::default_port(),
            query_timeout: Self::default_query_timeout(),
            query_timeout_grace_period: Self::default_query_timeout_grace_period(),
        }
    }
}

pub struct InsecureListener {
    binding_address: SocketAddr,
    context: ArcSwap<ServerContext>,
    timeouts: ArcSwap<[Duration; 2]>,
}

impl InsecureListener {
    pub fn new(server_config: &Config, context: ServerContext) -> Self {
        let binding_address = SocketAddr::new(server_config.bind_address, server_config.port);

        let timeouts = [
            Duration::from_secs(server_config.query_timeout),
            Duration::from_secs(server_config.query_timeout_grace_period),
        ];

        Self {
            binding_address,
            context: ArcSwap::from_pointee(context),
            timeouts: ArcSwap::from_pointee(timeouts),
        }
    }

    pub fn notify_config_change(&self, context: ServerContext) {
        self.context.store(Arc::new(context));
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        self.context.load().shutdown_with_timeout(timeout).await;
    }

    #[cfg(test)]
    pub fn current_context(&self) -> arc_swap::Guard<Arc<ServerContext>> {
        self.context.load()
    }

    pub async fn serve(&self) -> Result<(), Error> {
        info!("Listening on {} (non-TLS)", self.binding_address);
        let listener = build_listener(self.binding_address).await?;

        loop {
            debug!("Waiting for incoming connection");
            let (tcp, remote_address) = accept(&listener).await?;

            debug!("Accepted connection from {remote_address}");
            let stream = TokioIo::new(tcp);
            let context = Arc::clone(&self.context.load());
            let timeouts = Arc::clone(&self.timeouts.load());

            tokio::spawn(Box::pin(serve_request(
                stream,
                context,
                None,
                timeouts,
                remote_address,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::Ipv6Addr;

    use chrono::Utc;
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::command::server::server_context::tests::create_test_server_context;
    use crate::configuration::Configuration;
    use crate::event_webhook::event::{Event, EventKind};
    use crate::registry::{Registry, RegistryConfig};

    #[test]
    fn test_config_default_values() {
        let config = Config::default();

        assert_eq!(config.port, 8000);
        assert_eq!(config.query_timeout, 3600);
        assert_eq!(config.query_timeout_grace_period, 60);
        assert_eq!(config.bind_address, IpAddr::from(Ipv4Addr::from([0; 4])));
    }

    #[test]
    fn test_config_custom_values() {
        let toml = r#"
            bind_address = "192.168.1.100"
            port = 9000
            query_timeout = 7200
            query_timeout_grace_period = 120
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.port, 9000);
        assert_eq!(config.query_timeout, 7200);
        assert_eq!(config.query_timeout_grace_period, 120);
        assert_eq!(
            config.bind_address,
            "192.168.1.100".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn test_config_partial_defaults() {
        let toml = r#"
            bind_address = "10.0.0.1"
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.port, 8000);
        assert_eq!(config.query_timeout, 3600);
        assert_eq!(config.query_timeout_grace_period, 60);
    }

    #[test]
    fn test_config_ipv6_address() {
        let toml = r#"
            bind_address = "::1"
            port = 8443
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.bind_address, IpAddr::from(Ipv6Addr::LOCALHOST));
        assert_eq!(config.port, 8443);
    }

    #[test]
    fn test_insecure_listener_new() {
        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8080,
            query_timeout: 1800,
            query_timeout_grace_period: 30,
        };

        let context = create_test_server_context();
        let listener = InsecureListener::new(&config, context);

        assert_eq!(
            listener.binding_address,
            SocketAddr::from(([127, 0, 0, 1], 8080))
        );
    }

    #[test]
    fn test_insecure_listener_new_with_ipv6() {
        let config = Config {
            bind_address: "::1".parse().unwrap(),
            port: 9000,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
        };

        let context = create_test_server_context();
        let listener = InsecureListener::new(&config, context);

        assert_eq!(
            listener.binding_address.ip(),
            "::1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(listener.binding_address.port(), 9000);
    }

    #[test]
    fn test_insecure_listener_notify_config_change() {
        let config = Config::default();
        let context1 = create_test_server_context();
        let listener = InsecureListener::new(&config, context1);

        let context2 = create_test_server_context();
        listener.notify_config_change(context2);
    }

    #[test]
    fn test_insecure_listener_timeouts_initialization() {
        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8080,
            query_timeout: 5000,
            query_timeout_grace_period: 100,
        };

        let context = create_test_server_context();
        let listener = InsecureListener::new(&config, context);

        let timeouts = listener.timeouts.load();
        assert_eq!(timeouts[0], Duration::from_secs(5000));
        assert_eq!(timeouts[1], Duration::from_secs(100));
    }

    #[test]
    fn test_insecure_listener_with_zero_port() {
        let config = Config {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 0,
            query_timeout: 3600,
            query_timeout_grace_period: 60,
        };

        let context = create_test_server_context();
        let listener = InsecureListener::new(&config, context);

        assert_eq!(listener.binding_address.port(), 0);
    }

    #[test]
    fn test_insecure_listener_multiple_config_changes() {
        let config = Config::default();
        let context1 = create_test_server_context();
        let listener = InsecureListener::new(&config, context1);

        for _ in 0..5 {
            let context = create_test_server_context();
            listener.notify_config_change(context);
        }
    }

    fn create_config_with_webhook(webhook_url: &str) -> Configuration {
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
            url = "{webhook_url}"
            policy = "optional"
            events = ["manifest.push"]
        "#
        );

        toml::from_str(&toml).unwrap()
    }

    fn create_server_context_from_config(config: &Configuration) -> ServerContext {
        let blob_store = config.blob_store.to_backend().unwrap();
        let metadata_store = config.resolve_metadata_config().to_backend(None).unwrap();
        let repositories = Arc::new(HashMap::new());

        let registry_config = RegistryConfig::new()
            .update_pull_time(false)
            .enable_redirect(true)
            .concurrent_cache_jobs(10)
            .global_immutable_tags(false)
            .global_immutable_tags_exclusions(Vec::new());

        let registry =
            Registry::new(blob_store, metadata_store, repositories, registry_config).unwrap();

        ServerContext::new(config, registry).unwrap()
    }

    fn create_test_event() -> Event {
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

    #[test]
    fn test_hot_reload_updates_webhook_config() {
        let listener_config = Config::default();
        let context_without_webhooks = create_test_server_context();
        let listener = InsecureListener::new(&listener_config, context_without_webhooks);

        assert!(listener.current_context().event_dispatcher().is_none());

        let config_with_webhook = create_config_with_webhook("https://example.com/webhook");
        let context_with_webhooks = create_server_context_from_config(&config_with_webhook);
        listener.notify_config_change(context_with_webhooks);

        assert!(listener.current_context().event_dispatcher().is_some());
    }

    #[test]
    fn test_hot_reload_removes_webhooks() {
        let listener_config = Config::default();
        let config_with_webhook = create_config_with_webhook("https://example.com/webhook");
        let context_with_webhooks = create_server_context_from_config(&config_with_webhook);
        let listener = InsecureListener::new(&listener_config, context_with_webhooks);

        assert!(listener.current_context().event_dispatcher().is_some());

        let context_without_webhooks = create_test_server_context();
        listener.notify_config_change(context_without_webhooks);

        assert!(listener.current_context().event_dispatcher().is_none());
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

        let listener_config = Config::default();
        let config_a = create_config_with_webhook(&format!("{}/webhook", server_a.uri()));
        let context_a = create_server_context_from_config(&config_a);
        let listener = InsecureListener::new(&listener_config, context_a);

        let config_b = create_config_with_webhook(&format!("{}/webhook", server_b.uri()));
        let context_b = create_server_context_from_config(&config_b);
        listener.notify_config_change(context_b);

        let current_context = listener.current_context();
        let event = create_test_event();
        current_context.dispatch_event(&event).await.unwrap();
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

        toml::from_str(&toml).unwrap()
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

        let listener_config = Config::default();
        let config_one = create_config_with_webhook(&server_a.uri());
        let context_one = create_server_context_from_config(&config_one);
        let listener = InsecureListener::new(&listener_config, context_one);

        let config_two = create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
        let context_two = create_server_context_from_config(&config_two);
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

        let listener_config = Config::default();
        let config_two = create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
        let context_two = create_server_context_from_config(&config_two);
        let listener = InsecureListener::new(&listener_config, context_two);

        let config_one = create_config_with_webhook(&server_b.uri());
        let context_one = create_server_context_from_config(&config_one);
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

        let listener_config = Config::default();
        let config_old = create_config_with_webhook(&server_old.uri());
        let context_old = create_server_context_from_config(&config_old);
        let listener = InsecureListener::new(&listener_config, context_old);

        let old_context = Arc::clone(&listener.current_context());

        let config_new = create_config_with_webhook(&server_new.uri());
        let context_new = create_server_context_from_config(&config_new);
        listener.notify_config_change(context_new);

        old_context
            .dispatch_event(&create_test_event())
            .await
            .unwrap();
    }

    #[test]
    fn test_hot_reload_invalid_webhook_config_preserves_old_context() {
        let listener_config = Config::default();
        let config_valid = create_config_with_webhook("https://example.com/webhook");
        let context_valid = create_server_context_from_config(&config_valid);
        let listener = InsecureListener::new(&listener_config, context_valid);

        assert!(listener.current_context().event_dispatcher().is_some());

        let invalid_toml = r#"
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
            url = "https://example.com/new-webhook"
            policy = "optional"
            events = ["manifest.push"]
        "#;

        let invalid_config: Configuration = toml::from_str(invalid_toml).unwrap();
        let blob_store = invalid_config.blob_store.to_backend().unwrap();
        let metadata_store = invalid_config
            .resolve_metadata_config()
            .to_backend(None)
            .unwrap();
        let repositories = Arc::new(HashMap::new());

        let registry_config = RegistryConfig::new()
            .update_pull_time(false)
            .enable_redirect(true)
            .concurrent_cache_jobs(10)
            .global_immutable_tags(false)
            .global_immutable_tags_exclusions(Vec::new());

        let registry =
            Registry::new(blob_store, metadata_store, repositories, registry_config).unwrap();

        let result = ServerContext::new(&invalid_config, registry);
        assert!(result.is_err());

        assert!(listener.current_context().event_dispatcher().is_some());
    }
}
