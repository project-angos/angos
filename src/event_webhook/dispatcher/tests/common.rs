use std::collections::HashMap;

use reqwest::Client;
use url::Url;

use crate::{
    configuration::RegexPattern,
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        dispatcher::{EventDispatcher, WebhookEndpoint},
        event::{Event, EventKind},
    },
    metrics_provider,
    secret::Secret,
    test_fixtures::events::manifest_push_event,
};

pub fn build_dispatcher(webhooks: HashMap<String, EventWebhookConfig>) -> EventDispatcher {
    metrics_provider::init_for_tests();
    EventDispatcher::builder()
        .webhooks(webhooks)
        .build()
        .expect("dispatcher should build in tests")
}

/// Dispatcher over exactly one webhook registered as `name`; the remaining
/// parameters mirror [`create_test_webhook_config`].
pub fn single_hook_dispatcher(
    name: &str,
    url: &str,
    policy: DeliveryPolicy,
    token: Option<&str>,
    max_retries: u32,
) -> EventDispatcher {
    let mut webhooks = HashMap::new();
    webhooks.insert(
        name.to_string(),
        create_test_webhook_config(url, policy, token, max_retries),
    );
    build_dispatcher(webhooks)
}

pub fn create_test_event() -> Event {
    manifest_push_event("library/nginx", "docker-hub", Some("latest"))
}

pub fn create_test_config(
    events: Vec<EventKind>,
    repository_filter: Option<Vec<RegexPattern>>,
) -> EventWebhookConfig {
    EventWebhookConfig {
        url: Url::parse("https://example.com/webhook").unwrap(),
        policy: DeliveryPolicy::Optional,
        token: None,
        timeout_ms: 5000,
        max_retries: 0,
        events,
        repository_filter,
    }
}

pub fn build_endpoint(config: EventWebhookConfig) -> WebhookEndpoint {
    WebhookEndpoint {
        client: Client::new(),
        url: config.url,
        policy: config.policy,
        token: config.token,
        max_retries: config.max_retries,
        events: config.events,
        repository_filter: config.repository_filter,
    }
}

pub fn create_test_webhook_config(
    url: &str,
    policy: DeliveryPolicy,
    token: Option<&str>,
    max_retries: u32,
) -> EventWebhookConfig {
    EventWebhookConfig {
        url: Url::parse(url).unwrap(),
        policy,
        token: token.map(|t| Secret::new(t.to_string())),
        timeout_ms: 5000,
        max_retries,
        events: vec![EventKind::ManifestPush],
        repository_filter: None,
    }
}
