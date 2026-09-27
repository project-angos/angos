use std::collections::HashMap;

use reqwest::Client;
use url::Url;

use angos_oci::{Digest, Namespace, Reference};

use crate::{
    configuration::RegexPattern,
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        dispatcher::{EventDispatcher, WebhookEndpoint},
        event::{Event, EventKind},
    },
    metrics_provider,
};
use angos_secret::Secret;

const TEST_DIGEST: &str = "sha256:abc1230000000000000000000000000000000000000000000000000000000000";

pub fn build_dispatcher(webhooks: HashMap<String, EventWebhookConfig>) -> EventDispatcher {
    metrics_provider::init_for_tests();
    // Every webhook enabled for every repository: most tests exercise delivery alone.
    let global = webhooks.keys().cloned().collect();
    EventDispatcher::new(webhooks, global, HashMap::new())
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
    let digest: Digest = TEST_DIGEST.parse().expect("test digest should parse");
    Event::push_manifest(
        &Namespace::new("library/nginx").expect("test namespace should parse"),
        "docker-hub",
        &digest,
        &Reference::Digest(digest.clone()),
        None,
    )
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
        max_retries: Some(0),
        events,
        repository_filter,
    }
}

pub fn build_endpoint(config: EventWebhookConfig) -> WebhookEndpoint {
    WebhookEndpoint {
        client: Client::new(),
        max_retries: config.max_retries(),
        url: config.url,
        policy: config.policy,
        token: config.token,
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
        max_retries: Some(max_retries),
        events: vec![EventKind::ManifestPush],
        repository_filter: None,
    }
}
