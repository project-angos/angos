use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use reqwest::Client;
use url::Url;
use uuid::Uuid;

use super::super::{EventDispatcher, endpoint::WebhookEndpoint};
use crate::{
    configuration::RegexPattern,
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        event::{Event, EventKind},
    },
    secret::Secret,
};

pub const TEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub fn into_arc_map(
    webhooks: HashMap<String, EventWebhookConfig>,
) -> HashMap<String, Arc<EventWebhookConfig>> {
    let mut out = HashMap::with_capacity(webhooks.len());
    for (name, config) in webhooks {
        out.insert(name, Arc::new(config));
    }
    out
}

pub fn build_dispatcher(webhooks: HashMap<String, EventWebhookConfig>) -> EventDispatcher {
    EventDispatcher::new(into_arc_map(webhooks)).expect("dispatcher should build in tests")
}

pub fn create_test_event() -> Event {
    Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: "library/nginx".to_string(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: Some("latest".to_string()),
        actor: None,
        repository: "docker-hub".to_string(),
    }
}

pub fn create_test_config(
    events: Vec<EventKind>,
    repository_filter: Option<Vec<RegexPattern>>,
) -> EventWebhookConfig {
    EventWebhookConfig {
        name: String::new(),
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
        config: Arc::new(config),
    }
}

pub fn create_webhook_config_for_url(
    url: &str,
    events: Vec<EventKind>,
    token: Option<&str>,
) -> EventWebhookConfig {
    EventWebhookConfig {
        name: String::new(),
        url: Url::parse(url).unwrap(),
        policy: DeliveryPolicy::Required,
        token: token.map(|t| Secret::new(t.to_string())),
        timeout_ms: 5000,
        max_retries: 0,
        events,
        repository_filter: None,
    }
}

pub fn create_webhook_config_with_policy(
    url: &str,
    policy: DeliveryPolicy,
    events: Vec<EventKind>,
) -> EventWebhookConfig {
    EventWebhookConfig {
        name: String::new(),
        url: Url::parse(url).unwrap(),
        policy,
        token: None,
        timeout_ms: 5000,
        max_retries: 0,
        events,
        repository_filter: None,
    }
}

pub fn create_webhook_config_with_retries(
    url: &str,
    policy: DeliveryPolicy,
    max_retries: u32,
) -> EventWebhookConfig {
    EventWebhookConfig {
        name: String::new(),
        url: Url::parse(url).unwrap(),
        policy,
        token: None,
        timeout_ms: 5000,
        max_retries,
        events: vec![EventKind::ManifestPush],
        repository_filter: None,
    }
}
