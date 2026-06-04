use std::{collections::HashMap, time::Duration};

use chrono::Utc;
use reqwest::Client;
use url::Url;
use uuid::Uuid;

use crate::{
    configuration::RegexPattern,
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        dispatcher::{EventDispatcher, WebhookEndpoint},
        event::{Event, EventKind},
    },
    secret::Secret,
};

pub const TEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub fn build_dispatcher(webhooks: HashMap<String, EventWebhookConfig>) -> EventDispatcher {
    EventDispatcher::builder()
        .webhooks(webhooks)
        .build()
        .expect("dispatcher should build in tests")
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
        origin: None,
        repository: "docker-hub".to_string(),
    }
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
