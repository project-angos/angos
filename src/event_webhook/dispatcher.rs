use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use regex::Regex;
use reqwest::Client;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::configuration::Error;
use crate::event_webhook::config::{DeliveryPolicy, EventWebhookConfig};
use crate::event_webhook::event::{Event, EventKind};

static DELIVERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "event_webhook_deliveries_total",
        "Total event webhook deliveries",
        &["webhook", "event", "result"]
    )
    .unwrap()
});

static DELIVERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "event_webhook_delivery_duration_seconds",
        "Event webhook delivery duration",
        &["webhook", "event"]
    )
    .unwrap()
});

const BLOCK_SIZE: usize = 64;

pub fn compute_signature(secret: &str, body: &[u8]) -> String {
    let key = secret.as_bytes();

    let normalized_key = if key.len() > BLOCK_SIZE {
        let hash = Sha256::digest(key);
        let mut padded = [0u8; BLOCK_SIZE];
        padded[..hash.len()].copy_from_slice(&hash);
        padded
    } else {
        let mut padded = [0u8; BLOCK_SIZE];
        padded[..key.len()].copy_from_slice(key);
        padded
    };

    let mut ipad = [0x36u8; BLOCK_SIZE];
    let mut opad = [0x5cu8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ipad[i] ^= normalized_key[i];
        opad[i] ^= normalized_key[i];
    }

    let mut inner_hasher = Sha256::new();
    inner_hasher.update(ipad);
    inner_hasher.update(body);
    let inner_hash = inner_hasher.finalize();

    let mut outer_hasher = Sha256::new();
    outer_hasher.update(opad);
    outer_hasher.update(inner_hash);

    hex::encode(outer_hasher.finalize())
}

pub fn matches_event(
    config: &EventWebhookConfig,
    event_kind: &EventKind,
    repository: &str,
) -> bool {
    if !config.events.contains(event_kind) {
        return false;
    }

    match &config.repository_filter {
        None => true,
        Some(filters) => filters
            .iter()
            .any(|pattern| Regex::new(pattern).is_ok_and(|re| re.is_match(repository))),
    }
}

struct WebhookEndpoint {
    client: Client,
    config: EventWebhookConfig,
}

pub struct EventDispatcher {
    endpoints: HashMap<String, WebhookEndpoint>,
}

async fn send_request(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
) -> Result<(), String> {
    let mut request = client
        .post(url)
        .header("content-type", "application/json")
        .header("X-Registry-Event", event_kind_header);

    if let Some(token) = token {
        let signature = compute_signature(token, body);
        request = request
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Registry-Signature-256", format!("sha256={signature}"));
    }

    let response = request
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("returned status {}", response.status()));
    }

    Ok(())
}

async fn send_with_retries(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
    max_retries: u32,
) -> Result<(), String> {
    let mut last_err = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
            tokio::time::sleep(backoff).await;
        }

        match send_request(client, url, token, body, event_kind_header).await {
            Ok(()) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| "unknown error".to_string()))
}

impl EventDispatcher {
    pub fn new(webhooks: HashMap<String, EventWebhookConfig>) -> Result<Self, Error> {
        let mut endpoints = HashMap::new();

        for (name, config) in webhooks {
            let client = Client::builder()
                .timeout(Duration::from_millis(config.timeout_ms))
                .build()
                .map_err(|e| {
                    Error::Initialization(format!(
                        "Failed to create HTTP client for webhook '{name}': {e}"
                    ))
                })?;

            endpoints.insert(name, WebhookEndpoint { client, config });
        }

        Ok(Self { endpoints })
    }

    pub async fn dispatch(&self, event: &Event) -> Result<(), Error> {
        let body = serde_json::to_vec(event)
            .map_err(|e| Error::Initialization(format!("Failed to serialize event: {e}")))?;

        let event_kind_header = serde_json::to_value(&event.kind)
            .map_err(|e| Error::Initialization(format!("Failed to serialize event kind: {e}")))?
            .as_str()
            .unwrap_or_default()
            .to_string();

        for (name, endpoint) in &self.endpoints {
            if !matches_event(&endpoint.config, &event.kind, &event.repository) {
                continue;
            }

            let timer = DELIVERY_DURATION
                .with_label_values(&[name.as_str(), event_kind_header.as_str()])
                .start_timer();

            match endpoint.config.policy {
                DeliveryPolicy::Async => {
                    let client = endpoint.client.clone();
                    let url = endpoint.config.url.clone();
                    let token = endpoint.config.token.clone();
                    let body = body.clone();
                    let event_kind_header = event_kind_header.clone();
                    let name = name.clone();
                    let max_retries = endpoint.config.max_retries;
                    tokio::spawn(async move {
                        let result = send_with_retries(
                            &client,
                            &url,
                            token.as_deref(),
                            &body,
                            &event_kind_header,
                            max_retries,
                        )
                        .await;
                        let result_label = if result.is_ok() { "success" } else { "error" };
                        DELIVERY_TOTAL
                            .with_label_values(&[name.as_str(), &event_kind_header, result_label])
                            .inc();
                        if let Err(e) = result {
                            warn!("Async webhook '{name}' failed: {e}");
                        }
                    });
                }
                DeliveryPolicy::Required => {
                    let result = send_with_retries(
                        &endpoint.client,
                        &endpoint.config.url,
                        endpoint.config.token.as_deref(),
                        &body,
                        &event_kind_header,
                        endpoint.config.max_retries,
                    )
                    .await;
                    let result_label = if result.is_ok() { "success" } else { "error" };
                    DELIVERY_TOTAL
                        .with_label_values(&[name.as_str(), &event_kind_header, result_label])
                        .inc();
                    result.map_err(|e| {
                        Error::Initialization(format!("Webhook '{name}' failed: {e}"))
                    })?;
                }
                DeliveryPolicy::Optional => {
                    let result = send_with_retries(
                        &endpoint.client,
                        &endpoint.config.url,
                        endpoint.config.token.as_deref(),
                        &body,
                        &event_kind_header,
                        endpoint.config.max_retries,
                    )
                    .await;
                    let result_label = if result.is_ok() { "success" } else { "error" };
                    DELIVERY_TOTAL
                        .with_label_values(&[name.as_str(), &event_kind_header, result_label])
                        .inc();
                    if let Err(e) = result {
                        warn!("Optional webhook '{name}' failed: {e}");
                    }
                }
            }

            timer.observe_duration();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use chrono::Utc;
    use prometheus::proto::MetricType;
    use uuid::Uuid;
    use wiremock::matchers::{body_json, header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{EventDispatcher, compute_signature, matches_event};
    use crate::event_webhook::config::{DeliveryPolicy, EventWebhookConfig};
    use crate::event_webhook::event::{Event, EventKind};

    fn create_test_event() -> Event {
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

    #[test]
    fn compute_signature_returns_valid_hex() {
        let body = serde_json::to_vec(&create_test_event()).unwrap();
        let signature = compute_signature("my-secret", &body);
        assert!(
            signature.chars().all(|c| c.is_ascii_hexdigit()),
            "signature must be valid hex: {signature}"
        );
    }

    #[test]
    fn compute_signature_has_correct_length() {
        let body = serde_json::to_vec(&create_test_event()).unwrap();
        let signature = compute_signature("token", &body);
        assert_eq!(
            signature.len(),
            64,
            "SHA-256 HMAC hex digest must be 64 chars"
        );
    }

    #[test]
    fn compute_signature_is_deterministic() {
        let body = b"fixed payload";
        let sig1 = compute_signature("secret", body);
        let sig2 = compute_signature("secret", body);
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn compute_signature_differs_for_different_secrets() {
        let body = b"same payload";
        let sig1 = compute_signature("secret-a", body);
        let sig2 = compute_signature("secret-b", body);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn compute_signature_differs_for_different_bodies() {
        let sig1 = compute_signature("secret", b"body-a");
        let sig2 = compute_signature("secret", b"body-b");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn compute_signature_matches_known_value() {
        // Pre-computed HMAC-SHA256("test-secret", "hello world") using standard tools
        // echo -n "hello world" | openssl dgst -sha256 -hmac "test-secret"
        let expected = "046e2496e13e0bfd8dbef84244dd188311a48086646355161bc4ad0769a49cf4";
        let signature = compute_signature("test-secret", b"hello world");
        assert_eq!(signature, expected);
    }

    #[test]
    fn compute_signature_empty_body() {
        let sig = compute_signature("secret", b"");
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    fn create_test_config(
        events: Vec<EventKind>,
        repository_filter: Option<Vec<String>>,
    ) -> EventWebhookConfig {
        EventWebhookConfig {
            url: "https://example.com/webhook".to_string(),
            policy: DeliveryPolicy::Optional,
            token: None,
            timeout_ms: 5000,
            max_retries: 0,
            events,
            repository_filter,
        }
    }

    #[test]
    fn matches_event_no_filter_matches_all_repositories() {
        let config = create_test_config(vec![EventKind::ManifestPush], None);
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "myapp/backend"
        ));
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "other/thing"
        ));
        assert!(matches_event(&config, &EventKind::ManifestPush, "anything"));
    }

    #[test]
    fn matches_event_with_filter_matches_matching_repository() {
        let config = create_test_config(
            vec![EventKind::ManifestPush],
            Some(vec!["^myapp/.*".to_string()]),
        );
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "myapp/backend"
        ));
    }

    #[test]
    fn matches_event_with_filter_rejects_non_matching_repository() {
        let config = create_test_config(
            vec![EventKind::ManifestPush],
            Some(vec!["^myapp/.*".to_string()]),
        );
        assert!(!matches_event(
            &config,
            &EventKind::ManifestPush,
            "other/thing"
        ));
    }

    #[test]
    fn matches_event_multiple_filters_matches_if_any_pattern_matches() {
        let config = create_test_config(
            vec![EventKind::ManifestPush],
            Some(vec!["^myapp/.*".to_string(), "^library/.*".to_string()]),
        );
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "myapp/backend"
        ));
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "library/nginx"
        ));
        assert!(!matches_event(
            &config,
            &EventKind::ManifestPush,
            "other/repo"
        ));
    }

    #[test]
    fn matches_event_filters_by_event_kind() {
        let config = create_test_config(vec![EventKind::ManifestPush], None);
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "myapp/backend"
        ));
        assert!(!matches_event(
            &config,
            &EventKind::BlobPush,
            "myapp/backend"
        ));
        assert!(!matches_event(
            &config,
            &EventKind::TagCreate,
            "myapp/backend"
        ));
    }

    #[test]
    fn matches_event_multiple_event_kinds() {
        let config = create_test_config(vec![EventKind::ManifestPush, EventKind::TagCreate], None);
        assert!(matches_event(&config, &EventKind::ManifestPush, "repo"));
        assert!(matches_event(&config, &EventKind::TagCreate, "repo"));
        assert!(!matches_event(&config, &EventKind::ManifestDelete, "repo"));
        assert!(!matches_event(&config, &EventKind::BlobPush, "repo"));
    }

    #[test]
    fn matches_event_both_event_kind_and_repository_must_match() {
        let config = create_test_config(
            vec![EventKind::ManifestPush],
            Some(vec!["^myapp/.*".to_string()]),
        );
        assert!(matches_event(
            &config,
            &EventKind::ManifestPush,
            "myapp/backend"
        ));
        assert!(!matches_event(
            &config,
            &EventKind::BlobPush,
            "myapp/backend"
        ));
        assert!(!matches_event(
            &config,
            &EventKind::ManifestPush,
            "other/repo"
        ));
        assert!(!matches_event(&config, &EventKind::BlobPush, "other/repo"));
    }

    fn create_webhook_config_for_url(
        url: &str,
        events: Vec<EventKind>,
        token: Option<String>,
    ) -> EventWebhookConfig {
        EventWebhookConfig {
            url: url.to_string(),
            policy: DeliveryPolicy::Required,
            token,
            timeout_ms: 5000,
            max_retries: 0,
            events,
            repository_filter: None,
        }
    }

    #[test]
    fn event_dispatcher_new_constructs_from_configs() {
        let mut webhooks = HashMap::new();
        webhooks.insert(
            "hook1".to_string(),
            create_webhook_config_for_url(
                "https://example.com/hook1",
                vec![EventKind::ManifestPush],
                None,
            ),
        );
        webhooks.insert(
            "hook2".to_string(),
            create_webhook_config_for_url(
                "https://example.com/hook2",
                vec![EventKind::TagCreate],
                Some("secret".to_string()),
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks);
        assert!(dispatcher.is_ok());
    }

    #[test]
    fn event_dispatcher_new_empty_configs() {
        let webhooks = HashMap::new();
        let dispatcher = EventDispatcher::new(webhooks);
        assert!(dispatcher.is_ok());
    }

    #[tokio::test]
    async fn dispatch_sends_post_with_json_body() {
        let server = MockServer::start().await;
        let event = create_test_event();
        let expected_body = serde_json::to_value(&event).unwrap();

        Mock::given(method("POST"))
            .and(body_json(&expected_body))
            .and(header("content-type", "application/json"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "test-hook".to_string(),
            create_webhook_config_for_url(&server.uri(), vec![EventKind::ManifestPush], None),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_sends_event_kind_header() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .and(header("X-Registry-Event", "manifest.push"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "test-hook".to_string(),
            create_webhook_config_for_url(&server.uri(), vec![EventKind::ManifestPush], None),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_sends_authorization_bearer_header() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .and(header("Authorization", "Bearer my-token"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "test-hook".to_string(),
            create_webhook_config_for_url(
                &server.uri(),
                vec![EventKind::ManifestPush],
                Some("my-token".to_string()),
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_sends_hmac_signature_header() {
        let server = MockServer::start().await;
        let event = create_test_event();
        let body = serde_json::to_vec(&event).unwrap();
        let expected_sig = format!("sha256={}", compute_signature("hmac-secret", &body));

        Mock::given(method("POST"))
            .and(header("X-Registry-Signature-256", expected_sig.as_str()))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "test-hook".to_string(),
            create_webhook_config_for_url(
                &server.uri(),
                vec![EventKind::ManifestPush],
                Some("hmac-secret".to_string()),
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_no_signature_header_without_token() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "test-hook".to_string(),
            create_webhook_config_for_url(&server.uri(), vec![EventKind::ManifestPush], None),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .headers
                .get("X-Registry-Signature-256")
                .is_none()
        );
        assert!(requests[0].headers.get("Authorization").is_none());
    }

    #[tokio::test]
    async fn dispatch_skips_webhook_for_non_matching_event_kind() {
        let server = MockServer::start().await;
        let event = create_test_event(); // ManifestPush

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "test-hook".to_string(),
            create_webhook_config_for_url(
                &server.uri(),
                vec![EventKind::BlobPush], // does not match ManifestPush
                None,
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_skips_webhook_for_non_matching_repository() {
        let server = MockServer::start().await;
        let event = create_test_event(); // repository = "docker-hub"

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        let mut config =
            create_webhook_config_for_url(&server.uri(), vec![EventKind::ManifestPush], None);
        config.repository_filter = Some(vec!["^internal/.*".to_string()]);
        webhooks.insert("test-hook".to_string(), config);

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    fn create_webhook_config_with_policy(
        url: &str,
        policy: DeliveryPolicy,
        events: Vec<EventKind>,
    ) -> EventWebhookConfig {
        EventWebhookConfig {
            url: url.to_string(),
            policy,
            token: None,
            timeout_ms: 5000,
            max_retries: 0,
            events,
            repository_filter: None,
        }
    }

    #[tokio::test]
    async fn dispatch_required_policy_returns_error_on_server_error() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "required-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Required,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_err(), "Required policy must return error on 500");
    }

    #[tokio::test]
    async fn dispatch_required_policy_returns_ok_on_success() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "required-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Required,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_optional_policy_returns_ok_on_server_error() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "optional-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Optional,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok(), "Optional policy must return Ok even on 500");
    }

    #[tokio::test]
    async fn dispatch_optional_policy_returns_ok_on_success() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "optional-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Optional,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dispatch_async_policy_returns_ok_immediately_despite_slow_webhook() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("ok")
                    .set_delay(Duration::from_secs(2)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "async-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Async,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();

        let start = std::time::Instant::now();
        let result = dispatcher.dispatch(&event).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(
            elapsed < Duration::from_millis(500),
            "Async dispatch must return immediately, took {elapsed:?}"
        );

        // Wait for the background task to complete so wiremock can verify
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn dispatch_async_policy_eventually_delivers() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .and(header("X-Registry-Event", "manifest.push"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "async-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Async,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok());

        // Give the spawned task time to deliver
        tokio::time::sleep(Duration::from_millis(500)).await;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.len(),
            1,
            "Async webhook must eventually deliver the request"
        );
    }

    fn create_webhook_config_with_retries(
        url: &str,
        policy: DeliveryPolicy,
        max_retries: u32,
    ) -> EventWebhookConfig {
        EventWebhookConfig {
            url: url.to_string(),
            policy,
            token: None,
            timeout_ms: 5000,
            max_retries,
            events: vec![EventKind::ManifestPush],
            repository_filter: None,
        }
    }

    #[tokio::test]
    async fn dispatch_required_retries_until_success() {
        let server = MockServer::start().await;
        let event = create_test_event();

        // First 2 requests fail, third succeeds
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .expect(2)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "retry-hook".to_string(),
            create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Required, 2),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_ok(), "Should succeed after retrying: {result:?}");

        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.len(),
            3,
            "Expected 1 initial + 2 retries = 3 total"
        );
    }

    #[tokio::test]
    async fn dispatch_required_retries_exhausted_returns_error() {
        let server = MockServer::start().await;
        let event = create_test_event();

        // All requests fail (1 initial + 1 retry = 2 total)
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(2)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "retry-hook".to_string(),
            create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Required, 1),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(
            result.is_err(),
            "Required policy must error when all retries exhausted"
        );
    }

    #[tokio::test]
    async fn dispatch_optional_retries_exhausted_returns_ok() {
        let server = MockServer::start().await;
        let event = create_test_event();

        // All requests fail (1 initial + 2 retries = 3 total)
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(3)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "retry-hook".to_string(),
            create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Optional, 2),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(
            result.is_ok(),
            "Optional policy must return Ok even when all retries exhausted"
        );
    }

    #[tokio::test]
    async fn dispatch_no_retry_when_max_retries_zero() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "no-retry-hook".to_string(),
            create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Required, 0),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        let result = dispatcher.dispatch(&event).await;
        assert!(result.is_err());

        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.len(),
            1,
            "Should only make 1 attempt with max_retries=0"
        );
    }

    #[tokio::test]
    async fn dispatch_records_delivery_total_metric() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "metrics-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Required,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        dispatcher.dispatch(&event).await.unwrap();

        let families = prometheus::gather();
        let delivery_metric = families
            .iter()
            .find(|f| f.name() == "event_webhook_deliveries_total");
        assert!(
            delivery_metric.is_some(),
            "event_webhook_deliveries_total metric must exist"
        );

        let family = delivery_metric.unwrap();
        assert_eq!(family.get_field_type(), MetricType::COUNTER);

        let metrics = family.get_metric();
        let found = metrics.iter().any(|m| {
            let labels: Vec<_> = m
                .get_label()
                .iter()
                .map(|l| (l.name(), l.value()))
                .collect();
            labels.contains(&("webhook", "metrics-hook"))
                && labels.contains(&("event", "manifest.push"))
                && labels.contains(&("result", "success"))
        });
        assert!(
            found,
            "Must have metric with webhook=metrics-hook, event=manifest.push, result=success"
        );
    }

    #[tokio::test]
    async fn dispatch_records_delivery_duration_metric() {
        let server = MockServer::start().await;
        let event = create_test_event();

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "duration-hook".to_string(),
            create_webhook_config_with_policy(
                &server.uri(),
                DeliveryPolicy::Required,
                vec![EventKind::ManifestPush],
            ),
        );

        let dispatcher = EventDispatcher::new(webhooks).unwrap();
        dispatcher.dispatch(&event).await.unwrap();

        let families = prometheus::gather();
        let duration_metric = families
            .iter()
            .find(|f| f.name() == "event_webhook_delivery_duration_seconds");
        assert!(
            duration_metric.is_some(),
            "event_webhook_delivery_duration_seconds metric must exist"
        );

        let family = duration_metric.unwrap();
        assert_eq!(family.get_field_type(), MetricType::HISTOGRAM);

        let metrics = family.get_metric();
        let found = metrics.iter().any(|m| {
            let labels: Vec<_> = m
                .get_label()
                .iter()
                .map(|l| (l.name(), l.value()))
                .collect();
            labels.contains(&("webhook", "duration-hook"))
                && labels.contains(&("event", "manifest.push"))
        });
        assert!(
            found,
            "Must have duration metric with webhook=duration-hook, event=manifest.push"
        );
    }
}
