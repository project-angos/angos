use std::collections::HashMap;

use prometheus::proto::MetricType;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method},
};

use super::common::{
    build_dispatcher, create_test_event, create_webhook_config_for_url,
    create_webhook_config_with_policy, into_arc_map,
};
use crate::{
    configuration::RegexPattern,
    event_webhook::{config::DeliveryPolicy, event::EventKind},
};

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
            Some("secret"),
        ),
    );

    let dispatcher = super::super::EventDispatcher::new(into_arc_map(webhooks));
    assert!(dispatcher.is_ok());
}

#[test]
fn event_dispatcher_new_empty_configs() {
    let webhooks = HashMap::new();
    let dispatcher = super::super::EventDispatcher::new(into_arc_map(webhooks));
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

    let dispatcher = build_dispatcher(webhooks);
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

    let dispatcher = build_dispatcher(webhooks);
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
            Some("my-token"),
        ),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());
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

    let dispatcher = build_dispatcher(webhooks);
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
    config.repository_filter = Some(vec![RegexPattern::compile("^internal/.*").unwrap()]);
    webhooks.insert("test-hook".to_string(), config);

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());
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

    let dispatcher = build_dispatcher(webhooks);
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

    let dispatcher = build_dispatcher(webhooks);
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
