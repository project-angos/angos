use std::collections::HashMap;

use prometheus::proto::MetricType;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method},
};

use super::common::{build_dispatcher, create_test_event, create_test_webhook_config};
use crate::{
    configuration::RegexPattern,
    event_webhook::{EventSubscriber, config::DeliveryPolicy, event::EventKind},
};

#[test]
fn event_dispatcher_new_constructs_from_configs() {
    let mut webhooks = HashMap::new();
    webhooks.insert(
        "hook1".to_string(),
        create_test_webhook_config(
            "https://example.com/hook1",
            DeliveryPolicy::Required,
            None,
            0,
        ),
    );
    let mut hook2 = create_test_webhook_config(
        "https://example.com/hook2",
        DeliveryPolicy::Required,
        Some("secret"),
        0,
    );
    hook2.events = vec![EventKind::TagCreate];
    webhooks.insert("hook2".to_string(), hook2);

    let dispatcher = super::super::EventDispatcher::builder()
        .webhooks(webhooks)
        .build();
    assert!(dispatcher.is_ok());
}

#[test]
fn event_dispatcher_new_empty_configs() {
    let webhooks = HashMap::new();
    let dispatcher = super::super::EventDispatcher::builder()
        .webhooks(webhooks)
        .build();
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
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn on_event_delivers_via_event_subscriber_path() {
    // The EventDispatcher must deliver through the EventSubscriber::on_event
    // trait method (the surface ServerContext uses), not only its inherent
    // `dispatch`. This proves the trait delegation is wired correctly.
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "subscriber-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    // Drive through the trait method explicitly.
    let result = EventSubscriber::on_event(&dispatcher, &event).await;
    assert!(
        result.is_ok(),
        "on_event must deliver the event via the EventSubscriber path"
    );
}

#[tokio::test]
async fn on_event_surfaces_required_webhook_failure() {
    // A Required-policy webhook that returns 5xx must surface as an error from
    // the EventSubscriber path (so ServerContext records it as the overall
    // first error), preserving Required-vs-Optional semantics through the trait.
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "required-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = EventSubscriber::on_event(&dispatcher, &event).await;
    assert!(
        result.is_err(),
        "a Required webhook failure must surface through the EventSubscriber path"
    );
}

#[tokio::test]
async fn on_event_swallows_optional_webhook_failure() {
    // An Optional-policy webhook failure must NOT surface through the
    // EventSubscriber path (it is swallowed/logged inside the dispatcher), so it
    // never aborts a batch in ServerContext.
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "optional-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Optional, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = EventSubscriber::on_event(&dispatcher, &event).await;
    assert!(
        result.is_ok(),
        "an Optional webhook failure must be swallowed by the EventSubscriber path"
    );
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
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0),
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
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, Some("my-token"), 0),
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
    let mut config = create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0);
    config.events = vec![EventKind::BlobPush]; // does not match ManifestPush
    webhooks.insert("test-hook".to_string(), config);

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
    let mut config = create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0);
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
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0),
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
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0),
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
