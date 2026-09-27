use std::collections::HashMap;

use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method},
};

use super::common::{
    build_dispatcher, create_test_event, create_test_webhook_config, single_hook_dispatcher,
};
use crate::{
    configuration::RegexPattern,
    event_webhook::{config::DeliveryPolicy, dispatcher::EventDispatcher, event::EventKind},
    metrics_provider::{init_for_tests, metrics_provider},
};

#[test]
fn event_dispatcher_builder_constructs_from_configs() {
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

    let dispatcher = EventDispatcher::new(webhooks, Vec::new(), HashMap::new());
    assert!(dispatcher.is_ok());
}

#[test]
fn event_dispatcher_builder_empty_configs() {
    let webhooks = HashMap::new();
    let dispatcher = EventDispatcher::new(webhooks, Vec::new(), HashMap::new());
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

    let dispatcher = single_hook_dispatcher(
        "test-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        None,
        0,
    );
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

    let dispatcher = single_hook_dispatcher(
        "test-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        None,
        0,
    );
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

    let dispatcher = single_hook_dispatcher(
        "test-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        Some("my-token"),
        0,
    );
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

/// An event reaches the webhooks its repository names, else the global ones,
/// so a webhook no list names never fires.
#[tokio::test]
async fn dispatch_reaches_only_the_webhooks_the_repository_enables() {
    init_for_tests();
    let (audit, slack, unused) = (
        MockServer::start().await,
        MockServer::start().await,
        MockServer::start().await,
    );
    for (server, deliveries) in [(&audit, 1), (&slack, 1), (&unused, 0)] {
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(deliveries)
            .mount(server)
            .await;
    }
    let webhooks = [("audit", &audit), ("slack", &slack), ("unused", &unused)]
        .into_iter()
        .map(|(name, server)| {
            let config =
                create_test_webhook_config(&server.uri(), DeliveryPolicy::Required, None, 0);
            (name.to_string(), config)
        })
        .collect();
    let dispatcher = EventDispatcher::new(
        webhooks,
        vec!["slack".to_string()],
        HashMap::from([("docker-hub".to_string(), vec!["audit".to_string()])]),
    )
    .unwrap();

    // `docker-hub` names its own; a namespace under no repository gets the global.
    let named = create_test_event();
    let mut unmatched = create_test_event();
    unmatched.repository = String::new();
    dispatcher.dispatch(&named).await.unwrap();
    dispatcher.dispatch(&unmatched).await.unwrap();
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

    let dispatcher = single_hook_dispatcher(
        "metrics-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        None,
        0,
    );
    let counter = metrics_provider()
        .event_webhook_deliveries
        .with_label_values(&["metrics-hook", "manifest.push", "success"]);
    let before = counter.get();

    dispatcher.dispatch(&event).await.unwrap();

    assert_eq!(
        counter.get(),
        before + 1,
        "delivery must increment the success counter for this webhook/event"
    );

    // The counter must be exported by the same registry /metrics serves.
    let (_, payload) = metrics_provider().gather().unwrap();
    let text = String::from_utf8(payload).unwrap();
    assert!(
        text.contains("event_webhook_deliveries_total"),
        "event_webhook_deliveries_total must appear in the /metrics registry output"
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

    let dispatcher = single_hook_dispatcher(
        "duration-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        None,
        0,
    );
    let histogram = metrics_provider()
        .event_webhook_delivery_duration
        .with_label_values(&["duration-hook", "manifest.push"]);
    let before = histogram.get_sample_count();

    dispatcher.dispatch(&event).await.unwrap();

    assert_eq!(
        histogram.get_sample_count(),
        before + 1,
        "delivery must record one duration sample for this webhook/event"
    );

    // The histogram must be exported by the same registry /metrics serves.
    let (_, payload) = metrics_provider().gather().unwrap();
    let text = String::from_utf8(payload).unwrap();
    assert!(
        text.contains("event_webhook_delivery_duration_seconds"),
        "event_webhook_delivery_duration_seconds must appear in the /metrics registry output"
    );
}

#[tokio::test]
async fn dispatch_delivers_to_all_endpoints_despite_required_failure() {
    let failing = MockServer::start().await;
    let succeeding = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&failing)
        .await;

    // Regardless of endpoint iteration order, the succeeding hook must get its
    // delivery even though the failing required hook errors the dispatch.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&succeeding)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "failing-hook".to_string(),
        create_test_webhook_config(&failing.uri(), DeliveryPolicy::Required, None, 0),
    );
    webhooks.insert(
        "succeeding-hook".to_string(),
        create_test_webhook_config(&succeeding.uri(), DeliveryPolicy::Required, None, 0),
    );
    let dispatcher = build_dispatcher(webhooks);

    let result = dispatcher.dispatch(&event).await;
    assert!(
        result.is_err(),
        "the failing required hook must surface an error"
    );
}
