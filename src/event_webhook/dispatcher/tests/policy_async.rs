use std::{collections::HashMap, time::Duration};

use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method},
};

use super::common::{build_dispatcher, create_test_event, create_webhook_config_with_policy};
use crate::event_webhook::{config::DeliveryPolicy, event::EventKind};

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

    let dispatcher = build_dispatcher(webhooks);

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

    let dispatcher = build_dispatcher(webhooks);
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
