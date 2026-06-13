use std::{collections::HashMap, time::Duration};

use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::common::{
    TEST_SHUTDOWN_TIMEOUT, build_dispatcher, create_test_event, create_test_webhook_config,
};
use crate::event_webhook::config::DeliveryPolicy;

#[tokio::test]
async fn test_shutdown_completes_in_flight_async_delivery() {
    let server = MockServer::start().await;
    let event = create_test_event();

    // Slow endpoint: takes 500ms to respond
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "slow-async-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Async, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    dispatcher.dispatch(&event).await.unwrap();

    // Immediately call shutdown: it must block until the in-flight delivery finishes
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;

    // If shutdown drained the in-flight task, the server must have received the request
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "shutdown() must wait for in-flight async deliveries to complete"
    );
}

#[tokio::test]
async fn test_shutdown_rejects_new_async_dispatches() {
    let server = MockServer::start().await;
    let event = create_test_event();

    // The server should receive NO requests after shutdown
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "async-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Async, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;

    // Dispatch after shutdown: delivery must be skipped. Either an error or
    // Ok with no delivery is acceptable; the absence of HTTP requests is the
    // real assertion (verified below via server.received_requests()).
    drop(dispatcher.dispatch(&event).await);

    // Give any (unexpected) background task time to run
    tokio::time::sleep(Duration::from_millis(200)).await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        0,
        "No async deliveries should occur after shutdown()"
    );
}

#[tokio::test]
async fn test_shutdown_with_no_in_flight_returns_immediately() {
    let webhooks = HashMap::new();
    let dispatcher = build_dispatcher(webhooks);

    let start = std::time::Instant::now();
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(200),
        "shutdown() with no in-flight tasks must return quickly, took {elapsed:?}"
    );
}

#[tokio::test]
async fn test_multiple_in_flight_async_deliveries_drain_on_shutdown() {
    // Three independent slow servers, one async hook each
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;
    let server_c = MockServer::start().await;

    for server in [&server_a, &server_b, &server_c] {
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
            .expect(1)
            .mount(server)
            .await;
    }

    let mut webhooks = HashMap::new();
    for (name, server) in [
        ("hook-a", &server_a),
        ("hook-b", &server_b),
        ("hook-c", &server_c),
    ] {
        webhooks.insert(
            name.to_string(),
            create_test_webhook_config(&server.uri(), DeliveryPolicy::Async, None, 0),
        );
    }

    let dispatcher = build_dispatcher(webhooks);

    let event = create_test_event();
    dispatcher.dispatch(&event).await.unwrap();

    // shutdown() must drain all three concurrent in-flight deliveries
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;

    for (label, server) in [
        ("server_a", &server_a),
        ("server_b", &server_b),
        ("server_c", &server_c),
    ] {
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.len(),
            1,
            "shutdown() must drain all in-flight deliveries; {label} got {} requests",
            requests.len()
        );
    }
}

#[tokio::test]
async fn test_shutdown_with_timeout_returns_after_timeout_when_tasks_are_too_slow() {
    let server = MockServer::start().await;
    let event = create_test_event();

    // Endpoint takes 2 seconds, longer than our shutdown timeout of 300ms
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "very-slow-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Async, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    dispatcher.dispatch(&event).await.unwrap();

    let start = std::time::Instant::now();
    // shutdown_with_timeout should give up after 300ms, not wait the full 2s
    dispatcher
        .shutdown_with_timeout(Duration::from_millis(300))
        .await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(700),
        "shutdown_with_timeout must return after the timeout even if tasks are still running, took {elapsed:?}"
    );
}

#[tokio::test]
async fn test_shutdown_with_timeout_drains_fast_tasks_within_timeout() {
    let server = MockServer::start().await;
    let event = create_test_event();

    // Endpoint responds quickly (50ms), well within the 500ms timeout
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(50)))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "fast-async-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Async, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    dispatcher.dispatch(&event).await.unwrap();

    dispatcher
        .shutdown_with_timeout(Duration::from_millis(500))
        .await;

    // Fast task should have completed, so server must have received the request
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "Fast in-flight delivery must complete when it finishes within the timeout"
    );
}

#[tokio::test]
async fn test_shutdown_is_idempotent() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "idempotent-hook".to_string(),
        create_test_webhook_config(&server.uri(), DeliveryPolicy::Async, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    dispatcher.dispatch(&event).await.unwrap();

    // Calling shutdown twice must not panic or deadlock
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;
}

#[tokio::test]
async fn test_shutdown_with_timeout_is_idempotent() {
    let webhooks = HashMap::new();
    let dispatcher = build_dispatcher(webhooks);

    // Both calls must complete without panic or deadlock
    dispatcher
        .shutdown_with_timeout(Duration::from_millis(100))
        .await;
    dispatcher
        .shutdown_with_timeout(Duration::from_millis(100))
        .await;
}

#[tokio::test]
async fn test_shutdown_drains_mix_of_fast_and_slow_deliveries() {
    let fast_server = MockServer::start().await;
    let slow_server = MockServer::start().await;
    let event = create_test_event();

    // Fast endpoint: responds immediately
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&fast_server)
        .await;

    // Slow endpoint: takes 500ms
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .expect(1)
        .mount(&slow_server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "fast-hook".to_string(),
        create_test_webhook_config(&fast_server.uri(), DeliveryPolicy::Async, None, 0),
    );
    webhooks.insert(
        "slow-hook".to_string(),
        create_test_webhook_config(&slow_server.uri(), DeliveryPolicy::Async, None, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    dispatcher.dispatch(&event).await.unwrap();

    // shutdown() must drain both the fast and the slow delivery
    dispatcher
        .shutdown_with_timeout(TEST_SHUTDOWN_TIMEOUT)
        .await;

    let fast_reqs = fast_server.received_requests().await.unwrap();
    let slow_reqs = slow_server.received_requests().await.unwrap();
    assert_eq!(
        fast_reqs.len(),
        1,
        "Fast delivery must complete before shutdown returns"
    );
    assert_eq!(
        slow_reqs.len(),
        1,
        "Slow delivery must also complete before shutdown returns"
    );
}
