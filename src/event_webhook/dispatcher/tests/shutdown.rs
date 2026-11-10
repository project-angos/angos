use std::time::{Duration, Instant};

use tokio::time::sleep;

use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::common::{create_test_event, single_hook_dispatcher};
use crate::event_webhook::config::DeliveryPolicy;

/// A configuration reload builds a new dispatcher and drops the old one. The
/// deliveries the old one still has in flight belong to events already
/// performed, so they must outlive it rather than be cancelled with it.
#[tokio::test]
async fn a_displaced_dispatcher_still_delivers_what_it_had_in_flight() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dispatcher = single_hook_dispatcher(
        "displaced-hook",
        &server.uri(),
        DeliveryPolicy::Async,
        None,
        0,
    );
    dispatcher.dispatch(&event).await.unwrap();

    // The reload's drop, before the delivery task has had a chance to run.
    drop(dispatcher);

    for _ in 0..100 {
        if !server
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty()
        {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "a delivery in flight must survive the dispatcher being displaced"
    );
}

#[tokio::test]
async fn test_shutdown_rejects_new_async_dispatches() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let dispatcher =
        single_hook_dispatcher("async-hook", &server.uri(), DeliveryPolicy::Async, None, 0);
    dispatcher.shutdown().await;

    // Dispatch after shutdown: delivery must be skipped. Either an error or
    // Ok with no delivery is acceptable; the absence of HTTP requests is the
    // real assertion (verified below via server.received_requests()).
    drop(dispatcher.dispatch(&event).await);

    // Give any (unexpected) background task time to run
    sleep(Duration::from_millis(200)).await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        0,
        "No async deliveries should occur after shutdown()"
    );
}

#[tokio::test]
async fn test_shutdown_drains_slow_tasks_to_completion() {
    let server = MockServer::start().await;
    let event = create_test_event();

    // A slow endpoint must be waited for, not abandoned: the delivery is
    // bounded by its own request timeout, so the drain terminates.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(700)))
        .expect(1)
        .mount(&server)
        .await;

    let dispatcher = single_hook_dispatcher(
        "very-slow-hook",
        &server.uri(),
        DeliveryPolicy::Async,
        None,
        0,
    );
    dispatcher.dispatch(&event).await.unwrap();

    let start = Instant::now();
    dispatcher.shutdown().await;
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(700),
        "shutdown() must wait for the slow in-flight delivery, took {elapsed:?}"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "the slow in-flight delivery must be drained to completion"
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

    let dispatcher = single_hook_dispatcher(
        "idempotent-hook",
        &server.uri(),
        DeliveryPolicy::Async,
        None,
        0,
    );
    dispatcher.dispatch(&event).await.unwrap();

    // Calling shutdown twice must not panic or deadlock
    dispatcher.shutdown().await;
    dispatcher.shutdown().await;
}
