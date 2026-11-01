use std::time::{Duration, Instant};

use tokio::time::sleep;

use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::common::{create_test_event, single_hook_dispatcher};
use crate::event_webhook::config::DeliveryPolicy;

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

    let dispatcher =
        single_hook_dispatcher("async-hook", &server.uri(), DeliveryPolicy::Async, None, 0);

    let start = Instant::now();
    let result = dispatcher.dispatch(&event).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(
        elapsed < Duration::from_millis(500),
        "Async dispatch must return immediately, took {elapsed:?}"
    );

    dispatcher.shutdown().await;
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

    let dispatcher =
        single_hook_dispatcher("async-hook", &server.uri(), DeliveryPolicy::Async, None, 0);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());

    dispatcher.shutdown().await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "Async webhook must eventually deliver the request"
    );
}

#[tokio::test]
async fn async_deliveries_do_not_accumulate() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dispatcher =
        single_hook_dispatcher("async-hook", &server.uri(), DeliveryPolicy::Async, None, 0);

    // Deliveries complete quickly against the mock, and a finished task leaves
    // the tracker on its own, so the count stays bounded instead of growing by
    // one entry per event.
    for _ in 0..20 {
        dispatcher.dispatch(&event).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let retained = dispatcher.in_flight.len();
    assert!(
        retained < 20,
        "completed async deliveries must leave the tracker, got {retained} retained tasks"
    );

    dispatcher.shutdown().await;
}
