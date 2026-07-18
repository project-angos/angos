use std::time::Duration;

use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method},
};

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

    let start = std::time::Instant::now();
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
async fn async_deliveries_are_reaped_before_the_next_spawn() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dispatcher =
        single_hook_dispatcher("async-hook", &server.uri(), DeliveryPolicy::Async, None, 0);

    // Deliveries complete quickly against the mock; each dispatch reaps the
    // finished tasks first, so the set stays bounded instead of growing by
    // one entry per event.
    for _ in 0..20 {
        dispatcher.dispatch(&event).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let retained = dispatcher.in_flight.lock().await.len();
    assert!(
        retained < 20,
        "completed async deliveries must be reaped, got {retained} retained tasks"
    );

    dispatcher.shutdown().await;
}
