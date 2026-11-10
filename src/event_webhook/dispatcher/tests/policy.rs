use std::time::{Duration, Instant};

use tokio::time::sleep;

use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::common::{create_test_event, single_hook_dispatcher};
use crate::event_webhook::Error;
use crate::event_webhook::config::DeliveryPolicy;

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

    let dispatcher = single_hook_dispatcher(
        "retry-hook",
        &server.uri(),
        DeliveryPolicy::Optional,
        None,
        2,
    );
    let result = dispatcher.dispatch(&event).await;
    assert!(
        result.is_ok(),
        "Optional policy must return Ok even when all retries exhausted"
    );
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

    let dispatcher = single_hook_dispatcher(
        "retry-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        None,
        2,
    );
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

    let dispatcher = single_hook_dispatcher(
        "retry-hook",
        &server.uri(),
        DeliveryPolicy::Required,
        None,
        1,
    );
    let result = dispatcher.dispatch(&event).await;
    assert!(
        matches!(result, Err(Error::Dispatch(_))),
        "Required policy must return dispatch error when all retries exhausted"
    );
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
