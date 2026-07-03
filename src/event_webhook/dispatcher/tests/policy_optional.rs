use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::common::{create_test_event, single_hook_dispatcher};
use crate::event_webhook::config::DeliveryPolicy;

#[tokio::test]
async fn dispatch_optional_policy_returns_ok_on_server_error() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let dispatcher = single_hook_dispatcher(
        "optional-hook",
        &server.uri(),
        DeliveryPolicy::Optional,
        None,
        0,
    );
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

    let dispatcher = single_hook_dispatcher(
        "optional-hook",
        &server.uri(),
        DeliveryPolicy::Optional,
        None,
        0,
    );
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());
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
