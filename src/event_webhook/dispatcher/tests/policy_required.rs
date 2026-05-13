use std::collections::HashMap;

use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::common::{
    build_dispatcher, create_test_event, create_webhook_config_with_policy,
    create_webhook_config_with_retries,
};
use crate::event_webhook::{Error, config::DeliveryPolicy, event::EventKind};

#[tokio::test]
async fn dispatch_required_policy_returns_error_on_server_error() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "required-hook".to_string(),
        create_webhook_config_with_policy(
            &server.uri(),
            DeliveryPolicy::Required,
            vec![EventKind::ManifestPush],
        ),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(
        matches!(result, Err(Error::Dispatch(_))),
        "Required policy must return dispatch error on 500"
    );
}

#[tokio::test]
async fn dispatch_required_policy_returns_ok_on_success() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "required-hook".to_string(),
        create_webhook_config_with_policy(
            &server.uri(),
            DeliveryPolicy::Required,
            vec![EventKind::ManifestPush],
        ),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());
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

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "retry-hook".to_string(),
        create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Required, 2),
    );

    let dispatcher = build_dispatcher(webhooks);
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

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "retry-hook".to_string(),
        create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Required, 1),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(
        matches!(result, Err(Error::Dispatch(_))),
        "Required policy must return dispatch error when all retries exhausted"
    );
}

#[tokio::test]
async fn dispatch_no_retry_when_max_retries_zero() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "no-retry-hook".to_string(),
        create_webhook_config_with_retries(&server.uri(), DeliveryPolicy::Required, 0),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(matches!(result, Err(Error::Dispatch(_))));

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "Should only make 1 attempt with max_retries=0"
    );
}
