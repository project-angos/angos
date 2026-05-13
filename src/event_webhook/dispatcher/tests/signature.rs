use std::collections::HashMap;

use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method},
};

use crate::event_webhook::{
    dispatcher::{
        compute_signature,
        tests::common::{build_dispatcher, create_test_event, create_webhook_config_for_url},
    },
    event::EventKind,
};

#[tokio::test]
async fn dispatch_sends_hmac_signature_header() {
    let server = MockServer::start().await;
    let event = create_test_event();
    let body = serde_json::to_vec(&event).unwrap();
    let expected_sig = format!("sha256={}", compute_signature("hmac-secret", &body));

    Mock::given(method("POST"))
        .and(header("X-Registry-Signature-256", expected_sig.as_str()))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "test-hook".to_string(),
        create_webhook_config_for_url(
            &server.uri(),
            vec![EventKind::ManifestPush],
            Some("hmac-secret"),
        ),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_no_signature_header_without_token() {
    let server = MockServer::start().await;
    let event = create_test_event();

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut webhooks = HashMap::new();
    webhooks.insert(
        "test-hook".to_string(),
        create_webhook_config_for_url(&server.uri(), vec![EventKind::ManifestPush], None),
    );

    let dispatcher = build_dispatcher(webhooks);
    let result = dispatcher.dispatch(&event).await;
    assert!(result.is_ok());

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .headers
            .get("X-Registry-Signature-256")
            .is_none()
    );
    assert!(requests[0].headers.get("Authorization").is_none());
}
