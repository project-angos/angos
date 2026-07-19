use std::{collections::HashMap, sync::Arc};

use url::Url;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::{CACHE_ACTOR, CacheFillJobHandler, build_envelope, job_error};
use crate::{
    event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        dispatcher::EventDispatcher,
        event::EventKind,
    },
    jobs::store::{Error as JobError, JobHandler},
    oci::Namespace,
    registry::{
        Error as RegistryError,
        blob_ownership::BlobOwnership,
        repository_resolver::RepositoryResolver,
        test_utils::{FsTestStack, create_test_repositories, fs_test_stack, put_blob_direct},
    },
};

/// The grant path (bytes already cached) makes the blob visible to the
/// namespace and must emit one `blob.push` with the internal `cache` actor.
#[tokio::test]
async fn cache_fill_grant_emits_blob_push_with_internal_actor() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let webhook = EventWebhookConfig {
        url: Url::parse(&server.uri()).unwrap(),
        policy: DeliveryPolicy::Required,
        token: None,
        timeout_ms: 5_000,
        max_retries: 0,
        events: vec![EventKind::BlobPush],
        repository_filter: None,
    };
    let mut webhooks = HashMap::new();
    webhooks.insert("cache-hook".to_string(), webhook);
    let dispatcher = EventDispatcher::new(webhooks).expect("dispatcher build");

    let FsTestStack {
        dir: _dir,
        store: _,
        metadata_store,
        blob_store,
    } = fs_test_stack();
    let resolver = Arc::new(
        RepositoryResolver::new(create_test_repositories())
            .expect("test repositories must not have overlapping prefixes"),
    );
    let namespace = Namespace::new("test-repo/cached").unwrap();
    let digest = put_blob_direct(metadata_store.store(), b"already cached bytes").await;

    let handler = CacheFillJobHandler::new(
        resolver,
        blob_store,
        metadata_store.clone(),
        Some(Arc::new(dispatcher)),
    );
    let envelope = build_envelope(&namespace, &digest).unwrap();
    handler.execute(&envelope).await.expect("cache fill");

    assert!(
        BlobOwnership::new(metadata_store.as_ref())
            .can_read(&namespace, &digest)
            .await
            .unwrap(),
        "the fill must grant the namespace a reference"
    );

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "the fill must emit one event");
    let event: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(event["kind"], "blob.push");
    assert_eq!(event["digest"], digest.to_string());
    assert_eq!(
        event["actor"]["internal"], CACHE_ACTOR,
        "cache-fill events must carry the internal actor; got {event}"
    );
}

#[test]
fn job_error_preserves_denied_as_terminal() {
    // An upstream denial stays terminal so the worker dead-letters it; any other
    // registry error collapses to the retryable `Execution`.
    assert!(matches!(
        job_error(RegistryError::Denied(
            "upstream forbade the fetch".to_string()
        )),
        JobError::Denied(_)
    ));
    assert!(matches!(
        job_error(RegistryError::BlobUnknown),
        JobError::Execution(_)
    ));
}
