use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

use angos_tx_engine::transaction::Transaction;

use crate::{
    event_webhook::{
        dispatcher::EventDispatcher,
        event::{Event, EventActor, EventKind},
    },
    oci::{Digest, Namespace},
    registry::{
        blob::{cache_blob, grant_blob_reference},
        blob_store::BlobStore,
        job_store::{Error, JobEnvelope, JobHandler},
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

pub const CACHE_FETCH_BLOB_KIND: &str = "cache.fetch_blob";

/// Internal-process name stamped on the events cache fills emit.
pub const CACHE_ACTOR: &str = "cache";

/// JSON payload for a [`CACHE_FETCH_BLOB_KIND`] job on the `cache` queue.
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheFetchBlobPayload {
    pub namespace: Namespace,
    /// Serialized OCI digest, e.g. `sha256:abc...`.
    pub digest: String,
}

/// Pulls a blob from the upstream registry and writes it to the local blob
/// store. Skips the upstream fetch when the bytes are already present locally
/// (granting this namespace a reference instead), so concurrent fills of the
/// same blob dedup safely; otherwise it fetches and stores the bytes.
pub struct CacheJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    event_dispatcher: Option<Arc<EventDispatcher>>,
}

impl CacheJobHandler {
    pub fn new(
        resolver: Arc<RepositoryResolver>,
        blob_store: Arc<BlobStore>,
        metadata_store: Arc<MetadataStore>,
        event_dispatcher: Option<Arc<EventDispatcher>>,
    ) -> Self {
        Self {
            resolver,
            blob_store,
            metadata_store,
            event_dispatcher,
        }
    }

    /// The namespace gained a blob, so webhook consumers see it like any
    /// other push. Best effort: the fill is committed and idempotent, so a
    /// delivery failure must not fail (and re-run) the job.
    async fn emit_blob_push(&self, namespace: &Namespace, digest: &Digest) {
        let Some(dispatcher) = &self.event_dispatcher else {
            return;
        };
        let repository = self
            .resolver
            .resolve(namespace)
            .map(|r| r.name.to_string())
            .unwrap_or_default();
        let event = Event::new(EventKind::BlobPush, namespace.clone(), repository)
            .digest(Some(digest.to_string()))
            .actor(Some(EventActor::internal(CACHE_ACTOR)));
        if let Err(error) = dispatcher.dispatch(&event).await {
            warn!("Cache-fill event delivery failed: {error}");
        }
    }
}

#[async_trait]
impl JobHandler for CacheJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error> {
        if envelope.kind != CACHE_FETCH_BLOB_KIND {
            return Err(Error::Storage(format!(
                "unsupported job kind '{}'; expected '{CACHE_FETCH_BLOB_KIND}'",
                envelope.kind,
            )));
        }
        let payload: CacheFetchBlobPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Storage(format!("failed to deserialize job payload: {e}")))?;

        let namespace = payload.namespace;
        let digest: Digest = payload
            .digest
            .parse()
            .map_err(|e| Error::Storage(format!("invalid digest '{}': {e}", payload.digest)))?;

        // Bytes already present locally (cached by this or another namespace):
        // grant this namespace a reference without re-fetching. Gate on *byte
        // presence*, not on a `can_read` ownership link: a manifest pull records
        // the layer's ownership link before its bytes are fetched, so a
        // link-only short-circuit here would skip the fetch and the blob would
        // never be cached.
        //
        // The grant commits on the metadata store and the bytes on the blob
        // store; neither is folded into the job-completion transaction, because
        // those stores can be separate backends and a single executor cannot
        // commit across both. The work is idempotent, so it is safe to redo on a
        // retry even though it no longer commits atomically with job completion.
        if self.blob_store.size(&digest).await.is_ok() {
            grant_blob_reference(&self.metadata_store, &namespace, &digest)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            self.emit_blob_push(&namespace, &digest).await;
            return Ok(Transaction::builder().build());
        }

        let Some(repository) = self.resolver.resolve(&namespace) else {
            return Err(Error::Storage(format!(
                "no repository configured for namespace '{namespace}'"
            )));
        };

        if !repository.is_pull_through() {
            return Err(Error::Storage(
                "repository is not a pull-through proxy".to_string(),
            ));
        }

        let (content_length, stream) = repository
            .get_blob(&[], &namespace, &digest)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        cache_blob(
            &self.blob_store,
            &self.metadata_store,
            &namespace,
            &digest,
            stream,
            content_length,
        )
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        self.emit_blob_push(&namespace, &digest).await;

        Ok(Transaction::builder().build())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use url::Url;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::{CACHE_ACTOR, CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload, CacheJobHandler};
    use crate::{
        event_webhook::{
            config::{DeliveryPolicy, EventWebhookConfig},
            dispatcher::EventDispatcher,
            event::EventKind,
        },
        oci::Namespace,
        registry::{
            blob_ownership::BlobOwnership,
            job_store::{JobEnvelope, JobHandler, Queue},
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
        let dispatcher = EventDispatcher::builder()
            .webhooks(webhooks)
            .build()
            .expect("dispatcher build");

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

        let handler = CacheJobHandler::new(
            resolver,
            blob_store,
            metadata_store.clone(),
            Some(Arc::new(dispatcher)),
        );
        let envelope = JobEnvelope::new(
            Queue::Cache,
            CACHE_FETCH_BLOB_KIND,
            format!("cache:{digest}"),
            &CacheFetchBlobPayload {
                namespace: namespace.clone(),
                digest: digest.to_string(),
            },
        )
        .unwrap();
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
}
