use std::sync::Weak;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

use angos_tx_engine::transaction::Transaction;

use crate::{
    event_webhook::event::{Event, EventActor, EventKind},
    oci::{Digest, Namespace},
    registry::{
        Error as RegistryError, Registry,
        blob::{cache_blob, grant_blob_reference},
        job_store::{Error, JobEnvelope, JobHandler},
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

impl Registry {
    /// Cache-fill a blob for a pull-through namespace: emits the `blob.push`
    /// intent with the internal `cache` actor, then grants a reference when
    /// the bytes are already present locally, otherwise fetches them from the
    /// upstream and stores them. The emission is best effort: the fill is
    /// idempotent, so a delivery failure must not fail (and re-run) the job.
    pub async fn cache_fill_blob(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(), RegistryError> {
        let event = Event::new(
            EventKind::BlobPush,
            namespace.clone(),
            self.repository_name_for(namespace),
        )
        .digest(Some(digest.to_string()))
        .actor(Some(EventActor::internal(CACHE_ACTOR)));
        if let Err(error) = self.dispatch_events(&[event]).await {
            warn!("Cache-fill event delivery failed: {error}");
        }

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
        if self.blob_store.size(digest).await.is_ok() {
            grant_blob_reference(&self.metadata_store, namespace, digest).await?;
        } else {
            let repository = self.get_repository_for_namespace(namespace)?;
            if !repository.is_pull_through() {
                return Err(RegistryError::Internal(
                    "repository is not a pull-through proxy".to_string(),
                ));
            }

            let (content_length, stream) = repository.get_blob(&[], namespace, digest).await?;
            cache_blob(
                &self.blob_store,
                &self.metadata_store,
                namespace,
                digest,
                stream,
                content_length,
            )
            .await?;
        }

        Ok(())
    }
}

/// Thin job adapter over [`Registry::cache_fill_blob`], holding a weak
/// registry handle so the in-process claim loops it runs on never keep the
/// registry (and their own cancellation) alive.
pub struct CacheJobHandler {
    registry: Weak<Registry>,
}

impl CacheJobHandler {
    pub fn new(registry: Weak<Registry>) -> Self {
        Self { registry }
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

        let digest: Digest = payload
            .digest
            .parse()
            .map_err(|e| Error::Storage(format!("invalid digest '{}': {e}", payload.digest)))?;

        let Some(registry) = self.registry.upgrade() else {
            return Err(Error::Storage(
                "registry shut down; the job stays leased and is re-claimed later".to_string(),
            ));
        };
        registry
            .cache_fill_blob(&payload.namespace, &digest)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(Transaction::builder().build())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use url::Url;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::{CACHE_ACTOR, CACHE_FETCH_BLOB_KIND, CacheFetchBlobPayload};
    use crate::{
        event_webhook::{
            config::{DeliveryPolicy, EventWebhookConfig},
            dispatcher::EventDispatcher,
            event::EventKind,
        },
        oci::Namespace,
        registry::{
            Registry, RegistryConfig,
            blob_ownership::BlobOwnership,
            job_store::{JobEnvelope, JobStore, Queue},
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
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let resolver = Arc::new(
            RepositoryResolver::new(create_test_repositories())
                .expect("test repositories must not have overlapping prefixes"),
        );
        let namespace = Namespace::new("test-repo/cached").unwrap();
        let digest = put_blob_direct(metadata_store.store(), b"already cached bytes").await;

        let registry = Registry::new(
            blob_store,
            metadata_store.clone(),
            resolver,
            RegistryConfig::default()
                .job_queue(Arc::new(JobStore::new(store, "cache-test")))
                .event_dispatcher(Some(Arc::new(dispatcher))),
        )
        .unwrap();
        let handler = registry.cache_job_handler();
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
