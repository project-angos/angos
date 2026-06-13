//! [`ReplicationJobHandler`]: the [`JobHandler`] that drives the replication
//! push pipeline. It returns an empty [`Transaction`] on success (the effect is
//! an external HTTP push the engine cannot roll back), so the pipeline stays
//! idempotent via HEAD-before-PUT under the at-least-once contract.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use angos_tx_engine::transaction::Transaction;

use crate::{
    metrics_provider::metrics_provider,
    oci::{Digest, Namespace, Reference},
    registry::{
        blob_store::BlobStore,
        job_store::{Error, JobEnvelope, JobHandler},
        metadata_store::{Error as MetadataStoreError, MetadataStore, link_kind::LinkKind},
        repository_resolver::RepositoryResolver,
    },
    replication::{
        ReplicationDownstream,
        pipeline::{self, PushContext, PushOutcome},
    },
};

/// Single queue carrying every replication job; the downstream is encoded in the
/// `lock_key` and payload.
pub const REPLICATION_QUEUE: &str = "replication";
/// Push a manifest (and everything it references) to a downstream.
pub const REPLICATION_PUSH_MANIFEST_KIND: &str = "replication.push_manifest";
/// Delete a manifest on a downstream.
pub const REPLICATION_DELETE_MANIFEST_KIND: &str = "replication.delete_manifest";

/// JSON payload for a replication job on [`REPLICATION_QUEUE`]. The handler
/// re-resolves the current local `tag -> digest` at execute time and the queue
/// only coalesces not-yet-claimed jobs, so a write landing mid-push enqueues
/// its own job instead of being dropped.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPushPayload {
    /// Local identifier of the target downstream (selects the `RegistryClient`).
    pub downstream: String,
    pub namespace: String,
    /// Tag bound to the digest, when the change is tag-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Serialized OCI digest: informational for pushes, authoritative for
    /// digest deletes and tag-less pushes, absent for a tag delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Mirrors the envelope `kind` so a payload is self-describing for the
    /// scrub-side builder.
    pub kind: String,
    /// Event timestamp (RFC 3339) carried for receiver-side last-writer-wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ts: Option<String>,
}

/// Tag-or-digest segment shared by every replication `lock_key`.
fn lock_key_reference(payload: &ReplicationPushPayload) -> &str {
    payload
        .tag
        .as_deref()
        .or(payload.digest.as_deref())
        .unwrap_or("")
}

/// Base delete `lock_key`,
/// `{REPLICATION_QUEUE}.delete.{downstream}:{namespace}:{tag_or_digest}`, shared
/// by the event-path delete (which appends `@{source_ts}`) and the scrub prune
/// delete (which keys on this bare base). Defining it once keeps the invariant
/// "the prune key is the event delete key minus the timestamp suffix" in code.
fn delete_lock_key_base(payload: &ReplicationPushPayload) -> String {
    format!(
        "{REPLICATION_QUEUE}.delete.{}:{}:{}",
        payload.downstream,
        payload.namespace,
        lock_key_reference(payload)
    )
}

/// Builds the per-job `lock_key`,
/// `{REPLICATION_QUEUE}.{op}.{downstream}:{namespace}:{tag_or_digest}` plus
/// `@{source_ts}` for deletes, shared by the event path and the scrub push
/// reconcile so identical pending work coalesces. The `op` segment keeps a
/// delete from coalescing into a still-pending push, and the delete-only
/// timestamp keeps distinct deletion events apart because a delete cannot
/// re-derive its last-writer-wins timestamp at execute time (pushes re-resolve
/// digest and timestamp, so they safely coalesce on the bare reference). Scrub
/// prune deletes coalesce differently; see [`build_prune_delete_envelope`].
#[must_use]
fn replication_lock_key(payload: &ReplicationPushPayload) -> String {
    if payload.kind == REPLICATION_DELETE_MANIFEST_KIND {
        // The event-path delete is the bare base plus the `@{source_ts}` suffix.
        format!(
            "{}@{}",
            delete_lock_key_base(payload),
            payload.source_ts.as_deref().unwrap_or("")
        )
    } else {
        format!(
            "{REPLICATION_QUEUE}.push.{}:{}:{}",
            payload.downstream,
            payload.namespace,
            lock_key_reference(payload)
        )
    }
}

/// Builds a [`JobEnvelope`] from a [`ReplicationPushPayload`]; exposed so the
/// scrub checker enqueues the same envelope shape as the event path.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the payload cannot be serialized.
pub fn build_envelope(payload: &ReplicationPushPayload) -> Result<JobEnvelope, serde_json::Error> {
    JobEnvelope::new(
        REPLICATION_QUEUE,
        payload.kind.clone(),
        replication_lock_key(payload),
        payload,
    )
}

/// Builds a [`JobEnvelope`] for a scrub prune delete, whose `lock_key` omits
/// the `@{source_ts}` suffix so repeated reconcile runs coalesce on the bare
/// reference instead of stacking one job per run. Coalescing keeps the first
/// (older-ts) envelope, which is strictly more conservative under receiver
/// last-writer-wins, and a later scrub run re-enqueues once the pending job
/// clears, so convergence is preserved.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] when the payload cannot be serialized.
pub fn build_prune_delete_envelope(
    payload: &ReplicationPushPayload,
) -> Result<JobEnvelope, serde_json::Error> {
    JobEnvelope::new(
        REPLICATION_QUEUE,
        payload.kind.clone(),
        delete_lock_key_base(payload),
        payload,
    )
}

/// Mirrors a local repository's state to a configured downstream. Built via
/// [`ReplicationJobHandler::builder`] from resolved dependencies.
pub struct ReplicationJobHandler {
    resolver: Arc<RepositoryResolver>,
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

impl ReplicationJobHandler {
    /// Starts building a handler from individual resolved fields.
    #[must_use]
    pub fn builder() -> ReplicationJobHandlerBuilder {
        ReplicationJobHandlerBuilder::default()
    }

    /// Resolves the [`ReplicationDownstream`] for a payload, or errors when the
    /// namespace or downstream is not configured.
    fn resolve_downstream<'a>(
        &'a self,
        namespace: &Namespace,
        downstream_name: &str,
    ) -> Result<&'a ReplicationDownstream, Error> {
        let repository = self.resolver.resolve(namespace).ok_or_else(|| {
            Error::Storage(format!(
                "no repository configured for namespace '{namespace}'"
            ))
        })?;
        // Failing loudly on an unknown downstream is intentional: it surfaces
        // stale config (a removed or renamed downstream) and the orphaned jobs
        // dead-letter after max attempts instead of vanishing silently;
        // `scrub --replication-orphans` clears them.
        repository
            .replication
            .iter()
            .find(|d| d.name == downstream_name)
            .ok_or_else(|| {
                Error::Storage(format!(
                    "no downstream '{downstream_name}' configured for namespace '{namespace}'"
                ))
            })
    }

    /// Records the per-outcome counter and, for a converged outcome, the
    /// last-success gauge. `Unsupported` completed without error but did not
    /// converge (the downstream cannot delete this reference), so it counts but
    /// must not advance the staleness gauge.
    fn record_success(downstream: &str, outcome: PushOutcome) {
        let outcome_label = match outcome {
            PushOutcome::Pushed => "pushed",
            PushOutcome::Converged => "converged",
            PushOutcome::Superseded => "superseded",
            PushOutcome::Unsupported => "unsupported",
        };
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, outcome_label])
            .inc();
        if !matches!(outcome, PushOutcome::Unsupported) {
            metrics_provider()
                .replication_last_success_timestamp
                .with_label_values(&[downstream])
                .set(Utc::now().timestamp());
        }
    }

    /// Records a `failed` push for `downstream` before the handler returns `Err`.
    /// `failed` is per-attempt (each retry increments it) and covers every `Err`,
    /// including pre-flight and local-read failures, not only downstream HTTP errors.
    fn record_failure(downstream: &str) {
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, "failed"])
            .inc();
    }

    /// Re-resolves the current local target for a push: a tag is re-read at
    /// execute time (latest-wins) yielding its digest and `created_at`, while a
    /// tag-less job uses the payload digest and only confirms the revision link
    /// exists. Returns `Ok(None)` when the target no longer exists locally,
    /// which the caller treats as already-converged no-op success.
    async fn resolve_current_digest(
        &self,
        namespace: &Namespace,
        payload: &ReplicationPushPayload,
    ) -> Result<Option<(Digest, Option<DateTime<Utc>>)>, Error> {
        // Reads bypass the per-process link cache: a worker's cache can lag a
        // sibling process's write, and a stale resolve would replicate the old
        // digest and complete the job.
        if let Some(tag) = &payload.tag {
            match self
                .metadata_store
                .read_link_reference(namespace.as_ref(), &LinkKind::Tag(tag.clone()))
                .await
            {
                Ok(link) => Ok(Some((link.target, link.created_at))),
                Err(MetadataStoreError::ReferenceNotFound) => Ok(None),
                Err(e) => Err(Error::Storage(format!(
                    "failed to read tag '{tag}' in '{namespace}': {e}"
                ))),
            }
        } else {
            let digest_str = payload.digest.as_deref().ok_or_else(|| {
                Error::Storage(format!(
                    "tag-less push for '{namespace}' has no digest to resolve"
                ))
            })?;
            let digest: Digest = digest_str
                .parse()
                .map_err(|e| Error::Storage(format!("invalid digest '{digest_str}': {e}")))?;
            // A content-addressed digest carries no local version timestamp.
            match self
                .metadata_store
                .read_link_reference(namespace.as_ref(), &LinkKind::Digest(digest.clone()))
                .await
            {
                Ok(_) => Ok(Some((digest, None))),
                Err(MetadataStoreError::ReferenceNotFound) => Ok(None),
                Err(e) => Err(Error::Storage(format!(
                    "failed to read revision '{digest}' in '{namespace}': {e}"
                ))),
            }
        }
    }

    /// Runs the push/delete attempt for a validated payload and records the
    /// success metric. It must NOT record `failed` itself: every `Err` is
    /// counted once by the `inspect_err` in [`Self::execute`].
    async fn attempt(
        &self,
        envelope: &JobEnvelope,
        payload: &ReplicationPushPayload,
    ) -> Result<(), Error> {
        let namespace = Namespace::new(&payload.namespace).map_err(|e| {
            Error::Storage(format!("invalid namespace '{}': {e}", payload.namespace))
        })?;
        let downstream = self.resolve_downstream(&namespace, &payload.downstream)?;
        let registry_client = &downstream.registry_client;
        let max_concurrent_pushes = downstream.max_concurrent_pushes;

        if envelope.kind == REPLICATION_DELETE_MANIFEST_KIND {
            let reference = if let Some(tag) = &payload.tag {
                Reference::Tag(tag.clone())
            } else {
                let digest = payload.digest.as_deref().ok_or_else(|| {
                    Error::Storage(format!(
                        "tag-less delete for '{namespace}' has no digest reference"
                    ))
                })?;
                Reference::Digest(
                    digest
                        .parse()
                        .map_err(|e| Error::Storage(format!("invalid digest '{digest}': {e}")))?,
                )
            };
            let outcome = pipeline::delete_manifest(
                registry_client,
                &self.metadata_store,
                namespace.as_ref(),
                &reference,
                payload.source_ts.as_deref(),
            )
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            Self::record_success(&payload.downstream, outcome);
        } else {
            // Re-resolving the digest at execute time gives latest-wins for a
            // coalesced job.
            let Some((digest, created_at)) =
                self.resolve_current_digest(&namespace, payload).await?
            else {
                debug!(
                    namespace = %namespace,
                    tag = ?payload.tag,
                    digest = ?payload.digest,
                    "Push target no longer exists locally; treating as converged no-op"
                );
                return Ok(());
            };
            // Re-derive source_ts from the resolved tag's created_at (not the
            // payload) so receiver-side last-writer-wins compares against the
            // digest actually sent, for coalesced and reconcile pushes alike.
            // A tag-less push has no local timestamp, so the payload value
            // carries through (the receiver skips LWW for it).
            let source_ts = created_at
                .map(|ts| ts.to_rfc3339())
                .or_else(|| payload.source_ts.clone());
            let body = self.blob_store.read(&digest).await.map_err(|e| {
                Error::Storage(format!("failed to read local manifest '{digest}': {e}"))
            })?;
            let ctx = PushContext {
                downstream: registry_client,
                blob_store: &self.blob_store,
                metadata_store: &self.metadata_store,
                namespace: namespace.as_ref(),
                max_concurrent_pushes,
                source_ts: source_ts.as_deref(),
            };
            let outcome =
                pipeline::push_manifest(&ctx, &digest, None, payload.tag.as_deref(), body)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
            Self::record_success(&payload.downstream, outcome);
        }

        Ok(())
    }
}

#[async_trait]
impl JobHandler for ReplicationJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<Transaction, Error> {
        if envelope.kind != REPLICATION_PUSH_MANIFEST_KIND
            && envelope.kind != REPLICATION_DELETE_MANIFEST_KIND
        {
            return Err(Error::Storage(format!(
                "unsupported job kind '{}'; expected one of '{REPLICATION_PUSH_MANIFEST_KIND}', \
                 '{REPLICATION_DELETE_MANIFEST_KIND}'",
                envelope.kind,
            )));
        }

        let payload: ReplicationPushPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Storage(format!("failed to deserialize job payload: {e}")))?;

        // Loop prevention is no-op suppression at the mutation boundary: mutation
        // methods only dispatch replication when local state actually changed, so
        // converged replays are never re-dispatched and mesh cycles terminate.
        // The handler itself needs no origin filter.

        // Record `failed` exactly once on any Err; success metrics are recorded
        // inside the attempt.
        self.attempt(envelope, &payload)
            .await
            .inspect_err(|_| Self::record_failure(&payload.downstream))?;

        // Empty transaction: the HTTP push is the side effect.
        Ok(Transaction::builder().build())
    }
}

/// Builder for [`ReplicationJobHandler`]; all fields are required.
#[derive(Default)]
pub struct ReplicationJobHandlerBuilder {
    resolver: Option<Arc<RepositoryResolver>>,
    blob_store: Option<Arc<BlobStore>>,
    metadata_store: Option<Arc<MetadataStore>>,
}

impl ReplicationJobHandlerBuilder {
    /// Namespace -> repository resolver (required).
    #[must_use]
    pub fn resolver(mut self, resolver: Arc<RepositoryResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Blob store the manifest/blob bytes are read from (required).
    #[must_use]
    pub fn blob_store(mut self, blob_store: Arc<BlobStore>) -> Self {
        self.blob_store = Some(blob_store);
        self
    }

    /// Metadata store used to re-resolve the current `tag -> digest` (required).
    #[must_use]
    pub fn metadata_store(mut self, metadata_store: Arc<MetadataStore>) -> Self {
        self.metadata_store = Some(metadata_store);
        self
    }

    /// Builds the [`ReplicationJobHandler`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when a required field is missing.
    pub fn build(self) -> Result<ReplicationJobHandler, Error> {
        let resolver = self.resolver.ok_or_else(|| {
            Error::Initialization("replication handler builder requires a resolver".into())
        })?;
        let blob_store = self.blob_store.ok_or_else(|| {
            Error::Initialization("replication handler builder requires a blob_store".into())
        })?;
        let metadata_store = self.metadata_store.ok_or_else(|| {
            Error::Initialization("replication handler builder requires a metadata_store".into())
        })?;

        Ok(ReplicationJobHandler {
            resolver,
            blob_store,
            metadata_store,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;
    use tempfile::TempDir;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};

    use angos_tx_engine::store::Store;

    use crate::{
        cache, metrics_provider,
        oci::Digest,
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            DOCKER_CONTENT_DIGEST, Repository,
            blob_store::BlobStore,
            job_store::{JobEnvelope, JobHandler},
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{LinkOperation, MetadataStore, link_kind::LinkKind},
            repository_resolver::RepositoryResolver,
            test_utils::{build_store, build_test_fs_executor, put_blob_direct},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_DELETE_MANIFEST_KIND, REPLICATION_PUSH_MANIFEST_KIND,
            REPLICATION_SUPERSEDED_CODE, ReplicationDownstream, X_ANGOS_SOURCE_TIMESTAMP,
            handler::{
                ReplicationJobHandler, ReplicationPushPayload, build_envelope,
                build_prune_delete_envelope, replication_lock_key,
            },
        },
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";

    fn sample_payload() -> ReplicationPushPayload {
        ReplicationPushPayload {
            downstream: DOWNSTREAM.to_string(),
            namespace: NAMESPACE.to_string(),
            tag: Some("v1".to_string()),
            digest: Some(
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                    .to_string(),
            ),
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            source_ts: Some("2026-06-03T00:00:00Z".to_string()),
        }
    }

    /// Current value of `angos_replication_push_total{downstream, outcome}`.
    /// The prometheus registry is process-global, so tests assert deltas, not
    /// absolutes.
    fn push_total(downstream: &str, outcome: &str) -> u64 {
        metrics_provider::metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, outcome])
            .get()
    }

    /// Current value of `angos_replication_last_success_timestamp_seconds{downstream}`.
    fn last_success(downstream: &str) -> i64 {
        metrics_provider::metrics_provider()
            .replication_last_success_timestamp
            .with_label_values(&[downstream])
            .get()
    }

    #[test]
    fn payload_serde_round_trip() {
        let payload = sample_payload();
        let value = serde_json::to_value(&payload).unwrap();
        let parsed: ReplicationPushPayload = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, payload);
    }

    #[test]
    fn lock_key_uses_tag_when_set() {
        let payload = sample_payload();
        assert_eq!(
            replication_lock_key(&payload),
            "replication.push.eu-region:nginx:v1"
        );
    }

    #[test]
    fn lock_key_falls_back_to_digest_without_tag() {
        let mut payload = sample_payload();
        payload.tag = None;
        assert_eq!(
            replication_lock_key(&payload),
            format!(
                "replication.push.eu-region:nginx:{}",
                payload.digest.as_deref().unwrap()
            )
        );
    }

    /// A delete must never coalesce into (and be swallowed by) a pending push.
    #[test]
    fn lock_key_distinguishes_push_from_delete() {
        let push = sample_payload();
        let mut delete = sample_payload();
        delete.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
        assert_eq!(
            replication_lock_key(&push),
            "replication.push.eu-region:nginx:v1"
        );
        assert_eq!(
            replication_lock_key(&delete),
            "replication.delete.eu-region:nginx:v1@2026-06-03T00:00:00Z"
        );
        assert_ne!(
            replication_lock_key(&push),
            replication_lock_key(&delete),
            "a push and a delete for the same tag must not coalesce"
        );
    }

    /// Deletes with different `source_ts` are distinct events and must not
    /// coalesce (the stale timestamp would lose receiver-side LWW), while
    /// retries of the same event still do.
    #[test]
    fn lock_key_separates_distinct_delete_events() {
        let mut first = sample_payload();
        first.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
        let mut second = first.clone();
        second.source_ts = Some("2026-06-03T00:01:00Z".to_string());

        assert_ne!(
            replication_lock_key(&first),
            replication_lock_key(&second),
            "deletes with different source_ts must each get their own job"
        );
        assert_eq!(
            replication_lock_key(&first),
            replication_lock_key(&first.clone()),
            "a retry of the same deletion event must still coalesce"
        );
    }

    /// A scrub prune delete keys on the bare reference: repeated reconcile runs
    /// stamp fresh `source_ts` values, yet must coalesce into one pending job,
    /// and must never coalesce with a timestamped event-path delete.
    #[test]
    fn prune_delete_envelope_coalesces_on_bare_reference() {
        let mut payload = sample_payload();
        payload.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
        let mut later = payload.clone();
        later.source_ts = Some("2026-06-03T00:01:00Z".to_string());

        let first = build_prune_delete_envelope(&payload).unwrap();
        let second = build_prune_delete_envelope(&later).unwrap();
        assert_eq!(first.lock_key, "replication.delete.eu-region:nginx:v1");
        assert_eq!(
            first.lock_key, second.lock_key,
            "prune deletes with different source_ts must coalesce on the bare reference"
        );
        assert_ne!(
            first.lock_key,
            replication_lock_key(&payload),
            "a prune delete must not coalesce with an event-path delete"
        );
    }

    #[test]
    fn lock_key_handles_missing_tag_and_digest() {
        let mut payload = sample_payload();
        payload.tag = None;
        payload.digest = None;
        assert_eq!(
            replication_lock_key(&payload),
            "replication.push.eu-region:nginx:"
        );
    }

    #[test]
    fn build_envelope_sets_queue_kind_and_lock_key() {
        let payload = sample_payload();
        let envelope = build_envelope(&payload).unwrap();
        assert_eq!(envelope.queue, "replication");
        assert_eq!(envelope.kind, REPLICATION_PUSH_MANIFEST_KIND);
        assert_eq!(envelope.lock_key, "replication.push.eu-region:nginx:v1");
        let round_trip: ReplicationPushPayload = serde_json::from_value(envelope.payload).unwrap();
        assert_eq!(round_trip, payload);
    }

    fn downstream_client(uri: &str) -> Arc<RegistryClient> {
        let backend = cache::Config::Memory.to_backend().unwrap();
        Arc::new(
            RegistryClient::builder()
                .url(uri.to_string())
                .client(reqwest::Client::new())
                .cache(backend)
                .max_manifest_size_bytes(DEFAULT_MAX_MANIFEST_SIZE_BYTES)
                .build()
                .unwrap(),
        )
    }

    fn repository_with_downstream(client: Arc<RegistryClient>) -> Repository {
        repository_with_named_downstream(DOWNSTREAM, client)
    }

    /// Lets a test pick the downstream name so its metric label set is isolated
    /// from other tests.
    fn repository_with_named_downstream(name: &str, client: Arc<RegistryClient>) -> Repository {
        Repository {
            name: REPO.to_string(),
            upstreams: Vec::new(),
            replication: vec![
                ReplicationDownstream::builder()
                    .name(name.to_string())
                    .registry_client(client)
                    .max_concurrent_pushes(4)
                    .build()
                    .unwrap(),
            ],
            retention_policy: RetentionPolicy::new(
                &RetentionPolicyConfig::default(),
                Arc::new(SystemClock),
            ),
            immutable_tags: false,
            immutable_tags_exclusions: Vec::new(),
        }
    }

    /// Seeds config, layer, and manifest blobs plus a `v1` tag link; returns
    /// the (manifest, config, layer) digests.
    async fn seed_manifest(
        store: &Arc<Store>,
        metadata_store: &Arc<MetadataStore>,
    ) -> (Digest, Digest, Digest) {
        let config_bytes = br#"{"config":true}"#.to_vec();
        let layer_bytes = b"layer-bytes".to_vec();

        let config_digest = put_blob(store, &config_bytes).await;
        let layer_digest = put_blob(store, &layer_bytes).await;

        let manifest = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest.to_string(),
                "size": config_bytes.len(),
            },
            "layers": [{
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": layer_digest.to_string(),
                "size": layer_bytes.len(),
            }],
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_digest = put_blob(store, &manifest_bytes).await;

        metadata_store
            .update_links(
                NAMESPACE,
                &[
                    LinkOperation::create(LinkKind::Tag("v1".to_string()), manifest_digest.clone()),
                    LinkOperation::create(
                        LinkKind::Config(config_digest.clone()),
                        config_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        (manifest_digest, config_digest, layer_digest)
    }

    async fn put_blob(store: &Arc<Store>, content: &[u8]) -> Digest {
        put_blob_direct(store, content).await
    }

    #[tokio::test]
    async fn execute_rejects_unknown_kind() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client("https://unused.test")),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let envelope = JobEnvelope::new(
            "replication",
            "replication.bogus",
            "lock",
            &sample_payload(),
        )
        .unwrap();
        let result = handler.execute(&envelope).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported job kind")
        );
    }

    /// A job for a downstream no longer in config fails loudly (and so
    /// dead-letters after max attempts) instead of silently completing.
    #[tokio::test]
    async fn execute_errors_on_removed_downstream() {
        metrics_provider::init_for_tests();
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client("https://unused.test")),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let mut payload = sample_payload();
        payload.downstream = "removed-region".to_string();
        let envelope = build_envelope(&payload).unwrap();
        let result = handler.execute(&envelope).await;
        assert!(
            result
                .as_ref()
                .is_err_and(|e| e.to_string().contains("no downstream 'removed-region'")),
            "a job for a de-configured downstream must error, got: {:?}",
            result.map(|_| ())
        );
    }

    #[tokio::test]
    async fn execute_pushes_manifest_with_head_before_put() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        // Downstream is missing both blobs (404 on HEAD) -> upload sequence runs.
        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(ResponseTemplate::new(404))
                .expect(1)
                .mount(&mock_server)
                .await;
        }
        // Each missing blob: POST start, PATCH chunk, PUT complete.
        Mock::given(method("POST"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
            )
            .expect(2)
            .mount(&mock_server)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
            )
            .expect(2)
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
            .respond_with(ResponseTemplate::new(201))
            .expect(2)
            .mount(&mock_server)
            .await;
        // The manifest itself is PUT by tag (no OCI-Subject -> no fallback).
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let envelope = build_envelope(&sample_payload()).unwrap();
        let tx = handler.execute(&envelope).await.unwrap();
        assert!(tx.mutations.is_empty(), "push returns an empty transaction");
        // wiremock `.expect(...)` assertions are verified on MockServer drop.
        drop(mock_server);
    }

    /// The execute-time tag resolve must read the backend link, not the
    /// per-process cache: a worker's cache can lag a sibling process's write
    /// by up to its TTL, and a stale resolve would replicate the old digest
    /// and complete the job.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn execute_push_resolves_tag_past_the_link_cache() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .cache(cache::Config::Memory.to_backend().unwrap())
                .link_cache_ttl(300)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        // Two manifests sharing the same blobs; the tag starts on `stale`.
        let config_bytes = br#"{"config":true}"#.to_vec();
        let layer_bytes = b"layer-bytes".to_vec();
        let config_digest = put_blob(&store, &config_bytes).await;
        let layer_digest = put_blob(&store, &layer_bytes).await;
        let manifest_json = |rev: &str| {
            json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": config_digest.to_string(),
                    "size": config_bytes.len(),
                },
                "layers": [{
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": layer_digest.to_string(),
                    "size": layer_bytes.len(),
                }],
                "annotations": {"rev": rev},
            })
        };
        let stale_bytes = serde_json::to_vec(&manifest_json("stale")).unwrap();
        let stale_digest = put_blob(&store, &stale_bytes).await;
        let current_bytes = serde_json::to_vec(&manifest_json("current")).unwrap();
        let current_digest = put_blob(&store, &current_bytes).await;

        let link = LinkKind::Tag("v1".to_string());
        metadata_store
            .update_links(
                NAMESPACE,
                &[
                    LinkOperation::create(link.clone(), stale_digest.clone()),
                    LinkOperation::create(
                        LinkKind::Config(config_digest.clone()),
                        config_digest.clone(),
                    ),
                    LinkOperation::create(
                        LinkKind::Layer(layer_digest.clone()),
                        layer_digest.clone(),
                    ),
                ],
            )
            .await
            .unwrap();

        // Warm this process's cache with the stale target, then simulate a
        // sibling process re-pointing the tag behind it.
        metadata_store
            .read_link(NAMESPACE, &link, false)
            .await
            .unwrap();
        assert_eq!(
            metadata_store
                .cache_get(NAMESPACE, &link)
                .await
                .expect("the resolve under test must start from a warm cache")
                .target,
            stale_digest
        );
        let mut sibling = metadata_store
            .read_link_reference(NAMESPACE, &link)
            .await
            .unwrap();
        sibling.target = current_digest.clone();
        metadata_store
            .write_link_reference(NAMESPACE, &link, &sibling)
            .await
            .unwrap();

        // Both blobs already present downstream; unmatched manifest HEAD 404s
        // so the converged skip never fires and the PUT body is observable.
        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(&mock_server)
                .await;
        }
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, current_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let payload = ReplicationPushPayload {
            downstream: DOWNSTREAM.to_string(),
            namespace: NAMESPACE.to_string(),
            tag: Some("v1".to_string()),
            digest: Some(stale_digest.to_string()),
            kind: REPLICATION_PUSH_MANIFEST_KIND.to_string(),
            source_ts: Some("2026-06-03T00:00:00Z".to_string()),
        };
        let envelope = build_envelope(&payload).unwrap();
        handler.execute(&envelope).await.unwrap();

        let manifest_path = format!("/v2/{NAMESPACE}/manifests/v1");
        let received = mock_server.received_requests().await.unwrap_or_default();
        let put = received
            .iter()
            .find(|r| r.method.as_str() == "PUT" && r.url.path() == manifest_path)
            .expect("the push must PUT the manifest");
        assert_eq!(
            put.body, current_bytes,
            "the resolve must read the backend link, not the stale cached one"
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn execute_skips_blob_present_on_downstream() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        // Both blobs already present (200 on HEAD) -> NO upload sequence at all.
        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(&mock_server)
                .await;
        }
        // No POST/PATCH mounted: if the pipeline tried to upload, the request
        // would 404 and the push would error.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let envelope = build_envelope(&sample_payload()).unwrap();
        handler.execute(&envelope).await.unwrap();
    }

    /// The manifest PUT must carry an `X-Angos-Source-Timestamp` derived from
    /// the resolved tag's `created_at`, not the stale payload timestamp, so a
    /// coalesced push cannot ship a stale last-writer-wins version.
    #[tokio::test]
    async fn execute_push_stamps_resolved_source_timestamp() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        // Read back the tag's created_at to assert the exact stamped value.
        let expected_ts = metadata_store
            .read_link(NAMESPACE, &LinkKind::Tag("v1".to_string()), false)
            .await
            .unwrap()
            .created_at
            .unwrap()
            .to_rfc3339();

        // Blobs already present (200 on HEAD): the only mutating request is the
        // manifest PUT.
        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(&mock_server)
                .await;
        }
        // The PUT must carry the source timestamp; if the header is absent or
        // wrong, this mock does not match and the push fails.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .and(header(X_ANGOS_SOURCE_TIMESTAMP, expected_ts.as_str()))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let envelope = build_envelope(&sample_payload()).unwrap();
        handler.execute(&envelope).await.unwrap();
        // wiremock `.expect(1)` on the header-matched PUT is verified on drop.
        drop(mock_server);
    }

    /// A reconcile push enqueues with `source_ts = None`; the handler must still
    /// stamp the resolved tag's `created_at` so the receiver runs
    /// last-writer-wins instead of overwriting unconditionally.
    #[tokio::test]
    async fn execute_reconcile_push_derives_source_timestamp_from_local_tag() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;
        let expected_ts = metadata_store
            .read_link(NAMESPACE, &LinkKind::Tag("v1".to_string()), false)
            .await
            .unwrap()
            .created_at
            .unwrap()
            .to_rfc3339();

        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(&mock_server)
                .await;
        }
        // A missing or wrong header would not match this mock and the push fails.
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .and(header(X_ANGOS_SOURCE_TIMESTAMP, expected_ts.as_str()))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let mut payload = sample_payload();
        payload.source_ts = None;
        let envelope = build_envelope(&payload).unwrap();
        handler.execute(&envelope).await.unwrap();
        drop(mock_server);
    }

    /// A non-superseded `409 CONFLICT` (e.g. an immutable-tag rejection) must
    /// surface as `Err` so the queue retries instead of silently dropping the job.
    #[tokio::test]
    async fn execute_push_surfaces_immutable_conflict_409_as_error() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (_manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(&mock_server)
                .await;
        }
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(
                r#"{"errors":[{"code":"CONFLICT","message":"tag is immutable"}]}"#,
            ))
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let envelope = build_envelope(&sample_payload()).unwrap();
        let result = handler.execute(&envelope).await;
        assert!(
            result.is_err(),
            "a non-superseded 409 CONFLICT must surface as an error so the job retries"
        );
    }

    /// A `409` carrying `REPLICATION_SUPERSEDED` is a last-writer-wins loss,
    /// i.e. convergence, so `execute()` returns `Ok` and the job completes.
    #[tokio::test]
    async fn execute_push_treats_superseded_409_as_success() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (_manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        for blob in [&config_digest, &layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(&mock_server)
                .await;
        }
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(format!(
                r#"{{"errors":[{{"code":"{REPLICATION_SUPERSEDED_CODE}","message":"local copy is newer"}}]}}"#,
            )))
            .mount(&mock_server)
            .await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let envelope = build_envelope(&sample_payload()).unwrap();
        let tx = handler
            .execute(&envelope)
            .await
            .expect("a superseded 409 is convergence, not failure -> Ok so the job drops");
        assert!(
            tx.mutations.is_empty(),
            "a superseded push returns an empty transaction"
        );
    }

    #[tokio::test]
    async fn execute_delete_manifest_calls_downstream_delete() {
        metrics_provider::init_for_tests();
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&mock_server)
            .await;

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_downstream(downstream_client(&mock_server.uri())),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let mut payload = sample_payload();
        payload.kind = REPLICATION_DELETE_MANIFEST_KIND.to_string();
        let envelope = build_envelope(&payload).unwrap();
        handler.execute(&envelope).await.unwrap();
    }

    /// Builds FS-backed stores, a seeded `v1` manifest, and a handler with one
    /// downstream named `downstream` at `uri`. The `TempDir` is returned so the
    /// caller keeps the backing storage alive for the test's duration.
    async fn handler_with_downstream(
        downstream: &str,
        uri: &str,
    ) -> (ReplicationJobHandler, Digest, Digest, TempDir) {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        let (_manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_named_downstream(downstream, downstream_client(uri)),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        (handler, config_digest, layer_digest, dir)
    }

    /// Mounts a HEAD per blob that returns 200 (already present) so a push skips
    /// the upload sequence and only PUTs the manifest.
    async fn mock_blobs_present(mock_server: &MockServer, blobs: &[&Digest]) {
        for blob in blobs {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                        .insert_header("Content-Length", "10"),
                )
                .mount(mock_server)
                .await;
        }
    }

    #[tokio::test]
    async fn execute_push_records_pushed_metric_and_last_success() {
        metrics_provider::init_for_tests();
        let downstream = "metric-pushed";
        let mock_server = MockServer::start().await;
        let (handler, config_digest, layer_digest, _dir) =
            handler_with_downstream(downstream, &mock_server.uri()).await;
        mock_blobs_present(&mock_server, &[&config_digest, &layer_digest]).await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(201).insert_header(
                DOCKER_CONTENT_DIGEST,
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ))
            .mount(&mock_server)
            .await;

        let pushed_before = push_total(downstream, "pushed");

        let mut payload = sample_payload();
        payload.downstream = downstream.to_string();
        let envelope = build_envelope(&payload).unwrap();
        handler.execute(&envelope).await.unwrap();

        assert_eq!(
            push_total(downstream, "pushed"),
            pushed_before + 1,
            "a successful push must increment the pushed counter exactly once"
        );
        assert_eq!(
            push_total(downstream, "superseded"),
            0,
            "a plain push must not touch the superseded counter"
        );
        assert_eq!(
            push_total(downstream, "failed"),
            0,
            "a successful push must not touch the failed counter"
        );
        assert!(
            last_success(downstream) > 0,
            "a successful push must set the last-success timestamp gauge"
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn execute_push_records_superseded_metric_and_last_success() {
        metrics_provider::init_for_tests();
        let downstream = "metric-superseded";
        let mock_server = MockServer::start().await;
        let (handler, config_digest, layer_digest, _dir) =
            handler_with_downstream(downstream, &mock_server.uri()).await;
        mock_blobs_present(&mock_server, &[&config_digest, &layer_digest]).await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(format!(
                r#"{{"errors":[{{"code":"{REPLICATION_SUPERSEDED_CODE}","message":"local copy is newer"}}]}}"#,
            )))
            .mount(&mock_server)
            .await;

        let superseded_before = push_total(downstream, "superseded");
        let pushed_before = push_total(downstream, "pushed");

        let mut payload = sample_payload();
        payload.downstream = downstream.to_string();
        let envelope = build_envelope(&payload).unwrap();
        handler.execute(&envelope).await.unwrap();

        assert_eq!(
            push_total(downstream, "superseded"),
            superseded_before + 1,
            "an LWW-superseded push must increment the superseded counter"
        );
        assert_eq!(
            push_total(downstream, "pushed"),
            pushed_before,
            "an LWW-superseded push must NOT increment the pushed counter"
        );
        assert!(
            last_success(downstream) > 0,
            "a superseded push is convergence and must set the last-success gauge"
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn execute_push_records_failed_metric_on_error() {
        metrics_provider::init_for_tests();
        let downstream = "metric-failed";
        let mock_server = MockServer::start().await;
        let (handler, config_digest, layer_digest, _dir) =
            handler_with_downstream(downstream, &mock_server.uri()).await;
        mock_blobs_present(&mock_server, &[&config_digest, &layer_digest]).await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(409).set_body_string(
                r#"{"errors":[{"code":"CONFLICT","message":"tag is immutable"}]}"#,
            ))
            .mount(&mock_server)
            .await;

        let failed_before = push_total(downstream, "failed");
        let pushed_before = push_total(downstream, "pushed");
        let last_before = last_success(downstream);

        let mut payload = sample_payload();
        payload.downstream = downstream.to_string();
        let envelope = build_envelope(&payload).unwrap();
        let result = handler.execute(&envelope).await;

        assert!(result.is_err(), "a non-superseded 409 must surface as Err");
        assert_eq!(
            push_total(downstream, "failed"),
            failed_before + 1,
            "a failed push must increment the failed counter"
        );
        assert_eq!(
            push_total(downstream, "pushed"),
            pushed_before,
            "a failed push must NOT increment the pushed counter"
        );
        assert_eq!(
            last_success(downstream),
            last_before,
            "a failed push must NOT advance the last-success gauge"
        );
        drop(mock_server);
    }

    /// A converged no-op must not contact the downstream nor record any metric.
    #[tokio::test]
    async fn execute_push_with_deleted_tag_is_noop_success_records_no_failed() {
        metrics_provider::init_for_tests();
        let downstream = "metric-deleted-tag";

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        // No tag seeded, so the resolve short-circuits; the unreachable
        // downstream URL makes any wrongful push error.
        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_named_downstream(downstream, downstream_client("http://127.0.0.1:1")),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let failed_before = push_total(downstream, "failed");
        let pushed_before = push_total(downstream, "pushed");

        let mut payload = sample_payload();
        payload.downstream = downstream.to_string();
        let envelope = build_envelope(&payload).unwrap();
        let tx = handler
            .execute(&envelope)
            .await
            .expect("a deleted-tag push must be a converged no-op success");
        assert!(
            tx.mutations.is_empty(),
            "a no-op push returns an empty transaction"
        );
        assert_eq!(
            push_total(downstream, "failed"),
            failed_before,
            "a converged no-op must NOT increment the failed counter"
        );
        assert_eq!(
            push_total(downstream, "pushed"),
            pushed_before,
            "a converged no-op must NOT increment the pushed counter"
        );
    }

    #[tokio::test]
    async fn execute_tagless_push_with_deleted_revision_is_noop_success() {
        metrics_provider::init_for_tests();
        let downstream = "metric-deleted-revision";

        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> =
            Arc::new(StorageFsBackend::builder().root_dir(root).build().unwrap());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder()
                .store(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build()
                .unwrap(),
        );
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());

        // No revision link seeded, so the resolve short-circuits; the
        // unreachable downstream URL makes any wrongful push error.
        let mut repositories = HashMap::new();
        repositories.insert(
            REPO.to_string(),
            repository_with_named_downstream(downstream, downstream_client("http://127.0.0.1:1")),
        );
        let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap());

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver)
            .blob_store(blob_store)
            .metadata_store(metadata_store)
            .build()
            .unwrap();

        let failed_before = push_total(downstream, "failed");
        let pushed_before = push_total(downstream, "pushed");

        let mut payload = sample_payload();
        payload.downstream = downstream.to_string();
        payload.tag = None;
        let envelope = build_envelope(&payload).unwrap();
        let tx = handler
            .execute(&envelope)
            .await
            .expect("a deleted-revision by-digest push must be a converged no-op success");
        assert!(
            tx.mutations.is_empty(),
            "a no-op push returns an empty transaction"
        );
        assert_eq!(
            push_total(downstream, "failed"),
            failed_before,
            "a converged no-op must NOT increment the failed counter"
        );
        assert_eq!(
            push_total(downstream, "pushed"),
            pushed_before,
            "a converged no-op must NOT increment the pushed counter"
        );
    }
}
