//! [`ReplicationJobHandler`] — the [`JobHandler`] that drives the replication
//! push pipeline.
//!
//! Sibling of [`crate::registry::cache_job_handler::CacheJobHandler`]. The
//! exemplar is constructed via `new(...)`; this type exposes a `builder()`
//! instead (resolved `Arc` deps only), and kind-dispatch happens inside
//! [`ReplicationJobHandler::execute`] exactly as the exemplar rejects unknown
//! kinds.
//!
//! The handler returns an **empty** [`Transaction`] on success: the effect is an
//! external, non-transactional HTTP push, blessed by the [`JobHandler`] trait
//! doc — the queue still lands its cleanup mutations atomically via `complete`.
//! Because the engine cannot roll back an HTTP PUT, the pipeline is idempotent
//! (HEAD-before-PUT) per the at-least-once contract.

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
    replication::{PushOutcome, ReplicationDownstream, pipeline},
};

/// Single queue carrying every replication job; the downstream is encoded in the
/// `lock_key` and payload (mirrors the single `cache` queue).
pub const REPLICATION_QUEUE: &str = "replication";
/// Push a manifest (and everything it references) to a downstream.
pub const REPLICATION_PUSH_MANIFEST_KIND: &str = "replication.push_manifest";
/// Delete a manifest on a downstream.
pub const REPLICATION_DELETE_MANIFEST_KIND: &str = "replication.delete_manifest";

/// JSON payload for a replication job on [`REPLICATION_QUEUE`].
///
/// The digest is informational: the handler re-resolves the **current** local
/// `tag -> digest` at execute-time so coalesced jobs converge to latest-wins
/// (the digest only seeds delete and tag-less push jobs). `Event` is
/// `Serialize`-only and cannot survive a queue round-trip, so this small
/// `Serialize + Deserialize` struct carries the fields the pipeline needs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationPushPayload {
    /// Local identifier of the target downstream (selects the `RegistryClient`).
    pub downstream: String,
    /// Namespace being replicated.
    pub namespace: String,
    /// Tag bound to the digest, when the change is tag-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Serialized OCI digest, e.g. `sha256:abc...` (informational for pushes,
    /// authoritative for digest deletes / tag-less pushes). Absent for a tag
    /// delete, where the receiver keys off the tag and no digest is needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Job kind, mirrors the envelope `kind` (one of the `REPLICATION_*_KIND`
    /// consts) so a payload is self-describing for the scrub-side builder.
    pub kind: String,
    /// Event timestamp (RFC 3339) carried for receiver-side last-writer-wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ts: Option<String>,
}

/// Builds the per-job `lock_key` for a replication payload.
///
/// Format: `{REPLICATION_QUEUE}.{op}.{downstream}:{namespace}:{tag_or_digest}`.
/// `op` is `push` or `delete`, derived from the payload kind, so a pending push
/// and a delete for the same tag never coalesce into one another (a delete that
/// folded into a still-pending push would be silently lost). Pushes still
/// coalesce with pushes and deletes with deletes. The event path and the scrub
/// checker build the SAME key so a pending push and a reconcile-discovered
/// divergence coalesce on `enqueue`.
///
/// `tag_or_digest` is the tag when set, else the digest. When both are absent
/// (a degenerate payload) it falls back to an empty segment, which still yields
/// a well-formed, namespace-scoped key.
#[must_use]
pub fn replication_lock_key(payload: &ReplicationPushPayload) -> String {
    let op = if payload.kind == REPLICATION_DELETE_MANIFEST_KIND {
        "delete"
    } else {
        "push"
    };
    let tag_or_digest = payload
        .tag
        .as_deref()
        .or(payload.digest.as_deref())
        .unwrap_or("");
    format!(
        "{REPLICATION_QUEUE}.{op}.{}:{}:{}",
        payload.downstream, payload.namespace, tag_or_digest
    )
}

/// Builds a [`JobEnvelope`] from a [`ReplicationPushPayload`].
///
/// Exposed so the scrub reconciliation checker enqueues the exact same envelope
/// shape (queue, kind, `lock_key`) as the event path.
///
/// # Errors
///
/// Returns a [`serde_json::Error`] only when the payload cannot be serialized.
pub fn build_envelope(payload: &ReplicationPushPayload) -> Result<JobEnvelope, serde_json::Error> {
    JobEnvelope::new(
        REPLICATION_QUEUE,
        payload.kind.clone(),
        replication_lock_key(payload),
        payload,
    )
}

/// Mirrors a local repository's state to a configured downstream.
///
/// Holds resolved `Arc` dependencies only; built via
/// [`ReplicationJobHandler::builder`] (no `new(config)` — refactor-rule 4).
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

    /// Resolves the [`ReplicationDownstream`] for a payload (a single lookup
    /// yielding both the `registry_client` and `max_concurrent_pushes`), or
    /// returns an error mirroring the exemplar's "no repository configured"
    /// message.
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

    /// Records the per-downstream replication metrics for a successful push or
    /// delete: the `pushed` / `superseded` counter, and the last-success
    /// timestamp gauge (both [`PushOutcome`] arms are convergence, so both set
    /// the gauge to now).
    fn record_success(downstream: &str, outcome: PushOutcome) {
        let outcome_label = match outcome {
            PushOutcome::Pushed => "pushed",
            PushOutcome::Superseded => "superseded",
        };
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, outcome_label])
            .inc();
        metrics_provider()
            .replication_last_success_timestamp
            .with_label_values(&[downstream])
            .set(Utc::now().timestamp());
    }

    /// Records a `failed` replication push for `downstream` before the handler
    /// returns `Err` (so the job retries / dead-letters).
    ///
    /// `failed` is a **per-attempt** counter: the queue re-invokes `execute` on
    /// each retry, so a job that keeps erroring increments `failed` once per
    /// attempt (Prometheus counter convention — use `rate()` for a failure rate).
    /// `pushed` / `superseded` are terminal (incremented once when the attempt
    /// converges). Every `Err` returned from the push/delete attempt — including
    /// pre-flight (invalid namespace, missing downstream config) and local-read
    /// (unreadable manifest blob) failures, not only downstream HTTP failures —
    /// records `failed`, so the documented `outcome="failed"` failure-rate query
    /// also catches jobs stuck on a local/config error.
    fn record_failure(downstream: &str) {
        metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, "failed"])
            .inc();
    }

    /// Re-resolves the current local target for a push payload: a tag is re-read
    /// at execute-time (latest-wins), returning both its digest and its
    /// `created_at` (the local version timestamp); a tag-less job uses the
    /// payload digest and has no timestamp.
    ///
    /// Returns `Ok(None)` when the tag no longer exists locally
    /// ([`MetadataStoreError::ReferenceNotFound`]): a coalesced push whose tag
    /// was deleted out from under it has already converged, so the caller treats
    /// it as a no-op success rather than dead-lettering. Any other read error
    /// still surfaces as `Err`.
    async fn resolve_current_digest(
        &self,
        namespace: &Namespace,
        payload: &ReplicationPushPayload,
    ) -> Result<Option<(Digest, Option<DateTime<Utc>>)>, Error> {
        if let Some(tag) = &payload.tag {
            match self
                .metadata_store
                .read_link(namespace.as_ref(), &LinkKind::Tag(tag.clone()), false)
                .await
            {
                Ok(link) => Ok(Some((link.target, link.created_at))),
                Err(MetadataStoreError::ReferenceNotFound) => Ok(None),
                Err(e) => Err(Error::Storage(format!(
                    "failed to read tag '{tag}' in '{namespace}': {e}"
                ))),
            }
        } else {
            let digest = payload.digest.as_deref().ok_or_else(|| {
                Error::Storage(format!(
                    "tag-less push for '{namespace}' has no digest to resolve"
                ))
            })?;
            digest
                .parse()
                .map(|digest| Some((digest, None)))
                .map_err(|e| Error::Storage(format!("invalid digest '{digest}': {e}")))
        }
    }

    /// Runs the push/delete attempt for a validated payload: resolves the
    /// namespace + downstream, drives the pipeline, and records the
    /// `pushed` / `superseded` success metric. Any `Err` it returns is counted
    /// once as `failed` by the single `inspect_err` in [`Self::execute`] — so
    /// this body must NOT record `failed` itself (that would double-count a
    /// downstream HTTP failure).
    async fn attempt(
        &self,
        envelope: &JobEnvelope,
        payload: &ReplicationPushPayload,
    ) -> Result<(), Error> {
        let namespace = Namespace::new(&payload.namespace).map_err(|e| {
            Error::Storage(format!("invalid namespace '{}': {e}", payload.namespace))
        })?;
        // Single downstream lookup yielding both the client and the per-manifest
        // blob fan-out knob.
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
                namespace.as_ref(),
                &reference,
                payload.source_ts.as_deref(),
            )
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            Self::record_success(&payload.downstream, outcome);
        } else {
            // Push manifest (and, for a blob-push job, the manifest that
            // references it — re-resolving the current digest gives latest-wins;
            // the pipeline HEAD-skips already-present blobs so a blob-only job is
            // a cheap superset of a manifest push).
            let Some((digest, created_at)) =
                self.resolve_current_digest(&namespace, payload).await?
            else {
                // The tag was deleted out from under this push (a coalesced push
                // whose tag is gone): the system has already converged, so this
                // is a no-op success — not a `pushed`/`superseded`/`failed`.
                debug!(
                    namespace = %namespace,
                    tag = ?payload.tag,
                    "Push tag no longer exists locally; treating as converged no-op"
                );
                return Ok(());
            };
            // Stamp the push with the resolved tag's own creation time so the
            // receiver's last-writer-wins compares against the digest actually
            // being sent. Re-deriving here (rather than trusting the payload)
            // keeps both a COALESCED event push — which retains the first
            // envelope's older timestamp while the digest is re-resolved to a
            // newer one — and a RECONCILE push — which carries no timestamp —
            // correctly ordered. A tag-less (by-digest) push is content-addressed
            // and the receiver skips LWW, so its payload value is carried through.
            let source_ts = created_at
                .map(|ts| ts.to_rfc3339())
                .or_else(|| payload.source_ts.clone());
            let body = self.blob_store.read(&digest).await.map_err(|e| {
                Error::Storage(format!("failed to read local manifest '{digest}': {e}"))
            })?;
            let media_type = pipeline::media_type_of(&body);
            let outcome = pipeline::push_manifest(
                registry_client,
                &self.blob_store,
                &self.metadata_store,
                namespace.as_ref(),
                &digest,
                media_type.as_deref(),
                payload.tag.as_deref(),
                body,
                max_concurrent_pushes,
                source_ts.as_deref(),
            )
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
        // Reject unsupported kinds, mirroring `CacheJobHandler::execute`. Only the
        // two manifest kinds are accepted; anything else is rejected explicitly.
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

        // Loop prevention is no-op suppression at the mutation boundary: the
        // manifest mutation methods only call `dispatch_replication` when the write
        // actually changed local state, so a converged replay (a tag re-asserted to
        // the same digest, an already-present revision, a delete of an already-absent
        // ref) is never re-dispatched and mesh cycles terminate. There is no origin
        // filter and nothing for the handler itself to drop here.

        // Run the whole push/delete attempt and record `failed` once on ANY Err
        // from the attempt — pre-flight (invalid namespace, missing
        // downstream config), local-read (unreadable manifest blob), AND
        // downstream HTTP failures alike — so the documented
        // `replication_push_total{outcome="failed"}` failure-rate query also
        // catches jobs stuck on a local/config error, not only downstream pushes.
        // `record_success` is called inside the attempt (it needs the
        // `PushOutcome`); only the failure arm needs the outer single increment.
        self.attempt(envelope, &payload)
            .await
            .inspect_err(|_| Self::record_failure(&payload.downstream))?;

        // Empty transaction: the HTTP push is the side effect; the queue cleanup
        // mutations still land atomically on `complete`.
        Ok(Transaction::builder().build())
    }
}

/// Builder for [`ReplicationJobHandler`] taking individual resolved fields.
///
/// `resolver`, `blob_store` and `metadata_store` are all required.
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
        cache,
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
                ReplicationJobHandler, ReplicationPushPayload, build_envelope, replication_lock_key,
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
    ///
    /// The prometheus registry is process-global and shared across tests, so
    /// callers read this before and after an action and assert the **delta** (an
    /// absolute would be polluted by other tests touching the same label set).
    fn push_total(downstream: &str, outcome: &str) -> u64 {
        crate::metrics_provider::metrics_provider()
            .replication_push_total
            .with_label_values(&[downstream, outcome])
            .get()
    }

    /// Current value of `angos_replication_last_success_timestamp_seconds{downstream}`.
    fn last_success(downstream: &str) -> i64 {
        crate::metrics_provider::metrics_provider()
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

    /// A delete and a push for the same tag must NOT share a `lock_key`, so a
    /// delete can never coalesce into (and be swallowed by) a still-pending push.
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
            "replication.delete.eu-region:nginx:v1"
        );
        assert_ne!(
            replication_lock_key(&push),
            replication_lock_key(&delete),
            "a push and a delete for the same tag must not coalesce"
        );
    }

    /// A payload with neither tag nor digest still yields a well-formed,
    /// namespace-scoped key (empty `tag_or_digest` segment).
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

    /// Build a downstream `RegistryClient` pointed at `uri`.
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

    /// Like [`repository_with_downstream`] but lets a test pick the downstream
    /// name, so its `angos_replication_push_total{downstream=...}` label set is
    /// isolated from the other tests (all of which use `eu-region`).
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

    /// Seed a config blob, a layer blob, and a manifest blob (referencing both),
    /// plus a `v1` tag link pointing at the manifest. Returns the manifest
    /// digest and its referenced blob digests.
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
        crate::metrics_provider::init_for_tests();
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

    #[tokio::test]
    async fn execute_pushes_manifest_with_head_before_put() {
        crate::metrics_provider::init_for_tests();
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

    #[tokio::test]
    async fn execute_skips_blob_present_on_downstream() {
        crate::metrics_provider::init_for_tests();
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

    /// End-to-end through `execute()`: the manifest PUT carries an
    /// `X-Angos-Source-Timestamp` DERIVED from the resolved tag's `created_at` —
    /// NOT the (deliberately stale) payload timestamp. This pins that a coalesced
    /// push cannot ship a stale last-writer-wins version, plus the
    /// handler -> pipeline -> client header threading.
    #[tokio::test]
    async fn execute_push_stamps_resolved_source_timestamp() {
        crate::metrics_provider::init_for_tests();
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

        // The handler must stamp the resolved tag's created_at, not the payload's
        // stale source_ts ("2026-06-03..."). Read it back to assert the exact value.
        let expected_ts = metadata_store
            .read_link(NAMESPACE, &LinkKind::Tag("v1".to_string()), false)
            .await
            .unwrap()
            .created_at
            .unwrap()
            .to_rfc3339();

        // Both blobs already present (200 on HEAD) -> the only mutating request
        // is the manifest PUT, which we match on the replication headers.
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

        // `sample_payload()` carries a deliberately stale source_ts
        // ("2026-06-03..."), which the handler must IGNORE in favour of the
        // resolved tag's created_at.
        let envelope = build_envelope(&sample_payload()).unwrap();
        handler.execute(&envelope).await.unwrap();
        // wiremock `.expect(1)` on the header-matched PUT is verified on drop.
        drop(mock_server);
    }

    /// A reconcile push enqueues with `source_ts = None`. The
    /// handler must still stamp the resolved tag's `created_at` so the receiver
    /// runs last-writer-wins instead of overwriting the downstream unconditionally.
    #[tokio::test]
    async fn execute_reconcile_push_derives_source_timestamp_from_local_tag() {
        crate::metrics_provider::init_for_tests();
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
        // Even with a None payload timestamp, the PUT must carry the resolved
        // created_at; a missing/empty header would not match and the push fails.
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

    /// A downstream that returns a non-superseded `409 CONFLICT` (e.g. an
    /// immutable-tag rejection) must surface as `Err` from `execute()` so the
    /// queue retries / dead-letters the job — it MUST NOT be silently dropped.
    #[tokio::test]
    async fn execute_push_surfaces_immutable_conflict_409_as_error() {
        crate::metrics_provider::init_for_tests();
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
        // Manifest PUT rejected with 409 CONFLICT (immutable-tag style).
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

    /// A downstream that returns a `409` with the shared
    /// `REPLICATION_SUPERSEDED` OCI code is a last-writer-wins loss — convergence,
    /// not failure — so `execute()` returns `Ok` and the queue completes (drops)
    /// the job.
    #[tokio::test]
    async fn execute_push_treats_superseded_409_as_success() {
        crate::metrics_provider::init_for_tests();
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
        // Manifest PUT rejected with 409 + REPLICATION_SUPERSEDED (LWW loss).
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
        crate::metrics_provider::init_for_tests();
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

    /// Builds FS-backed stores + a seeded `v1` manifest + a handler whose
    /// repository carries a single downstream named `downstream` pointed at
    /// `uri`. Returns the handler and the seeded manifest's referenced blob
    /// digests (for the caller's HEAD mocks). Keeps `TempDir` alive by leaking it
    /// into the returned tuple is unnecessary — the store holds the data on disk
    /// and the handler reads it before any cleanup, so the dir is returned to the
    /// caller to keep alive for the duration of the test.
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

    /// A successful push increments `replication_push_total{.., "pushed"}` by one
    /// and advances `replication_last_success_timestamp{..}` to a fresh value.
    #[tokio::test]
    async fn execute_push_records_pushed_metric_and_last_success() {
        crate::metrics_provider::init_for_tests();
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

    /// An LWW-superseded 409 increments `replication_push_total{.., "superseded"}`
    /// (not `pushed`) and still advances the last-success timestamp (convergence
    /// is success).
    #[tokio::test]
    async fn execute_push_records_superseded_metric_and_last_success() {
        crate::metrics_provider::init_for_tests();
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

    /// A push that fails (non-superseded 409) increments
    /// `replication_push_total{.., "failed"}` and surfaces as an error so the job
    /// retries; it must not touch `pushed` or the last-success gauge.
    #[tokio::test]
    async fn execute_push_records_failed_metric_on_error() {
        crate::metrics_provider::init_for_tests();
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

    /// A push job whose tag was deleted out from under it (the tag link no longer
    /// exists locally) is a converged NO-OP: `execute` returns `Ok`, the
    /// downstream is never contacted, and no `failed` (nor `pushed`/`superseded`)
    /// metric is recorded.
    #[tokio::test]
    async fn execute_push_with_deleted_tag_is_noop_success_records_no_failed() {
        crate::metrics_provider::init_for_tests();
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

        // No tag seeded: `resolve_current_digest` returns `ReferenceNotFound`.
        // The downstream points at an unreachable URL — if the handler tried to
        // push instead of short-circuiting, the attempt would error.
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
}
