//! [`ReplicationChecker`] — the scrub reconciliation pass.
//!
//! For every namespace and every configured downstream, the checker:
//!
//! - iterates the LOCAL tag set and probes each tag on the downstream with
//!   `RegistryClient::head_manifest`, emitting an
//!   [`Action::EnqueueReplicationPush`] for every tag that diverges from or is
//!   missing on the downstream (this is the default, additive behavior); and
//! - **only when the downstream is marked `prune = true`** (an authoritative
//!   one-way mirror), enumerates the DOWNSTREAM tag set with
//!   `RegistryClient::list_tags` and emits an
//!   [`Action::EnqueueReplicationDelete`] for every downstream tag absent
//!   locally. Pruning is off by default: for an active-active peer, deleting a
//!   tag merely because it is absent locally would destroy a peer's
//!   legitimately-newer tag that has not yet replicated back.
//!
//! It does NOT push/delete inline: the `Executor` lands each action on the
//! durable replication queue, so reconcile-discovered divergences get the same
//! retry/backoff/dead-letter/coalescing as the event path, and the mutation
//! applier stays free of network fan-out.
//!
//! Deletion is `prune`-gated and off by default: a downstream-only tag is removed
//! only when the downstream is marked `prune = true`. When pruning is enabled and
//! the downstream `list_tags` enumeration fails, the cleanup step is skipped for
//! that downstream (a warning is logged) rather than aborting the run.
//!
//! `prune` is ONE-WAY-MIRROR-ONLY. The prune-delete action carries a `source_ts`
//! stamped at the moment reconcile decides to delete, so the receiver runs
//! last-writer-wins instead of deleting unconditionally; this guards against a
//! downstream tag dated in the future relative to the decision (clock skew, or a
//! tag pushed in the gap between the listing and the delete landing). It does NOT
//! make prune safe for active-active: a peer's legitimately-newer tag whose
//! `created_at` predates this reconcile run still has `created_at < source_ts`
//! and IS deleted. Enable `prune = true` only for a downstream that is an
//! authoritative one-way mirror.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::{debug, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::ActionSink,
    },
    oci::{Digest, Reference},
    registry::{
        Error as RegistryError,
        metadata_store::{MetadataStore, link_kind::LinkKind},
        repository_resolver::RepositoryResolver,
    },
    replication::ReplicationDownstream,
};

/// Reconciles every replicated namespace with all its downstreams by enqueuing a
/// replication push for each diverging or downstream-missing tag. For a downstream
/// marked `prune = true` (an authoritative one-way mirror), it additionally
/// enqueues a replication delete for each downstream-only tag; pruning is off by
/// default.
///
/// Holds only resolved `Arc` dependencies — no `*Config` field. Constructed
/// exclusively via [`ReplicationChecker::builder`].
pub struct ReplicationChecker {
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
}

impl ReplicationChecker {
    /// Starts building a checker from individual resolved fields.
    #[must_use]
    pub fn builder() -> ReplicationCheckerBuilder {
        ReplicationCheckerBuilder::default()
    }

    /// Whether this downstream participates in the current reconcile run.
    ///
    /// A downstream is included when its mode participates in reconciliation and
    /// its namespace filter matches `namespace`.
    fn downstream_included(downstream: &ReplicationDownstream, namespace: &str) -> bool {
        downstream.mode.participates_in_reconcile() && downstream.matches_namespace(namespace)
    }

    /// Resolves the current local digest for `tag` in `namespace`.
    async fn local_digest(&self, namespace: &str, tag: &str) -> Option<Digest> {
        match self
            .metadata_store
            .read_link(namespace, &LinkKind::Tag(tag.to_string()), false)
            .await
        {
            Ok(link) => Some(link.target),
            Err(e) => {
                warn!("Failed to read local tag '{namespace}:{tag}' during reconcile: {e}");
                None
            }
        }
    }

    /// Reconciles one downstream against the local tag set: emits an
    /// `EnqueueReplicationPush` for every diverging or downstream-missing tag, and,
    /// only when the downstream is marked `prune = true`, an
    /// `EnqueueReplicationDelete` for every downstream-only tag.
    async fn reconcile_downstream(
        &self,
        downstream: &ReplicationDownstream,
        namespace: &str,
        local_tags: &[String],
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        // 1. Push: iterate the LOCAL tag set and probe each tag on the downstream
        //    with `head_manifest`; a diverging or missing tag needs a push.
        for tag in local_tags {
            let Some(local) = self.local_digest(namespace, tag).await else {
                continue;
            };

            // Downstream digest for this tag via OCI HEAD (Docker-Content-Digest).
            // A genuinely-absent tag (404) needs a push; a transient HEAD failure
            // (5xx/timeout) is NOT absence, so skip the tag this pass rather than
            // enqueuing a spurious push — the next reconcile re-checks it.
            let location = downstream.registry_client.get_manifest_path(
                "",
                namespace,
                &Reference::Tag(tag.clone()),
            );
            let downstream_digest = match downstream
                .registry_client
                .head_manifest(&[], &location)
                .await
            {
                Ok((_, digest, _)) => Some(digest),
                Err(RegistryError::ManifestUnknown) => None,
                Err(e) => {
                    debug!(
                        "HEAD for '{namespace}:{tag}' on downstream '{}' failed transiently; skipping this pass: {e}",
                        downstream.name
                    );
                    continue;
                }
            };

            if downstream_digest.as_ref() == Some(&local) {
                debug!(
                    "Tag '{namespace}:{tag}' already converged on downstream '{}'",
                    downstream.name
                );
                continue;
            }

            if let Err(e) = sink
                .apply(Action::EnqueueReplicationPush {
                    downstream: downstream.name.clone(),
                    namespace: namespace.to_string(),
                    tag: tag.clone(),
                    digest: local,
                })
                .await
            {
                // A per-tag enqueue failure (e.g. a transient durable-queue write)
                // must not abort the rest of the namespace. Reconcile is
                // convergent: the next run re-enqueues whatever was missed.
                warn!(
                    "Failed to enqueue replication push for '{namespace}:{tag}' to downstream '{}'; continuing: {e}",
                    downstream.name
                );
            }
        }

        // 2. Prune (opt-in): when this downstream is an authoritative one-way
        //    mirror (`prune = true`), enumerate its tag set and delete any tag
        //    absent locally. Skipped by default — for an active-active peer this
        //    would delete a peer's legitimately-newer tag that has not yet
        //    replicated back, so absence-driven deletion is unsafe there. The
        //    receiver runs LWW on the stamped `source_ts` (see `Executor`), which
        //    only saves a future-dated downstream tag — it does NOT make prune
        //    active-active safe (a peer's newer tag predating this run is still
        //    deleted), so this is one-way-mirror-only. A failed enumeration warns
        //    and skips cleanup for this downstream rather than aborting the run.
        if !downstream.prune {
            return Ok(());
        }

        let location = downstream.registry_client.get_tags_list_path("", namespace);
        let downstream_tags = match downstream.registry_client.list_tags(&location).await {
            Ok(tags) => tags,
            Err(e) => {
                warn!(
                    "Failed to list tags on downstream '{}' for '{namespace}'; skipping cleanup: {e}",
                    downstream.name
                );
                return Ok(());
            }
        };

        let local_set: HashSet<&str> = local_tags.iter().map(String::as_str).collect();
        for tag in downstream_tags {
            if local_set.contains(tag.as_str()) {
                continue;
            }
            if let Err(e) = sink
                .apply(Action::EnqueueReplicationDelete {
                    downstream: downstream.name.clone(),
                    namespace: namespace.to_string(),
                    tag: tag.clone(),
                })
                .await
            {
                // As with the push loop above: a per-tag enqueue failure warns and
                // continues so one flaky write does not skip the rest of the prune.
                warn!(
                    "Failed to enqueue replication delete for '{namespace}:{tag}' on downstream '{}'; continuing: {e}",
                    downstream.name
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl NamespaceChecker for ReplicationChecker {
    async fn check(
        &self,
        namespace: &str,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let Some(repository) = self.resolver.resolve(namespace) else {
            return Ok(());
        };

        // Nothing to reconcile when no participating downstream exists for this
        // namespace — skip the (potentially large) tag listing entirely.
        let downstreams: Vec<&ReplicationDownstream> = repository
            .replication
            .iter()
            .filter(|d| Self::downstream_included(d, namespace))
            .collect();
        if downstreams.is_empty() {
            return Ok(());
        }

        // Local tag set, collected once and shared across every downstream.
        let mut local_tags: Vec<String> = Vec::new();
        let mut stream = list_all::tags(&self.metadata_store, namespace);
        while let Some(tag) = stream.next().await {
            local_tags.push(tag?);
        }

        for downstream in downstreams {
            self.reconcile_downstream(downstream, namespace, &local_tags, sink)
                .await?;
        }
        Ok(())
    }
}

/// Builder for [`ReplicationChecker`] taking individual resolved fields.
///
/// `metadata_store` and `resolver` are both required.
#[derive(Default)]
pub struct ReplicationCheckerBuilder {
    metadata_store: Option<Arc<MetadataStore>>,
    resolver: Option<Arc<RepositoryResolver>>,
}

impl ReplicationCheckerBuilder {
    /// Metadata store the local tag set + digests are read from (required).
    #[must_use]
    pub fn metadata_store(mut self, metadata_store: Arc<MetadataStore>) -> Self {
        self.metadata_store = Some(metadata_store);
        self
    }

    /// Namespace -> repository resolver yielding the downstream list (required).
    #[must_use]
    pub fn resolver(mut self, resolver: Arc<RepositoryResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Builds the [`ReplicationChecker`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when a required field is missing.
    pub fn build(self) -> Result<ReplicationChecker, Error> {
        let metadata_store = self.metadata_store.ok_or_else(|| {
            Error::Initialization("replication checker builder requires a metadata_store".into())
        })?;
        let resolver = self.resolver.ok_or_else(|| {
            Error::Initialization("replication checker builder requires a resolver".into())
        })?;
        Ok(ReplicationChecker {
            metadata_store,
            resolver,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use serde_json::json;
    use tempfile::TempDir;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::store::Store;

    use crate::{
        cache,
        command::{
            scrub::{
                action::Action,
                check::NamespaceChecker,
                check::replication::ReplicationChecker,
                error::Error,
                executor::{ActionSink, Executor},
            },
            worker::runner::execute_one,
        },
        oci::Digest,
        policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
        registry::{
            DOCKER_CONTENT_DIGEST, Repository,
            blob_store::BlobStore,
            job_store::JobStore,
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            metadata_store::{LinkOperation, MetadataStore, link_kind::LinkKind},
            repository_resolver::RepositoryResolver,
            test_utils::{build_store, build_test_fs_executor, put_blob_direct},
        },
        registry_client::RegistryClient,
        replication::{
            REPLICATION_QUEUE, ReplicationDownstream, ReplicationJobHandler, ReplicationMode,
        },
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";

    /// Build a single FS-backed metadata store (no S3 backend, so the test runs
    /// in any environment — mirrors the handler tests). Returns the store
    /// façade too so callers can seed manifest blobs.
    fn fs_metadata_store() -> (Arc<MetadataStore>, Arc<Store>, TempDir) {
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
        (metadata_store, store, dir)
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

    fn repository(client: Arc<RegistryClient>, mode: ReplicationMode, prune: bool) -> Repository {
        Repository {
            name: REPO.to_string(),
            upstreams: Vec::new(),
            replication: vec![
                ReplicationDownstream::builder()
                    .name(DOWNSTREAM.to_string())
                    .registry_client(client)
                    .mode(mode)
                    .max_concurrent_pushes(4)
                    .prune(prune)
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

    fn resolver_for(repo: Repository) -> Arc<RepositoryResolver> {
        let mut repositories = HashMap::new();
        repositories.insert(REPO.to_string(), repo);
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    #[tokio::test]
    async fn enqueues_push_for_tag_missing_on_downstream() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // Local: tag v1 -> manifest digest.
        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // Downstream: tag missing -> HEAD 404.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert_eq!(sink.len(), 1);
        assert!(matches!(
            &sink[0],
            Action::EnqueueReplicationPush { downstream, tag, digest, .. }
                if downstream == DOWNSTREAM && tag == "v1" && *digest == manifest
        ));
    }

    #[tokio::test]
    async fn transient_head_failure_skips_tag_without_enqueuing() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // Local: tag v1 -> manifest digest.
        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest,
                )],
            )
            .await
            .unwrap();

        // Downstream HEAD fails transiently (500). That is NOT a genuine absence,
        // so the tag is skipped for this pass and no push is enqueued (a 404 would
        // enqueue one — see enqueues_push_for_tag_missing_on_downstream).
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert!(
            sink.is_empty(),
            "a transient downstream HEAD failure must not enqueue a push, got {} action(s)",
            sink.len()
        );
    }

    /// An `ActionSink` that records every applied action and fails the first
    /// `fail_first` of them, simulating a transient durable-queue enqueue error.
    struct FlakySink {
        attempted: Vec<Action>,
        fail_first: usize,
    }

    #[async_trait]
    impl ActionSink for FlakySink {
        async fn apply(&mut self, action: Action) -> Result<(), Error> {
            self.attempted.push(action);
            if self.attempted.len() <= self.fail_first {
                return Err(Error::Initialization("simulated enqueue failure".into()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn enqueue_failure_does_not_abort_remaining_tags() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // Local: two diverging tags (both missing on the downstream -> need push).
        for tag in ["v1", "v2"] {
            let body = format!("manifest-{tag}");
            let manifest = put_blob_direct(&store, body.as_bytes()).await;
            metadata_store
                .update_links(
                    NAMESPACE,
                    &[LinkOperation::create(
                        LinkKind::Tag(tag.to_string()),
                        manifest,
                    )],
                )
                .await
                .unwrap();
        }

        // Every downstream HEAD is a 404 -> every local tag needs a push.
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        // The first enqueue fails; reconcile must still attempt the second tag
        // rather than aborting the namespace.
        let mut sink = FlakySink {
            attempted: Vec::new(),
            fail_first: 1,
        };
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        let attempted: Vec<&str> = sink
            .attempted
            .iter()
            .filter_map(|a| match a {
                Action::EnqueueReplicationPush { tag, .. } => Some(tag.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            attempted.contains(&"v1") && attempted.contains(&"v2"),
            "both diverging tags must be attempted despite the first enqueue failing (got {attempted:?})"
        );
    }

    #[tokio::test]
    async fn prune_enqueue_failure_does_not_abort_remaining_deletes() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // No local tags; the downstream holds two stray tags absent locally, so a
        // prune downstream enqueues a delete for each.
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["stray1", "stray2"] })),
            )
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            true,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        // The first delete enqueue fails; the prune sweep must still attempt the
        // second downstream-only tag rather than aborting.
        let mut sink = FlakySink {
            attempted: Vec::new(),
            fail_first: 1,
        };
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        let deleted: Vec<&str> = sink
            .attempted
            .iter()
            .filter_map(|a| match a {
                Action::EnqueueReplicationDelete { tag, .. } => Some(tag.as_str()),
                _ => None,
            })
            .collect();
        assert!(
            deleted.contains(&"stray1") && deleted.contains(&"stray2"),
            "both downstream-only tags must be attempted despite the first delete enqueue failing (got {deleted:?})"
        );
    }

    #[tokio::test]
    async fn no_action_when_downstream_digest_matches() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // Downstream HEAD returns the SAME digest -> converged -> no action.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert!(sink.is_empty(), "converged tag must not enqueue a push");
    }

    #[tokio::test]
    async fn enqueues_push_when_downstream_digest_diverges() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"new-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        let stale = Digest::Sha256(
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".into(),
        );
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, stale.to_string().as_str())
                    .insert_header("Content-Length", "9"),
            )
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert_eq!(sink.len(), 1, "diverging tag must enqueue a push");
    }

    #[tokio::test]
    async fn enqueues_delete_for_downstream_only_tag() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // Local: tag v1 -> manifest digest (converged on the downstream).
        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // Downstream HEAD on v1 returns the same digest -> no push.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        // Downstream tags/list reports v1 (local) AND a downstream-only `stray`.
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["v1", "stray"] })),
            )
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            true,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert_eq!(
            sink.len(),
            1,
            "only the downstream-only tag should produce an action"
        );
        assert!(matches!(
            &sink[0],
            Action::EnqueueReplicationDelete { downstream, namespace, tag }
                if downstream == DOWNSTREAM && namespace == NAMESPACE && tag == "stray"
        ));
    }

    #[tokio::test]
    async fn no_delete_when_prune_disabled() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // Local: tag v1 -> manifest digest (converged on the downstream).
        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // v1 converged on the downstream -> no push.
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        // With prune disabled the checker must NOT enumerate downstream tags, so
        // the downstream-only `stray` tag is left untouched. `.expect(0)` fails
        // the test if `tags/list` is called at all.
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["v1", "stray"] })),
            )
            .expect(0)
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert!(
            sink.is_empty(),
            "prune disabled: a downstream-only tag must not be deleted"
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn skips_event_only_downstream() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();

        let manifest = put_blob_direct(&store, b"event-only-bytes").await;
        metadata_store
            .update_links(
                NAMESPACE,
                &[LinkOperation::create(
                    LinkKind::Tag("v1".to_string()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // event-only downstream is excluded from reconcile, so the client is
        // never contacted: point it at an unreachable URL.
        let resolver = resolver_for(repository(
            downstream_client("http://127.0.0.1:1"),
            ReplicationMode::EventOnly,
            false,
        ));
        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver)
            .build()
            .unwrap();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut sink).await.unwrap();

        assert!(sink.is_empty(), "event-only downstream must be skipped");
    }

    // ------------------------------------------------------------------
    // End-to-end (`angos scrub --replicate`)
    // ------------------------------------------------------------------

    /// Seed a config blob, a layer blob, a manifest blob referencing both, and a
    /// `v1` tag link pointing at the manifest (mirrors the handler-stage seed).
    /// Returns `(manifest_digest, config_digest, layer_digest, manifest_bytes)`.
    async fn seed_manifest(
        store: &Arc<Store>,
        metadata_store: &Arc<MetadataStore>,
    ) -> (Digest, Digest, Digest) {
        let config_bytes = br#"{"config":true}"#.to_vec();
        let layer_bytes = b"layer-bytes".to_vec();
        let config_digest = put_blob_direct(store, &config_bytes).await;
        let layer_digest = put_blob_direct(store, &layer_bytes).await;

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
        let manifest_digest = put_blob_direct(store, &manifest_bytes).await;

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

    /// Mount the downstream so tag `v1` is absent (HEAD 404 -> needs push), both
    /// referenced blobs are missing (HEAD 404 -> upload sequence runs), and the
    /// tagged manifest PUT lands exactly once. `.expect(...)` counts are verified
    /// on `MockServer` drop.
    async fn mount_out_of_sync_downstream(
        mock_server: &MockServer,
        manifest_digest: &Digest,
        config_digest: &Digest,
        layer_digest: &Digest,
    ) {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(404))
            .expect(1..)
            .mount(mock_server)
            .await;
        for blob in [config_digest, layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/blobs/{blob}")))
                .respond_with(ResponseTemplate::new(404))
                .expect(1)
                .mount(mock_server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/")))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
            )
            .expect(2)
            .mount(mock_server)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", format!("/v2/{NAMESPACE}/blobs/uploads/s1")),
            )
            .expect(2)
            .mount(mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/blobs/uploads/s1")))
            .respond_with(ResponseTemplate::new(201))
            .expect(2)
            .mount(mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(mock_server)
            .await;
    }

    /// Full `angos scrub --replicate` chain against a wiremock downstream:
    ///
    /// 1. the `ReplicationChecker` HEAD-probes the out-of-sync downstream tag and
    ///    emits an `EnqueueReplicationPush` action (also asserted via a capture
    ///    sink) which the real-storage `Executor` lands on the durable queue
    ///    (`count_pending == 1`);
    /// 2. draining that queue (`claim_one` + `execute_one`, exactly as `Command`
    ///    does) drives the `ReplicationJobHandler` -> push pipeline, so the
    ///    downstream receives the blob-upload sequence + the tagged manifest PUT;
    /// 3. re-running the checker after convergence is a no-op, and a duplicate
    ///    enqueue while one job is pending coalesces on `lock_key`
    ///    (`count_pending` stays at 1).
    #[tokio::test]
    async fn scrub_replicate_enqueues_then_drains_and_converges() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());
        let mock_server = MockServer::start().await;

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store).await;

        mount_out_of_sync_downstream(
            &mock_server,
            &manifest_digest,
            &config_digest,
            &layer_digest,
        )
        .await;

        // One resolver shared between the checker (HEAD probe) and the handler
        // (push), exactly as `Command` wires a single `RepositoryResolver`.
        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));

        // One `Arc<JobStore>` as both producer (executor enqueue) and consumer
        // (drain), over the same `Store` façade — mirrors `Command::new`.
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));

        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver.clone())
            .build()
            .unwrap();

        // (a) Capture sink: assert the produced action without enqueuing.
        let mut captured: Vec<Action> = Vec::new();
        checker.check(NAMESPACE, &mut captured).await.unwrap();
        assert_eq!(captured.len(), 1, "out-of-sync tag must emit one action");
        assert!(matches!(
            &captured[0],
            Action::EnqueueReplicationPush { downstream, namespace, tag, digest }
                if downstream == DOWNSTREAM
                    && namespace == NAMESPACE
                    && tag == "v1"
                    && *digest == manifest_digest
        ));

        // (b) Real executor sink: the action lands on the durable queue.
        let mut executor: Box<dyn ActionSink + Send> = Box::new(
            Executor::builder()
                .blob_store(blob_store.clone())
                .metadata_store(metadata_store.clone())
                .job_store(job_store.clone())
                .build()
                .unwrap(),
        );
        checker.check(NAMESPACE, executor.as_mut()).await.unwrap();
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            1,
            "the divergent tag must enqueue exactly one replication job"
        );

        // (c) A duplicate enqueue while one is pending coalesces on `lock_key`.
        checker.check(NAMESPACE, executor.as_mut()).await.unwrap();
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            1,
            "a second reconcile pass must coalesce on lock_key (no new job)"
        );

        // (d) Drain to convergence exactly as `Command::drain_replication_jobs`:
        //     claim_one + execute_one until the queue empties.
        let handler = ReplicationJobHandler::builder()
            .resolver(resolver.clone())
            .blob_store(blob_store.clone())
            .metadata_store(metadata_store.clone())
            .build()
            .unwrap();

        let mut drained: u64 = 0;
        loop {
            let outcome = job_store.claim_one(REPLICATION_QUEUE).await.unwrap();
            let Some(claimed) = outcome.claimed else {
                break;
            };
            execute_one(&job_store, &handler, claimed).await;
            drained += 1;
        }
        assert_eq!(drained, 1, "exactly one coalesced job is drained");
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            0,
            "the queue must be empty after the drain"
        );

        // The downstream received the blob-upload sequence + the tagged manifest
        // PUT; the wiremock `.expect(...)` counts are verified on drop. Drop
        // explicitly so a mismatch surfaces here, not at end-of-test teardown.
        drop(mock_server);
    }

    /// Full `angos scrub --replicate` chain for the DELETE path: the downstream
    /// holds a `stray` tag that does not exist locally, so the checker enqueues an
    /// `EnqueueReplicationDelete`, and draining the queue issues a `DELETE` to the
    /// downstream `/v2/<ns>/manifests/stray` exactly once.
    #[tokio::test]
    async fn scrub_replicate_deletes_downstream_only_tag() {
        crate::metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::builder().store(store.clone()).build().unwrap());
        let mock_server = MockServer::start().await;

        // Local: tag v1 -> manifest digest, already converged on the downstream.
        let (manifest_digest, _config_digest, _layer_digest) =
            seed_manifest(&store, &metadata_store).await;
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        // Downstream tags/list reports v1 (local) AND a downstream-only `stray`.
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["v1", "stray"] })),
            )
            .mount(&mock_server)
            .await;
        // The drain must issue exactly one DELETE for the stray downstream tag.
        Mock::given(method("DELETE"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/stray")))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            true,
        ));
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));

        let checker = ReplicationChecker::builder()
            .metadata_store(metadata_store.clone())
            .resolver(resolver.clone())
            .build()
            .unwrap();

        let mut executor: Box<dyn ActionSink + Send> = Box::new(
            Executor::builder()
                .blob_store(blob_store.clone())
                .metadata_store(metadata_store.clone())
                .job_store(job_store.clone())
                .build()
                .unwrap(),
        );
        checker.check(NAMESPACE, executor.as_mut()).await.unwrap();
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            1,
            "the downstream-only tag must enqueue exactly one delete job"
        );

        let handler = ReplicationJobHandler::builder()
            .resolver(resolver.clone())
            .blob_store(blob_store.clone())
            .metadata_store(metadata_store.clone())
            .build()
            .unwrap();

        let mut drained: u64 = 0;
        loop {
            let outcome = job_store.claim_one(REPLICATION_QUEUE).await.unwrap();
            let Some(claimed) = outcome.claimed else {
                break;
            };
            execute_one(&job_store, &handler, claimed).await;
            drained += 1;
        }
        assert_eq!(drained, 1, "exactly one delete job is drained");
        assert_eq!(
            job_store.count_pending(REPLICATION_QUEUE, 0).await.unwrap(),
            0,
            "the queue must be empty after the drain"
        );

        // The `.expect(1)` on the DELETE is verified on drop.
        drop(mock_server);
    }
}
