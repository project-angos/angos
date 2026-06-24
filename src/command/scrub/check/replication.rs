//! [`ReplicationChecker`]: the scrub reconciliation pass, emitting queue-enqueue
//! actions rather than pushing inline so reconcile-discovered divergences get the
//! same retry/backoff/coalescing as the event path. Prune is off by default and
//! one-way-mirror-only: absence-driven deletion is unsafe for active-active peers.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tracing::{debug, warn};

use crate::{
    command::scrub::{
        action::Action,
        check::{NamespaceChecker, list_all},
        error::Error,
        executor::{ActionSink, record_reconcile_outcome},
    },
    oci::{Digest, Namespace, Reference, Tag},
    registry::{
        Error as RegistryError,
        metadata_store::{LinkKind, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
    replication::{ReplicationDownstream, manifest_accept_types},
};

/// Enqueues a replication push for each diverging or downstream-missing tag, and
/// for a `prune = true` downstream (one-way mirror) a replication delete for each
/// downstream-only tag.
pub struct ReplicationChecker {
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
}

impl ReplicationChecker {
    /// Construct a checker from its resolved fields: the `metadata_store` the
    /// local tag set + digests are read from, and the namespace -> repository
    /// `resolver` yielding the downstream list.
    #[must_use]
    pub fn new(metadata_store: Arc<MetadataStore>, resolver: Arc<RepositoryResolver>) -> Self {
        Self {
            metadata_store,
            resolver,
        }
    }

    /// Whether this downstream participates in the reconcile run for `namespace`.
    fn downstream_included(downstream: &ReplicationDownstream, namespace: &Namespace) -> bool {
        downstream.mode.participates_in_reconcile()
            && downstream.matches_namespace(namespace.as_ref())
    }

    /// Resolves the current local digest for `tag` in `namespace`, bypassing
    /// the link cache so a reconcile never enqueues a stale digest.
    async fn local_digest(&self, namespace: &Namespace, tag: &Tag) -> Option<Digest> {
        match self
            .metadata_store
            .read_link_reference(namespace, &LinkKind::Tag(tag.clone()))
            .await
        {
            Ok(link) => Some(link.target),
            Err(e) => {
                warn!("Failed to read local tag '{namespace}:{tag}' during reconcile: {e}");
                None
            }
        }
    }

    /// Reconciles one downstream: a push for every diverging or downstream-missing
    /// tag, and for a `prune = true` downstream a delete for every downstream-only
    /// tag.
    async fn reconcile_downstream(
        &self,
        downstream: &ReplicationDownstream,
        namespace: &Namespace,
        local_tags: &[(Tag, Option<Digest>)],
        sink: &mut (dyn ActionSink + Send),
    ) {
        reconcile_push_step(downstream, namespace, local_tags, sink).await;

        // Prune step (opt-in, one-way-mirror-only): the stamped `source_ts` LWW
        // only saves a future-dated downstream tag and does not make
        // absence-driven deletion safe for active-active peers.
        if !downstream.prune {
            return;
        }

        // `Err` is unreachable for a routed namespace; warn-and-skip is defensive.
        let remote = match downstream.remote(namespace) {
            Ok(remote) => remote,
            Err(e) => {
                warn!(
                    "Invalid downstream namespace on '{}' for '{namespace}'; skipping cleanup: {e}",
                    downstream.name
                );
                return;
            }
        };
        let location = downstream
            .registry_client
            .get_tags_list_path(remote.as_ref());
        let downstream_tags = match downstream.registry_client.list_tags(&location).await {
            Ok(tags) => tags,
            Err(e) => {
                warn!(
                    "Failed to list tags on downstream '{}' for '{namespace}'; skipping cleanup: {e}",
                    downstream.name
                );
                return;
            }
        };

        // Tags whose digest read failed (`None`) still count as local: prune
        // must never delete a tag that exists locally.
        let local_set: HashSet<&str> = local_tags.iter().map(|(tag, _)| tag.as_ref()).collect();
        for tag in downstream_tags {
            if local_set.contains(tag.as_ref()) {
                continue;
            }
            if let Err(e) = sink
                .apply(Action::EnqueueReplicationDelete {
                    downstream: downstream.name.clone(),
                    namespace: namespace.clone(),
                    tag: tag.clone(),
                })
                .await
            {
                // A per-tag enqueue failure warns and continues so one flaky
                // write does not skip the rest of the prune.
                warn!(
                    "Failed to enqueue replication delete for '{namespace}:{tag}' on downstream '{}'; continuing: {e}",
                    downstream.name
                );
            }
        }
    }
}

/// Push step of a reconcile: HEAD every local tag against the downstream
/// (concurrently, bounded by `max_concurrent_pushes`) and enqueue a push for
/// each diverging or absent one. The probe phase fans out; the enqueue is
/// applied serially because the sink is `&mut`. A tag whose local read failed
/// (`None`) is skipped here but still counts as local for the prune step.
async fn reconcile_push_step(
    downstream: &ReplicationDownstream,
    namespace: &Namespace,
    local_tags: &[(Tag, Option<Digest>)],
    sink: &mut (dyn ActionSink + Send),
) {
    enum Probe {
        Push { tag: Tag, digest: Digest },
        Converged,
        Skipped,
    }

    // `Err` is unreachable for a routed namespace; warn-and-skip is defensive.
    let remote = match downstream.remote(namespace) {
        Ok(remote) => remote,
        Err(e) => {
            warn!(
                "Invalid downstream namespace on '{}' for '{namespace}'; skipping push: {e}",
                downstream.name
            );
            return;
        }
    };
    let remote = &remote;

    let candidates: Vec<(Tag, Digest)> = local_tags
        .iter()
        .filter_map(|(tag, local)| local.as_ref().map(|digest| (tag.clone(), digest.clone())))
        .collect();
    let probes = stream::iter(candidates)
        .map(|(tag, local)| async move {
            let reference = Reference::Tag(tag.clone());
            // Only a 404 means absence; any other HEAD failure skips the tag this
            // pass rather than enqueuing a spurious push.
            let location = downstream
                .registry_client
                .get_manifest_path(remote.as_ref(), &reference);
            match downstream
                .registry_client
                .head_manifest(&manifest_accept_types(), &location)
                .await
            {
                Ok((_, digest, _)) if digest == local => {
                    debug!(
                        "Tag '{namespace}:{tag}' already converged on downstream '{}'",
                        downstream.name
                    );
                    Probe::Converged
                }
                Ok(_) | Err(RegistryError::ManifestUnknown) => Probe::Push {
                    tag,
                    digest: local,
                },
                Err(e) => {
                    debug!(
                        "HEAD for '{namespace}:{tag}' on downstream '{}' failed; skipping this pass: {e}",
                        downstream.name
                    );
                    Probe::Skipped
                }
            }
        })
        .buffer_unordered(downstream.max_concurrent_pushes.max(1))
        .collect::<Vec<_>>()
        .await;

    // Apply phase (serial): the sink is `&mut`. Skips are counted so a
    // persistently failing downstream stays visible.
    let mut skipped: usize = 0;
    for probe in probes {
        match probe {
            Probe::Converged => {}
            Probe::Skipped => {
                skipped += 1;
                record_reconcile_outcome("skipped");
            }
            Probe::Push { tag, digest } => {
                if let Err(e) = sink
                    .apply(Action::EnqueueReplicationPush {
                        downstream: downstream.name.clone(),
                        namespace: namespace.clone(),
                        tag: tag.clone(),
                        digest,
                    })
                    .await
                {
                    // A per-tag enqueue failure must not abort the namespace; the
                    // next run re-enqueues whatever was missed.
                    warn!(
                        "Failed to enqueue replication push for '{namespace}:{tag}' to downstream '{}'; continuing: {e}",
                        downstream.name
                    );
                }
            }
        }
    }

    // One warn per downstream per pass so a dead downstream with thousands of
    // tags does not flood the log.
    if skipped > 0 {
        warn!(
            "Skipped {skipped} of {} tag(s) of '{namespace}' on downstream '{}': the downstream \
             HEAD probes failed (see debug logs); they stay unreconciled until a pass succeeds",
            local_tags.len(),
            downstream.name
        );
    }
}

#[async_trait]
impl NamespaceChecker for ReplicationChecker {
    async fn check(
        &self,
        namespace: &Namespace,
        sink: &mut (dyn ActionSink + Send),
    ) -> Result<(), Error> {
        let Some(repository) = self.resolver.resolve(namespace) else {
            return Ok(());
        };

        // Skip the potentially large tag listing when no downstream participates.
        let downstreams: Vec<&ReplicationDownstream> = repository
            .replication
            .iter()
            .filter(|d| Self::downstream_included(d, namespace))
            .collect();
        if downstreams.is_empty() {
            return Ok(());
        }

        // Collect and digest-resolve the tag set once to avoid O(downstreams x
        // tags) metadata reads. A failed link read resolves to `None`: skipped
        // for push but still counted as local so prune never deletes a live tag.
        let mut local_tags: Vec<(Tag, Option<Digest>)> = Vec::new();
        let mut stream = list_all::tags(&self.metadata_store, namespace);
        while let Some(tag) = stream.next().await {
            let tag = tag?;
            let digest = self.local_digest(namespace, &tag).await;
            local_tags.push((tag, digest));
        }

        for downstream in downstreams {
            self.reconcile_downstream(downstream, namespace, &local_tags, sink)
                .await;
        }
        Ok(())
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
        metrics_provider,
        oci::{Digest, Namespace, Tag},
        registry::{
            DOCKER_CONTENT_DIGEST, Repository,
            blob_store::BlobStore,
            job_store::{JobStore, Queue},
            metadata_store::{LinkKind, LinkOperation, MetadataStore},
            repository_resolver::RepositoryResolver,
            test_utils::{
                build_store, build_test_fs_executor, downstream_client, put_blob_direct,
                put_link_raw, repository_with_replication, seed_manifest,
            },
        },
        registry_client::RegistryClient,
        replication::{ReplicationDownstream, ReplicationJobHandler, ReplicationMode},
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";

    /// The validated [`Namespace`] form of [`NAMESPACE`], for the store/checker
    /// APIs that now take `&Namespace`.
    fn namespace() -> Namespace {
        Namespace::new(NAMESPACE).unwrap()
    }

    /// FS-backed metadata store so the tests run without S3; also returns the
    /// store façade for seeding blobs.
    fn fs_metadata_store() -> (Arc<MetadataStore>, Arc<Store>, TempDir) {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_str().unwrap();
        let object: Arc<dyn ObjectStore> = Arc::new(StorageFsBackend::builder(root).build());
        let executor = build_test_fs_executor(root, false);
        let store = build_store(object, executor);
        let metadata_store = Arc::new(
            MetadataStore::builder(store.clone())
                .link_cache_ttl(0)
                .access_time_debounce_secs(0)
                .build(),
        );
        (metadata_store, store, dir)
    }

    fn repository(client: Arc<RegistryClient>, mode: ReplicationMode, prune: bool) -> Repository {
        repository_with_replication(
            REPO,
            vec![
                ReplicationDownstream::builder(DOWNSTREAM.to_string(), client, 4)
                    .mode(mode)
                    .prune(prune)
                    .build(),
            ],
        )
    }

    /// A path-prefixed downstream: strips `nginx` and prepends `mirror`, so content
    /// `nginx/app` maps to remote `mirror/app`. `REPO == "nginx"` keeps the resolver
    /// routing here.
    fn prefixed_repository(
        client: Arc<RegistryClient>,
        mode: ReplicationMode,
        prune: bool,
    ) -> Repository {
        repository_with_replication(
            REPO,
            vec![
                ReplicationDownstream::builder(DOWNSTREAM.to_string(), client, 4)
                    .namespace_mapping(
                        Some(Namespace::new("nginx").unwrap()),
                        Some(Namespace::new("mirror").unwrap()),
                    )
                    .mode(mode)
                    .prune(prune)
                    .build(),
            ],
        )
    }

    fn resolver_for(repo: Repository) -> Arc<RepositoryResolver> {
        let mut repositories = HashMap::new();
        repositories.insert(REPO.to_string(), repo);
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    #[tokio::test]
    async fn enqueues_push_for_tag_missing_on_downstream() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert_eq!(sink.len(), 1);
        assert!(matches!(
            &sink[0],
            Action::EnqueueReplicationPush { downstream, tag, digest, .. }
                if downstream == DOWNSTREAM && tag == "v1" && *digest == manifest
        ));
    }

    #[tokio::test]
    async fn enqueues_push_for_prefixed_downstream_at_mapped_path() {
        // A prefixed downstream must probe the mapped path `mirror/app`, not the
        // local `nginx/app`. The `.expect(1)` on the mapped HEAD asserts this,
        // verified on `MockServer` drop.
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let content = Namespace::new("nginx/app").unwrap();
        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        metadata_store
            .update_links(
                &content,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        Mock::given(method("HEAD"))
            .and(path("/v2/mirror/app/manifests/v1"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(prefixed_repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&content, &mut sink).await.unwrap();

        assert_eq!(sink.len(), 1);
        assert!(matches!(
            &sink[0],
            Action::EnqueueReplicationPush { downstream, namespace, tag, digest }
                if downstream == DOWNSTREAM
                    && namespace == "nginx/app"
                    && tag == "v1"
                    && *digest == manifest
        ));

        // Drop explicitly so the mapped-path `.expect(1)` is verified here.
        drop(mock_server);
    }

    #[tokio::test]
    async fn prunes_prefixed_downstream_at_mapped_path() {
        // Prune on a prefixed downstream must list and delete at the mapped path
        // `mirror/app`, not the local `nginx/app`. The `.expect(1)` on the mapped
        // list and delete asserts that.
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::new(store.clone()));
        let mock_server = MockServer::start().await;

        let content = Namespace::new("nginx/app").unwrap();
        // Local tag `v1` converges; `stray` is downstream-only and must be pruned.
        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                &content,
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        Mock::given(method("HEAD"))
            .and(path("/v2/mirror/app/manifests/v1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v2/mirror/app/tags/list"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["v1", "stray"] })),
            )
            .expect(1)
            .mount(&mock_server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v2/mirror/app/manifests/stray"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(prefixed_repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            true,
        ));
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));

        let checker = ReplicationChecker::new(metadata_store.clone(), resolver.clone());

        let mut executor: Box<dyn ActionSink + Send> = Box::new(Executor::new(
            blob_store.clone(),
            metadata_store.clone(),
            job_store.clone(),
        ));
        checker.check(&content, executor.as_mut()).await.unwrap();
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "the downstream-only tag must enqueue exactly one delete job"
        );

        let handler = ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        );

        let mut drained: u64 = 0;
        loop {
            let outcome = job_store.claim_one(Queue::Replication).await.unwrap();
            let Some(claimed) = outcome.claimed else {
                break;
            };
            execute_one(&job_store, &handler, claimed).await;
            drained += 1;
        }
        assert_eq!(drained, 1, "exactly one delete job is drained");

        // Drop explicitly so the mapped-path list and DELETE `.expect(1)` are
        // verified here.
        drop(mock_server);
    }

    #[tokio::test]
    async fn transient_head_failure_skips_tag_without_enqueuing() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest,
                )],
            )
            .await
            .unwrap();

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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        // Metrics are process-global and shared across tests: assert the DELTA.
        let skipped_before = crate::metrics_provider::metrics_provider()
            .replication_reconcile_total
            .with_label_values(&["skipped"])
            .get();

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert!(
            sink.is_empty(),
            "a transient downstream HEAD failure must not enqueue a push, got {} action(s)",
            sink.len()
        );
        let skipped_after = crate::metrics_provider::metrics_provider()
            .replication_reconcile_total
            .with_label_values(&["skipped"])
            .get();
        assert_eq!(
            skipped_after - skipped_before,
            1,
            "a skipped tag must record replication_reconcile_total{{outcome=\"skipped\"}} \
             so a persistently failing downstream (e.g. bad credentials) is visible"
        );
    }

    /// An `ActionSink` that fails the first `fail_first` applies, simulating
    /// transient enqueue errors.
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
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        for tag in ["v1", "v2"] {
            let body = format!("manifest-{tag}");
            let manifest = put_blob_direct(&store, body.as_bytes()).await;
            metadata_store
                .update_links(
                    &namespace(),
                    &[LinkOperation::create(
                        LinkKind::Tag(Tag::new(tag).unwrap()),
                        manifest,
                    )],
                )
                .await
                .unwrap();
        }

        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink = FlakySink {
            attempted: Vec::new(),
            fail_first: 1,
        };
        checker.check(&namespace(), &mut sink).await.unwrap();

        let attempted: Vec<&str> = sink
            .attempted
            .iter()
            .filter_map(|a| match a {
                Action::EnqueueReplicationPush { tag, .. } => Some(tag.as_ref()),
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
        metrics_provider::init_for_tests();
        let (metadata_store, _store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        // No local tags: both downstream tags are prune-eligible.
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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink = FlakySink {
            attempted: Vec::new(),
            fail_first: 1,
        };
        checker.check(&namespace(), &mut sink).await.unwrap();

        let deleted: Vec<&str> = sink
            .attempted
            .iter()
            .filter_map(|a| match a {
                Action::EnqueueReplicationDelete { tag, .. } => Some(tag.as_ref()),
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
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert!(sink.is_empty(), "converged tag must not enqueue a push");
    }

    #[tokio::test]
    async fn enqueues_push_when_downstream_digest_diverges() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"new-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        let stale =
            Digest::sha256("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap();
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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert_eq!(sink.len(), 1, "diverging tag must enqueue a push");
    }

    #[tokio::test]
    async fn enqueues_pushes_for_every_diverging_tag() {
        // The probe phase fans out across tags; every diverging tag must still
        // enqueue a push (no tag dropped by the concurrency).
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"new-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[
                    LinkOperation::create(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone()),
                    LinkOperation::create(LinkKind::Tag(Tag::new("v2").unwrap()), manifest.clone()),
                    LinkOperation::create(LinkKind::Tag(Tag::new("v3").unwrap()), manifest.clone()),
                ],
            )
            .await
            .unwrap();

        // All three tags are absent on the downstream (404 HEAD) -> all enqueue.
        for tag in ["v1", "v2", "v3"] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/{NAMESPACE}/manifests/{tag}")))
                .respond_with(ResponseTemplate::new(404))
                .mount(&mock_server)
                .await;
        }

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        let mut tags: Vec<&str> = sink
            .iter()
            .filter_map(|action| match action {
                Action::EnqueueReplicationPush { tag, .. } => Some(tag.as_ref()),
                _ => None,
            })
            .collect();
        tags.sort_unstable();
        assert_eq!(
            tags,
            vec!["v1", "v2", "v3"],
            "every diverging tag must enqueue a push"
        );
    }

    #[tokio::test]
    async fn enqueues_delete_for_downstream_only_tag() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

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
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        // `.expect(0)` fails the test if the checker enumerates downstream tags
        // at all.
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
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert!(
            sink.is_empty(),
            "prune disabled: a downstream-only tag must not be deleted"
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn unreadable_local_tag_is_skipped_but_never_pruned() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        put_link_raw(
            &store,
            &namespace(),
            &LinkKind::Tag(Tag::new("broken").unwrap()),
            b"not-a-link",
        )
        .await;

        // No HEAD mock: the push loop never probes an unreadable tag.
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tags": ["broken"] })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            true,
        ));
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert!(
            sink.is_empty(),
            "an unreadable local tag must be skipped for push AND protected from prune, got {} action(s)",
            sink.len()
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn skips_event_only_downstream() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();

        let manifest = put_blob_direct(&store, b"event-only-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        // Unreachable URL: an event-only downstream must never be contacted.
        let resolver = resolver_for(repository(
            downstream_client("http://127.0.0.1:1"),
            ReplicationMode::EventOnly,
            false,
        ));
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert!(sink.is_empty(), "event-only downstream must be skipped");
    }

    #[tokio::test]
    async fn enqueues_push_for_reconcile_only_downstream() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"reconcile-only-bytes").await;
        metadata_store
            .update_links(
                &namespace(),
                &[LinkOperation::create(
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    manifest.clone(),
                )],
            )
            .await
            .unwrap();

        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::ReconcileOnly,
            false,
        ));
        let checker = ReplicationChecker::new(metadata_store.clone(), resolver);

        let mut sink: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut sink).await.unwrap();

        assert_eq!(
            sink.len(),
            1,
            "reconcile-only downstream must reconcile a missing tag"
        );
        assert!(matches!(
            &sink[0],
            Action::EnqueueReplicationPush { downstream, tag, digest, .. }
                if downstream == DOWNSTREAM && tag == "v1" && *digest == manifest
        ));
    }

    // ------------------------------------------------------------------
    // End-to-end (`angos scrub --replicate`)
    // ------------------------------------------------------------------

    /// Mounts a downstream missing tag `v1` and both blobs, expecting the full
    /// blob-upload sequence and exactly one tagged manifest PUT. The
    /// `.expect(...)` counts are verified on `MockServer` drop.
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

    /// Full `angos scrub --replicate` push chain against a wiremock downstream,
    /// including `lock_key` coalescing of a duplicate enqueue.
    #[tokio::test]
    async fn scrub_replicate_enqueues_then_drains_and_converges() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::new(store.clone()));
        let mock_server = MockServer::start().await;

        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&store, &metadata_store, &namespace()).await;

        mount_out_of_sync_downstream(
            &mock_server,
            &manifest_digest,
            &config_digest,
            &layer_digest,
        )
        .await;

        let resolver = resolver_for(repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            false,
        ));

        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "scrub-test"));

        let checker = ReplicationChecker::new(metadata_store.clone(), resolver.clone());

        let mut captured: Vec<Action> = Vec::new();
        checker.check(&namespace(), &mut captured).await.unwrap();
        assert_eq!(captured.len(), 1, "out-of-sync tag must emit one action");
        assert!(matches!(
            &captured[0],
            Action::EnqueueReplicationPush { downstream, namespace, tag, digest }
                if downstream == DOWNSTREAM
                    && namespace == NAMESPACE
                    && tag == "v1"
                    && *digest == manifest_digest
        ));

        let mut executor: Box<dyn ActionSink + Send> = Box::new(Executor::new(
            blob_store.clone(),
            metadata_store.clone(),
            job_store.clone(),
        ));
        checker
            .check(&namespace(), executor.as_mut())
            .await
            .unwrap();
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "the divergent tag must enqueue exactly one replication job"
        );

        checker
            .check(&namespace(), executor.as_mut())
            .await
            .unwrap();
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "a second reconcile pass must coalesce on lock_key (no new job)"
        );

        let handler = ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        );

        let mut drained: u64 = 0;
        loop {
            let outcome = job_store.claim_one(Queue::Replication).await.unwrap();
            let Some(claimed) = outcome.claimed else {
                break;
            };
            execute_one(&job_store, &handler, claimed).await;
            drained += 1;
        }
        assert_eq!(drained, 1, "exactly one coalesced job is drained");
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            0,
            "the queue must be empty after the drain"
        );

        // Drop explicitly so a wiremock `.expect(...)` mismatch surfaces here,
        // not at end-of-test teardown.
        drop(mock_server);
    }

    /// Full `angos scrub --replicate` delete chain: a downstream-only tag is
    /// enqueued for delete and the drain issues exactly one downstream DELETE.
    #[tokio::test]
    async fn scrub_replicate_deletes_downstream_only_tag() {
        metrics_provider::init_for_tests();
        let (metadata_store, store, _dir) = fs_metadata_store();
        let blob_store = Arc::new(BlobStore::new(store.clone()));
        let mock_server = MockServer::start().await;

        let (manifest_digest, _config_digest, _layer_digest) =
            seed_manifest(&store, &metadata_store, &namespace()).await;
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{NAMESPACE}/manifests/v1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str())
                    .insert_header("Content-Length", "15"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["v1", "stray"] })),
            )
            .mount(&mock_server)
            .await;
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

        let checker = ReplicationChecker::new(metadata_store.clone(), resolver.clone());

        let mut executor: Box<dyn ActionSink + Send> = Box::new(Executor::new(
            blob_store.clone(),
            metadata_store.clone(),
            job_store.clone(),
        ));
        checker
            .check(&namespace(), executor.as_mut())
            .await
            .unwrap();
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            1,
            "the downstream-only tag must enqueue exactly one delete job"
        );

        let handler = ReplicationJobHandler::new(
            resolver.clone(),
            blob_store.clone(),
            metadata_store.clone(),
        );

        let mut drained: u64 = 0;
        loop {
            let outcome = job_store.claim_one(Queue::Replication).await.unwrap();
            let Some(claimed) = outcome.claimed else {
                break;
            };
            execute_one(&job_store, &handler, claimed).await;
            drained += 1;
        }
        assert_eq!(drained, 1, "exactly one delete job is drained");
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            0,
            "the queue must be empty after the drain"
        );

        // Drop explicitly so the `.expect(1)` on the DELETE is verified here.
        drop(mock_server);
    }
}
