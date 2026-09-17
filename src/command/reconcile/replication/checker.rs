//! [`ReplicationChecker`]: the reconciliation pass. It enqueues rather than
//! pushes inline, so reconcile-discovered divergences get the same
//! retry/backoff/coalescing as the event path.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};
use tracing::{debug, warn};

use angos_oci::{
    Digest, Namespace, Reference, Tag, manifest_accept_types,
    request::{HeadManifestRequest, ListTagsRequest},
    response::ManifestHeadResponse,
};
use angos_oci_client::Error as ClientError;

use crate::{
    command::maintenance::{Error, action::Action, check::NamespaceChecker, executor::ActionSink},
    registry::{
        Error as RegistryError,
        metadata_store::{LinkKind, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
    replication::{ReplicationDownstream, record_reconcile_outcome},
};

/// Enqueues a replication push for each diverging or downstream-missing tag, and
/// for a `prune = true` downstream (one-way mirror) a replication delete for each
/// downstream-only tag.
pub struct ReplicationChecker {
    metadata_store: Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
}

impl ReplicationChecker {
    #[must_use]
    pub fn new(metadata_store: Arc<MetadataStore>, resolver: Arc<RepositoryResolver>) -> Self {
        Self {
            metadata_store,
            resolver,
        }
    }

    fn downstream_included(downstream: &ReplicationDownstream, namespace: &Namespace) -> bool {
        downstream.mode.participates_in_reconcile()
            && downstream.matches_namespace(namespace.as_ref())
    }

    /// Re-checks a prune candidate against live state, since the local tag
    /// walk is stale by the time the downstream listing returns. A read error
    /// counts as present, so uncertainty never deletes.
    async fn tag_absent_locally(&self, namespace: &Namespace, tag: &Tag) -> bool {
        matches!(
            self.metadata_store
                .read_link(namespace, &LinkKind::Tag(tag.clone()))
                .await,
            Err(RegistryError::NotFound)
        )
    }

    /// A push for every diverging or downstream-missing tag, and for a
    /// `prune = true` downstream a delete for every downstream-only tag.
    async fn reconcile_downstream(
        &self,
        downstream: &ReplicationDownstream,
        namespace: &Namespace,
        local_tags: &[(Tag, Digest)],
        sink: &dyn ActionSink,
    ) {
        reconcile_push_step(downstream, namespace, local_tags, sink).await;

        // Prune is opt-in and one-way-mirror-only: the stamped `source_ts` LWW
        // only saves a future-dated downstream tag, so absence-driven deletion
        // stays unsafe for active-active peers.
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
        let downstream_tags = match downstream
            .registry_client
            .list_tags(ListTagsRequest {
                namespace: remote.clone(),
                n: None,
                last: None,
            })
            .await
        {
            Ok(tags) => tags,
            Err(e) => {
                warn!(
                    "Failed to list tags on downstream '{}' for '{namespace}'; skipping cleanup: {e}",
                    downstream.name
                );
                return;
            }
        };

        let local_set: HashSet<&str> = local_tags.iter().map(|(tag, _)| tag.as_ref()).collect();
        for tag in downstream_tags {
            if local_set.contains(tag.as_ref()) {
                continue;
            }
            // A tag pushed locally after the walk is absent here
            // yet present downstream, so re-read live state before reaping it.
            if !self.tag_absent_locally(namespace, &tag).await {
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
                // Continue so one flaky write does not skip the rest.
                warn!(
                    "Failed to enqueue replication delete for '{namespace}:{tag}' on downstream '{}'; continuing: {e}",
                    downstream.name
                );
            }
        }
    }
}

/// HEADs every local tag against the downstream, bounded by
/// `max_concurrent_pushes`, and enqueues a push for each diverging or absent
/// one. An unresolved tag is skipped here but still counts as local for prune.
async fn reconcile_push_step(
    downstream: &ReplicationDownstream,
    namespace: &Namespace,
    local_tags: &[(Tag, Digest)],
    sink: &dyn ActionSink,
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

    let probes = stream::iter(local_tags.to_vec())
        .map(|(tag, local)| async move {
            let reference = Reference::Tag(tag.clone());
            // Only a 404 means absence; any other HEAD failure skips the tag
            // rather than enqueuing a spurious push.
            match downstream
                .registry_client
                .head_manifest(HeadManifestRequest {
                    namespace: remote.clone(),
                    reference: reference.clone(),
                    accepted_types: manifest_accept_types(),
                })
                .await
            {
                Ok(ManifestHeadResponse {
                    digest: Some(digest),
                    ..
                }) if digest == local => {
                    debug!(
                        "Tag '{namespace}:{tag}' already converged on downstream '{}'",
                        downstream.name
                    );
                    Probe::Converged
                }
                Ok(_) | Err(ClientError::ManifestUnknown) => Probe::Push {
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

    // Actions apply serially in probe order; skips are counted so a
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
                    // Never abort the namespace; the next run re-enqueues
                    // whatever was missed.
                    warn!(
                        "Failed to enqueue replication push for '{namespace}:{tag}' to downstream '{}'; continuing: {e}",
                        downstream.name
                    );
                }
            }
        }
    }

    // One warn per pass so a dead downstream does not flood the log.
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
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
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

        // Walked once, not per downstream, to avoid O(downstreams x tags)
        // listings; the walk resolves each tag, so no tag costs a read.
        let local_tags: Vec<(Tag, Digest)> = self
            .metadata_store
            .stream_live_tags(namespace, None)
            .err_into::<Error>()
            .map_ok(|(tag, metadata)| (tag, metadata.target))
            .try_collect()
            .await?;

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
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use angos_oci::{Digest, Namespace, Tag, header::DOCKER_CONTENT_DIGEST};
    use angos_oci_client::RegistryClient;
    use angos_storage::{
        Error as StorageError, ObjectStore,
        test_util::{HookedStore, StoreHook, StoreOp},
    };

    use crate::{
        command::{
            maintenance::{
                Error,
                action::Action,
                check::NamespaceChecker,
                executor::{ActionSink, Executor},
            },
            reconcile::replication::checker::ReplicationChecker,
        },
        jobs::{
            Queue,
            runner::execute_one,
            store::{ClaimMode, JobStore},
        },
        registry::{
            Repository,
            keys::NamespaceKeys,
            metadata_store::LinkKind,
            repository_resolver::RepositoryResolver,
            test_utils::{
                FsTestStack, downstream_client, fs_test_stack, metadata_store_over,
                put_blob_direct, repository_with_replication, seed_links, seed_manifest,
            },
        },
        replication::{ReplicationDownstream, ReplicationJobHandler, ReplicationMode},
    };

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";

    fn namespace() -> Namespace {
        Namespace::new(NAMESPACE).unwrap()
    }

    fn repository(client: Arc<RegistryClient>, mode: ReplicationMode, prune: bool) -> Repository {
        repository_with_replication(
            REPO,
            vec![ReplicationDownstream {
                mode,
                prune,
                ..ReplicationDownstream::new(DOWNSTREAM.to_string(), client, 4)
            }],
        )
    }

    /// A path-prefixed downstream: content `nginx/app` maps to remote
    /// `mirror/app`.
    fn prefixed_repository(
        client: Arc<RegistryClient>,
        mode: ReplicationMode,
        prune: bool,
    ) -> Repository {
        repository_with_replication(
            REPO,
            vec![ReplicationDownstream {
                local_namespace: Some(Namespace::new("nginx").unwrap()),
                target_namespace: Some(Namespace::new("mirror").unwrap()),
                mode,
                prune,
                ..ReplicationDownstream::new(DOWNSTREAM.to_string(), client, 4)
            }],
        )
    }

    fn resolver_for(repo: Repository) -> Arc<RepositoryResolver> {
        let mut repositories = HashMap::new();
        repositories.insert(REPO.to_string(), repo);
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    #[tokio::test]
    async fn enqueues_push_for_tag_missing_on_downstream() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert_eq!(sink.lock().unwrap().len(), 1);
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::EnqueueReplicationPush { downstream, tag, digest, .. }
                if downstream == DOWNSTREAM && tag == "v1" && *digest == manifest
        ));
    }

    #[tokio::test]
    async fn enqueues_push_for_prefixed_downstream_at_mapped_path() {
        // The probe must hit `mirror/app`, not the local `nginx/app`; the
        // `.expect(1)` asserts it on `MockServer` drop.
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let content = Namespace::new("nginx/app").unwrap();
        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        seed_links(
            &metadata_store,
            &content,
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&content, &sink).await.unwrap();

        assert_eq!(sink.lock().unwrap().len(), 1);
        assert!(matches!(
            &sink.lock().unwrap()[0],
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
        // The list and delete must hit `mirror/app`, not `nginx/app`; the
        // `.expect(1)` on each asserts it.
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let content = Namespace::new("nginx/app").unwrap();
        // `v1` converges; `stray` is downstream-only and must be pruned.
        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        seed_links(
            &metadata_store,
            &content,
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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
        let job_store = Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "scrub-test",
            ClaimMode::Atomic,
        ));

        let checker = ReplicationChecker::new(metadata_store.clone(), resolver.clone());

        let mut executor: Box<dyn ActionSink> = Box::new(Executor::new(
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

        // Drop explicitly so the `.expect(1)` counts are verified here.
        drop(mock_server);
    }

    #[tokio::test]
    async fn transient_head_failure_skips_tag_without_enqueuing() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"manifest-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest)],
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

        // Metrics are process-global and shared across tests: assert a delta.
        let skipped_before = crate::metrics_provider::metrics_provider()
            .replication_reconcile_total
            .with_label_values(&["skipped"])
            .get();

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "a transient downstream HEAD failure must not enqueue a push, got {} action(s)",
            sink.lock().unwrap().len()
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

    /// An `ActionSink` that fails the first `fail_first` applies.
    struct FlakySink {
        attempted: std::sync::Mutex<Vec<Action>>,
        fail_first: usize,
    }

    #[async_trait]
    impl ActionSink for FlakySink {
        async fn apply(&self, action: Action) -> Result<(), Error> {
            let mut attempted = self.attempted.lock().unwrap();
            attempted.push(action);
            if attempted.len() <= self.fail_first {
                return Err(Error::Initialization("simulated enqueue failure".into()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn enqueue_failure_does_not_abort_remaining_tags() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        for tag in ["v1", "v2"] {
            let body = format!("manifest-{tag}");
            let manifest = put_blob_direct(&store, body.as_bytes()).await;
            seed_links(
                &metadata_store,
                &namespace(),
                &[(LinkKind::Tag(Tag::new(tag).unwrap()), manifest)],
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

        let sink = FlakySink {
            attempted: std::sync::Mutex::new(Vec::new()),
            fail_first: 1,
        };
        checker.check(&namespace(), &sink).await.unwrap();

        let attempted = sink.attempted.lock().unwrap();
        let attempted: Vec<&str> = attempted
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
        let FsTestStack {
            dir: _dir,
            metadata_store,
            ..
        } = fs_test_stack();
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

        let sink = FlakySink {
            attempted: std::sync::Mutex::new(Vec::new()),
            fail_first: 1,
        };
        checker.check(&namespace(), &sink).await.unwrap();

        let deleted = sink.attempted.lock().unwrap();
        let deleted: Vec<&str> = deleted
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
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "converged tag must not enqueue a push"
        );
    }

    #[tokio::test]
    async fn enqueues_push_when_downstream_digest_diverges() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"new-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "diverging tag must enqueue a push"
        );
    }

    #[tokio::test]
    async fn enqueues_pushes_for_every_diverging_tag() {
        // The probe phase fans out, so no tag may be dropped by the concurrency.
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"new-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[
                (LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone()),
                (LinkKind::Tag(Tag::new("v2").unwrap()), manifest.clone()),
                (LinkKind::Tag(Tag::new("v3").unwrap()), manifest.clone()),
            ],
        )
        .await
        .unwrap();

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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        let sink = sink.into_inner().unwrap();
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
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "only the downstream-only tag should produce an action"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::EnqueueReplicationDelete { downstream, namespace, tag }
                if downstream == DOWNSTREAM && namespace == NAMESPACE && tag == "stray"
        ));
    }

    #[tokio::test]
    async fn prune_rechecks_live_state_and_spares_a_tag_pushed_after_the_walk() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        // `fresh` was pushed locally after the reconcile walk read the tags.
        let manifest = put_blob_direct(&store, b"fresh-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("fresh").unwrap()), manifest.clone())],
        )
        .await
        .unwrap();

        // The downstream carries the fresh tag plus a genuinely-gone one.
        Mock::given(method("GET"))
            .and(path(format!("/v2/{NAMESPACE}/tags/list")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "tags": ["fresh", "stray"] })),
            )
            .mount(&mock_server)
            .await;

        let repo = repository(
            downstream_client(&mock_server.uri()),
            ReplicationMode::EventReconcile,
            true,
        );
        let downstream = &repo.replication[0];
        let checker = ReplicationChecker::new(
            metadata_store.clone(),
            resolver_for(repository(
                downstream_client(&mock_server.uri()),
                ReplicationMode::EventReconcile,
                true,
            )),
        );

        // The empty list stands in for a walk that ran before `fresh` existed.
        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker
            .reconcile_downstream(downstream, &namespace(), &[], &sink)
            .await;

        let actions = sink.lock().unwrap();
        assert_eq!(
            actions.len(),
            1,
            "the live re-check must spare 'fresh' and delete only 'stray'"
        );
        assert!(matches!(
            &actions[0],
            Action::EnqueueReplicationDelete { tag, .. } if tag == "stray"
        ));
    }

    #[tokio::test]
    async fn no_delete_when_prune_disabled() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"converged-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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
        // `.expect(0)` fails if the checker enumerates downstream tags at all.
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "prune disabled: a downstream-only tag must not be deleted"
        );
        drop(mock_server);
    }

    /// Fails every listing of one tag's entry directory, so resolving that tag
    /// errors rather than reading as absent.
    struct FailTagResolution {
        dir: String,
    }

    #[async_trait]
    impl StoreHook for FailTagResolution {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            match op {
                StoreOp::List { prefix } if prefix == self.dir => {
                    Err(StorageError::Backend("store is down".to_string()))
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn unreadable_local_tag_is_skipped_but_never_pruned() {
        let FsTestStack {
            dir: _dir, store, ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let tag = Tag::new("broken").unwrap();
        let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
            store,
            FailTagResolution {
                dir: namespace().tag_entry_dir(&tag),
            },
        ));
        let metadata_store = metadata_store_over(hooked);

        // No HEAD mock: the push loop must never probe an unreadable tag.
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "an unreadable local tag must be skipped for push AND protected from prune, got {} action(s)",
            sink.lock().unwrap().len()
        );
        drop(mock_server);
    }

    #[tokio::test]
    async fn skips_event_only_downstream() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();

        let manifest = put_blob_direct(&store, b"event-only-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert!(
            sink.lock().unwrap().is_empty(),
            "event-only downstream must be skipped"
        );
    }

    #[tokio::test]
    async fn enqueues_push_for_reconcile_only_downstream() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            ..
        } = fs_test_stack();
        let mock_server = MockServer::start().await;

        let manifest = put_blob_direct(&store, b"reconcile-only-bytes").await;
        seed_links(
            &metadata_store,
            &namespace(),
            &[(LinkKind::Tag(Tag::new("v1").unwrap()), manifest.clone())],
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

        let sink: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &sink).await.unwrap();

        assert_eq!(
            sink.lock().unwrap().len(),
            1,
            "reconcile-only downstream must reconcile a missing tag"
        );
        assert!(matches!(
            &sink.lock().unwrap()[0],
            Action::EnqueueReplicationPush { downstream, tag, digest, .. }
                if downstream == DOWNSTREAM && tag == "v1" && *digest == manifest
        ));
    }

    /// Mounts a downstream missing tag `v1` and both blobs, expecting the full
    /// blob-upload sequence and exactly one tagged manifest PUT.
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

    /// Full push chain against a wiremock downstream, including `lock_key`
    /// coalescing of a duplicate enqueue.
    #[tokio::test]
    async fn scrub_replicate_enqueues_then_drains_and_converges() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
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

        let job_store = Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "scrub-test",
            ClaimMode::Atomic,
        ));

        let checker = ReplicationChecker::new(metadata_store.clone(), resolver.clone());

        let captured: std::sync::Mutex<Vec<Action>> = std::sync::Mutex::new(Vec::new());
        checker.check(&namespace(), &captured).await.unwrap();
        let captured = captured.into_inner().unwrap();
        assert_eq!(captured.len(), 1, "out-of-sync tag must emit one action");
        assert!(matches!(
            &captured[0],
            Action::EnqueueReplicationPush { downstream, namespace, tag, digest }
                if downstream == DOWNSTREAM
                    && namespace == NAMESPACE
                    && tag == "v1"
                    && *digest == manifest_digest
        ));

        let mut executor: Box<dyn ActionSink> = Box::new(Executor::new(
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

        // Drop explicitly so a `.expect(...)` mismatch surfaces here.
        drop(mock_server);
    }

    /// Full delete chain: a downstream-only tag is enqueued and the drain
    /// issues exactly one downstream DELETE.
    #[tokio::test]
    async fn scrub_replicate_deletes_downstream_only_tag() {
        let FsTestStack {
            dir: _dir,
            store,
            metadata_store,
            blob_store,
        } = fs_test_stack();
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
        let job_store = Arc::new(JobStore::new(
            metadata_store.object_store().clone(),
            "scrub-test",
            ClaimMode::Atomic,
        ));

        let checker = ReplicationChecker::new(metadata_store.clone(), resolver.clone());

        let mut executor: Box<dyn ActionSink> = Box::new(Executor::new(
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
