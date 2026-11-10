//! [`ReplicationChecker`]: the reconciliation pass. It enqueues rather than
//! pushes inline, so reconcile-discovered divergences get the same
//! retry/backoff/coalescing as the event path.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};
use tracing::{debug, warn};

use angos_oci::request::{HeadManifestRequest, ListTagsRequest};
use angos_oci::response::ManifestHeadResponse;
use angos_oci::{Digest, Namespace, Reference, Tag, manifest_accept_types};
use angos_oci_client::Error as ClientError;

use crate::command::maintenance::Error;
use crate::command::maintenance::action::{Action, JobAction};
use crate::command::maintenance::check::NamespaceChecker;
use crate::command::maintenance::executor::ActionSink;
use crate::registry::Error as RegistryError;
use crate::registry::metadata_store::{LinkKind, MetadataStore};
use crate::registry::repository_resolver::RepositoryResolver;
use crate::replication::{ReplicationDownstream, record_reconcile_outcome};

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

    /// A push for every diverging or downstream-missing tag, and for a
    /// `prune = true` downstream a delete for every downstream-only tag.
    async fn reconcile_downstream(
        &self,
        downstream: &ReplicationDownstream,
        namespace: &Namespace,
        local_tags: &[(Tag, Digest)],
        sink: &dyn ActionSink,
    ) {
        // `Err` is unreachable for a routed namespace; warn-and-skip is defensive.
        let remote = match downstream.remote(namespace) {
            Ok(remote) => remote,
            Err(e) => {
                warn!(
                    "Invalid downstream namespace on '{}' for '{namespace}'; skipping: {e}",
                    downstream.name
                );
                return;
            }
        };
        reconcile_push_step(downstream, &remote, namespace, local_tags, sink).await;

        // Prune is opt-in and one-way-mirror-only: the stamped `source_ts` LWW
        // only saves a future-dated downstream tag, so absence-driven deletion
        // stays unsafe for active-active peers.
        if !downstream.prune {
            return;
        }

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
            // A tag pushed locally after the walk is absent from it yet live,
            // so re-read it; a read error counts as present, so uncertainty
            // never deletes.
            let live = self
                .metadata_store
                .read_link(namespace, &LinkKind::Tag(tag.clone()))
                .await;
            if !matches!(live, Err(RegistryError::NotFound)) {
                continue;
            }
            if let Err(e) = sink
                .apply(Action::Job(JobAction::EnqueueReplicationDelete {
                    downstream: downstream.name.clone(),
                    namespace: namespace.clone(),
                    tag: tag.clone(),
                }))
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
    remote: &Namespace,
    namespace: &Namespace,
    local_tags: &[(Tag, Digest)],
    sink: &dyn ActionSink,
) {
    enum Probe {
        Push { tag: Tag, digest: Digest },
        Converged,
        Skipped,
    }

    let probes: Vec<Probe> = stream::iter(local_tags.to_vec())
        .map(|(tag, local)| async move {
            // Only a 404 means absence; any other HEAD failure skips the tag
            // rather than enqueuing a spurious push.
            match downstream
                .registry_client
                .head_manifest(HeadManifestRequest {
                    namespace: remote.clone(),
                    reference: Reference::Tag(tag.clone()),
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
        .buffer_unordered(downstream.max_concurrent_pushes.get())
        .collect()
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
                    .apply(Action::Job(JobAction::EnqueueReplicationPush {
                        downstream: downstream.name.clone(),
                        namespace: namespace.clone(),
                        tag: tag.clone(),
                        digest,
                    }))
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
            .filter(|d| {
                d.mode.participates_in_reconcile() && d.matches_namespace(namespace.as_ref())
            })
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
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use angos_oci::header::DOCKER_CONTENT_DIGEST;
    use angos_oci::{Digest, Namespace, Tag};
    use angos_storage::test_util::{HookedStore, StoreHook, StoreOp};
    use angos_storage::{Error as StorageError, ObjectStore};

    use crate::command::maintenance::Error;
    use crate::command::maintenance::action::{Action, JobAction};
    use crate::command::maintenance::check::NamespaceChecker;
    use crate::command::maintenance::executor::{ActionSink, Executor};
    use crate::command::reconcile::replication::checker::ReplicationChecker;
    use crate::jobs::Queue;
    use crate::jobs::runner::execute_one;
    use crate::jobs::store::{ClaimMode, JobStore};
    use crate::metrics_provider::metrics_provider;
    use crate::registry::Repository;
    use crate::registry::blob_store::BlobStore;
    use crate::registry::keys::NamespaceExt;
    use crate::registry::metadata_store::{LinkKind, MetadataStore};
    use crate::registry::repository_resolver::RepositoryResolver;
    use crate::registry::test_utils::{
        FsTestStack, downstream_client, fs_test_stack, metadata_store_over, put_blob_direct,
        repository_with_replication, seed_links, seed_manifest, test_downstream,
    };
    use crate::replication::{ReplicationDownstream, ReplicationJobHandler, ReplicationMode};

    const NAMESPACE: &str = "nginx";
    const REPO: &str = "nginx";
    const DOWNSTREAM: &str = "eu-region";

    fn namespace() -> Namespace {
        Namespace::new(NAMESPACE).unwrap()
    }

    /// A repository replicating to the mock at `uri`; `prefixed` maps content
    /// `nginx/app` to remote `mirror/app`.
    fn resolver(
        uri: &str,
        mode: ReplicationMode,
        prune: bool,
        prefixed: bool,
    ) -> Arc<RepositoryResolver> {
        let mut downstream = ReplicationDownstream {
            mode,
            prune,
            ..test_downstream(DOWNSTREAM.to_string(), downstream_client(uri))
        };
        if prefixed {
            downstream.local_namespace = Some(Namespace::new("nginx").unwrap());
            downstream.target_namespace = Some(Namespace::new("mirror").unwrap());
        }
        let repositories: HashMap<String, Repository> = HashMap::from([(
            REPO.to_string(),
            repository_with_replication(REPO, vec![downstream]),
        )]);
        Arc::new(RepositoryResolver::new(Arc::new(repositories)).unwrap())
    }

    fn checker(
        metadata_store: &Arc<MetadataStore>,
        server: &MockServer,
        prune: bool,
    ) -> ReplicationChecker {
        ReplicationChecker::new(
            metadata_store.clone(),
            resolver(&server.uri(), ReplicationMode::EventReconcile, prune, false),
        )
    }

    async fn seed_tag(stack: &FsTestStack, namespace: &Namespace, tag: &str) -> Digest {
        let manifest =
            put_blob_direct(&stack.blob_store, format!("manifest-{tag}").as_bytes()).await;
        seed_links(
            &stack.metadata_store,
            namespace,
            &[(LinkKind::Tag(Tag::new(tag).unwrap()), manifest.clone())],
        )
        .await
        .unwrap();
        manifest
    }

    /// The downstream answers a manifest HEAD with `digest`, or 404 without one.
    async fn mount_head(server: &MockServer, remote: &str, tag: &str, digest: Option<&Digest>) {
        let response = match digest {
            Some(digest) => ResponseTemplate::new(200)
                .insert_header(DOCKER_CONTENT_DIGEST, digest.to_string().as_str())
                .insert_header("Content-Length", "15"),
            None => ResponseTemplate::new(404),
        };
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{remote}/manifests/{tag}")))
            .respond_with(response)
            .mount(server)
            .await;
    }

    fn tag_list(remote: &str, tags: &[&str]) -> Mock {
        Mock::given(method("GET"))
            .and(path(format!("/v2/{remote}/tags/list")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "tags": tags })))
    }

    async fn actions(checker: &ReplicationChecker, namespace: &Namespace) -> Vec<Action> {
        let sink = Mutex::new(Vec::new());
        checker.check(namespace, &sink).await.unwrap();
        sink.into_inner().unwrap()
    }

    fn job_store(metadata_store: &Arc<MetadataStore>) -> Arc<JobStore> {
        Arc::new(JobStore::new(
            metadata_store,
            "scrub-test",
            ClaimMode::Atomic,
        ))
    }

    /// Enqueues through a real executor, as `angos reconcile` does.
    async fn check_into(
        checker: &ReplicationChecker,
        namespace: &Namespace,
        blob_store: &Arc<BlobStore>,
        metadata_store: &Arc<MetadataStore>,
        job_store: &Arc<JobStore>,
    ) -> u64 {
        let executor = Executor::new(
            blob_store.clone(),
            metadata_store.clone(),
            job_store.clone(),
            None,
        );
        checker.check(namespace, &executor).await.unwrap();
        job_store
            .count_pending(Queue::Replication, 0)
            .await
            .unwrap()
    }

    async fn drain(job_store: &JobStore, handler: &ReplicationJobHandler) -> u64 {
        let mut drained = 0;
        while let Some(claimed) = job_store
            .claim_one(Queue::Replication)
            .await
            .unwrap()
            .claimed
        {
            execute_one(job_store, handler, claimed).await;
            drained += 1;
        }
        drained
    }

    fn is_push(action: &Action, tag: &str, digest: &Digest) -> bool {
        matches!(
            action,
            Action::Job(JobAction::EnqueueReplicationPush { downstream, tag: t, digest: d, .. })
                if downstream == DOWNSTREAM && t == tag && d == digest
        )
    }

    /// Event-only downstreams are left to the event path; the other modes get
    /// a push for a tag missing downstream.
    #[tokio::test]
    async fn the_mode_decides_whether_a_missing_tag_is_pushed() {
        let stack = fs_test_stack();
        let manifest = seed_tag(&stack, &namespace(), "v1").await;
        let server = MockServer::start().await;
        mount_head(&server, NAMESPACE, "v1", None).await;

        for (mode, pushed) in [
            (ReplicationMode::EventReconcile, true),
            (ReplicationMode::ReconcileOnly, true),
            (ReplicationMode::EventOnly, false),
        ] {
            let checker = ReplicationChecker::new(
                stack.metadata_store.clone(),
                resolver(&server.uri(), mode, false, false),
            );
            let actions = actions(&checker, &namespace()).await;
            if pushed {
                assert!(
                    actions.len() == 1 && is_push(&actions[0], "v1", &manifest),
                    "{mode:?}: {} action(s)",
                    actions.len()
                );
            } else {
                assert!(actions.is_empty(), "{mode:?}: {} action(s)", actions.len());
            }
        }
    }

    /// A converged tag is left alone, a diverging one pushed.
    #[tokio::test]
    async fn the_downstream_digest_decides_the_push() {
        let stack = fs_test_stack();
        let converged = seed_tag(&stack, &namespace(), "converged").await;
        let diverged = seed_tag(&stack, &namespace(), "diverged").await;
        let server = MockServer::start().await;
        mount_head(&server, NAMESPACE, "converged", Some(&converged)).await;
        let stale =
            Digest::sha256("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap();
        mount_head(&server, NAMESPACE, "diverged", Some(&stale)).await;

        let actions = actions(
            &checker(&stack.metadata_store, &server, false),
            &namespace(),
        )
        .await;
        assert!(
            actions.len() == 1 && is_push(&actions[0], "diverged", &diverged),
            "{} action(s)",
            actions.len()
        );
    }

    #[tokio::test]
    async fn enqueues_push_for_prefixed_downstream_at_mapped_path() {
        let stack = fs_test_stack();
        let content = Namespace::new("nginx/app").unwrap();
        let manifest = seed_tag(&stack, &content, "v1").await;
        let server = MockServer::start().await;
        // The probe must hit `mirror/app`, not the local `nginx/app`.
        mount_head(&server, "mirror/app", "v1", None).await;

        let checker = ReplicationChecker::new(
            stack.metadata_store.clone(),
            resolver(&server.uri(), ReplicationMode::EventReconcile, false, true),
        );
        let actions = actions(&checker, &content).await;
        assert!(
            actions.len() == 1 && is_push(&actions[0], "v1", &manifest),
            "{} action(s)",
            actions.len()
        );
        assert!(matches!(
            &actions[0],
            Action::Job(JobAction::EnqueueReplicationPush { namespace, .. }) if namespace == "nginx/app"
        ));
    }

    #[tokio::test]
    async fn prunes_prefixed_downstream_at_mapped_path() {
        let stack = fs_test_stack();
        let content = Namespace::new("nginx/app").unwrap();
        // `v1` converges; `stray` is downstream-only and must be pruned.
        let manifest = seed_tag(&stack, &content, "v1").await;
        let server = MockServer::start().await;
        mount_head(&server, "mirror/app", "v1", Some(&manifest)).await;
        tag_list("mirror/app", &["v1", "stray"])
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v2/mirror/app/manifests/stray"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;

        let resolver = resolver(&server.uri(), ReplicationMode::EventReconcile, true, true);
        let checker = ReplicationChecker::new(stack.metadata_store.clone(), resolver.clone());
        let job_store = job_store(&stack.metadata_store);

        let pending = check_into(
            &checker,
            &content,
            &stack.blob_store,
            &stack.metadata_store,
            &job_store,
        )
        .await;
        assert_eq!(
            pending, 1,
            "the downstream-only tag must enqueue exactly one delete job"
        );
        let handler = ReplicationJobHandler::new(
            resolver,
            stack.blob_store.clone(),
            stack.metadata_store.clone(),
        );
        assert_eq!(drain(&job_store, &handler).await, 1);

        // Drop explicitly so the `.expect(1)` counts are verified here.
        drop(server);
    }

    #[tokio::test]
    async fn transient_head_failure_skips_tag_without_enqueuing() {
        let stack = fs_test_stack();
        seed_tag(&stack, &namespace(), "v1").await;
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        // Metrics are process-global and shared across tests: assert a delta.
        let skipped = || {
            metrics_provider()
                .replication_reconcile_total
                .with_label_values(&["skipped"])
                .get()
        };
        let skipped_before = skipped();

        let actions = actions(
            &checker(&stack.metadata_store, &server, false),
            &namespace(),
        )
        .await;
        assert!(actions.is_empty(), "{} action(s)", actions.len());
        assert_eq!(
            skipped() - skipped_before,
            1,
            "a skipped tag must be counted so a persistently failing downstream is visible"
        );
    }

    /// The probe phase fans out, so no tag may be dropped by the concurrency.
    #[tokio::test]
    async fn enqueues_pushes_for_every_diverging_tag() {
        let stack = fs_test_stack();
        for tag in ["v1", "v2", "v3"] {
            seed_tag(&stack, &namespace(), tag).await;
        }
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let actions = actions(
            &checker(&stack.metadata_store, &server, false),
            &namespace(),
        )
        .await;
        let mut tags: Vec<&str> = actions
            .iter()
            .filter_map(|action| match action {
                Action::Job(JobAction::EnqueueReplicationPush { tag, .. }) => Some(tag.as_ref()),
                _ => None,
            })
            .collect();
        tags.sort_unstable();
        assert_eq!(tags, ["v1", "v2", "v3"]);
    }

    /// An `ActionSink` that fails its first apply.
    #[derive(Default)]
    struct FlakySink {
        attempted: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ActionSink for FlakySink {
        async fn apply(&self, action: Action) -> Result<(), Error> {
            let mut attempted = self.attempted.lock().unwrap();
            if let Action::Job(
                JobAction::EnqueueReplicationPush { tag, .. }
                | JobAction::EnqueueReplicationDelete { tag, .. },
            ) = action
            {
                attempted.push(tag.to_string());
            }
            if attempted.len() == 1 {
                return Err(Error::Initialization("simulated enqueue failure".into()));
            }
            Ok(())
        }
    }

    /// One failed enqueue skips neither the remaining pushes nor the remaining
    /// deletes.
    #[tokio::test]
    async fn an_enqueue_failure_does_not_abort_the_pass() {
        let stack = fs_test_stack();
        for tag in ["v1", "v2"] {
            seed_tag(&stack, &namespace(), tag).await;
        }
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        tag_list(NAMESPACE, &["v1", "v2", "stray1", "stray2"])
            .mount(&server)
            .await;

        let sink = FlakySink::default();
        checker(&stack.metadata_store, &server, true)
            .check(&namespace(), &sink)
            .await
            .unwrap();

        let mut attempted = sink.attempted.into_inner().unwrap();
        attempted.sort_unstable();
        assert_eq!(attempted, ["stray1", "stray2", "v1", "v2"]);
    }

    #[tokio::test]
    async fn enqueues_delete_for_downstream_only_tag() {
        let stack = fs_test_stack();
        let manifest = seed_tag(&stack, &namespace(), "v1").await;
        let server = MockServer::start().await;
        mount_head(&server, NAMESPACE, "v1", Some(&manifest)).await;
        tag_list(NAMESPACE, &["v1", "stray"]).mount(&server).await;

        let actions = actions(&checker(&stack.metadata_store, &server, true), &namespace()).await;
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::Job(JobAction::EnqueueReplicationDelete { downstream, namespace, tag })
                if downstream == DOWNSTREAM && namespace == NAMESPACE && tag == "stray"
        ));
    }

    #[tokio::test]
    async fn prune_rechecks_live_state_and_spares_a_tag_pushed_after_the_walk() {
        let stack = fs_test_stack();
        // `fresh` was pushed locally after the reconcile walk read the tags.
        seed_tag(&stack, &namespace(), "fresh").await;
        let server = MockServer::start().await;
        tag_list(NAMESPACE, &["fresh", "stray"])
            .mount(&server)
            .await;

        let resolver = resolver(&server.uri(), ReplicationMode::EventReconcile, true, false);
        let downstream = &resolver.resolve(&namespace()).unwrap().replication[0];
        let checker = ReplicationChecker::new(stack.metadata_store.clone(), resolver.clone());

        // The empty list stands in for a walk that ran before `fresh` existed.
        let sink = Mutex::new(Vec::new());
        checker
            .reconcile_downstream(downstream, &namespace(), &[], &sink)
            .await;

        let actions = sink.into_inner().unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::Job(JobAction::EnqueueReplicationDelete { tag, .. }) if tag == "stray"
        ));
    }

    #[tokio::test]
    async fn no_delete_when_prune_disabled() {
        let stack = fs_test_stack();
        let manifest = seed_tag(&stack, &namespace(), "v1").await;
        let server = MockServer::start().await;
        mount_head(&server, NAMESPACE, "v1", Some(&manifest)).await;
        // `.expect(0)` fails if the checker enumerates downstream tags at all.
        tag_list(NAMESPACE, &["v1", "stray"])
            .expect(0)
            .mount(&server)
            .await;

        let actions = actions(
            &checker(&stack.metadata_store, &server, false),
            &namespace(),
        )
        .await;
        assert!(actions.is_empty(), "{} action(s)", actions.len());
        drop(server);
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
        let stack = fs_test_stack();
        let hooked: Arc<dyn ObjectStore> = Arc::new(HookedStore::new(
            stack.metadata_store.object_store().clone(),
            FailTagResolution {
                dir: namespace().tag_entry_dir(&Tag::new("broken").unwrap()),
            },
        ));
        let metadata_store = metadata_store_over(hooked);
        let server = MockServer::start().await;
        // No HEAD mock: the push loop must never probe an unreadable tag.
        tag_list(NAMESPACE, &["broken"])
            .expect(1)
            .mount(&server)
            .await;

        let actions = actions(&checker(&metadata_store, &server, true), &namespace()).await;
        assert!(
            actions.is_empty(),
            "skipped for push and protected from prune: {} action(s)",
            actions.len()
        );
        drop(server);
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
        let stack = fs_test_stack();
        let server = MockServer::start().await;
        let (manifest_digest, config_digest, layer_digest) =
            seed_manifest(&stack.blob_store, &stack.metadata_store, &namespace()).await;
        mount_out_of_sync_downstream(&server, &manifest_digest, &config_digest, &layer_digest)
            .await;

        let resolver = resolver(&server.uri(), ReplicationMode::EventReconcile, false, false);
        let checker = ReplicationChecker::new(stack.metadata_store.clone(), resolver.clone());
        let job_store = job_store(&stack.metadata_store);

        for pass in 0..2 {
            let pending = check_into(
                &checker,
                &namespace(),
                &stack.blob_store,
                &stack.metadata_store,
                &job_store,
            )
            .await;
            assert_eq!(
                pending, 1,
                "pass {pass}: a second pass coalesces on lock_key"
            );
        }

        let handler = ReplicationJobHandler::new(
            resolver,
            stack.blob_store.clone(),
            stack.metadata_store.clone(),
        );
        assert_eq!(drain(&job_store, &handler).await, 1);
        assert_eq!(
            job_store
                .count_pending(Queue::Replication, 0)
                .await
                .unwrap(),
            0
        );

        // Drop explicitly so a `.expect(...)` mismatch surfaces here.
        drop(server);
    }
}
