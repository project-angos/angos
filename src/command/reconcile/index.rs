//! `angos reconcile index`: enqueues an index job for every tar layer of an
//! image its repository's index policy applies to that has no listing yet,
//! or for every such layer with `--force`, and reclaims the listings of every
//! other layer. The running server or a worker drains the jobs. Any image indexes
//! itself the first time its filesystem is opened, so this is for having the
//! listings ready ahead of that, for walking layers again after a change to
//! what a listing holds, and for dropping what on-demand opens left behind
//! in repositories that do not index.

use std::{
    collections::HashSet,
    pin::pin,
    sync::{Arc, Mutex, PoisonError},
};

use argh::FromArgs;
use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::info;

use angos_oci::{Digest, Namespace};

use crate::{
    command::{
        bootstrap,
        maintenance::{
            Error,
            action::Action,
            check::{self, NamespaceChecker},
            executor::{ActionSink, DryRunSink, Executor, run_job_store},
            tags::Rankings,
            walk::for_each_key,
        },
    },
    configuration::Configuration,
    layer::{IndexLayerPayload, filesystem_layers, read_listing},
    registry::{
        blob_store::BlobStore,
        keys::{LAYERS_ROOT, parse_layer_key},
        manifest::read_manifest,
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "index",
    description = "Enqueue filesystem indexing for layers without a listing, or for every layer with --force, and reclaim the listings no indexing repository uses"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(switch)]
    /// walk every layer again, listings or not
    pub force: bool,
}

/// Enqueues one index job per unlisted tar layer of the images an index
/// policy applies to; `force` drops the listing check, `enqueue` off only
/// collects. A layer shared by several images is enqueued once per run, and
/// `seen` ends up holding every layer those images use, the ones whose
/// listing is kept.
pub struct IndexChecker {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub resolver: Arc<RepositoryResolver>,
    pub force: bool,
    pub enqueue: bool,
    pub seen: Mutex<HashSet<Digest>>,
}

#[async_trait]
impl NamespaceChecker for IndexChecker {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
        let Some(policy) = self
            .resolver
            .resolve(namespace)
            .and_then(|repository| repository.index.as_ref())
        else {
            return Ok(());
        };
        // Only the rules read the tags, so a policy without any skips them.
        let rankings = if policy.has_rules() {
            Some(Rankings::read(&self.metadata_store, namespace).await?)
        } else {
            None
        };
        let empty = Rankings {
            tags: Vec::new(),
            last_pushed: Vec::new(),
            last_pulled: Vec::new(),
        };
        let rankings = rankings.as_ref().unwrap_or(&empty);
        let mut revisions = pin!(self.metadata_store.stream_revisions(namespace));
        while let Some(digest) = revisions.next().await {
            let digest = digest?;
            let Some(manifest) = read_manifest(&self.blob_store, &digest).await? else {
                continue;
            };
            if !rankings
                .applies(policy, &self.metadata_store, namespace, &digest, 0)
                .await?
            {
                continue;
            }
            for layer in filesystem_layers(&manifest) {
                let first = self
                    .seen
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(layer.clone());
                if !first || !self.enqueue {
                    continue;
                }
                if !self.force && read_listing(&self.metadata_store, &layer).await?.is_some() {
                    continue;
                }
                sink.apply(Action::EnqueueIndex(IndexLayerPayload {
                    namespace: namespace.clone(),
                    digest: layer,
                    force: self.force,
                }))
                .await?;
            }
        }
        Ok(())
    }
}

/// Reclaims the listing of every layer outside `kept`: one an image indexed
/// on demand comes back the next time the image is opened.
async fn reclaim_listings(
    metadata_store: &Arc<MetadataStore>,
    kept: &HashSet<Digest>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let unwanted = Mutex::new(HashSet::new());
    let found = &unwanted;
    for_each_key(
        metadata_store.object_store(),
        LAYERS_ROOT,
        1,
        |key| async move {
            if let Some(digest) = parse_layer_key(&key).filter(|digest| !kept.contains(digest)) {
                found
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(digest);
            }
        },
    )
    .await?;
    for digest in unwanted
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner)
    {
        sink.apply(Action::ReclaimListing(digest)).await?;
    }
    Ok(())
}

/// Walks every namespace with `checker`, then reclaims the listings of the
/// layers it did not see under an image an index policy applies to.
async fn check_and_reclaim(
    checker: IndexChecker,
    metadata_store: &Arc<MetadataStore>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    check::check_namespaces(metadata_store, &checker, sink, 1).await?;
    let kept = checker
        .seen
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    reclaim_listings(metadata_store, &kept, sink).await
}

/// Reclaims the listings no image an index policy applies to uses,
/// enqueueing nothing: what `angos scrub` runs after its walk.
pub async fn reclaim_unused_listings(
    blob_store: Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: Arc<RepositoryResolver>,
    sink: &dyn ActionSink,
) -> Result<(), Error> {
    let checker = IndexChecker {
        blob_store,
        metadata_store: metadata_store.clone(),
        resolver,
        force: false,
        enqueue: false,
        seen: Mutex::new(HashSet::new()),
    };
    check_and_reclaim(checker, metadata_store, sink).await
}

pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let bootstrap::MaintenanceContext {
        blob_store,
        metadata_store,
        repositories,
    } = bootstrap::maintenance_context(config).await?;
    let checker = IndexChecker {
        blob_store: blob_store.clone(),
        metadata_store: metadata_store.clone(),
        resolver: repositories,
        force: options.force,
        enqueue: true,
        seen: Mutex::new(HashSet::new()),
    };
    let sink: Box<dyn ActionSink> = if options.dry_run {
        info!("Dry-run mode: no changes will be made to the storage");
        Box::new(DryRunSink)
    } else {
        Box::new(Executor::new(
            blob_store,
            metadata_store.clone(),
            run_job_store(&metadata_store, "reconcile"),
        ))
    };
    check_and_reclaim(checker, &metadata_store, sink.as_ref()).await?;
    info!("Index reconciliation complete; the server or a worker drains the enqueued jobs");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Mutex};

    use bytes::Bytes;

    use angos_oci::{Digest, Namespace};

    use super::{IndexChecker, reclaim_listings};
    use crate::{
        command::maintenance::{action::Action, check::NamespaceChecker},
        layer::IndexAction,
        policy::{ImagePolicy, PolicyConfig},
        registry::{
            keys::DigestKeys,
            metadata_store::LinkKind,
            test_utils::{
                fs_test_stack, repository_with_replication, seed_links, seed_manifest,
                single_repo_resolver,
            },
        },
    };

    fn enqueued(actions: &Mutex<Vec<Action>>) -> Vec<(String, bool)> {
        actions
            .lock()
            .unwrap()
            .iter()
            .filter_map(|action| match action {
                Action::EnqueueIndex(index) => Some((index.digest.to_string(), index.force)),
                _ => None,
            })
            .collect()
    }

    /// An indexing repository's unlisted tar layer is enqueued, a listed one
    /// only with `--force`, and a repository without the flag enqueues
    /// nothing.
    #[tokio::test]
    async fn reconcile_enqueues_unlisted_layers_of_indexing_repositories_only() {
        let stack = fs_test_stack();
        let namespace = Namespace::new("apps/web").unwrap();
        let (image, _, layer) =
            seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
        // The walk is over revision records, which the seed helper leaves to
        // the push path.
        seed_links(
            &stack.metadata_store,
            &namespace,
            &[(LinkKind::Digest(image.clone()), image.clone())],
        )
        .await
        .unwrap();
        let checker = |force: bool, indexes: bool| {
            let mut repository = repository_with_replication("apps", Vec::new());
            repository.index = indexes.then(|| {
                ImagePolicy::new(&PolicyConfig {
                    default: Some(IndexAction::Index),
                    rules: Vec::new(),
                })
            });
            IndexChecker {
                blob_store: stack.blob_store.clone(),
                metadata_store: stack.metadata_store.clone(),
                resolver: single_repo_resolver("apps", repository),
                force,
                enqueue: true,
                seen: Mutex::new(HashSet::new()),
            }
        };

        let sink = Mutex::new(Vec::new());
        checker(false, true).check(&namespace, &sink).await.unwrap();
        assert_eq!(enqueued(&sink), vec![(layer.to_string(), false)]);

        let sink = Mutex::new(Vec::new());
        checker(false, false)
            .check(&namespace, &sink)
            .await
            .unwrap();
        assert!(enqueued(&sink).is_empty(), "no index policy, no job");

        // Listed: only a forced run walks it again. The seeded layer holds no
        // tar, so the listing is written by hand.
        let store = stack.metadata_store.object_store();
        store
            .put(
                &layer.layer_entries_path(),
                Bytes::from_static(br#"{"compressed":false,"uncompressed_size":0,"entries":[]}"#),
            )
            .await
            .unwrap();
        let sink = Mutex::new(Vec::new());
        checker(false, true).check(&namespace, &sink).await.unwrap();
        assert!(enqueued(&sink).is_empty(), "a listed layer is left alone");
        let sink = Mutex::new(Vec::new());
        checker(true, true).check(&namespace, &sink).await.unwrap();
        assert_eq!(enqueued(&sink), vec![(layer.to_string(), true)]);
    }

    /// A listed layer no indexing repository uses is reclaimed, one they use
    /// is kept.
    #[tokio::test]
    async fn reconcile_reclaims_listings_outside_indexing_repositories() {
        let stack = fs_test_stack();
        let store = stack.metadata_store.object_store();
        let kept = Digest::sha256_of_bytes(b"kept layer");
        let stray = Digest::sha256_of_bytes(b"stray layer");
        for layer in [&kept, &stray] {
            store
                .put(&layer.layer_entries_path(), Bytes::from_static(b"{}"))
                .await
                .unwrap();
            store
                .put(&layer.layer_checkpoints_path(), Bytes::from_static(b"{}"))
                .await
                .unwrap();
        }

        let sink = Mutex::new(Vec::new());
        reclaim_listings(&stack.metadata_store, &HashSet::from([kept]), &sink)
            .await
            .unwrap();
        let reclaimed: Vec<String> = sink
            .lock()
            .unwrap()
            .iter()
            .filter_map(|action| match action {
                Action::ReclaimListing(digest) => Some(digest.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            reclaimed,
            vec![stray.to_string()],
            "the stray layer once, the kept one never"
        );
    }
}
