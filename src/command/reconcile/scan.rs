//! `angos reconcile scan`: enqueues a scan job for every image manifest in a
//! `scan = true` repository that has no report yet, or for every one with
//! `--force`. The running server or a worker drains the jobs.

use std::{pin::pin, sync::Arc};

use argh::FromArgs;
use async_trait::async_trait;
use futures_util::StreamExt;
use tracing::info;

use angos_oci::Namespace;

use crate::{
    command::{
        bootstrap,
        maintenance::{
            Error,
            action::Action,
            check::{self, NamespaceChecker},
            executor::{ActionSink, DryRunSink, Executor, run_job_store},
        },
    },
    configuration::Configuration,
    registry::{
        blob_store::BlobStore, manifest::read_manifest, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    scan::{ScanImagePayload, already_reported, is_scan_subject},
};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "scan",
    description = "Enqueue scans for images without a report, or for every image with --force"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(switch)]
    /// scan every image again, reports or not
    pub force: bool,
}

/// Enqueues one scan per unreported image manifest of a `scan = true`
/// repository; `force` drops the report check.
pub struct ScanChecker {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub resolver: Arc<RepositoryResolver>,
    pub force: bool,
}

#[async_trait]
impl NamespaceChecker for ScanChecker {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
        if !self.resolver.resolve(namespace).is_some_and(|r| r.scan) {
            return Ok(());
        }
        let mut revisions = pin!(self.metadata_store.stream_revisions(namespace));
        while let Some(digest) = revisions.next().await {
            let digest = digest?;
            let Some(manifest) = read_manifest(&self.blob_store, &digest).await? else {
                continue;
            };
            if !is_scan_subject(&manifest) {
                continue;
            }
            if !self.force && already_reported(&self.metadata_store, namespace, &digest).await? {
                continue;
            }
            sink.apply(Action::EnqueueScan(ScanImagePayload {
                namespace: namespace.clone(),
                digest,
                force: self.force,
            }))
            .await?;
        }
        Ok(())
    }
}

pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let bootstrap::MaintenanceContext {
        blob_store,
        metadata_store,
        repositories,
    } = bootstrap::maintenance_context(config).await?;
    let checker = ScanChecker {
        blob_store: blob_store.clone(),
        metadata_store: metadata_store.clone(),
        resolver: repositories,
        force: options.force,
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
    check::check_namespaces(&metadata_store, &checker, sink.as_ref(), 1).await?;
    info!("Scan reconciliation complete; the server or a worker drains the enqueued jobs");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use angos_oci::Namespace;

    use super::ScanChecker;
    use crate::{
        command::maintenance::{action::Action, check::NamespaceChecker},
        registry::{
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
                Action::EnqueueScan(scan) => Some((scan.digest.to_string(), scan.force)),
                _ => None,
            })
            .collect()
    }

    /// A scanning repository's image is enqueued; the same walk on a
    /// repository without the flag enqueues nothing.
    #[tokio::test]
    async fn reconcile_enqueues_images_of_scanning_repositories_only() {
        let stack = fs_test_stack();
        let namespace = Namespace::new("apps/web").unwrap();
        let (image, _, _) = seed_manifest(&stack.store, &stack.metadata_store, &namespace).await;
        // The walk is over revision records, which the seed helper leaves to
        // the push path.
        seed_links(
            &stack.metadata_store,
            &namespace,
            &[(LinkKind::Digest(image.clone()), image.clone())],
        )
        .await
        .unwrap();

        let mut repository = repository_with_replication("apps", Vec::new());
        repository.scan = true;
        let checker = ScanChecker {
            blob_store: stack.blob_store.clone(),
            metadata_store: stack.metadata_store.clone(),
            resolver: single_repo_resolver("apps", repository),
            force: false,
        };
        let sink = Mutex::new(Vec::new());
        checker.check(&namespace, &sink).await.unwrap();
        assert_eq!(enqueued(&sink), vec![(image.to_string(), false)]);

        let checker = ScanChecker {
            blob_store: stack.blob_store.clone(),
            metadata_store: stack.metadata_store.clone(),
            resolver: single_repo_resolver("apps", repository_with_replication("apps", Vec::new())),
            force: true,
        };
        let sink = Mutex::new(Vec::new());
        checker.check(&namespace, &sink).await.unwrap();
        assert!(enqueued(&sink).is_empty(), "no scan = true, no scan");
    }
}
