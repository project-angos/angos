//! `angos reconcile scan`: enqueues a scan job for every image manifest in a
//! scanning repository that has no report yet or whose newest report the
//! repository's refresh rules find due, or for every one with `--force`. The
//! running server or a worker drains the jobs. Nothing runs this pass on its
//! own: a `CronJob` or a timer schedules it, as for `prune`.

use std::{pin::pin, sync::Arc};

use argh::FromArgs;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
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
            tags::{TagWithMetadata, live_tags, rank_by},
        },
        scrub::default_concurrency,
    },
    configuration::Configuration,
    policy::{ManifestImage, RetentionPolicy},
    registry::{
        Error as RegistryError,
        blob_store::BlobStore,
        manifest::read_manifest,
        metadata_store::{LinkKind, MetadataStore},
        repository_resolver::RepositoryResolver,
    },
    scan::{ScanImagePayload, ScanReport, is_scan_subject, refresh_due, scan_reports},
};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "scan",
    description = "Enqueue scans for images without a report or due for one, or for every image with --force"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(switch)]
    /// scan every image again, reports or not
    pub force: bool,
    #[argh(option, default = "default_concurrency()")]
    /// number of namespaces checked concurrently; each adds a small fixed
    /// tag-read fan-out of its own when refresh rules apply
    pub concurrency: usize,
}

/// Enqueues one scan per image manifest of a scanning repository that has no
/// report, or whose newest report the repository's refresh rules find due;
/// `force` enqueues every image with a fresh report forced.
pub struct ScanChecker {
    pub blob_store: Arc<BlobStore>,
    pub metadata_store: Arc<MetadataStore>,
    pub resolver: Arc<RepositoryResolver>,
    pub force: bool,
}

/// The tag rankings a namespace's rules are evaluated against.
struct Rankings {
    tags: Vec<TagWithMetadata>,
    last_pushed: Vec<String>,
    last_pulled: Vec<String>,
}

#[async_trait]
impl NamespaceChecker for ScanChecker {
    async fn check(&self, namespace: &Namespace, sink: &dyn ActionSink) -> Result<(), Error> {
        let Some(scan) = self
            .resolver
            .resolve(namespace)
            .and_then(|repository| repository.scan.as_ref())
        else {
            return Ok(());
        };
        let policy = scan.refresh.as_ref().filter(|_| !self.force);
        // Taken before any report is read, so a report attached from here on
        // is created after it and the handler sees the push it came from.
        let now = Utc::now();
        // Only the rules read the tags, so a namespace without any skips them.
        let rankings = match policy {
            Some(_) => {
                let tags = live_tags(&self.metadata_store, namespace).await?;
                Some(Rankings {
                    last_pushed: rank_by(&tags, |t| t.metadata.created_at),
                    last_pulled: rank_by(&tags, |t| t.pulled_at),
                    tags,
                })
            }
            None => None,
        };

        let mut revisions = pin!(self.metadata_store.stream_revisions(namespace));
        while let Some(digest) = revisions.next().await {
            let digest = digest?;
            let Some(manifest) = read_manifest(&self.blob_store, &digest).await? else {
                continue;
            };
            if !is_scan_subject(&manifest) {
                continue;
            }
            let payload = ScanImagePayload {
                namespace: namespace.clone(),
                digest: digest.clone(),
                force: false,
                reported_before: None,
            };
            let payload = if self.force {
                Some(ScanImagePayload {
                    force: true,
                    ..payload
                })
            } else {
                let reports = scan_reports(&self.metadata_store, namespace, &digest).await?;
                match (reports.first(), policy.zip(rankings.as_ref())) {
                    (None, _) => Some(payload),
                    (Some(newest), Some((policy, rankings)))
                        if self
                            .selects(policy, namespace, &digest, newest, rankings, now)
                            .await? =>
                    {
                        Some(ScanImagePayload {
                            reported_before: Some(now),
                            ..payload
                        })
                    }
                    _ => None,
                }
            };
            if let Some(payload) = payload {
                sink.apply(Action::EnqueueScan(payload)).await?;
            }
        }
        Ok(())
    }
}

impl ScanChecker {
    /// Whether the rules find the image due: any tag pointing at it is tried
    /// in turn, and an untagged image once with `image.tag == null`, the way
    /// retention judges each.
    async fn selects(
        &self,
        policy: &RetentionPolicy,
        namespace: &Namespace,
        digest: &Digest,
        newest: &ScanReport,
        rankings: &Rankings,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        let scanned_at = newest
            .created
            .map_or(0, |created| created.timestamp().max(0));
        let image = |tag: Option<String>,
                     pushed_at: Option<DateTime<Utc>>,
                     pulled_at: Option<DateTime<Utc>>| {
            let mut image = ManifestImage::new(tag, pushed_at, pulled_at, now);
            image.scanned_at = scanned_at;
            image
        };
        let mut images: Vec<ManifestImage> = rankings
            .tags
            .iter()
            .filter(|tag| tag.metadata.target == *digest)
            .map(|tag| {
                image(
                    Some(tag.name.to_string()),
                    tag.metadata.created_at,
                    tag.pulled_at,
                )
            })
            .collect();
        if images.is_empty() {
            let revision = LinkKind::Digest(digest.clone());
            let pushed_at = match self.metadata_store.read_link(namespace, &revision).await {
                Ok(metadata) => metadata.created_at,
                Err(RegistryError::NotFound) => None,
                Err(e) => return Err(e.into()),
            };
            let pulled_at = self
                .metadata_store
                .read_access_time(namespace, &revision)
                .await?;
            images.push(image(None, pushed_at, pulled_at));
        }
        Ok(images
            .iter()
            .any(|image| refresh_due(policy, image, &rankings.last_pushed, &rankings.last_pulled)))
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
    check::check_namespaces(
        &metadata_store,
        &checker,
        sink.as_ref(),
        options.concurrency,
    )
    .await?;
    info!("Scan reconciliation complete; the server or a worker drains the enqueued jobs");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, TimeDelta, Utc};

    use angos_oci::{Digest, Namespace};

    use super::ScanChecker;
    use crate::{
        command::maintenance::{
            action::Action,
            check::{self, NamespaceChecker},
            executor::Executor,
        },
        jobs::{
            Queue,
            store::{ClaimMode, JobStore},
        },
        policy::CelRule,
        registry::{
            Repository,
            metadata_store::LinkKind,
            test_utils::{
                FsTestStack, angos_report, fs_test_stack, repository_with_replication, seed_links,
                seed_manifest, single_repo_resolver,
            },
        },
        scan::{RefreshConfig, ScanImagePayload, ScanPolicy, refresh_rules},
    };

    /// A scanning repository refreshing reports under `rules`, or never with
    /// none.
    fn scanning_repository(rules: &[&str]) -> Repository {
        let mut repository = repository_with_replication("apps", Vec::new());
        let config = RefreshConfig {
            rules: rules.iter().map(|r| CelRule::compile(r).unwrap()).collect(),
        };
        repository.scan = Some(ScanPolicy {
            refresh: refresh_rules(None, Some(&config)),
        });
        repository
    }

    /// Records a report angos attached to `image` at `created`, the way the
    /// checker reads one: as the referrer record's descriptor.
    async fn seed_report(
        stack: &FsTestStack,
        namespace: &Namespace,
        image: &Digest,
        created: DateTime<Utc>,
    ) -> Digest {
        let (_, descriptor) = angos_report(image, &created.to_rfc3339());
        stack
            .metadata_store
            .put_referrer(namespace, image, &descriptor.digest, Some(&descriptor))
            .await
            .unwrap();
        descriptor.digest
    }

    /// An image with a revision record, which the walk is over.
    async fn seed_image(stack: &FsTestStack, namespace: &Namespace) -> Digest {
        let (image, _, _) = seed_manifest(&stack.store, &stack.metadata_store, namespace).await;
        seed_links(
            &stack.metadata_store,
            namespace,
            &[(LinkKind::Digest(image.clone()), image.clone())],
        )
        .await
        .unwrap();
        image
    }

    fn build_checker(stack: &FsTestStack, repository: Repository, force: bool) -> ScanChecker {
        ScanChecker {
            blob_store: stack.blob_store.clone(),
            metadata_store: stack.metadata_store.clone(),
            resolver: single_repo_resolver("apps", repository),
            force,
        }
    }

    async fn enqueued(checker: &ScanChecker, namespace: &Namespace) -> Vec<ScanImagePayload> {
        let sink = Mutex::new(Vec::new());
        checker.check(namespace, &sink).await.unwrap();
        sink.into_inner()
            .unwrap()
            .into_iter()
            .filter_map(|action| match action {
                Action::EnqueueScan(scan) => Some(scan),
                _ => None,
            })
            .collect()
    }

    /// An image without a report is enqueued, rules or not; a repository
    /// that does not scan enqueues nothing even forced.
    #[tokio::test]
    async fn an_unreported_image_is_enqueued_and_a_non_scanning_repository_is_skipped() {
        let stack = fs_test_stack();
        let namespace = Namespace::new("apps/web").unwrap();
        let image = seed_image(&stack, &namespace).await;

        let checker = build_checker(&stack, scanning_repository(&[]), false);
        let scans = enqueued(&checker, &namespace).await;
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].digest, image);
        assert!(!scans[0].force);
        assert_eq!(scans[0].reported_before, None);

        let checker = build_checker(
            &stack,
            repository_with_replication("apps", Vec::new()),
            true,
        );
        assert!(enqueued(&checker, &namespace).await.is_empty());
    }

    /// A reported image is enqueued only when the rules find its newest
    /// report due, and then with the run's time as `reported_before`;
    /// forced, it is enqueued whatever the rules say.
    #[tokio::test]
    async fn the_rules_decide_when_a_reported_image_is_scanned_again() {
        let stack = fs_test_stack();
        let namespace = Namespace::new("apps/web").unwrap();
        let image = seed_image(&stack, &namespace).await;
        seed_report(&stack, &namespace, &image, Utc::now() - TimeDelta::hours(3)).await;
        let due = || scanning_repository(&["image.scanned_at < now() - hours(2)"]);

        let checker = build_checker(&stack, scanning_repository(&[]), false);
        assert!(
            enqueued(&checker, &namespace).await.is_empty(),
            "a repository without rules never refreshes a report"
        );
        let checker = build_checker(&stack, scanning_repository(&[]), true);
        let scans = enqueued(&checker, &namespace).await;
        assert_eq!(scans.len(), 1);
        assert!(scans[0].force && scans[0].reported_before.is_none());

        let before = Utc::now();
        let checker = build_checker(&stack, due(), false);
        let scans = enqueued(&checker, &namespace).await;
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].digest, image);
        assert!(!scans[0].force);
        let cutoff = scans[0].reported_before.expect("the run's time");
        assert!(cutoff >= before && cutoff <= Utc::now());

        seed_report(&stack, &namespace, &image, Utc::now()).await;
        let checker = build_checker(&stack, due(), false);
        assert!(
            enqueued(&checker, &namespace).await.is_empty(),
            "the newest report is not yet due"
        );
    }

    /// The rules see the image's tags, so a rule can pick which due images
    /// are scanned again.
    #[tokio::test]
    async fn rules_select_by_tag() {
        let stack = fs_test_stack();
        let namespace = Namespace::new("apps/web").unwrap();
        let image = seed_image(&stack, &namespace).await;
        seed_report(&stack, &namespace, &image, Utc::now() - TimeDelta::days(2)).await;

        let checker = build_checker(
            &stack,
            scanning_repository(&["image.tag == 'v2' && image.scanned_at < now() - days(1)"]),
            false,
        );
        assert!(enqueued(&checker, &namespace).await.is_empty());

        let checker = build_checker(
            &stack,
            scanning_repository(&["image.tag == 'v1' && image.scanned_at < now() - days(1)"]),
            false,
        );
        assert_eq!(enqueued(&checker, &namespace).await.len(), 1);
    }

    /// A walk over the namespaces lands the due scans on the queue, and a
    /// second walk coalesces on the image.
    #[tokio::test]
    async fn a_walk_enqueues_the_due_scans_once() {
        let stack = fs_test_stack();
        let namespace = Namespace::new("apps/web").unwrap();
        let image = seed_image(&stack, &namespace).await;
        stack.metadata_store.index_namespace(&namespace).await;
        seed_report(&stack, &namespace, &image, Utc::now() - TimeDelta::days(2)).await;
        let job_store = Arc::new(JobStore::new(
            stack.store.clone(),
            "reconcile-test",
            ClaimMode::Atomic,
        ));
        let executor = Executor::new(
            stack.blob_store.clone(),
            stack.metadata_store.clone(),
            job_store.clone(),
        );
        let checker = build_checker(
            &stack,
            scanning_repository(&["image.scanned_at < now() - days(1)"]),
            false,
        );

        for _ in 0..2 {
            check::check_namespaces(&stack.metadata_store, &checker, &executor, 1)
                .await
                .unwrap();
            assert_eq!(job_store.count_pending(Queue::Scan, 0).await.unwrap(), 1);
        }
    }
}
