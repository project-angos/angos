use std::sync::Arc;

use chrono::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    command::scrub::{
        check::{
            JobChecker, MultipartChecker, NamespaceChecker, OrphanJobChecker, OrphanQueue,
            ReplicationChecker, RetentionChecker, UploadChecker,
        },
        command::Options,
        error::Error,
    },
    configuration::Configuration,
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        blob_store::BlobStore, job_store::JobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

fn global_retention_policy(config: &RetentionPolicyConfig) -> Option<Arc<RetentionPolicy>> {
    if config.rules.is_empty() {
        return None;
    }

    Some(Arc::new(RetentionPolicy::new(
        config,
        Arc::new(SystemClock),
    )))
}

/// Build the per-namespace checkers for the enabled flags shared across `scrub`,
/// `policy`, and `replication`. The scrub-only rebuild is built in scrub's
/// `Command::new`, not here.
pub fn namespace_checkers(
    options: &Options,
    config: &Configuration,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
) -> Result<Vec<Box<dyn NamespaceChecker>>, Error> {
    let mut checkers: Vec<Box<dyn NamespaceChecker>> = Vec::new();

    if options.retention {
        let policy = global_retention_policy(&config.global.retention_policy);
        checkers.push(Box::new(RetentionChecker::new(
            metadata_store.clone(),
            resolver.clone(),
            policy,
        )));
    }

    if let Some(upload_timeout) = options.uploads {
        let upload_timeout = Duration::from_std(upload_timeout.into())
            .map_err(|e| Error::Initialization(format!("Upload timeout is invalid: {e}")))?;
        info!(
            "Upload timeout set to {} second(s)",
            upload_timeout.num_seconds()
        );
        checkers.push(Box::new(UploadChecker::new(
            blob_store.clone(),
            upload_timeout,
        )));
    }

    if options.replicate {
        checkers.push(Box::new(ReplicationChecker::new(
            metadata_store.clone(),
            resolver.clone(),
        )));
    }

    Ok(checkers)
}

pub fn multipart_checker(
    options: &Options,
    blob_store: &Arc<BlobStore>,
) -> Result<Option<MultipartChecker>, Error> {
    let Some(multipart_timeout) = options.multipart else {
        return Ok(None);
    };
    let multipart_timeout = Duration::from_std(multipart_timeout.into())
        .map_err(|e| Error::Initialization(format!("Multipart timeout is invalid: {e}")))?;
    info!(
        "Multipart cleanup timeout set to {} second(s)",
        multipart_timeout.num_seconds()
    );
    Ok(Some(MultipartChecker::new(
        blob_store.clone(),
        multipart_timeout,
    )))
}

/// Builds the structural `--jobs` checker, or `None` when the flag is off. It
/// reconciles through the `JobStore`'s own conditional transactions (not the
/// action sink), so its gate is `!commit` rather than the sink selection.
/// `--prune-unknown` gates the unknown-queue removal on top of that; `grace`
/// is the operator's maintenance grace (an unknown queue directory may belong
/// to a newer replica, so it must stay quiescent that long before removal).
pub fn job_checker(
    options: &Options,
    metadata_store: &Arc<MetadataStore>,
    grace: Duration,
    cancel: CancellationToken,
) -> Option<JobChecker> {
    if !options.jobs {
        return None;
    }
    let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), ""));
    Some(JobChecker::new(
        job_store,
        !options.commit,
        options.prune_unknown,
        grace,
        cancel,
    ))
}

/// Builds one orphan-job checker per queue selected by `--replication-orphans`
/// and `--cache-orphans`. The `JobStore` handle is read-only here, so the
/// consumer id is left empty.
pub fn orphan_job_checkers(
    options: &Options,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
) -> Vec<OrphanJobChecker> {
    let mut queues = Vec::new();
    if options.replication_orphans {
        queues.push(OrphanQueue::Replication);
    }
    if options.cache_orphans {
        queues.push(OrphanQueue::Cache);
    }
    if queues.is_empty() {
        return Vec::new();
    }
    let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), ""));
    queues
        .into_iter()
        .map(|queue| OrphanJobChecker::new(job_store.clone(), resolver.clone(), queue))
        .collect()
}

#[cfg(test)]
mod tests {
    use angos_storage::{ObjectStore, fs::Backend as StorageFsBackend};
    use angos_tx_engine::store::Store;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        command::scrub::{check::StoreChecker, executor::DryRunSink},
        metrics_provider,
        policy::CelRule,
        registry::{
            job_store::{JobEnvelope, Queue},
            test_utils::{build_store, build_test_fs_executor},
        },
    };

    fn rule(s: &str) -> CelRule {
        CelRule::compile(s).unwrap()
    }

    /// FS-backed metadata store plus its raw `Store`, so a test can plant and
    /// inspect the dangling lock-key index directly.
    fn fs_store() -> (Arc<MetadataStore>, Arc<Store>, TempDir) {
        metrics_provider::init_for_tests();
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

    /// Enqueue a cache job then delete its pending envelope out-of-band, leaving a
    /// dangling lock-key index the `--jobs` reconcile would retire.
    async fn plant_dangling_lock_key_index(metadata_store: &Arc<MetadataStore>, store: &Store) {
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "jobs-test"));
        let envelope = JobEnvelope::new(
            Queue::Cache,
            "cache.fetch_blob",
            "cache.ns/app",
            &json!({ "namespace": "ns/app", "digest": "sha256:abc" }),
        )
        .unwrap();
        job_store.enqueue(envelope).await.unwrap();
        let keys = job_store.list_pending(Queue::Cache, 10).await.unwrap();
        store
            .delete(&format!("_jobs/pending/cache/{}.json", keys[0]))
            .await
            .unwrap();
    }

    async fn remaining_lock_key_indexes(metadata_store: &Arc<MetadataStore>) -> usize {
        let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), "probe"));
        job_store
            .list_lock_key_index_keys(Queue::Cache)
            .await
            .unwrap()
            .len()
    }

    /// `scrub --jobs` without `--commit` is report-only: the reconcile mutates
    /// through the `JobStore`, so its gate rides `--commit` and the dangling index
    /// survives.
    #[tokio::test]
    async fn job_checker_report_only_leaves_dangling_index() {
        let (metadata_store, store, _dir) = fs_store();
        plant_dangling_lock_key_index(&metadata_store, &store).await;

        let options = Options {
            jobs: true,
            ..Default::default()
        };
        let checker = job_checker(
            &options,
            &metadata_store,
            Duration::hours(24),
            CancellationToken::new(),
        )
        .expect("--jobs builds a checker");
        checker.check_all(&mut (DryRunSink)).await.unwrap();

        assert_eq!(
            remaining_lock_key_indexes(&metadata_store).await,
            1,
            "report-only --jobs (no --commit) must leave the dangling lock-key index in place"
        );
    }

    /// `scrub --jobs --commit` retires the dangling lock-key index.
    #[tokio::test]
    async fn job_checker_commit_retires_dangling_index() {
        let (metadata_store, store, _dir) = fs_store();
        plant_dangling_lock_key_index(&metadata_store, &store).await;

        let options = Options {
            jobs: true,
            commit: true,
            ..Default::default()
        };
        let checker = job_checker(
            &options,
            &metadata_store,
            Duration::hours(24),
            CancellationToken::new(),
        )
        .expect("--jobs builds a checker");
        checker.check_all(&mut (DryRunSink)).await.unwrap();

        assert_eq!(
            remaining_lock_key_indexes(&metadata_store).await,
            0,
            "--jobs --commit must retire the dangling lock-key index"
        );
    }

    #[test]
    fn global_retention_policy_empty_returns_none() {
        let config = RetentionPolicyConfig { rules: vec![] };
        let result = global_retention_policy(&config);

        assert!(result.is_none());
    }

    #[test]
    fn global_retention_policy_with_rules_returns_some() {
        let config = RetentionPolicyConfig {
            rules: vec![rule("image.pushed_at > now() - days(30)")],
        };
        let result = global_retention_policy(&config);

        assert!(result.is_some());
    }

    #[test]
    fn invalid_retention_rule_fails_at_deserialize() {
        let toml = r#"rules = ["invalid cel expression!!!"]"#;
        let result: Result<RetentionPolicyConfig, _> = toml::from_str(toml);
        assert!(
            result.is_err(),
            "invalid CEL rule must fail at deserialization"
        );
    }
}
