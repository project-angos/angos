mod checker;
pub mod orphan_jobs;
mod orphan_namespaces;
mod uploads;

use std::sync::Arc;
use std::time::Duration as StdDuration;

use argh::FromArgs;
use chrono::Duration;
use humantime::Duration as HumanDuration;
use tracing::{info, warn};

pub use checker::RetentionChecker;

use crate::{
    command::{
        bootstrap,
        scrub::{
            Error, check,
            executor::{ActionSink, DryRunSink, Executor, run_job_store},
        },
    },
    configuration::Configuration,
    jobs::store::JobStore,
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "prune",
    description = "Enforce retention policies and reclaim aged upload-lifecycle leftovers"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(
        option,
        short = 'u',
        default = "HumanDuration::from(StdDuration::from_secs(3600))"
    )]
    /// age window for upload-lifecycle reclamation: upload sessions, orphan S3
    /// multiparts, grant-only blob ownership, and byteless blob-index entries
    /// older than this are removed (default 1h)
    pub uploads: HumanDuration,
}

/// The registry-wide retention policy, or `None` when no global rules are
/// configured (per-repository policies still apply).
pub fn global_retention_policy(config: &RetentionPolicyConfig) -> Option<Arc<RetentionPolicy>> {
    if config.rules.is_empty() {
        return None;
    }

    Some(Arc::new(RetentionPolicy::new(
        config,
        Arc::new(SystemClock),
    )))
}

/// Applies the global and per-repository retention policies to every
/// namespace, deleting the tags the policies no longer retain; then reclaims
/// aged upload-lifecycle leftovers within the `-u` window and queued jobs
/// whose configuration is gone.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let window = Duration::from_std(options.uploads.into())
        .map_err(|e| Error::Initialization(format!("Upload window is invalid: {e}")))?;
    let bootstrap::MaintenanceContext {
        blob_store: blob_backend,
        metadata_store,
        repositories,
    } = bootstrap::maintenance_context(config).await?;

    let global_policy = global_retention_policy(&config.global.retention_policy);
    let checker = RetentionChecker::new(
        metadata_store.clone(),
        repositories.clone(),
        global_policy.clone(),
    );
    let mut registry = None;
    let sink: Box<dyn ActionSink> = if options.dry_run {
        info!("Dry-run mode: no changes will be made to the storage");
        Box::new(DryRunSink)
    } else {
        let job_store = run_job_store(&metadata_store, "prune");
        let retention = bootstrap::registry(
            config,
            blob_backend.clone(),
            metadata_store.clone(),
            repositories.clone(),
            job_store.clone(),
        )?;
        registry = Some(retention.clone());
        Box::new(
            Executor::new(blob_backend.clone(), metadata_store.clone(), job_store)
                .with_registry(retention),
        )
    };

    check::check_namespaces(&metadata_store, &checker, sink.as_ref()).await?;

    // Config-relative and window-gated reclamation. Ordering matters for the
    // first two: orphan-namespace clearing cascades the manifest links whose
    // absence then lets the grant sweep revoke and reclaim. Each is
    // best-effort: a failed sweep warns and the run continues.
    let sweeps = [
        orphan_namespaces::sweep_orphan_namespaces(
            &blob_backend,
            &metadata_store,
            &repositories,
            sink.as_ref(),
        )
        .await,
        uploads::sweep_upload_sessions(&blob_backend, window, sink.as_ref()).await,
        uploads::sweep_orphan_multiparts(blob_backend.as_ref(), window, sink.as_ref()).await,
        checker::sweep_orphan_grants(
            &blob_backend,
            &metadata_store,
            &repositories,
            global_policy.as_deref(),
            window,
            sink.as_ref(),
        )
        .await,
        uploads::sweep_byteless_shards(&blob_backend, &metadata_store, window, sink.as_ref()).await,
        orphan_jobs::sweep_orphan_jobs(
            &Arc::new(JobStore::new(metadata_store.store_arc(), "prune-orphans")),
            &repositories,
            sink.as_ref(),
        )
        .await,
    ];
    for failed in sweeps.into_iter().filter_map(Result::err) {
        warn!("prune: sweep failed: {failed}");
    }

    // The registry shutdown flushes pending writes and drains in-flight async
    // webhook deliveries to completion before the process exits.
    match registry {
        Some(registry) => registry.shutdown().await,
        None => metadata_store.flush_access_times().await,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::CelRule;

    fn rule(s: &str) -> CelRule {
        CelRule::compile(s).unwrap()
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
}
