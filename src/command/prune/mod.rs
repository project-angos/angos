mod checker;

use std::sync::Arc;

use argh::FromArgs;
use tracing::info;

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
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "prune",
    description = "Enforce retention policies by deleting tags that fall outside them"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
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
/// namespace, deleting the tags the policies no longer retain.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let bootstrap::MaintenanceContext {
        blob_store: blob_backend,
        metadata_store,
        repositories,
    } = bootstrap::maintenance_context(config).await?;

    let checker = RetentionChecker::new(
        metadata_store.clone(),
        repositories.clone(),
        global_retention_policy(&config.global.retention_policy),
    );
    let mut registry = None;
    let mut sink: Box<dyn ActionSink + Send> = if options.dry_run {
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

    check::check_namespaces(&metadata_store, &checker, sink.as_mut()).await?;
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
