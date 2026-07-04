mod checker;

use std::sync::Arc;

use argh::FromArgs;
use tracing::info;
use uuid::Uuid;

pub use checker::RetentionChecker;

use crate::{
    command::{
        bootstrap,
        scrub::{
            Error, check,
            executor::{ActionSink, DryRunSink, Executor},
        },
    },
    configuration::Configuration,
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::job_store::JobStore,
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
/// namespace, deleting the tags the policies no longer retain. Supersedes the
/// deprecated `scrub --retention`.
pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    let auth_cache = bootstrap::auth_cache(&config.cache)?;
    let blob_backend = Arc::new(config.blob_store.build_backend()?);
    let metadata_store =
        bootstrap::metadata_store(&config.resolve_registry_storage(), &auth_cache).await?;
    let repositories = bootstrap::repositories(
        &config.repository,
        &auth_cache,
        config.global.max_manifest_size_bytes(),
    )
    .await?;

    let checker = RetentionChecker::new(
        metadata_store.clone(),
        repositories.clone(),
        global_retention_policy(&config.global.retention_policy),
    );
    let mut sink: Box<dyn ActionSink + Send> = if options.dry_run {
        info!("Dry-run mode: no changes will be made to the storage");
        Box::new(DryRunSink)
    } else {
        let job_store = Arc::new(JobStore::new(
            metadata_store.store_arc(),
            format!("prune-{}", Uuid::new_v4()),
        ));
        Box::new(Executor::new(
            blob_backend.clone(),
            metadata_store.clone(),
            job_store,
        ))
    };

    check::check_namespaces(&metadata_store, &checker, sink.as_mut()).await?;
    metadata_store.flush_access_times().await;
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
