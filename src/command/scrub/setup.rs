use std::sync::Arc;

use chrono::Duration;
use tracing::{info, warn};

use crate::{
    command::scrub::{
        check::{
            BlobChecker, BlobIndexChecker, DigestLinkChecker, JobChecker, LayoutChecker,
            LinkReferencesChecker, ManifestChecker, MediaTypeChecker, MultipartChecker,
            NamespaceChecker, OrphanGrantChecker, OrphanJobChecker, OrphanNamespaceChecker,
            OrphanQueue, ReferrerChecker, ReplicationChecker, RetentionChecker, TagChecker,
            UploadChecker,
        },
        command::Options,
        error::Error,
        report::Findings,
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

/// Build the per-namespace checkers for the enabled flags.
///
/// `findings` is the shared report-only accumulator from the run's `Ctx`; it is
/// attached to the `ManifestChecker` (under `-m`) so its dangling-reference
/// warnings also surface in the end-of-run summary. Threading it here lights the
/// findings up for `scrub`, `policy`, and `replication` alike, since all three
/// build their checkers through this one function.
pub fn namespace_checkers(
    options: &Options,
    config: &Configuration,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
    findings: &Findings,
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

    if options.manifests {
        checkers.push(Box::new(ManifestChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
            findings.clone(),
        )));
    }

    if options.links {
        checkers.push(Box::new(LinkReferencesChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
        )));
    }

    if options.reconcile_blob_index {
        checkers.push(Box::new(BlobIndexChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
        )));
    }

    // Media-type backfill is the deprecated standalone `-M` (media_types) only;
    // it deliberately does NOT ride `-l`. `media_type` is now recorded in link
    // metadata at manifest-push time, so newly pushed content never needs
    // backfill and `-l` stays a cheap link-format/referrer repair (no full
    // manifest content scan). `-M` exists solely to backfill legacy links
    // written before media_type was recorded at push time.
    // Position kept right after `LinkReferencesChecker`.
    if options.media_types {
        checkers.push(Box::new(MediaTypeChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
        )));
    }

    if options.referrers {
        checkers.push(Box::new(ReferrerChecker::new(metadata_store.clone())));
    }

    if options.replicate {
        checkers.push(Box::new(ReplicationChecker::new(
            metadata_store.clone(),
            resolver.clone(),
        )));
    }

    Ok(checkers)
}

/// The per-tag checkers enabled by `--tags`, or `None` when tag scrubbing is off
/// (which also disables the invalid-tag gate). Driven by the tag walk in the
/// scrub `metadata` node.
pub fn tag_checkers(
    options: &Options,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
) -> Option<Vec<Box<dyn TagChecker>>> {
    options.tags.then(|| {
        vec![Box::new(DigestLinkChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
        )) as Box<dyn TagChecker>]
    })
}

pub fn layout_checker(blob_store: &Arc<BlobStore>) -> LayoutChecker {
    LayoutChecker::new(blob_store.clone())
}

pub fn blob_checker(
    options: &Options,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
) -> Option<BlobChecker> {
    options
        .blobs
        .then(|| BlobChecker::new(blob_store.clone(), metadata_store.clone()))
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

pub fn orphan_grant_checker(
    options: &Options,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
) -> Result<Option<OrphanGrantChecker>, Error> {
    let Some(min_age) = options.orphan_grants else {
        return Ok(None);
    };
    let min_age = Duration::from_std(min_age.into())
        .map_err(|e| Error::Initialization(format!("Orphan-grant age is invalid: {e}")))?;
    info!(
        "Orphan blob-grant minimum age set to {} second(s)",
        min_age.num_seconds()
    );
    Ok(Some(OrphanGrantChecker::new(
        blob_store.clone(),
        metadata_store.clone(),
        min_age,
    )))
}

/// Builds the `--orphan-namespaces` checker, or `None` (with a warning) when no
/// repositories are configured, since the flag must never wipe the whole registry.
pub fn orphan_namespace_checker(
    options: &Options,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
) -> Option<OrphanNamespaceChecker> {
    if !options.orphan_namespaces {
        return None;
    }
    if resolver.len() == 0 {
        warn!(
            "scrub --orphan-namespaces: no repositories configured; refusing to delete every namespace"
        );
        return None;
    }
    info!(
        "Orphan-namespace clearing enabled for {} configured repositories",
        resolver.len()
    );
    Some(OrphanNamespaceChecker::new(
        blob_store.clone(),
        metadata_store.clone(),
        resolver.clone(),
    ))
}

/// Builds the structural `--jobs` checker, or `None` when the flag is off. The
/// `JobStore` handle reconciles dangling lock-key indexes through its own
/// conditional engine transactions (not the action sink), so the consumer id is
/// irrelevant and left empty; `dry_run` / `prune_unknown` are copied so the
/// checker can honour `-d` and gate the net-new unknown-queue removal.
pub fn job_checker(options: &Options, metadata_store: &Arc<MetadataStore>) -> Option<JobChecker> {
    if !options.jobs {
        return None;
    }
    let job_store = Arc::new(JobStore::new(metadata_store.store_arc(), ""));
    Some(JobChecker::new(
        job_store,
        options.dry_run,
        options.prune_unknown,
    ))
}

/// Builds one orphan-job checker per queue selected by `--replication-orphans`
/// and `--cache-orphans`. The `JobStore` handle is read-only here (the executor
/// deletes through its own), so the consumer id is irrelevant and left empty.
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
