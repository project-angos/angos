use std::sync::Arc;

use chrono::Duration;
use tracing::{info, warn};

use crate::{
    command::{
        prune::{RetentionChecker, global_retention_policy},
        replicate::ReplicationChecker,
        scrub::{
            check::{
                BlobChecker, BlobIndexChecker, DigestLinkChecker, LayoutChecker,
                LinkReferencesChecker, ManifestChecker, MediaTypeChecker, MultipartChecker,
                NamespaceChecker, OrphanGrantChecker, OrphanJobChecker, OrphanNamespaceChecker,
                OrphanQueue, ReferrerChecker, StoreChecker, TagChecker, UploadChecker,
            },
            command::Options,
            error::Error,
        },
    },
    configuration::Configuration,
    registry::{
        blob_store::BlobStore, job_store::JobStore, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

/// Store-wide checkers paired with a stable label used to name a failing checker
/// in the scrub log.
pub type LabeledStoreCheckers = Vec<(&'static str, Box<dyn StoreChecker>)>;

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

    if options.manifests {
        checkers.push(Box::new(ManifestChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
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
/// (which also disables the invalid-tag gate). Driven by the tag walk in
/// `Command::scrub_metadata`.
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

/// Build the enabled store-wide checkers in the order [`super::command`] must
/// apply them: orphan-namespace clearing first (its link deletions free manifest
/// bytes the blob checker then reclaims), then blob / orphan-grant / multipart
/// reclamation, and finally the orphan-job sweep, which must precede the
/// replication drain. Each is included only when its flag is set, and paired with
/// a stable label so a failing checker can be named in the scrub log.
pub fn store_checkers(
    options: &Options,
    blob_store: &Arc<BlobStore>,
    metadata_store: &Arc<MetadataStore>,
    resolver: &Arc<RepositoryResolver>,
) -> Result<LabeledStoreCheckers, Error> {
    let mut checkers: LabeledStoreCheckers = Vec::new();
    if let Some(checker) = orphan_namespace_checker(options, blob_store, metadata_store, resolver) {
        checkers.push(("orphan-namespaces", Box::new(checker)));
    }
    if let Some(checker) = blob_checker(options, blob_store, metadata_store) {
        checkers.push(("blobs", Box::new(checker)));
    }
    if let Some(checker) = orphan_grant_checker(options, blob_store, metadata_store)? {
        checkers.push(("orphan-grants", Box::new(checker)));
    }
    if let Some(checker) = multipart_checker(options, blob_store)? {
        checkers.push(("multipart", Box::new(checker)));
    }
    for checker in orphan_job_checkers(options, metadata_store, resolver) {
        checkers.push(("orphan-jobs", Box::new(checker)));
    }
    Ok(checkers)
}

#[cfg(test)]
mod tests {
    use crate::policy::RetentionPolicyConfig;

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
