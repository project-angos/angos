use std::sync::Arc;

use chrono::Duration;
use tracing::info;

use crate::{
    command::scrub::{
        check::{
            BlobChecker, LayoutChecker, LinkReferencesChecker, ManifestChecker, MediaTypeChecker,
            MultipartChecker, NamespaceChecker, ReferrerChecker, RetentionChecker, TagChecker,
            UploadChecker,
        },
        command::Options,
        error::Error,
    },
    configuration::Configuration,
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        blob_store, metadata_store::MetadataStore, repository_resolver::RepositoryResolver,
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

pub fn namespace_checkers(
    options: &Options,
    config: &Configuration,
    blob_store: &Arc<blob_store::BlobStore>,
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

    if options.tags {
        checkers.push(Box::new(TagChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
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

    if options.media_types {
        checkers.push(Box::new(MediaTypeChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
        )));
    }

    if options.referrers {
        checkers.push(Box::new(ReferrerChecker::new(metadata_store.clone())));
    }

    Ok(checkers)
}

pub fn layout_checker(blob_store: &Arc<blob_store::BlobStore>) -> LayoutChecker {
    LayoutChecker::new(blob_store.clone())
}

pub fn blob_checker(
    options: &Options,
    blob_store: &Arc<blob_store::BlobStore>,
    metadata_store: &Arc<MetadataStore>,
) -> Option<BlobChecker> {
    options
        .blobs
        .then(|| BlobChecker::new(blob_store.clone(), metadata_store.clone()))
}

pub fn multipart_checker(
    options: &Options,
    blob_store: &Arc<blob_store::BlobStore>,
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
