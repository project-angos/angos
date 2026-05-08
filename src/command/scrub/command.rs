use std::{collections::HashMap, sync::Arc};

use argh::FromArgs;
use chrono::Duration;
use tracing::info;

use crate::{
    command::{
        bootstrap,
        scrub::{
            check::{
                BlobChecker, LinkReferencesChecker, ManifestChecker, MediaTypeChecker,
                MultipartChecker, NamespaceChecker, RetentionChecker, StoreChecker, TagChecker,
                UploadChecker,
            },
            error::Error,
            executor::Executor,
        },
    },
    configuration::Configuration,
    policy::{RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{
        Repository,
        blob_store::{BlobStore, MultipartCleanup, UploadStore},
        metadata_store::MetadataStore,
        pagination::collect_all_pages,
    },
};

#[derive(FromArgs, PartialEq, Debug)]
#[allow(clippy::struct_excessive_bools)]
#[argh(
    subcommand,
    name = "scrub",
    description = "Check the storage backend for inconsistencies"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(option, short = 'u')]
    /// check for obsolete uploads with specified timeout
    pub uploads: Option<humantime::Duration>,
    #[argh(option, short = 'p')]
    /// cleanup orphan S3 multipart uploads older than specified timeout
    pub multipart: Option<humantime::Duration>,
    #[argh(switch, short = 't')]
    /// check for invalid tag digests
    pub tags: bool,
    #[argh(switch, short = 'm')]
    /// check for manifests inconsistencies
    pub manifests: bool,
    #[argh(switch, short = 'b')]
    /// check for blob inconsistencies
    pub blobs: bool,
    #[argh(switch, short = 'r')]
    /// enforce retention policies
    pub retention: bool,
    #[argh(switch, short = 'l')]
    /// fix links format inconsistencies
    pub links: bool,
    #[argh(switch, short = 'M')]
    /// backfill missing `media_type` on manifest links
    pub media_types: bool,
}

pub struct Command {
    metadata_store: Arc<dyn MetadataStore + Send + Sync>,
    namespace_checkers: Vec<Box<dyn NamespaceChecker>>,
    blob_checker: Option<BlobChecker>,
    multipart_checker: Option<MultipartChecker>,
    executor: Executor,
}

fn build_global_retention_policy(config: &RetentionPolicyConfig) -> Option<Arc<RetentionPolicy>> {
    if config.rules.is_empty() {
        return None;
    }

    Some(Arc::new(RetentionPolicy::new(
        config,
        Arc::new(SystemClock),
    )))
}

fn build_namespace_checkers(
    options: &Options,
    config: &Configuration,
    blob_store: &Arc<dyn BlobStore>,
    upload_store: &Arc<dyn UploadStore>,
    metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
    repositories: &Arc<HashMap<String, Repository>>,
) -> Result<Vec<Box<dyn NamespaceChecker>>, Error> {
    let mut checkers: Vec<Box<dyn NamespaceChecker>> = Vec::new();

    if options.retention {
        let global_retention_policy =
            build_global_retention_policy(&config.global.retention_policy);
        checkers.push(Box::new(RetentionChecker::new(
            metadata_store.clone(),
            repositories.clone(),
            global_retention_policy,
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
            upload_store.clone(),
            upload_timeout,
        )));
    }

    if options.tags {
        checkers.push(Box::new(TagChecker::new(metadata_store.clone())));
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

    Ok(checkers)
}

fn build_blob_checker(
    options: &Options,
    blob_store: &Arc<dyn BlobStore>,
    metadata_store: &Arc<dyn MetadataStore + Send + Sync>,
) -> Option<BlobChecker> {
    options
        .blobs
        .then(|| BlobChecker::new(blob_store.clone(), metadata_store.clone()))
}

fn build_multipart_checker(
    options: &Options,
    cleanup: Arc<dyn MultipartCleanup + Send + Sync>,
) -> Result<Option<MultipartChecker>, Error> {
    let Some(multipart_timeout) = options.multipart else {
        return Ok(None);
    };
    let multipart_timeout = Duration::from_std(multipart_timeout.into())
        .map_err(|e| Error::Initialization(format!("Multipart timeout is invalid: {e}")))?;
    Ok(Some(MultipartChecker::new(cleanup, multipart_timeout)))
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let blob_handles = bootstrap::blob_stores(&config.blob_store, &auth_cache)?;
        let (metadata_store, _) =
            bootstrap::metadata_store(&config.resolve_metadata_config(), &auth_cache).await?;
        let repositories = bootstrap::repositories(&config.repository, &auth_cache)?;

        let namespace_checkers = build_namespace_checkers(
            options,
            config,
            &blob_handles.blob_store,
            &blob_handles.upload_store,
            &metadata_store,
            &repositories,
        )?;
        let blob_checker = build_blob_checker(options, &blob_handles.blob_store, &metadata_store);
        let multipart_checker =
            build_multipart_checker(options, blob_handles.multipart_cleanup.clone())?;

        let executor = Executor::new(
            options.dry_run,
            blob_handles.blob_store.clone(),
            metadata_store.clone(),
            blob_handles.upload_store.clone(),
            blob_handles.multipart_cleanup,
        );

        if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
        }

        Ok(Self {
            metadata_store,
            namespace_checkers,
            blob_checker,
            multipart_checker,
            executor,
        })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.scrub_metadata().await?;
        self.scrub_blobs().await?;
        self.scrub_multipart_uploads().await?;

        Ok(())
    }

    async fn scrub_metadata(&mut self) -> Result<(), Error> {
        let namespaces =
            collect_all_pages(|marker| self.metadata_store.list_namespaces(100, marker)).await?;

        for namespace in namespaces {
            for i in 0..self.namespace_checkers.len() {
                if let Err(e) = self.namespace_checkers[i]
                    .check(&namespace, &mut self.executor)
                    .await
                {
                    tracing::warn!("Scrub checker failed for namespace '{namespace}': {e}");
                }
            }
        }

        Ok(())
    }

    async fn scrub_blobs(&mut self) -> Result<(), Error> {
        if let Some(checker) = self.blob_checker.take() {
            if let Err(e) = checker.check_all(&mut self.executor).await {
                tracing::warn!("Blob scrub checker failed: {e}");
            }
            self.blob_checker = Some(checker);
        }
        Ok(())
    }

    async fn scrub_multipart_uploads(&mut self) -> Result<(), Error> {
        if let Some(checker) = self.multipart_checker.take() {
            let result = checker.check_all(&mut self.executor).await;
            self.multipart_checker = Some(checker);
            result?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::CelRule;

    fn rule(s: &str) -> CelRule {
        CelRule::compile(s).unwrap()
    }

    #[test]
    fn test_build_global_retention_policy_empty() {
        let config = RetentionPolicyConfig { rules: vec![] };
        let result = build_global_retention_policy(&config);

        assert!(result.is_none());
    }

    #[test]
    fn test_build_global_retention_policy_with_rules() {
        let config = RetentionPolicyConfig {
            rules: vec![rule("image.pushed_at > now() - days(30)")],
        };
        let result = build_global_retention_policy(&config);

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

    #[tokio::test]
    async fn test_command_new_with_valid_config() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let config_content = format!(
            r#"
            [blob_store.fs]
            root_dir = "{path}"

            [metadata_store.fs]
            root_dir = "{path}"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [global.retention_policy]
            rules = []
            "#
        );

        let config: Configuration = toml::from_str(&config_content).unwrap();

        let options = Options {
            dry_run: true,
            uploads: Some(humantime::Duration::from(std::time::Duration::from_secs(
                3600,
            ))),
            multipart: None,
            tags: true,
            manifests: true,
            blobs: true,
            retention: true,
            links: false,
            media_types: false,
        };

        let command = Command::new(&options, &config).await;

        assert!(command.is_ok());
        let cmd = command.unwrap();
        // retention, uploads, tags, manifests = 4 namespace checkers
        assert_eq!(cmd.namespace_checkers.len(), 4);
        assert!(cmd.blob_checker.is_some());
        assert!(cmd.multipart_checker.is_none());
    }

    #[tokio::test]
    async fn test_command_run_executes_checks() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();

        let config_content = format!(
            r#"
            [blob_store.fs]
            root_dir = "{path}"

            [metadata_store.fs]
            root_dir = "{path}"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [global.retention_policy]
            rules = []
            "#
        );

        let config: crate::configuration::Configuration = toml::from_str(&config_content).unwrap();

        let options = Options {
            dry_run: true,
            uploads: None,
            multipart: None,
            tags: false,
            manifests: false,
            blobs: false,
            retention: false,
            links: false,
            media_types: false,
        };

        let mut command = Command::new(&options, &config).await.unwrap();
        let result = command.run().await;

        assert!(result.is_ok());
    }
}
