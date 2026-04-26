use std::{collections::HashMap, sync::Arc};

use argh::FromArgs;
use chrono::Duration;
use tracing::{error, info};

use crate::{
    cache,
    cache::Cache,
    command::scrub::{
        check::{
            BlobChecker, LinkReferencesChecker, ManifestChecker, MediaTypeChecker,
            MultipartChecker, NamespaceChecker, RetentionChecker, TagChecker, UploadChecker,
        },
        error::Error,
    },
    configuration::Configuration,
    policy::{RetentionPolicy, RetentionPolicyConfig},
    registry::{
        Repository, blob_store,
        blob_store::{BlobStore, UploadStore},
        metadata_store::MetadataStore,
        pagination::collect_all_pages,
        repository,
    },
};

type BlobStores = (Arc<dyn BlobStore>, Arc<dyn UploadStore>);

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
}

fn build_stores(config: &blob_store::BlobStorageConfig) -> Result<BlobStores, Error> {
    match config.to_backend(None) {
        Ok((blob_store, upload_store, _)) => Ok((blob_store, upload_store)),
        Err(_) => Err(Error::Initialization(
            "Failed to initialize blob store".to_string(),
        )),
    }
}

async fn build_metadata_store(config: &Configuration) -> Result<Arc<dyn MetadataStore>, Error> {
    match config.resolve_metadata_config().to_backend(None).await {
        Ok((store, _)) => Ok(store),
        Err(err) => {
            let msg = format!("Failed to initialize metadata store: {err}");
            Err(Error::Initialization(msg))
        }
    }
}

fn build_auth_cache(config: &cache::Config) -> Result<Arc<dyn Cache>, Error> {
    match config.to_backend() {
        Ok(cache) => Ok(cache),
        Err(err) => {
            let msg = format!("Failed to initialize auth token cache: {err}");
            Err(Error::Initialization(msg))
        }
    }
}

fn build_repository(
    name: &str,
    config: &repository::Config,
    auth_cache: &Arc<dyn Cache>,
) -> Result<Repository, Error> {
    match Repository::new(name, config, auth_cache) {
        Ok(repo) => Ok(repo),
        Err(err) => {
            let msg = format!("Failed to initialize repository '{name}': {err}");
            Err(Error::Initialization(msg))
        }
    }
}

fn build_repositories(
    configs: &HashMap<String, repository::Config>,
    auth_cache: &Arc<dyn Cache>,
) -> Result<Arc<HashMap<String, Repository>>, Error> {
    let mut repositories = HashMap::new();
    for (name, config) in configs {
        let repo = build_repository(name, config, auth_cache)?;
        repositories.insert(name.clone(), repo);
    }

    Ok(Arc::new(repositories))
}

fn build_global_retention_policy(config: &RetentionPolicyConfig) -> Option<Arc<RetentionPolicy>> {
    if config.rules.is_empty() {
        return None;
    }

    Some(Arc::new(RetentionPolicy::new(config)))
}

fn build_namespace_checkers(
    options: &Options,
    config: &Configuration,
    blob_store: &Arc<dyn BlobStore>,
    upload_store: &Arc<dyn UploadStore>,
    metadata_store: &Arc<dyn MetadataStore>,
    repositories: &Arc<HashMap<String, Repository>>,
) -> Vec<Box<dyn NamespaceChecker>> {
    let mut checkers: Vec<Box<dyn NamespaceChecker>> = Vec::new();

    if options.retention {
        let global_retention_policy =
            build_global_retention_policy(&config.global.retention_policy);
        checkers.push(Box::new(RetentionChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
            repositories.clone(),
            global_retention_policy,
            options.dry_run,
        )));
    }

    if let Some(upload_timeout) = options.uploads {
        let upload_timeout =
            Duration::from_std(upload_timeout.into()).expect("Upload timeout must be valid");
        info!(
            "Upload timeout set to {} second(s)",
            upload_timeout.num_seconds()
        );
        checkers.push(Box::new(UploadChecker::new(
            upload_store.clone(),
            upload_timeout,
            options.dry_run,
        )));
    }

    if options.tags {
        checkers.push(Box::new(TagChecker::new(
            metadata_store.clone(),
            options.dry_run,
        )));
    }

    if options.manifests {
        checkers.push(Box::new(ManifestChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
            options.dry_run,
        )));
    }

    if options.links {
        checkers.push(Box::new(LinkReferencesChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
            options.dry_run,
        )));
    }

    if options.media_types {
        checkers.push(Box::new(MediaTypeChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
            options.dry_run,
        )));
    }

    checkers
}

fn build_blob_checker(
    options: &Options,
    blob_store: &Arc<dyn BlobStore>,
    metadata_store: &Arc<dyn MetadataStore>,
) -> Option<BlobChecker> {
    if options.blobs {
        Some(BlobChecker::new(
            blob_store.clone(),
            metadata_store.clone(),
            options.dry_run,
        ))
    } else {
        None
    }
}

fn build_multipart_checker(
    options: &Options,
    blob_store_config: &blob_store::BlobStorageConfig,
) -> Option<MultipartChecker> {
    let multipart_timeout = options.multipart?;
    let multipart_timeout =
        Duration::from_std(multipart_timeout.into()).expect("Multipart timeout must be valid");
    blob_store_config
        .to_multipart_cleanup()
        .map(|cleanup| MultipartChecker::new(cleanup, multipart_timeout, options.dry_run))
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let (blob_store, upload_store) = build_stores(&config.blob_store)?;
        let metadata_store = build_metadata_store(config).await?;
        let auth_cache = build_auth_cache(&config.cache)?;
        let repositories = build_repositories(&config.repository, &auth_cache)?;

        let namespace_checkers = build_namespace_checkers(
            options,
            config,
            &blob_store,
            &upload_store,
            &metadata_store,
            &repositories,
        );
        let blob_checker = build_blob_checker(options, &blob_store, &metadata_store);
        let multipart_checker = build_multipart_checker(options, &config.blob_store);

        if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
        }

        Ok(Self {
            metadata_store,
            namespace_checkers,
            blob_checker,
            multipart_checker,
        })
    }

    pub async fn run(&self) -> Result<(), Error> {
        self.scrub_metadata().await?;
        self.scrub_blobs().await?;
        self.scrub_multipart_uploads().await?;

        Ok(())
    }

    async fn scrub_metadata(&self) -> Result<(), Error> {
        let namespaces =
            collect_all_pages(|marker| self.metadata_store.list_namespaces(100, marker))
                .await
                .map_err(|_| {
                    error!("Failed to read catalog");
                    Error::Execution("Failed to read catalog".to_string())
                })?;

        for namespace in namespaces {
            for checker in &self.namespace_checkers {
                let _ = checker.check_namespace(&namespace).await;
            }
        }

        Ok(())
    }

    async fn scrub_blobs(&self) -> Result<(), Error> {
        if let Some(checker) = &self.blob_checker {
            let _ = checker.check_all().await;
        }
        Ok(())
    }

    async fn scrub_multipart_uploads(&self) -> Result<(), Error> {
        if let Some(checker) = &self.multipart_checker {
            checker
                .check_all()
                .await
                .map_err(|e| Error::Execution(format!("Multipart cleanup failed: {e}")))?;
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

        let command = Command::new(&options, &config).await.unwrap();
        let result = command.run().await;

        assert!(result.is_ok());
    }
}
