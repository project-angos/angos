use std::{collections::HashMap, sync::Arc};

use argh::FromArgs;
use chrono::Duration;
use tracing::{error, info};

use crate::cache;
use crate::cache::Cache;
use crate::command::scrub::check::{
    BlobChecker, LinkReferencesChecker, ManifestChecker, MediaTypeChecker, MultipartChecker,
    RetentionChecker, TagChecker, UploadChecker,
};
use crate::command::scrub::error::Error;
use crate::configuration::Configuration;
use crate::policy::{RetentionPolicy, RetentionPolicyConfig};
use crate::registry::{
    Repository, blob_store, blob_store::BlobStore, metadata_store::MetadataStore, repository,
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
    /// display only, no axtual changes applied
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
    retention_enforcer: Option<RetentionChecker>,
    upload_checker: Option<UploadChecker>,
    tags_checker: Option<TagChecker>,
    revisions: Option<ManifestChecker>,
    blob_checker: Option<BlobChecker>,
    multipart_checker: Option<MultipartChecker>,
    link_references_checker: Option<LinkReferencesChecker>,
    media_type_checker: Option<MediaTypeChecker>,
}

fn build_blob_store(config: &blob_store::BlobStorageConfig) -> Result<Arc<dyn BlobStore>, Error> {
    let Ok(blob_store) = config.to_backend() else {
        let msg = "Failed to initialize blob store".to_string();
        return Err(Error::Initialization(msg));
    };

    Ok(blob_store)
}

fn build_metadata_store(config: &Configuration) -> Result<Arc<dyn MetadataStore>, Error> {
    match config.resolve_metadata_config().to_backend(None) {
        Ok(store) => Ok(store),
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

fn build_global_retention_policy(
    config: &RetentionPolicyConfig,
) -> Result<Option<Arc<RetentionPolicy>>, Error> {
    if config.rules.is_empty() {
        return Ok(None);
    }

    match RetentionPolicy::new(config) {
        Ok(policy) => Ok(Some(Arc::new(policy))),
        Err(err) => {
            let msg = format!("Failed to initialize global retention policy: {err}");
            Err(Error::Initialization(msg))
        }
    }
}

impl Command {
    pub fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let blob_store = build_blob_store(&config.blob_store)?;
        let metadata_store = build_metadata_store(config)?;
        let auth_cache = build_auth_cache(&config.cache)?;
        let repositories = build_repositories(&config.repository, &auth_cache)?;

        let retention_enforcer = if options.retention {
            let global_retention_policy =
                build_global_retention_policy(&config.global.retention_policy)?;

            Some(RetentionChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                repositories.clone(),
                global_retention_policy.clone(),
                options.dry_run,
            ))
        } else {
            None
        };

        let upload_checker = if let Some(upload_timeout) = options.uploads {
            let upload_timeout =
                Duration::from_std(upload_timeout.into()).expect("Upload timeout must be valid");

            info!(
                "Upload timeout set to {} second(s)",
                upload_timeout.num_seconds()
            );

            Some(UploadChecker::new(
                blob_store.clone(),
                upload_timeout,
                options.dry_run,
            ))
        } else {
            None
        };

        let tags_checker = if options.tags {
            Some(TagChecker::new(metadata_store.clone(), options.dry_run))
        } else {
            None
        };

        let revisions = if options.manifests {
            Some(ManifestChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                options.dry_run,
            ))
        } else {
            None
        };

        let blob_checker = if options.blobs {
            Some(BlobChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                options.dry_run,
            ))
        } else {
            None
        };

        let multipart_checker = if let Some(multipart_timeout) = options.multipart {
            let multipart_timeout =
                Duration::from_std(multipart_timeout.into()).expect("Upload timeout must be valid");

            config
                .blob_store
                .to_multipart_cleanup()
                .map(|cleanup| MultipartChecker::new(cleanup, multipart_timeout, options.dry_run))
        } else {
            None
        };

        let link_references_checker = if options.links {
            Some(LinkReferencesChecker::new(
                blob_store.clone(),
                metadata_store.clone(),
                options.dry_run,
            ))
        } else {
            None
        };

        let media_type_checker = if options.media_types {
            Some(MediaTypeChecker::new(
                blob_store,
                metadata_store.clone(),
                options.dry_run,
            ))
        } else {
            None
        };

        if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
        }

        Ok(Self {
            metadata_store,
            retention_enforcer,
            upload_checker,
            tags_checker,
            revisions,
            blob_checker,
            multipart_checker,
            link_references_checker,
            media_type_checker,
        })
    }

    pub async fn run(&self) -> Result<(), Error> {
        self.scrub_metadata().await?;
        self.scrub_blobs().await?;
        self.scrub_multipart_uploads().await?;

        Ok(())
    }

    async fn scrub_metadata(&self) -> Result<(), Error> {
        let mut marker = None;
        loop {
            let Ok((namespaces, next_marker)) =
                self.metadata_store.list_namespaces(100, marker).await
            else {
                error!("Failed to read catalog");
                return Err(Error::Execution("Failed to read catalog".to_string()));
            };

            for namespace in namespaces {
                if let Some(retention_enforcer) = &self.retention_enforcer {
                    let _ = retention_enforcer.check_namespace(&namespace).await;
                }

                if let Some(uploads_checker) = &self.upload_checker {
                    let _ = uploads_checker.check_namespace(&namespace).await;
                }

                if let Some(tags_checker) = &self.tags_checker {
                    let _ = tags_checker.check_namespace(&namespace).await;
                }

                if let Some(revisions_checker) = &self.revisions {
                    let _ = revisions_checker.check_namespace(&namespace).await;
                }

                if let Some(link_references_checker) = &self.link_references_checker {
                    let _ = link_references_checker.check_namespace(&namespace).await;
                }

                if let Some(media_type_checker) = &self.media_type_checker {
                    let _ = media_type_checker.check_namespace(&namespace).await;
                }
            }

            if next_marker.is_none() {
                break;
            }

            marker = next_marker;
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

    #[test]
    fn test_build_global_retention_policy_empty() {
        let config = RetentionPolicyConfig { rules: vec![] };
        let result = build_global_retention_policy(&config);

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_build_global_retention_policy_with_rules() {
        let config = RetentionPolicyConfig {
            rules: vec!["image.pushed_at > now() - days(30)".to_string()],
        };
        let result = build_global_retention_policy(&config);

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_build_global_retention_policy_invalid_rule() {
        let config = RetentionPolicyConfig {
            rules: vec!["invalid cel expression!!!".to_string()],
        };
        let result = build_global_retention_policy(&config);

        assert!(result.is_err());
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

        let command = Command::new(&options, &config);

        assert!(command.is_ok());
        let cmd = command.unwrap();
        assert!(cmd.upload_checker.is_some());
        assert!(cmd.tags_checker.is_some());
        assert!(cmd.revisions.is_some());
        assert!(cmd.blob_checker.is_some());
        assert!(cmd.retention_enforcer.is_some());
        assert!(cmd.multipart_checker.is_none());
        assert!(cmd.link_references_checker.is_none());
        assert!(cmd.media_type_checker.is_none());
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

        let command = Command::new(&options, &config).unwrap();
        let result = command.run().await;

        assert!(result.is_ok());
    }
}
