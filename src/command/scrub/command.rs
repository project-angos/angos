use std::sync::Arc;

use argh::FromArgs;
use futures_util::StreamExt;
use tracing::info;

use crate::{
    command::{
        bootstrap,
        scrub::{
            check::{BlobChecker, MultipartChecker, NamespaceChecker, StoreChecker, list_all},
            error::Error,
            executor::{ActionSink, DryRunSink, Executor},
            setup,
        },
    },
    configuration::Configuration,
    registry::metadata_store::MetadataStore,
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
    sink: Box<dyn ActionSink + Send>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let auth_cache = bootstrap::auth_cache(&config.cache)?;
        let blob_handles = bootstrap::blob_stores(&config.blob_store, &auth_cache)?;
        let (metadata_store, _) =
            bootstrap::metadata_store(&config.resolve_metadata_config(), &auth_cache).await?;
        let repositories = bootstrap::repositories(&config.repository, &auth_cache)?;

        let namespace_checkers = setup::namespace_checkers(
            options,
            config,
            &blob_handles.blob_store,
            &blob_handles.upload_store,
            &metadata_store,
            &repositories,
        )?;
        let blob_checker = setup::blob_checker(options, &blob_handles.blob_store, &metadata_store);
        let multipart_checker =
            setup::multipart_checker(options, blob_handles.multipart_cleanup.clone())?;

        let sink: Box<dyn ActionSink + Send> = if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
            Box::new(DryRunSink)
        } else {
            Box::new(Executor::new(
                blob_handles.blob_store.clone(),
                metadata_store.clone(),
                blob_handles.upload_store.clone(),
                blob_handles.multipart_cleanup,
            ))
        };

        Ok(Self {
            metadata_store,
            namespace_checkers,
            blob_checker,
            multipart_checker,
            sink,
        })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.scrub_metadata().await?;
        self.scrub_blobs().await?;
        self.scrub_multipart_uploads().await?;

        Ok(())
    }

    async fn scrub_metadata(&mut self) -> Result<(), Error> {
        let mut namespaces = list_all::namespaces(&self.metadata_store);
        while let Some(namespace) = namespaces.next().await {
            let namespace = namespace?;
            for i in 0..self.namespace_checkers.len() {
                if let Err(e) = self.namespace_checkers[i]
                    .check(&namespace, self.sink.as_mut())
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
            if let Err(e) = checker.check_all(self.sink.as_mut()).await {
                tracing::warn!("Blob scrub checker failed: {e}");
            }
            self.blob_checker = Some(checker);
        }
        Ok(())
    }

    async fn scrub_multipart_uploads(&mut self) -> Result<(), Error> {
        if let Some(checker) = self.multipart_checker.take() {
            let result = checker.check_all(self.sink.as_mut()).await;
            self.multipart_checker = Some(checker);
            result?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
