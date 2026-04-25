use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use argh::FromArgs;
use async_trait::async_trait;
use tracing::error;

use super::{
    ServerContext,
    listeners::{
        insecure::InsecureListener,
        tls::{ServerTlsConfig, TlsListener},
    },
};
use crate::{
    cache,
    cache::Cache,
    command::server::error::Error,
    configuration::{Configuration, ServerConfig, watcher::ConfigNotifier},
    registry::{
        Registry, RegistryConfig, Repository, blob_store,
        blob_store::{BlobStore, PresignedBlobStore, UploadStore},
        metadata_store::{ConditionalCapabilities, MetadataStore, MetadataStoreConfig},
        repository,
    },
};

type BlobStoreTriple = (
    Arc<dyn BlobStore>,
    Arc<dyn UploadStore>,
    Option<Arc<dyn PresignedBlobStore>>,
);

pub enum ServiceListener {
    Insecure(InsecureListener),
    Secure(TlsListener),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "server",
    description = "Run the registry listeners"
)]
pub struct Options {}

pub struct Command {
    listener: ServiceListener,
    cached_capabilities: Arc<Mutex<Option<ConditionalCapabilities>>>,
}

fn build_blob_stores(
    config: &blob_store::BlobStorageConfig,
    cache: &Arc<dyn Cache>,
) -> Result<BlobStoreTriple, Error> {
    config
        .to_backend(Some(cache.clone()))
        .map_err(|_| Error::Initialization("Failed to initialize blob store".to_string()))
}

async fn build_metadata_store(
    config: &Configuration,
    cache: &Arc<dyn Cache>,
    cached_capabilities: &Arc<Mutex<Option<ConditionalCapabilities>>>,
) -> Result<Arc<dyn MetadataStore>, Error> {
    let mut metadata_config = config.resolve_metadata_config();

    if let MetadataStoreConfig::S3(ref mut backend_config) = metadata_config
        && backend_config.capabilities.is_none()
    {
        let guard = cached_capabilities
            .lock()
            .expect("capabilities mutex poisoned");
        if guard.is_some() {
            backend_config.capabilities.clone_from(&guard);
        }
    }

    match metadata_config.to_backend(Some(cache.clone())).await {
        Ok(store) => {
            let mut guard = cached_capabilities
                .lock()
                .expect("capabilities mutex poisoned");
            *guard = store.conditional_capabilities();
            Ok(store)
        }
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

async fn build_registry(
    config: &Configuration,
    cached_capabilities: &Arc<Mutex<Option<ConditionalCapabilities>>>,
) -> Result<Registry, Error> {
    let auth_cache = build_auth_cache(&config.cache)?;
    let (blob_store, upload_store, presigned_blob_store) =
        build_blob_stores(&config.blob_store, &auth_cache)?;
    let metadata_store = build_metadata_store(config, &auth_cache, cached_capabilities).await?;
    let repositories = build_repositories(&config.repository, &auth_cache)?;

    let registry_config = RegistryConfig::new()
        .update_pull_time(config.global.update_pull_time)
        .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
        .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    let Ok(registry) = Registry::new(
        blob_store,
        upload_store,
        presigned_blob_store,
        metadata_store,
        repositories,
        registry_config,
    ) else {
        let msg = "Failed to initialize registry".to_string();
        return Err(Error::Initialization(msg));
    };

    Ok(registry)
}

impl Command {
    pub async fn new(config: &Configuration) -> Result<Command, Error> {
        let cached_capabilities = Arc::new(Mutex::new(None));
        let registry = build_registry(config, &cached_capabilities).await?;
        let context = ServerContext::new(config, registry)?;

        let listener = match &config.server {
            ServerConfig::Insecure(server_config) => {
                ServiceListener::Insecure(InsecureListener::new(server_config, context))
            }
            ServerConfig::Tls(server_config) => {
                ServiceListener::Secure(TlsListener::new(server_config, context)?)
            }
        };

        Ok(Command {
            listener,
            cached_capabilities,
        })
    }

    pub async fn notify_config_change(&self, config: &Configuration) -> Result<(), Error> {
        let registry = build_registry(config, &self.cached_capabilities).await?;
        let context = ServerContext::new(config, registry)?;

        match (&self.listener, &config.server) {
            (ServiceListener::Insecure(listener), _) => listener.notify_config_change(context),
            (ServiceListener::Secure(listener), ServerConfig::Tls(server_config)) => {
                listener.notify_config_change(server_config, context)?;
            }
            _ => {}
        }

        Ok(())
    }

    pub fn notify_tls_config_change(&self, server_config: &ServerTlsConfig) -> Result<(), Error> {
        if let ServiceListener::Secure(listener) = &self.listener {
            listener.notify_tls_config_change(server_config)?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn insecure_listener(&self) -> &InsecureListener {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener,
            ServiceListener::Secure(_) => panic!("Expected insecure listener"),
        }
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener.shutdown_with_timeout(timeout).await,
            ServiceListener::Secure(listener) => listener.shutdown_with_timeout(timeout).await,
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener.serve().await?,
            ServiceListener::Secure(listener) => listener.serve().await?,
        }

        Ok(())
    }
}

#[async_trait]
impl ConfigNotifier for Command {
    async fn notify_config_change(&self, config: &Configuration) {
        if let Err(e) = self.notify_config_change(config).await {
            error!("Failed to apply configuration: {e}");
        }
    }

    fn notify_tls_config_change(&self, tls: &ServerTlsConfig) {
        if let Err(e) = self.notify_tls_config_change(tls) {
            error!("Failed to reload TLS configuration: {e}");
        }
    }
}

#[cfg(test)]
mod tests;
