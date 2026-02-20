use std::collections::HashMap;
use std::sync::Arc;

use argh::FromArgs;
use tracing::error;

use super::ServerContext;
use super::listeners::insecure::InsecureListener;
use super::listeners::tls::{ServerTlsConfig, TlsListener};
use crate::cache;
use crate::cache::Cache;
use crate::command::server::error::Error;
use crate::configuration::{Configuration, ServerConfig};
use crate::registry::blob_store::BlobStore;
use crate::registry::metadata_store::MetadataStore;
use crate::registry::{Registry, RegistryConfig, Repository, blob_store, repository};
use crate::watcher::ConfigNotifier;

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
}

// TODO: deduplicate!
fn build_blob_store(config: &blob_store::BlobStorageConfig) -> Result<Arc<dyn BlobStore>, Error> {
    let Ok(blob_store) = config.to_backend() else {
        let msg = "Failed to initialize blob store".to_string();
        return Err(Error::Initialization(msg));
    };

    Ok(blob_store)
}

fn build_metadata_store(config: &Configuration) -> Result<Arc<dyn MetadataStore>, Error> {
    match config.resolve_metadata_config().to_backend() {
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

fn build_registry(config: &Configuration) -> Result<Registry, Error> {
    let blob_store = build_blob_store(&config.blob_store)?;
    let metadata_store = build_metadata_store(config)?;
    let auth_cache = build_auth_cache(&config.cache)?;
    let repositories = build_repositories(&config.repository, &auth_cache)?;

    let registry_config = RegistryConfig::new()
        .update_pull_time(config.global.update_pull_time)
        .enable_redirect(config.global.enable_redirect)
        .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    let Ok(registry) = Registry::new(blob_store, metadata_store, repositories, registry_config)
    else {
        let msg = "Failed to initialize registry".to_string();
        return Err(Error::Initialization(msg));
    };

    Ok(registry)
}

impl Command {
    pub fn new(config: &Configuration) -> Result<Command, Error> {
        // TODO: non-overlapping configuration subset (?) for each of those helpers
        let registry = build_registry(config)?;
        let context = ServerContext::new(config, registry)?;

        let listener = match &config.server {
            ServerConfig::Insecure(server_config) => {
                ServiceListener::Insecure(InsecureListener::new(server_config, context))
            }
            ServerConfig::Tls(server_config) => {
                ServiceListener::Secure(TlsListener::new(server_config, context)?)
            }
        };

        Ok(Command { listener })
    }

    pub fn notify_config_change(&self, config: &Configuration) -> Result<(), Error> {
        let registry = build_registry(config)?;
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

    pub async fn run(&self) -> Result<(), Error> {
        match &self.listener {
            ServiceListener::Insecure(listener) => listener.serve().await?,
            ServiceListener::Secure(listener) => listener.serve().await?,
        }

        Ok(())
    }
}

impl ConfigNotifier for Command {
    fn notify_config_change(&self, config: &Configuration) {
        if let Err(e) = self.notify_config_change(config) {
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
mod tests {
    use super::*;
    use crate::command::server::listeners::tls::tests::build_config;

    fn create_minimal_config() -> Configuration {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
        "#;

        toml::from_str(toml).unwrap()
    }

    fn create_config_with_repository() -> Configuration {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [repository.test-repo.access_policy]
            default_allow = true
            rules = []
        "#;

        toml::from_str(toml).unwrap()
    }

    #[test]
    fn test_build_blob_store_filesystem_success() {
        let config = create_minimal_config();
        let result = build_blob_store(&config.blob_store);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_metadata_store_filesystem_success() {
        let config = create_minimal_config();
        let result = build_metadata_store(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_metadata_store_with_explicit_config() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata-explicit"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let result = build_metadata_store(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_auth_cache_memory_success() {
        let config = cache::Config::Memory;
        let result = build_auth_cache(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_repository_success() {
        let toml = r"
            [access_policy]
            default_allow = true
            rules = []
        ";

        let repo_config: repository::Config = toml::from_str(toml).unwrap();
        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();

        let result = build_repository("test-repo", &repo_config, &cache);

        assert!(result.is_ok());
        let repo = result.unwrap();
        assert_eq!(repo.name, "test-repo");
    }

    #[test]
    fn test_build_repository_with_upstream() {
        let toml = r#"
            [access_policy]
            default_allow = true
            rules = []

            [[upstream]]
            url = "https://registry-1.docker.io"
            username = "testuser"
            password = "testpass"
        "#;

        let repo_config: repository::Config = toml::from_str(toml).unwrap();
        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();

        let result = build_repository("cached-repo", &repo_config, &cache);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_repository_with_immutable_tags() {
        let toml = r#"
            [access_policy]
            default_allow = true
            rules = []

            immutable_tags = true
            immutable_tags_exclusions = ["latest", "dev-.*"]
        "#;

        let repo_config: repository::Config = toml::from_str(toml).unwrap();
        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();

        let result = build_repository("immutable-repo", &repo_config, &cache);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_repositories_empty() {
        let configs = HashMap::new();
        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();

        let result = build_repositories(&configs, &cache);

        assert!(result.is_ok());
        let repos = result.unwrap();
        assert_eq!(repos.len(), 0);
    }

    #[test]
    fn test_build_repositories_single() {
        let toml = r"
            [access_policy]
            default_allow = true
            rules = []
        ";

        let repo_config: repository::Config = toml::from_str(toml).unwrap();
        let mut configs = HashMap::new();
        configs.insert("repo1".to_string(), repo_config);

        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();

        let result = build_repositories(&configs, &cache);

        assert!(result.is_ok());
        let repos = result.unwrap();
        assert_eq!(repos.len(), 1);
        assert!(repos.contains_key("repo1"));
    }

    #[test]
    fn test_build_repositories_multiple() {
        let toml = r"
            [access_policy]
            default_allow = true
            rules = []
        ";

        let repo_config: repository::Config = toml::from_str(toml).unwrap();
        let mut configs = HashMap::new();
        configs.insert("repo1".to_string(), repo_config.clone());
        configs.insert("repo2".to_string(), repo_config.clone());
        configs.insert("repo3".to_string(), repo_config);

        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();

        let result = build_repositories(&configs, &cache);

        assert!(result.is_ok());
        let repos = result.unwrap();
        assert_eq!(repos.len(), 3);
        assert!(repos.contains_key("repo1"));
        assert!(repos.contains_key("repo2"));
        assert!(repos.contains_key("repo3"));
    }

    #[test]
    fn test_build_registry_minimal_config() {
        let config = create_minimal_config();
        let result = build_registry(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_registry_with_repositories() {
        let config = create_config_with_repository();
        let result = build_registry(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_registry_with_update_pull_time() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = true
            max_concurrent_cache_jobs = 20
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let result = build_registry(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_command_new_insecure_listener() {
        let config = create_minimal_config();
        let result = Command::new(&config);

        assert!(result.is_ok());
        let command = result.unwrap();

        match command.listener {
            ServiceListener::Insecure(_) => {}
            ServiceListener::Secure(_) => panic!("Expected insecure listener"),
        }
    }

    #[test]
    fn test_command_new_with_repositories() {
        let config = create_config_with_repository();
        let result = Command::new(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_command_notify_config_change_insecure() {
        let config = create_minimal_config();
        let command = Command::new(&config).unwrap();

        let new_config = create_minimal_config();
        let result = command.notify_config_change(&new_config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_command_notify_tls_config_change_with_insecure_listener() {
        let config = create_minimal_config();
        let command = Command::new(&config).unwrap();

        let (tls_config, _temp_files) = build_config(false);
        let result = command.notify_tls_config_change(&tls_config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_service_listener_enum_variants() {
        let config = create_minimal_config();
        let ServerConfig::Insecure(insecure_config) = &config.server else {
            panic!("Expected insecure config")
        };

        let registry = build_registry(&config).unwrap();
        let context = ServerContext::new(&config, registry).unwrap();

        let insecure_listener = InsecureListener::new(insecure_config, context);
        let _service_listener = ServiceListener::Insecure(insecure_listener);
    }

    #[test]
    fn test_build_repositories_preserves_names() {
        let toml = r"
            [access_policy]
            default_allow = true
            rules = []
        ";

        let repo_config: repository::Config = toml::from_str(toml).unwrap();
        let mut configs = HashMap::new();
        configs.insert("alpha".to_string(), repo_config.clone());
        configs.insert("beta".to_string(), repo_config.clone());
        configs.insert("gamma".to_string(), repo_config);

        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();
        let repos = build_repositories(&configs, &cache).unwrap();

        assert!(repos.get("alpha").is_some());
        assert!(repos.get("beta").is_some());
        assert!(repos.get("gamma").is_some());
        assert!(repos.get("delta").is_none());
    }

    #[test]
    fn test_build_registry_components_integration() {
        let config = create_config_with_repository();

        let blob_store = build_blob_store(&config.blob_store).unwrap();
        let metadata_store = build_metadata_store(&config).unwrap();
        let auth_cache = build_auth_cache(&config.cache).unwrap();
        let repositories = build_repositories(&config.repository, &auth_cache).unwrap();

        let registry_config = RegistryConfig::new()
            .update_pull_time(config.global.update_pull_time)
            .enable_redirect(config.global.enable_redirect)
            .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
            .global_immutable_tags(config.global.immutable_tags)
            .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

        let registry = Registry::new(blob_store, metadata_store, repositories, registry_config);

        assert!(registry.is_ok());
    }

    #[test]
    fn test_command_new_validates_configuration() {
        let config = create_minimal_config();
        let result = Command::new(&config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_build_repositories_with_different_configs() {
        let toml1 = r"
            [access_policy]
            default_allow = true
            rules = []
        ";

        let toml2 = r#"
            [access_policy]
            default_allow = false
            rules = ["identity.username == 'admin'"]
        "#;

        let repo_config1: repository::Config = toml::from_str(toml1).unwrap();
        let repo_config2: repository::Config = toml::from_str(toml2).unwrap();

        let mut configs = HashMap::new();
        configs.insert("public".to_string(), repo_config1);
        configs.insert("private".to_string(), repo_config2);

        let cache_config = cache::Config::Memory;
        let cache = build_auth_cache(&cache_config).unwrap();
        let result = build_repositories(&configs, &cache);

        assert!(result.is_ok());
        let repos = result.unwrap();
        assert_eq!(repos.len(), 2);
    }

    fn create_config_with_webhook(url: &str) -> Configuration {
        let toml = format!(
            r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [event_webhook.test_hook]
            url = "{url}"
            policy = "optional"
            events = ["manifest.push"]
        "#
        );

        toml::from_str(&toml).unwrap()
    }

    fn create_config_with_two_webhooks(url_a: &str, url_b: &str) -> Configuration {
        let toml = format!(
            r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata"

            [cache.memory]

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [event_webhook.hook_a]
            url = "{url_a}"
            policy = "optional"
            events = ["manifest.push"]

            [event_webhook.hook_b]
            url = "{url_b}"
            policy = "optional"
            events = ["manifest.push"]
        "#
        );

        toml::from_str(&toml).unwrap()
    }

    fn create_test_event() -> crate::event_webhook::event::Event {
        use chrono::Utc;
        use uuid::Uuid;

        use crate::event_webhook::event::{Event, EventKind};

        Event {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            kind: EventKind::ManifestPush,
            namespace: "test/repo".to_string(),
            digest: Some("sha256:abc123".to_string()),
            reference: Some("sha256:abc123".to_string()),
            tag: None,
            actor: None,
            repository: "test-repo".to_string(),
        }
    }

    #[test]
    fn test_hot_reload_adds_webhook_via_command() {
        let config = create_minimal_config();
        let command = Command::new(&config).unwrap();

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_none()
        );

        let new_config = create_config_with_webhook("https://example.com/webhook");
        command.notify_config_change(&new_config).unwrap();

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_some()
        );
    }

    #[test]
    fn test_hot_reload_removes_webhook_via_command() {
        let config = create_config_with_webhook("https://example.com/webhook");
        let command = Command::new(&config).unwrap();

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_some()
        );

        let new_config = create_minimal_config();
        command.notify_config_change(&new_config).unwrap();

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_hot_reload_changes_webhook_url_via_command() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server_a = MockServer::start().await;
        let server_b = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/webhook"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server_a)
            .await;

        Mock::given(method("POST"))
            .and(path("/webhook"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_b)
            .await;

        let config_a = create_config_with_webhook(&format!("{}/webhook", server_a.uri()));
        let command = Command::new(&config_a).unwrap();

        let config_b = create_config_with_webhook(&format!("{}/webhook", server_b.uri()));
        command.notify_config_change(&config_b).unwrap();

        let context = command.insecure_listener().current_context();
        context.dispatch_event(&create_test_event()).await.unwrap();
    }

    #[tokio::test]
    async fn test_hot_reload_adds_second_webhook() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server_a = MockServer::start().await;
        let server_b = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_a)
            .await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_b)
            .await;

        let config_one = create_config_with_webhook(&server_a.uri());
        let command = Command::new(&config_one).unwrap();

        let config_two = create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
        command.notify_config_change(&config_two).unwrap();

        let context = command.insecure_listener().current_context();
        context.dispatch_event(&create_test_event()).await.unwrap();
    }

    #[tokio::test]
    async fn test_hot_reload_removes_one_of_two_webhooks() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server_a = MockServer::start().await;
        let server_b = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_a)
            .await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server_b)
            .await;

        let config_two = create_config_with_two_webhooks(&server_a.uri(), &server_b.uri());
        let command = Command::new(&config_two).unwrap();

        let config_one = create_config_with_webhook(&server_a.uri());
        command.notify_config_change(&config_one).unwrap();

        let context = command.insecure_listener().current_context();
        context.dispatch_event(&create_test_event()).await.unwrap();
    }

    #[tokio::test]
    async fn test_hot_reload_inflight_old_dispatcher_still_works() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server_old = MockServer::start().await;
        let server_new = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_old)
            .await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_new)
            .await;

        let config_old = create_config_with_webhook(&server_old.uri());
        let command = Command::new(&config_old).unwrap();

        let old_context = Arc::clone(&command.insecure_listener().current_context());

        let config_new = create_config_with_webhook(&server_new.uri());
        command.notify_config_change(&config_new).unwrap();

        old_context
            .dispatch_event(&create_test_event())
            .await
            .unwrap();

        let new_context = command.insecure_listener().current_context();
        new_context
            .dispatch_event(&create_test_event())
            .await
            .unwrap();
    }

    fn create_invalid_cache_config_with_webhook(url: &str) -> Configuration {
        let toml = format!(
            r#"
            [blob_store.fs]
            root_dir = "/tmp/test-blobs"

            [metadata_store.fs]
            root_dir = "/tmp/test-metadata"

            [cache.redis]
            url = "redis://invalid:99999"
            key_prefix = "test:"

            [server]
            bind_address = "127.0.0.1"
            port = 8080

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [event_webhook.test_hook]
            url = "{url}"
            policy = "optional"
            events = ["manifest.push"]
        "#
        );

        toml::from_str(&toml).unwrap()
    }

    #[tokio::test]
    async fn test_failed_reload_preserves_old_context() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let valid_config = create_config_with_webhook(&server.uri());
        let command = Command::new(&valid_config).unwrap();

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_some()
        );

        let invalid_config = create_invalid_cache_config_with_webhook("https://other.example.com");
        let result = command.notify_config_change(&invalid_config);
        assert!(result.is_err());

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_some()
        );

        let context = command.insecure_listener().current_context();
        context.dispatch_event(&create_test_event()).await.unwrap();
    }

    #[tokio::test]
    async fn test_valid_reload_works_after_failed_reload() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server_a = MockServer::start().await;
        let server_b = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server_a)
            .await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server_b)
            .await;

        let config_a = create_config_with_webhook(&server_a.uri());
        let command = Command::new(&config_a).unwrap();

        let invalid_config = create_invalid_cache_config_with_webhook("https://bad.example.com");
        let result = command.notify_config_change(&invalid_config);
        assert!(result.is_err());

        let config_b = create_config_with_webhook(&server_b.uri());
        command.notify_config_change(&config_b).unwrap();

        let context = command.insecure_listener().current_context();
        context.dispatch_event(&create_test_event()).await.unwrap();
    }

    #[test]
    fn test_config_notifier_trait_handles_reload_error_gracefully() {
        let valid_config = create_minimal_config();
        let command = Command::new(&valid_config).unwrap();

        let invalid_config = create_invalid_cache_config_with_webhook("https://example.com");

        ConfigNotifier::notify_config_change(&command, &invalid_config);

        assert!(
            command
                .insecure_listener()
                .current_context()
                .event_dispatcher()
                .is_none()
        );
    }
}
