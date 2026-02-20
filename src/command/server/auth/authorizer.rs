use std::collections::HashMap;
use std::sync::Arc;

use hyper::http::request::Parts;
use regex::Regex;
use tracing::{debug, error, info, instrument};

use crate::cache::Cache;
use crate::command::server::auth::webhook::WebhookAuthorizer;
use crate::command::server::error::Error;
use crate::configuration::Configuration;
use crate::identity::{ClientIdentity, Route};
use crate::oci::Reference;
use crate::registry::{AccessPolicy, Registry};

const ACCESS_DENIED: &str = "Access denied";

/// Centralized authorization component that handles all access control decisions
pub struct Authorizer {
    global_access_policy: AccessPolicy,
    global_authorization_webhook: Option<String>,
    global_immutable_tags: bool,
    global_immutable_tags_exclusions: Vec<Regex>,
    webhooks: HashMap<String, Arc<WebhookAuthorizer>>,
    repositories: HashMap<String, AuthorizerRepository>,
}

/// Repository-specific authorization configuration
struct AuthorizerRepository {
    access_policy: Option<AccessPolicy>,
    authorization_webhook: Option<String>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<Regex>,
}

impl Authorizer {
    pub fn new(config: &Configuration, cache: &Arc<dyn Cache>) -> Result<Self, Error> {
        let global_access_policy =
            AccessPolicy::new(&config.global.access_policy).map_err(|e| {
                Error::Initialization(format!("Failed to create global access policy: {e}"))
            })?;

        let mut webhooks = HashMap::new();
        for (name, webhook_config) in &config.auth.webhook {
            let authorizer = Arc::new(
                WebhookAuthorizer::new(name.clone(), webhook_config.clone(), cache.clone())
                    .map_err(|e| {
                        Error::Initialization(format!("Failed to create webhook '{name}': {e}"))
                    })?,
            );
            webhooks.insert(name.clone(), authorizer);
        }

        let mut repositories = HashMap::new();
        for (name, repo_config) in &config.repository {
            let access_policy = if repo_config.access_policy.rules.is_empty()
                && !repo_config.access_policy.default_allow
            {
                None
            } else {
                Some(AccessPolicy::new(&repo_config.access_policy).map_err(|e| {
                    Error::Initialization(format!(
                        "Failed to create access policy for repository '{name}': {e}"
                    ))
                })?)
            };

            let immutable_tags_exclusions = repo_config
                .immutable_tags_exclusions
                .iter()
                .filter_map(|p| match Regex::new(p) {
                    Ok(regex) => Some(regex),
                    Err(e) => {
                        error!("Invalid regex pattern '{p}' in repository '{name}': {e}");
                        None
                    }
                })
                .collect();

            let auth_repo = AuthorizerRepository {
                access_policy,
                authorization_webhook: repo_config.authorization_webhook.clone(),
                immutable_tags: repo_config.immutable_tags,
                immutable_tags_exclusions,
            };
            repositories.insert(name.clone(), auth_repo);
        }

        let global_immutable_tags_exclusions = config
            .global
            .immutable_tags_exclusions
            .iter()
            .filter_map(|p| match Regex::new(p) {
                Ok(regex) => Some(regex),
                Err(e) => {
                    error!("Invalid global regex pattern '{}': {}", p, e);
                    None
                }
            })
            .collect();

        Ok(Self {
            global_access_policy,
            global_authorization_webhook: config.global.authorization_webhook.clone(),
            global_immutable_tags: config.global.immutable_tags,
            global_immutable_tags_exclusions,
            webhooks,
            repositories,
        })
    }

    #[instrument(skip(self, request, registry))]
    pub async fn authorize_request(
        &self,
        route: &Route<'_>,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<(), Error> {
        debug!("Evaluating global access policy");
        if self.global_access_policy.evaluate(route, identity) != Ok(true) {
            log_denial("global policy", identity);
            return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
        }

        if let Some(namespace) = route.get_namespace() {
            if let Ok(repository) = registry.get_repository_for_namespace(namespace) {
                debug!(
                    "Evaluating repository access policy for namespace: {namespace} ({})",
                    repository.name
                );

                let auth_repo = self.repositories.get(&repository.name).ok_or_else(|| {
                    Error::Execution(format!(
                        "Repository '{}' not found in authorizer",
                        repository.name
                    ))
                })?;

                if let Some(ref access_policy) = auth_repo.access_policy
                    && access_policy.evaluate(route, identity) != Ok(true)
                {
                    log_denial(
                        &format!("repository '{}' policy", repository.name),
                        identity,
                    );
                    return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
                }

                if let Route::PutManifest {
                    reference: Reference::Tag(tag),
                    ..
                } = route
                    && !self.is_tag_mutable(auth_repo, tag)
                {
                    let msg = format!("Tag '{tag}' is immutable and cannot be overwritten");
                    return Err(Error::Conflict(msg));
                }

                let webhook_name = auth_repo
                    .authorization_webhook
                    .as_ref()
                    .filter(|name| !name.is_empty())
                    .or(self.global_authorization_webhook.as_ref());

                if let Some(webhook_name) = webhook_name {
                    debug!("Evaluating webhook authorization: {}", webhook_name);

                    let webhook = self.webhooks.get(webhook_name).ok_or_else(|| {
                        Error::Execution(format!("Webhook '{webhook_name}' not found"))
                    })?;

                    let allowed = webhook.authorize(route, identity, request).await?;
                    if !allowed {
                        log_denial(&format!("webhook '{webhook_name}'"), identity);
                        return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
                    }
                }

                if repository.is_pull_through() && route.is_push() {
                    let msg =
                        "Push operations are not supported on pull-through cache repositories"
                            .to_string();
                    return Err(Error::Unauthorized(msg));
                }
            }
        } else if let Some(webhook_name) = &self.global_authorization_webhook {
            debug!("Evaluating global webhook authorization: {}", webhook_name);

            let webhook = self
                .webhooks
                .get(webhook_name)
                .ok_or_else(|| Error::Execution(format!("Webhook '{webhook_name}' not found")))?;

            let allowed = webhook.authorize(route, identity, request).await?;
            if !allowed {
                log_denial(&format!("global webhook '{webhook_name}'"), identity);
                return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
            }
        }

        Ok(())
    }

    fn is_tag_mutable(&self, auth_repo: &AuthorizerRepository, tag: &str) -> bool {
        let immutable = auth_repo.immutable_tags || self.global_immutable_tags;

        if !immutable {
            return true;
        }

        let exclusions = if auth_repo.immutable_tags_exclusions.is_empty() {
            &self.global_immutable_tags_exclusions
        } else {
            &auth_repo.immutable_tags_exclusions
        };

        exclusions.iter().any(|pattern| pattern.is_match(tag))
    }

    pub fn is_tag_immutable(&self, namespace: &str, tag: &str) -> bool {
        let auth_repo = self.repositories.iter().find(|(name, _)| {
            namespace == name.as_str() || namespace.starts_with(&format!("{name}/"))
        });

        if let Some((_, auth_repo)) = auth_repo {
            !self.is_tag_mutable(auth_repo, tag)
        } else {
            self.global_immutable_tags
                && !self
                    .global_immutable_tags_exclusions
                    .iter()
                    .any(|pattern| pattern.is_match(tag))
        }
    }
}

fn log_denial(reason: &str, identity: &ClientIdentity) {
    info!("Access denied: {reason} | Identity: {identity:?}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache;
    use crate::configuration::Configuration;

    fn create_minimal_config() -> Configuration {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
        "#;

        toml::from_str(toml).unwrap()
    }

    #[test]
    fn test_authorizer_new_minimal() {
        let config = create_minimal_config();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
        let authorizer = authorizer.unwrap();
        assert!(authorizer.global_authorization_webhook.is_none());
        assert!(!authorizer.global_immutable_tags);
        assert!(authorizer.global_immutable_tags_exclusions.is_empty());
        assert!(authorizer.webhooks.is_empty());
        assert!(authorizer.repositories.is_empty());
    }

    #[test]
    fn test_authorizer_new_with_global_access_policy() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [global.access_policy]
            default_allow = true
            rules = ["identity.username == 'admin'"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
    }

    #[test]
    fn test_authorizer_new_with_global_immutable_tags() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$", "^dev-.*"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
        let authorizer = authorizer.unwrap();
        assert!(authorizer.global_immutable_tags);
        assert_eq!(authorizer.global_immutable_tags_exclusions.len(), 2);
    }

    #[test]
    fn test_authorizer_new_with_repository_config() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["^test-.*"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
        let authorizer = authorizer.unwrap();
        assert_eq!(authorizer.repositories.len(), 1);
        assert!(authorizer.repositories.contains_key("myrepo"));
    }

    #[test]
    fn test_authorizer_new_with_invalid_global_regex() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = true
            immutable_tags_exclusions = ["[invalid"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
        let authorizer = authorizer.unwrap();
        assert!(authorizer.global_immutable_tags_exclusions.is_empty());
    }

    #[test]
    fn test_authorizer_new_with_invalid_repository_regex() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["[invalid"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
        let authorizer = authorizer.unwrap();
        let repo = authorizer.repositories.get("myrepo").unwrap();
        assert!(repo.immutable_tags_exclusions.is_empty());
    }

    #[test]
    fn test_is_tag_immutable_with_global_setting() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = true
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();

        assert!(authorizer.is_tag_immutable("unknown-namespace", "v1.0.0"));
    }

    #[test]
    fn test_is_tag_immutable_with_global_exclusions() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$", "^dev-.*"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();

        assert!(!authorizer.is_tag_immutable("unknown-namespace", "latest"));
        assert!(!authorizer.is_tag_immutable("unknown-namespace", "dev-branch"));
        assert!(authorizer.is_tag_immutable("unknown-namespace", "v1.0.0"));
    }

    #[test]
    fn test_is_tag_immutable_with_repository_setting() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = false

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();

        assert!(authorizer.is_tag_immutable("myrepo", "v1.0.0"));
    }

    #[test]
    fn test_is_tag_immutable_with_repository_exclusions() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$"]

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["^test-.*"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();

        assert!(!authorizer.is_tag_immutable("myrepo", "test-123"));
        assert!(authorizer.is_tag_immutable("myrepo", "latest"));
        assert!(authorizer.is_tag_immutable("myrepo", "v1.0.0"));
    }

    #[test]
    fn test_is_tag_immutable_with_sub_namespace() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = false

            [repository."docker-io"]
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$"]
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();

        assert!(authorizer.is_tag_immutable("docker-io", "v1.0.0"));
        assert!(authorizer.is_tag_immutable("docker-io/library/nginx", "v1.0.0"));
        assert!(!authorizer.is_tag_immutable("docker-io/library/nginx", "latest"));
        assert!(!authorizer.is_tag_immutable("other/namespace", "v1.0.0"));
    }

    #[test]
    fn test_is_tag_mutable_when_not_immutable() {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10
            immutable_tags = false

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = false
        "#;

        let config: Configuration = toml::from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let auth_repo = authorizer.repositories.get("myrepo").unwrap();

        assert!(authorizer.is_tag_mutable(auth_repo, "any-tag"));
    }

    #[test]
    fn test_log_denial() {
        let identity = ClientIdentity::new(None);
        log_denial("test reason", &identity);
    }

    fn create_pull_through_config() -> Configuration {
        let toml = r#"
            [blob_store.fs]
            root_dir = "/tmp/test"

            [metadata_store.fs]
            root_dir = "/tmp/test"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false
            max_concurrent_cache_jobs = 10

            [global.access_policy]
            default_allow = true

            [repository."docker-io"]

            [[repository."docker-io".upstream]]
            url = "https://registry-1.docker.io"
        "#;

        toml::from_str(toml).unwrap()
    }

    fn create_pull_through_registry(config: &Configuration) -> Registry {
        use std::collections::HashMap;
        use std::sync::Arc;

        use crate::registry::{RegistryConfig, Repository};

        let blob_store = config.blob_store.to_backend().unwrap();
        let metadata_store = config.resolve_metadata_config().to_backend(None).unwrap();
        let auth_cache = config.cache.to_backend().unwrap();

        let mut repositories_map = HashMap::new();
        for (name, repo_config) in &config.repository {
            let repo = Repository::new(name, repo_config, &auth_cache).unwrap();
            repositories_map.insert(name.clone(), repo);
        }
        let repositories = Arc::new(repositories_map);

        let registry_config = RegistryConfig::new()
            .update_pull_time(config.global.update_pull_time)
            .enable_redirect(config.global.enable_redirect)
            .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
            .global_immutable_tags(config.global.immutable_tags)
            .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

        Registry::new(blob_store, metadata_store, repositories, registry_config).unwrap()
    }

    #[tokio::test]
    async fn test_pull_through_repo_allows_delete_manifest() {
        use std::str::FromStr;

        use crate::oci::{Namespace, Reference};

        let config = create_pull_through_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config);

        let namespace = Namespace::new("docker-io/library/nginx").unwrap();
        let reference = Reference::from_str("latest").unwrap();
        let route = Route::DeleteManifest {
            namespace,
            reference,
        };
        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let result = authorizer
            .authorize_request(&route, &identity, &parts, &registry)
            .await;

        assert!(
            result.is_ok(),
            "DeleteManifest should be allowed on pull-through cache repositories, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_pull_through_repo_allows_delete_blob() {
        use std::str::FromStr;

        use crate::oci::{Digest, Namespace};

        let config = create_pull_through_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config);

        let namespace = Namespace::new("docker-io/library/nginx").unwrap();
        let digest = Digest::from_str(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let route = Route::DeleteBlob { namespace, digest };
        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let result = authorizer
            .authorize_request(&route, &identity, &parts, &registry)
            .await;

        assert!(
            result.is_ok(),
            "DeleteBlob should be allowed on pull-through cache repositories, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_pull_through_repo_blocks_push_operations() {
        use std::str::FromStr;

        use crate::oci::{Namespace, Reference};

        let config = create_pull_through_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config);

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let namespace = Namespace::new("docker-io/library/nginx").unwrap();
        let reference = Reference::from_str("latest").unwrap();
        let put_manifest_route = Route::PutManifest {
            namespace: namespace.clone(),
            reference,
        };
        let result = authorizer
            .authorize_request(&put_manifest_route, &identity, &parts, &registry)
            .await;
        assert!(
            result.is_err(),
            "PutManifest should be blocked on pull-through cache repositories"
        );

        let start_upload_route = Route::StartUpload {
            namespace,
            digest: None,
        };
        let result = authorizer
            .authorize_request(&start_upload_route, &identity, &parts, &registry)
            .await;
        assert!(
            result.is_err(),
            "StartUpload should be blocked on pull-through cache repositories"
        );
    }
}
