use std::{collections::HashMap, sync::Arc};

use hyper::http::request::Parts;
use tracing::{debug, info, instrument};

use crate::{
    auth::webhook::{self, WebhookAuthorizer},
    cache::Cache,
    command::server::Error,
    configuration::{Configuration, RegexPattern},
    identity::{Action, ClientIdentity},
    oci::{Namespace, Reference},
    policy::AccessMode,
    registry::{AccessPolicy, Registry},
};

const ACCESS_DENIED: &str = "Access denied";

/// Centralized authorization component that handles all access control decisions
pub struct Authorizer {
    global_access_policy: AccessPolicy,
    global_authorization_webhook: Option<Arc<WebhookAuthorizer>>,
    global_immutable_tags: bool,
    global_immutable_tags_exclusions: Vec<RegexPattern>,
    repositories: HashMap<String, AuthorizerRepository>,
}

/// Repository-specific authorization configuration
struct AuthorizerRepository {
    access_policy: Option<AccessPolicy>,
    authorization_webhook: Option<Arc<WebhookAuthorizer>>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<RegexPattern>,
}

impl Authorizer {
    pub fn new(config: &Configuration, cache: &Arc<dyn Cache>) -> Result<Self, Error> {
        let global_access_policy = AccessPolicy::new(&config.global.access_policy);

        let webhook_authorizers = build_webhooks(config, cache)?;

        let global_authorization_webhook = config
            .global
            .authorization_webhook
            .as_ref()
            .map(|cfg| lookup_webhook(&webhook_authorizers, cfg))
            .transpose()?;

        let repositories = build_repositories(config, &webhook_authorizers)?;

        let global_immutable_tags_exclusions = config.global.immutable_tags_exclusions.clone();

        Ok(Self {
            global_access_policy,
            global_authorization_webhook,
            global_immutable_tags: config.global.immutable_tags,
            global_immutable_tags_exclusions,
            repositories,
        })
    }

    #[instrument(skip(self, request, registry))]
    pub async fn authorize_request(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<(), Error> {
        debug!("Evaluating global access policy");
        if self.global_access_policy.evaluate(action, identity) != Ok(true) {
            log_denial("global policy", identity);
            return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
        }

        if let Some(namespace) = action.get_namespace() {
            self.authorize_namespace_request(namespace, action, identity, request, registry)
                .await?;
        } else if let Some(webhook) = &self.global_authorization_webhook {
            debug!(
                "Evaluating global webhook authorization: {}",
                webhook.name()
            );

            let allowed = webhook.authorize(action, identity, request).await?;
            if !allowed {
                log_denial(&format!("global webhook '{}'", webhook.name()), identity);
                return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
            }
        }

        Ok(())
    }

    async fn authorize_namespace_request(
        &self,
        namespace: &Namespace,
        action: &Action,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<(), Error> {
        let Ok(repository) = registry.get_repository_for_namespace(namespace) else {
            return Ok(());
        };

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
            && access_policy.evaluate(action, identity) != Ok(true)
        {
            log_denial(
                &format!("repository '{}' policy", repository.name),
                identity,
            );
            return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
        }

        self.check_immutable_tag(auth_repo, action)?;

        let webhook = auth_repo
            .authorization_webhook
            .as_ref()
            .or(self.global_authorization_webhook.as_ref());

        if let Some(webhook) = webhook {
            debug!("Evaluating webhook authorization: {}", webhook.name());

            let allowed = webhook.authorize(action, identity, request).await?;
            if !allowed {
                log_denial(&format!("webhook '{}'", webhook.name()), identity);
                return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
            }
        }

        if repository.is_pull_through() && action.is_push() {
            return Err(Error::Unauthorized(
                "Push operations are not supported on pull-through cache repositories".to_string(),
            ));
        }

        Ok(())
    }

    fn check_immutable_tag(
        &self,
        auth_repo: &AuthorizerRepository,
        action: &Action,
    ) -> Result<(), Error> {
        if let Action::PutManifest {
            reference: Reference::Tag(tag),
            ..
        } = action
            && !self.is_tag_mutable(auth_repo, tag)
        {
            return Err(Error::Conflict(format!(
                "Tag '{tag}' is immutable and cannot be overwritten"
            )));
        }
        Ok(())
    }

    fn is_tag_mutable(&self, auth_repo: &AuthorizerRepository, tag: &str) -> bool {
        let is_immutable = auth_repo.immutable_tags || self.global_immutable_tags;
        let exclusions = if auth_repo.immutable_tags_exclusions.is_empty() {
            &self.global_immutable_tags_exclusions
        } else {
            &auth_repo.immutable_tags_exclusions
        };
        let is_excluded = exclusions.iter().any(|pattern| pattern.is_match(tag));

        !is_immutable || is_excluded
    }

    pub fn is_tag_immutable(&self, registry: &Registry, namespace: &Namespace, tag: &str) -> bool {
        let auth_repo = registry
            .get_repository_for_namespace(namespace)
            .ok()
            .and_then(|repo| self.repositories.get(&repo.name));

        if let Some(auth_repo) = auth_repo {
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

fn build_webhooks(
    config: &Configuration,
    cache: &Arc<dyn Cache>,
) -> Result<HashMap<String, Arc<WebhookAuthorizer>>, Error> {
    let mut webhooks = HashMap::with_capacity(config.auth.webhook.len());
    for (name, webhook_config) in &config.auth.webhook {
        let authorizer =
            WebhookAuthorizer::new(name.clone(), webhook_config.as_ref().clone(), cache.clone())
                .map_err(|e| {
                    Error::Initialization(format!("Failed to create webhook '{name}': {e}"))
                })?;
        webhooks.insert(name.clone(), Arc::new(authorizer));
    }
    Ok(webhooks)
}

fn build_repositories(
    config: &Configuration,
    webhook_authorizers: &HashMap<String, Arc<WebhookAuthorizer>>,
) -> Result<HashMap<String, AuthorizerRepository>, Error> {
    let mut repositories = HashMap::with_capacity(config.repository.len());
    for (name, repo_config) in &config.repository {
        let access_policy = if repo_config.access_policy.rules.is_empty()
            && repo_config.access_policy.default == AccessMode::Deny
        {
            None
        } else {
            Some(AccessPolicy::new(&repo_config.access_policy))
        };

        let authorization_webhook = repo_config
            .authorization_webhook
            .as_ref()
            .map(|cfg| lookup_webhook(webhook_authorizers, cfg))
            .transpose()?;

        repositories.insert(
            name.clone(),
            AuthorizerRepository {
                access_policy,
                authorization_webhook,
                immutable_tags: repo_config.immutable_tags,
                immutable_tags_exclusions: repo_config.immutable_tags_exclusions.clone(),
            },
        );
    }
    Ok(repositories)
}

fn log_denial(reason: &str, identity: &ClientIdentity) {
    info!("Access denied: {reason} | Identity: {identity:?}");
}

fn lookup_webhook(
    authorizers: &HashMap<String, Arc<WebhookAuthorizer>>,
    cfg: &webhook::Config,
) -> Result<Arc<WebhookAuthorizer>, Error> {
    authorizers.get(&cfg.name).cloned().ok_or_else(|| {
        Error::Initialization(format!(
            "Internal: webhook '{}' missing from authorizer map",
            cfg.name
        ))
    })
}

#[cfg(test)]
mod tests {
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;
    use crate::{cache, configuration::Configuration};

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

        Configuration::load_from_str(toml).unwrap()
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
            default = "allow"
            rules = ["identity.username == 'admin'"]
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
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

        let config = Configuration::load_from_str(toml).unwrap();
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();

        let authorizer = Authorizer::new(&config, &cache);

        assert!(authorizer.is_ok());
        let authorizer = authorizer.unwrap();
        assert_eq!(authorizer.repositories.len(), 1);
        assert!(authorizer.repositories.contains_key("myrepo"));
    }

    #[test]
    fn test_invalid_global_regex_fails_at_deserialize() {
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

        let result = Configuration::load_from_str(toml);
        assert!(
            result.is_err(),
            "invalid global regex must fail at deserialize time"
        );
    }

    #[test]
    fn test_invalid_repository_regex_fails_at_deserialize() {
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

        let result = Configuration::load_from_str(toml);
        assert!(
            result.is_err(),
            "invalid repository regex must fail at deserialize time"
        );
    }

    #[tokio::test]
    async fn test_is_tag_immutable_with_global_setting() {
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let ns = Namespace::new("unknown-namespace").unwrap();
        assert!(authorizer.is_tag_immutable(&registry, &ns, "v1.0.0"));
    }

    #[tokio::test]
    async fn test_is_tag_immutable_with_global_exclusions() {
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let ns = Namespace::new("unknown-namespace").unwrap();
        assert!(!authorizer.is_tag_immutable(&registry, &ns, "latest"));
        assert!(!authorizer.is_tag_immutable(&registry, &ns, "dev-branch"));
        assert!(authorizer.is_tag_immutable(&registry, &ns, "v1.0.0"));
    }

    #[tokio::test]
    async fn test_is_tag_immutable_with_repository_setting() {
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let ns = Namespace::new("myrepo").unwrap();
        assert!(authorizer.is_tag_immutable(&registry, &ns, "v1.0.0"));
    }

    #[tokio::test]
    async fn test_is_tag_immutable_with_repository_exclusions() {
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let ns = Namespace::new("myrepo").unwrap();
        assert!(!authorizer.is_tag_immutable(&registry, &ns, "test-123"));
        assert!(authorizer.is_tag_immutable(&registry, &ns, "latest"));
        assert!(authorizer.is_tag_immutable(&registry, &ns, "v1.0.0"));
    }

    #[tokio::test]
    async fn test_is_tag_immutable_with_sub_namespace() {
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let ns_root = Namespace::new("docker-io").unwrap();
        let ns_sub = Namespace::new("docker-io/library/nginx").unwrap();
        let ns_other = Namespace::new("other/namespace").unwrap();
        assert!(authorizer.is_tag_immutable(&registry, &ns_root, "v1.0.0"));
        assert!(authorizer.is_tag_immutable(&registry, &ns_sub, "v1.0.0"));
        assert!(!authorizer.is_tag_immutable(&registry, &ns_sub, "latest"));
        assert!(!authorizer.is_tag_immutable(&registry, &ns_other, "v1.0.0"));
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

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let auth_repo = authorizer.repositories.get("myrepo").unwrap();

        assert!(authorizer.is_tag_mutable(auth_repo, "any-tag"));
    }

    // When immutable_tags is true and the tag matches an exclusion pattern, the
    // tag is mutable (the exclusion carves out a writable subset).
    #[test]
    fn is_tag_mutable_returns_true_when_immutable_but_excluded() {
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
            immutable_tags_exclusions = ["^latest$", "^dev-.*"]
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let auth_repo = authorizer.repositories.get("myrepo").unwrap();

        assert!(
            authorizer.is_tag_mutable(auth_repo, "latest"),
            "'latest' must be mutable because it matches the exclusion pattern"
        );
        assert!(
            authorizer.is_tag_mutable(auth_repo, "dev-feature"),
            "'dev-feature' must be mutable because it matches 'dev-.*'"
        );
    }

    // When immutable_tags is true and the tag does not match any exclusion,
    // the tag is immutable.
    #[test]
    fn is_tag_mutable_returns_false_when_immutable_and_not_excluded() {
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
            immutable_tags_exclusions = ["^latest$"]
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let auth_repo = authorizer.repositories.get("myrepo").unwrap();

        assert!(
            !authorizer.is_tag_mutable(auth_repo, "v1.0.0"),
            "'v1.0.0' must be immutable: immutable_tags=true and not excluded"
        );
    }

    #[test]
    fn test_check_immutable_tag_returns_conflict_for_tagged_putmanifest() {
        use std::str::FromStr;

        use crate::{
            command::server::Error,
            oci::{Namespace, Reference},
        };

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

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let auth_repo = authorizer.repositories.get("myrepo").unwrap();

        let action = Action::PutManifest {
            namespace: Namespace::new("myrepo/app").unwrap(),
            reference: Reference::from_str("v1.0.0").unwrap(),
        };

        let result = authorizer.check_immutable_tag(auth_repo, &action);

        let Err(Error::Conflict(msg)) = result else {
            panic!("expected Err(Error::Conflict(_)), got: {result:?}");
        };
        assert!(
            msg.contains("v1.0.0") && msg.contains("immutable"),
            "error message must mention the tag and 'immutable', got: {msg}"
        );
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
            default = "allow"

            [repository."docker-io"]

            [[repository."docker-io".upstream]]
            url = "https://registry-1.docker.io"
        "#;

        Configuration::load_from_str(toml).unwrap()
    }

    async fn create_pull_through_registry(config: &Configuration) -> Registry {
        use std::{collections::HashMap, sync::Arc};

        use crate::registry::{RegistryConfig, Repository};

        let blob_handles = config.blob_store.to_backend(None).unwrap();
        let (metadata_store, _) = config
            .resolve_metadata_config()
            .to_backend(None)
            .await
            .unwrap();
        let auth_cache = config.cache.to_backend().unwrap();

        let mut repositories_map = HashMap::new();
        for (name, repo_config) in &config.repository {
            let repo = Repository::new(name, repo_config, &auth_cache).unwrap();
            repositories_map.insert(name.clone(), repo);
        }
        let repositories = Arc::new(repositories_map);

        let registry_config = RegistryConfig::new()
            .update_pull_time(config.global.update_pull_time)
            .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
            .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
            .concurrent_cache_jobs(config.global.max_concurrent_cache_jobs)
            .global_immutable_tags(config.global.immutable_tags)
            .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

        Registry::new(
            blob_handles.blob_store,
            blob_handles.upload_store,
            blob_handles.presigned_store,
            metadata_store,
            repositories,
            registry_config,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_pull_through_repo_allows_delete_manifest() {
        use std::str::FromStr;

        use crate::oci::{Namespace, Reference};

        let config = create_pull_through_config();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let namespace = Namespace::new("docker-io/library/nginx").unwrap();
        let reference = Reference::from_str("latest").unwrap();
        let route = Action::DeleteManifest {
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
        let registry = create_pull_through_registry(&config).await;

        let namespace = Namespace::new("docker-io/library/nginx").unwrap();
        let digest = Digest::from_str(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let route = Action::DeleteBlob { namespace, digest };
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
        let registry = create_pull_through_registry(&config).await;

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let namespace = Namespace::new("docker-io/library/nginx").unwrap();
        let reference = Reference::from_str("latest").unwrap();
        let put_manifest_route = Action::PutManifest {
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

        let start_upload_route = Action::StartUpload {
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

    // Global deny policy rejects every request regardless of action.
    //
    // The `[global.access_policy]` block with `default = "deny"` and no allow-rules
    // causes `AccessPolicy::evaluate` to return `Ok(false)` for all identities.
    // `authorize_request` must short-circuit before consulting any webhook or
    // repository.
    #[tokio::test]
    async fn global_deny_policy_rejects_all_requests() {
        use crate::command::server::Error as ServerError;

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
            default = "deny"
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let result = authorizer
            .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
            .await;

        assert!(
            matches!(result, Err(ServerError::Unauthorized(_))),
            "global deny policy must reject ApiVersion, got: {result:?}"
        );
    }

    // Global allow policy + global webhook returning 200 → request allowed.
    //
    // `Action::ApiVersion` carries no namespace, so `authorize_request` takes the
    // non-namespace branch and calls the global webhook directly.  A 200 response
    // means the webhook grants access.
    #[tokio::test]
    async fn global_webhook_path_allow_when_webhook_returns_200() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let toml = format!(
            r#"
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
            authorization_webhook = "gatekeeper"

            [global.access_policy]
            default = "allow"

            [auth.webhook.gatekeeper]
            url = "{url}"
            timeout_ms = 1000
            "#,
            url = mock_server.uri()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let result = authorizer
            .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
            .await;

        assert!(
            result.is_ok(),
            "global webhook returning 200 must allow the request, got: {result:?}"
        );
    }

    // Global allow policy + global webhook returning 403 → request denied.
    #[tokio::test]
    async fn global_webhook_path_deny_when_webhook_returns_403() {
        use crate::command::server::Error as ServerError;

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&mock_server)
            .await;

        let toml = format!(
            r#"
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
            authorization_webhook = "gatekeeper"

            [global.access_policy]
            default = "allow"

            [auth.webhook.gatekeeper]
            url = "{url}"
            timeout_ms = 1000
            "#,
            url = mock_server.uri()
        );

        let config = Configuration::load_from_str(&toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let result = authorizer
            .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
            .await;

        assert!(
            matches!(result, Err(ServerError::Unauthorized(_))),
            "global webhook returning 403 must deny the request, got: {result:?}"
        );
    }

    // Invalid-regex unreachability.
    //
    // `RegexPattern` compiles the regex at TOML deserialise time. An invalid
    // pattern causes `Configuration::load_from_str` to return `Err` before any
    // `Authorizer` is constructed, so it can never reach `is_tag_mutable`. This
    // test documents the `RegexPattern::compile` API directly so the
    // compile-time rejection invariant is grep-able from here.
    #[test]
    fn regex_pattern_compile_rejects_invalid_pattern() {
        let err = RegexPattern::compile("[invalid");
        assert!(
            err.is_err(),
            "an invalid regex must be rejected by RegexPattern::compile; \
             it can therefore never reach is_tag_mutable"
        );
    }

    // A namespace that maps to no configured repository is passed through
    // without error or panic under an allow policy.
    //
    // `authorize_namespace_request` returns `Ok(())` early when
    // `get_repository_for_namespace` returns `Err`, so the default policy is
    // applied implicitly (allow here).
    #[tokio::test]
    async fn authorize_request_unknown_namespace_is_allowed_under_allow_policy() {
        use crate::oci::Namespace;

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
            default = "allow"
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let action = Action::GetManifest {
            namespace: Namespace::new("no-such-repo/image").unwrap(),
            reference: Reference::Tag("latest".to_string()),
        };

        let result = authorizer
            .authorize_request(&action, &identity, &parts, &registry)
            .await;

        assert!(
            result.is_ok(),
            "a namespace that matches no repository must not panic or deny under an allow policy, got: {result:?}"
        );
    }

    // Webhook unreachable → fail-closed.
    //
    // When the webhook endpoint is unreachable, `WebhookAuthorizer::authorize`
    // returns `Err(Error::Unauthorized(...))`.  `authorize_namespace_request`
    // propagates that error so the authorizer returns `Err`, not `Ok(false)`.
    // This distinguishes a transport failure from an explicit deny on dashboards.
    #[tokio::test]
    async fn webhook_unreachable_fails_closed() {
        use crate::oci::Namespace;

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
            default = "allow"

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            authorization_webhook = "gatekeeper"

            [auth.webhook.gatekeeper]
            url = "http://127.0.0.1:1"
            timeout_ms = 500
        "#;

        let config = Configuration::load_from_str(toml).unwrap();
        let cache = cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();
        let registry = create_pull_through_registry(&config).await;

        let identity = ClientIdentity::new(None);
        let (parts, ()) = hyper::Request::builder()
            .uri("/v2/")
            .body(())
            .unwrap()
            .into_parts();

        let action = Action::GetManifest {
            namespace: Namespace::new("myrepo/app").unwrap(),
            reference: Reference::Tag("latest".to_string()),
        };

        let result = authorizer
            .authorize_request(&action, &identity, &parts, &registry)
            .await;

        assert!(
            result.is_err(),
            "an unreachable webhook must produce Err (fail-closed), not Ok; got: {result:?}"
        );
    }
}
