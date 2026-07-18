use std::{collections::HashMap, fmt, path::PathBuf, sync::Arc};

use hyper::http::request::Parts;
use reqwest::Client;
use tracing::{debug, info, instrument, warn};

use crate::{
    auth::Error,
    auth::webhook::{self, WebhookAuthorizer},
    cache::Cache,
    configuration::{Configuration, RegexPattern},
    http_client::HttpClientBuilder,
    identity::{Action, ClientIdentity},
    oci::{Namespace, Tag},
    policy::PolicyDecision,
    registry::{AccessPolicy, BlobMount, Registry},
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

#[derive(Clone, Hash, Eq, PartialEq)]
struct WebhookClientConfig {
    server_ca_bundle: Option<PathBuf>,
    client_certificate_bundle: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
}

impl From<&webhook::Config> for WebhookClientConfig {
    fn from(config: &webhook::Config) -> Self {
        Self {
            server_ca_bundle: config.server_ca_bundle.clone(),
            client_certificate_bundle: config.client_certificate_bundle.clone(),
            client_private_key: config.client_private_key.clone(),
        }
    }
}

struct AuditIdentity<'a> {
    auth_type: &'static str,
    id: Option<&'a str>,
    username: Option<&'a str>,
    client_ip: Option<&'a str>,
    certificate_organizations: &'a [String],
    certificate_common_names: &'a [String],
    oidc_provider_name: Option<&'a str>,
    oidc_provider_type: Option<&'a str>,
}

impl fmt::Debug for AuditIdentity<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuditIdentity")
            .field("auth_type", &self.auth_type)
            .field("id", &self.id)
            .field("username", &self.username)
            .field("client_ip", &self.client_ip)
            .field("certificate_organizations", &self.certificate_organizations)
            .field("certificate_common_names", &self.certificate_common_names)
            .field("oidc_provider_name", &self.oidc_provider_name)
            .field("oidc_provider_type", &self.oidc_provider_type)
            .finish()
    }
}

impl<'a> From<&'a ClientIdentity> for AuditIdentity<'a> {
    fn from(identity: &'a ClientIdentity) -> Self {
        Self {
            // The single classification the authenticator computed, so the
            // audit log and the request span never disagree.
            auth_type: identity.auth_method.unwrap_or("anonymous"),
            id: identity.id.as_deref(),
            username: identity.username.as_deref(),
            client_ip: identity.client_ip.as_deref(),
            certificate_organizations: &identity.certificate.organizations,
            certificate_common_names: &identity.certificate.common_names,
            oidc_provider_name: identity
                .oidc
                .as_ref()
                .map(|oidc| oidc.provider_name.as_str()),
            oidc_provider_type: identity
                .oidc
                .as_ref()
                .map(|oidc| oidc.provider_type.as_str()),
        }
    }
}

impl Authorizer {
    pub fn new(config: &Configuration, cache: &Arc<Cache>) -> Result<Self, Error> {
        let global_access_policy = AccessPolicy::new(config.global.access_policy.clone());

        let webhook_authorizers = build_webhooks(config, cache)?;

        let global_authorization_webhook = config
            .global
            .authorization_webhook
            .as_ref()
            .map(|name| lookup_webhook(&webhook_authorizers, name))
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

    #[instrument(skip(self, request, registry, identity))]
    pub async fn authorize_request(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<(), Error> {
        debug!("Evaluating global access policy");
        match self.global_access_policy.evaluate(action, identity) {
            PolicyDecision::Allow => {}
            PolicyDecision::Deny => {
                log_denial("global policy", identity);
                return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
            }
            PolicyDecision::Indeterminate(err) => {
                warn!("Global access policy indeterminate, denying: {err}");
                log_denial("global policy (indeterminate)", identity);
                return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
            }
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

    /// Resolves a source namespace from which `identity` may already read
    /// `mount.digest`: the precondition for granting a cross-repo blob mount.
    /// Without it a caller could mount, then read, any blob held by any other
    /// namespace, bypassing the source's read policy; `None` means degrade to a
    /// normal upload session.
    #[instrument(skip(self, request, registry, identity))]
    pub async fn authorize_mount_source(
        &self,
        mount: &BlobMount,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<Option<Namespace>, Error> {
        for source in registry.mount_source_candidates(mount).await? {
            let action = Action::GetBlob {
                namespace: source.clone(),
                digest: mount.digest.clone(),
            };
            match self
                .authorize_request(&action, identity, request, registry)
                .await
            {
                Ok(()) => return Ok(Some(source)),
                Err(Error::Unauthorized(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
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
            // Unconfigured namespaces have no repository policy or webhook to evaluate.
            // The global policy was already enforced by authorize_request.
            return Ok(());
        };

        debug!(
            "Evaluating repository access policy for namespace: {namespace} ({})",
            repository.name
        );

        let auth_repo = self
            .repositories
            .get(repository.name.as_ref())
            .ok_or_else(|| {
                Error::Execution(format!(
                    "Repository '{}' not found in authorizer",
                    repository.name
                ))
            })?;

        if let Some(ref access_policy) = auth_repo.access_policy {
            match access_policy.evaluate(action, identity) {
                PolicyDecision::Allow => {}
                PolicyDecision::Deny => {
                    log_denial(
                        &format!("repository '{}' policy", repository.name),
                        identity,
                    );
                    return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
                }
                PolicyDecision::Indeterminate(err) => {
                    warn!(
                        "Repository '{}' access policy indeterminate, denying: {err}",
                        repository.name
                    );
                    log_denial(
                        &format!("repository '{}' policy (indeterminate)", repository.name),
                        identity,
                    );
                    return Err(Error::Unauthorized(ACCESS_DENIED.to_string()));
                }
            }
        }

        self.check_immutable_tag(repository.name.as_ref(), action)?;

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

    fn check_immutable_tag(&self, repository_name: &str, action: &Action) -> Result<(), Error> {
        let Action::PutManifest { target, .. } = action else {
            return Ok(());
        };

        for tag in target.created_tags() {
            if !self.is_tag_mutable(Some(repository_name), tag) {
                return Err(Error::Conflict(format!(
                    "Tag '{tag}' is immutable and cannot be overwritten"
                )));
            }
        }
        Ok(())
    }

    pub fn is_tag_mutable(&self, repository_name: Option<&str>, tag: &Tag) -> bool {
        let repository = repository_name.and_then(|name| self.repositories.get(name));
        let immutable_tags =
            self.global_immutable_tags || repository.is_some_and(|repo| repo.immutable_tags);
        if !immutable_tags {
            return true;
        }

        let exclusions = match repository {
            Some(repo) if !repo.immutable_tags_exclusions.is_empty() => {
                &repo.immutable_tags_exclusions
            }
            _ => &self.global_immutable_tags_exclusions,
        };

        exclusions
            .iter()
            .any(|pattern| pattern.is_match(tag.as_ref()))
    }
}

fn build_webhooks(
    config: &Configuration,
    cache: &Arc<Cache>,
) -> Result<HashMap<String, Arc<WebhookAuthorizer>>, Error> {
    let mut webhooks = HashMap::with_capacity(config.auth.webhook.len());
    let mut clients: HashMap<WebhookClientConfig, Client> = HashMap::new();
    for (name, webhook_config) in &config.auth.webhook {
        let client_config = WebhookClientConfig::from(webhook_config);
        let client = if let Some(client) = clients.get(&client_config) {
            client.clone()
        } else {
            let client = build_webhook_client(webhook_config).map_err(|e| {
                Error::Initialization(format!("Failed to create webhook '{name}': {e}"))
            })?;
            clients.insert(client_config, client.clone());
            client
        };
        let authorizer =
            WebhookAuthorizer::new(name.clone(), webhook_config.clone(), client, cache.clone())
                .map_err(|e| {
                    Error::Initialization(format!("Failed to create webhook '{name}': {e}"))
                })?;
        webhooks.insert(name.clone(), Arc::new(authorizer));
    }
    Ok(webhooks)
}

fn build_webhook_client(config: &webhook::Config) -> Result<Client, String> {
    HttpClientBuilder::new()
        .rustls_tls()
        .redirect(reqwest::redirect::Policy::none())
        .tls_files(
            config.server_ca_bundle.as_deref(),
            config.client_certificate_bundle.as_deref(),
            config.client_private_key.as_deref(),
        )?
        .build()
}

fn build_repositories(
    config: &Configuration,
    webhook_authorizers: &HashMap<String, Arc<WebhookAuthorizer>>,
) -> Result<HashMap<String, AuthorizerRepository>, Error> {
    let mut repositories = HashMap::with_capacity(config.repository.len());
    for (name, repo_config) in &config.repository {
        let access_policy = repo_config.access_policy.clone().map(AccessPolicy::new);

        let authorization_webhook = repo_config
            .authorization_webhook
            .as_ref()
            .map(|name| lookup_webhook(webhook_authorizers, name))
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
    info!(
        "Access denied: {reason} | Identity: {:?}",
        AuditIdentity::from(identity)
    );
}

fn lookup_webhook(
    authorizers: &HashMap<String, Arc<WebhookAuthorizer>>,
    name: &str,
) -> Result<Arc<WebhookAuthorizer>, Error> {
    authorizers.get(name).cloned().ok_or_else(|| {
        Error::Initialization(format!(
            "Internal: webhook '{name}' missing from authorizer map",
        ))
    })
}

#[cfg(test)]
mod tests;
